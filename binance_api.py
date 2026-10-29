import time
import json
import hmac
import hashlib
import urllib.request
import urllib.parse
from config import BINANCE_API_KEY, BINANCE_API_SECRET, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

time_offset = 0

def sync_binance_time():
    global time_offset
    for ep in ["https://api.binance.com/api/v3/time", "https://data-api.binance.vision/api/v3/time"]:
        try:
            req = urllib.request.Request(ep, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=3) as resp:
                data = json.loads(resp.read().decode())
                server_time = data["serverTime"]
                local_time = int(time.time() * 1000)
                time_offset = server_time - local_time
                return
        except Exception:
            continue

sync_binance_time()

def binance_signed_request(endpoint, method="GET", params=None):
    global time_offset
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        return None, "API Key বা Secret খালি"
    try:
        if params is None:
            params = {}
        sync_binance_time()
        params["timestamp"] = int(time.time() * 1000) + time_offset
        params["recvWindow"] = 60000
        query_string = urllib.parse.urlencode(params)
        signature = hmac.new(BINANCE_API_SECRET.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256).hexdigest()
        
        full_url = f"https://api.binance.com{endpoint}?{query_string}&signature={signature}"
        req = urllib.request.Request(full_url, method=method)
        req.add_header("X-MBX-APIKEY", BINANCE_API_KEY)
        req.add_header("User-Agent", "Mozilla/5.0")
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read().decode()), None
    except urllib.error.HTTPError as e:
        try:
            err_data = json.loads(e.read().decode())
            return None, f"Binance Error {e.code}: {err_data.get('msg', 'Error')}"
        except Exception:
            return None, f"HTTP Error {e.code}"
    except Exception as ex:
        return None, str(ex)

def get_live_balance():
    res, err = binance_signed_request("/api/v3/account", method="GET")
    if err:
        return 0.0, err
    if res and "balances" in res:
        for b in res["balances"]:
            if b["asset"] == "USDT":
                return float(b["free"]), None
    return 0.0, "USDT Asset পাওয়া যায়নি"

def execute_binance_order(symbol, side, quote_order_qty=None, quantity=None):
    params = {"symbol": symbol, "side": side, "type": "MARKET"}
    if side == "BUY" and quote_order_qty:
        params["quoteOrderQty"] = f"{quote_order_qty:.2f}"
    elif side == "SELL":
        if quantity:
            params["quantity"] = f"{quantity:.5f}"
        elif quote_order_qty:
            params["quoteOrderQty"] = f"{quote_order_qty:.2f}"
    return binance_signed_request("/api/v3/order", method="POST", params=params)

def fetch_klines(symbol):
    url = f"https://data-api.binance.vision/api/v3/klines?symbol={symbol}&interval=1m&limit=40"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=3) as r:
            return json.loads(r.read().decode())
    except Exception:
        return None

def fetch_depth(symbol):
    url = f"https://data-api.binance.vision/api/v3/depth?symbol={symbol}&limit=5"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=3) as r:
            return json.loads(r.read().decode())
    except Exception:
        return {"bids": [], "asks": []}

def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({"chat_id": TELEGRAM_CHAT_ID, "text": message}).encode('utf-8')
        req = urllib.request.Request(url, data=data, headers={"User-Agent": "Mozilla/5.0"})
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass
