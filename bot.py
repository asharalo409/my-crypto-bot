#!/usr/bin/env python3
# Crypto Bot Pro — Binance Spot + Telegram Dashboard

import os
import time
import hmac
import hashlib
import sqlite3
import threading
import logging
from decimal import Decimal, ROUND_DOWN
from datetime import datetime, timezone
from urllib.parse import urlencode

import requests
from flask import Flask, jsonify, request, render_template_string

APP = Flask(__name__)

# Binance API
BINANCE_API_KEY = "0QzotPVZt9eAbkMSuPpDSe0OQXwbQqsYM59o7HmrDHlu46ClaAemMNlnDTIXrNke"
BINANCE_API_SECRET = "qvhNafkMmHSgsFuOX4bPafqk45bDU1x8HUcasCSDFPBgMd9msH4uoTv2X6CjhQNb"

# Telegram API
TELEGRAM_BOT_TOKEN = "9089337002:AAFDg1eJBFuQzvEkN5FD1gVh8zk1J1WhZdo"
TELEGRAM_CHAT_ID = "7741119923"

DB = "trades.db"
HOST = "127.0.0.1"
PORT = int(os.getenv("PORT", "5000"))
BASE = "https://api.binance.com"

SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT",
    "XRPUSDT", "ADAUSDT", "DOGEUSDT", "AVAXUSDT"
]

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("CryptoBot")
lock = threading.RLock()
trade_lock = threading.Lock()

state = {
    "running": False,
    "live": False,
    "strategy": False,
    "no_loss": True,
    "trade_usdt": 10.0,
    "sl": 1.5,
    "tp": 2.0,
    "maxpos": 3,
    "poll": 15,
    "error": "",
    "updated": "",
    "symbols": {
        s: {"enabled": False, "price": 0.0, "status": "Paused"}
        for s in SYMBOLS
    }
}

last_prices = {}
last_buy_time = {}
exchange_filters = {}


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def db():
    c = sqlite3.connect(DB, timeout=20, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c


def init_db():
    with db() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS positions (
                symbol TEXT PRIMARY KEY,
                qty TEXT NOT NULL,
                entry TEXT NOT NULL,
                spent TEXT NOT NULL,
                opened TEXT NOT NULL,
                mode TEXT NOT NULL,
                order_id TEXT,
                stop TEXT,
                take TEXT
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                time TEXT,
                symbol TEXT,
                side TEXT,
                price TEXT,
                qty TEXT,
                quote TEXT,
                pnl TEXT,
                reason TEXT,
                mode TEXT,
                order_id TEXT
            )
        """)


def positions():
    with db() as c:
        return [
            dict(r) for r in
            c.execute("SELECT * FROM positions ORDER BY opened DESC").fetchall()
        ]


def history():
    with db() as c:
        return [
            dict(r) for r in
            c.execute("SELECT * FROM trades ORDER BY id DESC LIMIT 100").fetchall()
        ]


def get_position(symbol):
    with db() as c:
        r = c.execute(
            "SELECT * FROM positions WHERE symbol=?", (symbol,)
        ).fetchone()
        return dict(r) if r else None


def save_position(symbol, qty, entry, spent, mode, order_id=""):
    stop = float(entry) * (1 - float(state["sl"]) / 100)
    take = float(entry) * (1 + float(state["tp"]) / 100)

    with db() as c:
        c.execute("""
            INSERT OR REPLACE INTO positions
            (symbol, qty, entry, spent, opened, mode, order_id, stop, take)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            symbol, str(qty), str(entry), str(spent), utcnow(),
            mode, str(order_id), str(stop), str(take)
        ))


def remove_position(symbol):
    with db() as c:
        c.execute("DELETE FROM positions WHERE symbol=?", (symbol,))


def log_trade(symbol, side, price, qty, quote, pnl, reason, mode, order_id=""):
    with db() as c:
        c.execute("""
            INSERT INTO trades
            (time, symbol, side, price, qty, quote, pnl, reason, mode, order_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            utcnow(), symbol, side, str(price), str(qty), str(quote),
            str(pnl), reason, mode, str(order_id)
        ))


def telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False

    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message[:3900]
            },
            timeout=10
        )
        return r.ok and r.json().get("ok", False)
    except Exception:
        log.exception("Telegram send failed")
        return False


class Binance:
    def __init__(self):
        self.key = BINANCE_API_KEY.strip()
        self.secret = BINANCE_API_SECRET.strip()
        self.session = requests.Session()

        if self.key:
            self.session.headers.update({"X-MBX-APIKEY": self.key})

    def public(self, path, params=None):
        r = self.session.get(
            BASE + path,
            params=params or {},
            timeout=15
        )
        r.raise_for_status()
        return r.json()

    def signed(self, method, path, params=None):
        if not self.key or not self.secret:
            raise RuntimeError("Binance API Key/Secret সেট করা নেই")

        p = dict(params or {})
        p["timestamp"] = int(time.time() * 1000)
        p["recvWindow"] = 5000

        query = urlencode(p)
        signature = hmac.new(
            self.secret.encode(),
            query.encode(),
            hashlib.sha256
        ).hexdigest()

        url = BASE + path + "?" + query + "&signature=" + signature
        r = self.session.request(method, url, timeout=20)

        if not r.ok:
            try:
                message = r.json().get("msg", r.text)
            except Exception:
                message = r.text
            raise RuntimeError("Binance API error: " + str(message)[:250])

        return r.json()

    def price(self, symbol):
        return float(
            self.public(
                "/api/v3/ticker/price",
                {"symbol": symbol}
            )["price"]
        )

    def account(self):
        return self.signed("GET", "/api/v3/account")

    def load_filters(self):
        info = self.public("/api/v3/exchangeInfo")

        for item in info["symbols"]:
            if item["symbol"] not in SYMBOLS:
                continue

            fs = {f["filterType"]: f for f in item["filters"]}
            lot = fs.get("LOT_SIZE", {})
            notional = fs.get("NOTIONAL") or fs.get("MIN_NOTIONAL", {})

            exchange_filters[item["symbol"]] = {
                "step": lot.get("stepSize", "0.000001"),
                "min_qty": lot.get("minQty", "0"),
                "min_notional": notional.get("minNotional", "5")
            }

    def market_buy(self, symbol, usdt):
        return self.signed("POST", "/api/v3/order", {
            "symbol": symbol,
            "side": "BUY",
            "type": "MARKET",
            "quoteOrderQty": f"{float(usdt):.2f}",
            "newOrderRespType": "FULL"
        })

    def market_sell(self, symbol, qty):
        if symbol not in exchange_filters:
            self.load_filters()

        f = exchange_filters[symbol]
        step = Decimal(f["step"])
        amount = Decimal(str(qty))

        if step > 0:
            amount = (
                amount / step
            ).to_integral_value(rounding=ROUND_DOWN) * step

        if amount <= 0 or amount < Decimal(f["min_qty"]):
            raise RuntimeError(
                "বিক্রির quantity Binance minimum/step অনুযায়ী বৈধ নয়"
            )

        return self.signed("POST", "/api/v3/order", {
            "symbol": symbol,
            "side": "SELL",
            "type": "MARKET",
            "quantity": format(amount, "f"),
            "newOrderRespType": "FULL"
        })


bn = Binance()


def order_average(order):
    fills = order.get("fills") or []
    qty = sum(float(f.get("qty", 0)) for f in fills)
    quote = sum(
        float(f.get("qty", 0)) * float(f.get("price", 0))
        for f in fills
    )

    if qty > 0 and quote > 0:
        return quote / qty, qty, quote

    qty = float(order.get("executedQty", 0))
    quote = float(order.get("cummulativeQuoteQty", 0))

    if qty > 0 and quote > 0:
        return quote / qty, qty, quote

    raise RuntimeError("Order fill পাওয়া যায়নি")


def paper_buy(symbol, price, usdt, reason):
    qty = usdt / price
    save_position(symbol, qty, price, usdt, "DEMO")
    log_trade(symbol, "BUY", price, qty, usdt, 0, reason, "DEMO")

    telegram(
        f"🟢 DEMO BUY\n{symbol}\nPrice: {price}\nAmount: {usdt:.2f} USDT"
    )

    return {"mode": "DEMO", "qty": qty, "price": price}


def live_buy(symbol, usdt, reason):
    if not state["live"]:
        raise RuntimeError("LIVE mode চালু নেই")

    if symbol not in exchange_filters:
        bn.load_filters()

    f = exchange_filters[symbol]

    if usdt < float(f["min_notional"]):
        raise RuntimeError(
            f"{symbol} minimum order প্রায় {f['min_notional']} USDT"
        )

    order = bn.market_buy(symbol, usdt)
    avg, qty, quote = order_average(order)

    save_position(
        symbol, qty, avg, quote, "LIVE", order.get("orderId", "")
    )

    log_trade(
        symbol, "BUY", avg, qty, quote, 0,
        reason, "LIVE", order.get("orderId", "")
    )

    telegram(
        f"🔴 LIVE BUY FILLED\n{symbol}\nAvg: {avg}\n"
        f"Qty: {qty}\nSpent: {quote:.4f} USDT"
    )

    return {"mode": "LIVE", "qty": qty, "price": avg, "quote": quote}


def buy_symbol(symbol, reason="Manual buy"):
    if symbol not in SYMBOLS:
        raise ValueError("অজানা symbol")

    with trade_lock:
        if get_position(symbol):
            raise RuntimeError(f"{symbol} position আগে থেকেই আছে")

        if len(positions()) >= int(state["maxpos"]):
            raise RuntimeError("সর্বোচ্চ open position limit পূর্ণ")

        if reason != "Manual buy" and not state["symbols"][symbol]["enabled"]:
            raise RuntimeError(f"{symbol} disabled")

        price = bn.price(symbol)
        usdt = float(state["trade_usdt"])

        if usdt < 5:
            raise RuntimeError("Trade size অন্তত 5 USDT হতে হবে")

        if state["live"]:
            return live_buy(symbol, usdt, reason)

        return paper_buy(symbol, price, usdt, reason)


def sell_position(symbol, price, reason):
    with trade_lock:
        pos = get_position(symbol)

        if not pos:
            return {"ok": False, "error": "Position পাওয়া যায়নি"}

        entry = float(pos["entry"])
        qty = float(pos["qty"])
        mode = pos["mode"]

        if mode == "LIVE":
            if not state["live"]:
                return {
                    "ok": False,
                    "error": "LIVE বন্ধ; বিক্রির আগে LIVE চালু করো"
                }

            account = bn.account()
            base_asset = (
                symbol[:-4] if symbol.endswith("USDT") else symbol
            )

            free = next(
                (
                    float(b["free"])
                    for b in account.get("balances", [])
                    if b["asset"] == base_asset
                ),
                0.0
            )

            qty = min(qty, free)

            if qty <= 0:
                raise RuntimeError("বিক্রির জন্য free balance পাওয়া যায়নি")

            order = bn.market_sell(symbol, qty)
            avg, sold_qty, quote = order_average(order)
            order_id = order.get("orderId", "")

        else:
            avg = price
            sold_qty = qty
            quote = qty * price
            order_id = ""

        pnl = (avg - entry) * sold_qty
        pnl_pct = ((avg - entry) / entry * 100) if entry else 0

        log_trade(
            symbol, "SELL", avg, sold_qty, quote, pnl,
            reason, mode, order_id
        )

        remove_position(symbol)

        telegram(
            f"🔻 {mode} SELL\n{symbol}\nPrice: {avg}\n"
            f"P/L before fees: {pnl:.4f} USDT ({pnl_pct:.2f}%)\n"
            f"Reason: {reason}"
        )

        return {
            "ok": True,
            "symbol": symbol,
            "price": avg,
            "qty": sold_qty,
            "pnl": pnl,
            "mode": mode
        }


def tick():
    for symbol in SYMBOLS:
        try:
            price = bn.price(symbol)

            with lock:
                state["symbols"][symbol]["price"] = price
                state["updated"] = utcnow()

            previous = last_prices.get(symbol)
            last_prices[symbol] = price
            pos = get_position(symbol)

            if pos:
                entry = float(pos["entry"])
                pnl_pct = (price - entry) / entry * 100

                state["symbols"][symbol]["status"] = (
                    f"Open: {pnl_pct:+.2f}%"
                )

                if pnl_pct >= float(state["tp"]):
                    sell_position(symbol, price, "Take Profit")

                elif pnl_pct <= -float(state["sl"]):
                    if state["no_loss"]:
                        state["symbols"][symbol]["status"] = (
                            "NO-LOSS HOLD: loss threshold reached"
                        )
                    else:
                        sell_position(symbol, price, "Stop Loss")

            elif (
                state["running"]
                and state["strategy"]
                and state["symbols"][symbol]["enabled"]
            ):
                if previous and previous > 0:
                    change = (price - previous) / previous * 100

                    if (
                        change >= 0.15
                        and time.time() - last_buy_time.get(symbol, 0) > 300
                    ):
                        buy_symbol(symbol, "Momentum signal")
                        last_buy_time[symbol] = time.time()

            if not get_position(symbol):
                state["symbols"][symbol]["status"] = (
                    "Watching"
                    if state["symbols"][symbol]["enabled"]
                    else "Paused"
                )

        except Exception as e:
            log.warning("%s: %s", symbol, e)
            state["error"] = str(e)[:300]
            state["symbols"][symbol]["status"] = "Error"


def worker():
    while True:
        if state["running"]:
            tick()
        time.sleep(max(5, int(state["poll"])))


HTML = r"""<!doctype html>
<html lang="bn">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Crypto Bot Pro</title>
<style>
body{margin:0;background:#0b1020;color:#e8eefc;font:15px system-ui;padding:16px}
main{max-width:1100px;margin:auto}
.card{background:#151e33;border:1px solid #2d3b5b;border-radius:14px;padding:16px;margin:12px 0;overflow:auto}
button,input{padding:9px;border-radius:8px;border:1px solid #415477;margin:4px;background:#10192b;color:#fff}
button{cursor:pointer;background:#165dce}
table{width:100%;border-collapse:collapse}
td,th{text-align:left;padding:8px;border-bottom:1px solid #34415a}
small{color:#a9b8d4}
.warn{color:#ffcb6b}
pre{white-space:pre-wrap}
</style>
</head>
<body>
<main>
<h1>⚡ Crypto Bot Pro</h1>
<p>Binance Spot · Telegram · Dashboard</p>

<div class="card">
<h2>Bot status</h2>
<p id="status">Loading…</p>
<button onclick="post('/api/start')">Start bot</button>
<button onclick="post('/api/stop')">Stop bot</button>
</div>

<div class="card">
<h2>Trading mode</h2>
<p class="warn">LIVE চালু থাকলে আসল অর্ডার হতে পারে।</p>
<button onclick="toggleLive(true)">Enable LIVE</button>
<button onclick="toggleLive(false)">Disable LIVE</button>
<button onclick="post('/api/check-api')">Check Binance API</button>
<button onclick="post('/api/test-telegram')">Test Telegram</button>
<pre id="msg"></pre>
</div>

<div class="card">
<h2>Settings</h2>
<label>Trade USDT <input id="amount" type="number" value="10" min="5"></label>
<label>Stop Loss % <input id="sl" type="number" value="1.5" min=".1" step=".1"></label>
<label>Take Profit % <input id="tp" type="number" value="2" min=".1" step=".1"></label>
<label>Max positions <input id="maxpos" type="number" value="3" min="1" max="8"></label>
<label>Poll seconds <input id="poll" type="number" value="15" min="5"></label>
<p>
<label><input id="strategy" type="checkbox"> Automatic strategy</label>
<label><input id="noloss" type="checkbox" checked> No-Loss Hold</label>
</p>
<button onclick="saveSettings()">Save settings</button>
<p class="warn">No-Loss Hold Stop Loss threshold-এ পৌঁছালেও position ধরে রাখে।</p>
</div>

<div class="card">
<h2>Markets</h2>
<table>
<thead><tr><th>Symbol</th><th>Price (USDT)</th><th>Status</th><th>Enabled</th><th>Actions</th></tr></thead>
<tbody id="markets"></tbody>
</table>
</div>

<div class="card">
<h2>Open positions</h2>
<table>
<thead><tr><th>Symbol</th><th>Qty</th><th>Entry</th><th>Spent</th><th>Mode</th><th>Action</th></tr></thead>
<tbody id="positions"></tbody>
</table>
</div>

<div class="card">
<h2>Trade history</h2>
<table>
<thead><tr><th>Time</th><th>Symbol</th><th>Side</th><th>Price</th><th>P/L</th><th>Reason</th><th>Mode</th></tr></thead>
<tbody id="history"></tbody>
</table>
</div>
</main>

<script>
async function post(url,data={}){
 try{
  const r=await fetch(url,{
   method:'POST',
   headers:{'Content-Type':'application/json'},
   body:JSON.stringify(data)
  });
  const d=await r.json();
  document.getElementById('msg').textContent=JSON.stringify(d,null,2);
  await refresh();
  return d;
 }catch(e){
  document.getElementById('msg').textContent=String(e);
 }
}

async function toggleLive(enabled){
 if(enabled&&!confirm('LIVE চালু করবেন? এটি real order দিতে পারে।'))return;
 await post('/api/live',{enabled});
}

async function saveSettings(){
 await post('/api/settings',{
  trade_usdt:+document.getElementById('amount').value,
  sl:+document.getElementById('sl').value,
  tp:+document.getElementById('tp').value,
  maxpos:+document.getElementById('maxpos').value,
  poll:+document.getElementById('poll').value,
  strategy:document.getElementById('strategy').checked,
  no_loss:document.getElementById('noloss').checked
 });
}

async function sym(symbol,enabled){
 await post('/api/symbol',{symbol,enabled});
}

async function buy(symbol){
 if(confirm(symbol+' BUY করবেন?'))await post('/api/buy',{symbol});
}

async function sell(symbol){
 if(confirm(symbol+' SELL করবেন?'))await post('/api/sell',{symbol});
}

async function refresh(){
 try{
  const d=await(await fetch('/api/state')).json();
  const s=d.state;

  document.getElementById('status').textContent=
   (s.running?'Running':'Stopped')+' · '+(s.live?'LIVE':'DEMO')+
   ' · '+(s.updated||'—')+(s.error?' · Error: '+s.error:'');

  document.getElementById('amount').value=s.trade_usdt;
  document.getElementById('sl').value=s.sl;
  document.getElementById('tp').value=s.tp;
  document.getElementById('maxpos').value=s.maxpos;
  document.getElementById('poll').value=s.poll;
  document.getElementById('strategy').checked=s.strategy;
  document.getElementById('noloss').checked=s.no_loss;

  document.getElementById('markets').innerHTML=
   Object.entries(s.symbols).map(([k,v])=>
    '<tr><td>'+k+'</td><td>'+Number(v.price).toPrecision(8)+
    '</td><td>'+v.status+'</td><td><input type="checkbox" '+
    (v.enabled?'checked':'')+' onchange="sym(\\''+k+'\\',this.checked)">'+
    '</td><td><button onclick="buy(\\''+k+'\\')">BUY</button></td></tr>'
   ).join('');

  document.getElementById('positions').innerHTML=
   d.positions.map(p=>
    '<tr><td>'+p.symbol+'</td><td>'+p.qty+'</td><td>'+p.entry+
    '</td><td>'+p.spent+'</td><td>'+p.mode+
    '</td><td><button onclick="sell(\\''+p.symbol+'\\')">SELL/CLOSE</button></td></tr>'
   ).join('');

  document.getElementById('history').innerHTML=
   d.history.map(h=>
    '<tr><td>'+h.time+'</td><td>'+h.symbol+'</td><td>'+h.side+
    '</td><td>'+h.price+'</td><td>'+h.pnl+'</td><td>'+h.reason+
    '</td><td>'+h.mode+'</td></tr>'
   ).join('');

 }catch(e){
  document.getElementById('status').textContent='Dashboard error: '+e;
 }
}

setInterval(refresh,3000);
refresh();
</script>
</body>
</html>"""


@APP.get("/")
def dashboard():
    return render_template_string(HTML)


@APP.get("/api/state")
def api_state():
    with lock:
        snapshot = {
            k: v for k, v in state.items()
            if k != "symbols"
        }
        snapshot["symbols"] = {
            k: dict(v) for k, v in state["symbols"].items()
        }

    return jsonify({
        "state": snapshot,
        "positions": positions(),
        "history": history()
    })


@APP.post("/api/settings")
def api_settings():
    data = request.get_json(silent=True) or {}

    try:
        amount = float(data.get("trade_usdt", state["trade_usdt"]))
        sl = float(data.get("sl", state["sl"]))
        tp = float(data.get("tp", state["tp"]))
        maxpos = int(data.get("maxpos", state["maxpos"]))
        poll = int(data.get("poll", state["poll"]))

        if not 5 <= amount <= 100000:
            raise ValueError("Trade size 5–100000 USDT হতে হবে")
        if not 0.1 <= sl <= 50:
            raise ValueError("Stop Loss 0.1–50% হতে হবে")
        if not 0.1 <= tp <= 100:
            raise ValueError("Take Profit 0.1–100% হতে হবে")
        if not 1 <= maxpos <= len(SYMBOLS):
            raise ValueError("Max positions 1–8 হতে হবে")
        if not 5 <= poll <= 300:
            raise ValueError("Poll interval 5–300 seconds হতে হবে")

        with lock:
            state.update(
                trade_usdt=amount,
                sl=sl,
                tp=tp,
                maxpos=maxpos,
                poll=poll,
                no_loss=bool(data.get("no_loss", state["no_loss"])),
                strategy=bool(data.get("strategy", state["strategy"]))
            )

        return jsonify({"ok": True, "message": "Settings saved"})

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@APP.post("/api/symbol")
def api_symbol():
    data = request.get_json(silent=True) or {}
    symbol = data.get("symbol")

    if symbol not in SYMBOLS:
        return jsonify({"ok": False, "error": "Invalid symbol"}), 400

    with lock:
        state["symbols"][symbol]["enabled"] = bool(data.get("enabled"))

    return jsonify({"ok": True})


@APP.post("/api/live")
def api_live():
    data = request.get_json(silent=True) or {}
    enabled = bool(data.get("enabled"))

    if not enabled:
        with lock:
            state["live"] = False

        telegram("LIVE mode disabled. New live orders are blocked.")

        return jsonify({
            "ok": True,
            "live": False,
            "message": "LIVE mode বন্ধ"
        })

    try:
        if not bn.key or not bn.secret:
            raise RuntimeError("Binance API Key/Secret সেট করো")

        account = bn.account()

        if not account.get("canTrade", False):
            raise RuntimeError("Binance API account trading অনুমোদন করছে না")

        bn.load_filters()

        with lock:
            state["live"] = True
            state["error"] = ""

        telegram("LIVE TRADING ENABLED. Real orders may use real funds.")

        return jsonify({
            "ok": True,
            "live": True,
            "message": "LIVE চালু"
        })

    except Exception as e:
        with lock:
            state["live"] = False
            state["error"] = str(e)[:300]

        return jsonify({
            "ok": False,
            "live": False,
            "error": str(e)
        }), 400


@APP.post("/api/start")
def api_start():
    with lock:
        state["running"] = True

    telegram(
        "Bot started in " +
        ("LIVE" if state["live"] else "DEMO") +
        " mode"
    )

    return jsonify({"ok": True})


@APP.post("/api/stop")
def api_stop():
    with lock:
        state["running"] = False

    telegram("Bot stopped. Automatic strategy paused.")

    return jsonify({"ok": True})


@APP.post("/api/buy")
def api_buy():
    data = request.get_json(silent=True) or {}

    try:
        return jsonify({
            "ok": True,
            "result": buy_symbol(data.get("symbol"), "Manual buy")
        })

    except Exception as e:
        log.exception("Buy failed")
        return jsonify({"ok": False, "error": str(e)}), 400


@APP.post("/api/sell")
def api_sell():
    data = request.get_json(silent=True) or {}
    symbol = data.get("symbol")

    if symbol not in SYMBOLS:
        return jsonify({"ok": False, "error": "Invalid symbol"}), 400

    try:
        return jsonify(
            sell_position(symbol, bn.price(symbol), "Manual sell")
        )

    except Exception as e:
        log.exception("Sell failed")
        return jsonify({"ok": False, "error": str(e)}), 400


@APP.post("/api/test-telegram")
def api_test_telegram():
    ok = telegram("Crypto Bot Pro Telegram test")

    return jsonify({
        "ok": ok,
        "message": "Message sent" if ok else "Telegram credentials যাচাই করো"
    }), (200 if ok else 400)


@APP.post("/api/check-api")
def api_check_api():
    try:
        result = bn.account()

        return jsonify({
            "ok": True,
            "canTrade": result.get("canTrade"),
            "canWithdraw": result.get("canWithdraw")
        })

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


if __name__ == "__main__":
    init_db()

    try:
        bn.load_filters()
    except Exception as e:
        log.warning("Exchange filters unavailable at startup: %s", e)

    threading.Thread(target=worker, daemon=True).start()

    print(f"Crypto Bot Pro dashboard: http://{HOST}:{PORT}")
    print("LIVE mode OFF at startup.")

    APP.run(host=HOST, port=PORT, debug=False, use_reloader=False)
