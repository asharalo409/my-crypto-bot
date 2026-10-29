#!/usr/bin/env python3
# Crypto Bot Pro — Binance Spot + Telegram Dashboard
# DEMO/LIVE separated wallets, positions and trade history.

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

try:
    import config as _cfg
except ImportError:
    _cfg = object()


def credential(env_name, *config_names):
    value = os.getenv(env_name, "").strip()
    if value:
        return value
    for name in config_names:
        value = getattr(_cfg, name, "")
        if value:
            return str(value).strip()
    return ""


BINANCE_API_KEY = credential("BINANCE_API_KEY", "BINANCE_API_KEY", "API_KEY")
BINANCE_API_SECRET = credential("BINANCE_API_SECRET", "BINANCE_API_SECRET", "API_SECRET")
TELEGRAM_BOT_TOKEN = credential("TELEGRAM_BOT_TOKEN", "TELEGRAM_BOT_TOKEN", "TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = credential("TELEGRAM_CHAT_ID", "TELEGRAM_CHAT_ID", "CHAT_ID")

DB = os.getenv("BOT_DB", "trades.db")
HOST = os.getenv("HOST", "127.0.0.1")
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
    "mode": "DEMO",
    "live_armed": False,
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
live_balance_cache = {"USDT": 0.0, "updated": "", "error": ""}


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def db():
    c = sqlite3.connect(DB, timeout=20, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c


def init_db():
    with db() as c:
        cols = {r["name"] for r in c.execute("PRAGMA table_info(positions)")}

        if cols and not {"mode", "symbol", "qty", "entry", "spent", "opened"}.issubset(cols):
            c.execute("ALTER TABLE positions RENAME TO positions_old")
            cols = set()

        if not cols:
            c.execute("""
                CREATE TABLE IF NOT EXISTS positions (
                    mode TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    qty TEXT NOT NULL,
                    entry TEXT NOT NULL,
                    spent TEXT NOT NULL,
                    opened TEXT NOT NULL,
                    order_id TEXT,
                    stop TEXT,
                    take TEXT,
                    PRIMARY KEY(mode, symbol)
                )
            """)

            old = c.execute("""
                SELECT name FROM sqlite_master
                WHERE type='table' AND name='positions_old'
            """).fetchone()

            if old:
                old_cols = {r["name"] for r in c.execute("PRAGMA table_info(positions_old)")}
                for row in c.execute("SELECT * FROM positions_old").fetchall():
                    d = dict(row)
                    mode = str(d.get("mode", "DEMO")).upper()
                    if mode not in ("DEMO", "LIVE"):
                        mode = "DEMO"
                    symbol = d.get("symbol", "")
                    if symbol not in SYMBOLS:
                        continue
                    c.execute("""
                        INSERT OR IGNORE INTO positions
                        (mode,symbol,qty,entry,spent,opened,order_id,stop,take)
                        VALUES(?,?,?,?,?,?,?,?,?)
                    """, (
                        mode, symbol, str(d.get("qty", "0")),
                        str(d.get("entry", "0")), str(d.get("spent", "0")),
                        d.get("opened", utcnow()), str(d.get("order_id", "")),
                        str(d.get("stop", "")), str(d.get("take", ""))
                    ))
                c.execute("DROP TABLE positions_old")

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
                mode TEXT DEFAULT 'DEMO',
                order_id TEXT
            )
        """)

        trade_cols = {r["name"] for r in c.execute("PRAGMA table_info(trades)")}
        if "mode" not in trade_cols:
            c.execute("ALTER TABLE trades ADD COLUMN mode TEXT DEFAULT 'DEMO'")
        if "order_id" not in trade_cols:
            c.execute("ALTER TABLE trades ADD COLUMN order_id TEXT")

        c.execute("""
            CREATE TABLE IF NOT EXISTS wallets (
                mode TEXT PRIMARY KEY,
                balance REAL NOT NULL,
                realized_pnl REAL NOT NULL DEFAULT 0,
                buys INTEGER NOT NULL DEFAULT 0,
                sells INTEGER NOT NULL DEFAULT 0,
                wins INTEGER NOT NULL DEFAULT 0
            )
        """)
        c.execute("INSERT OR IGNORE INTO wallets(mode,balance) VALUES('DEMO',10000)")
        c.execute("INSERT OR IGNORE INTO wallets(mode,balance) VALUES('LIVE',0)")


def current_mode():
    return state["mode"]


def positions(mode=None):
    mode = mode or current_mode()
    with db() as c:
        return [dict(r) for r in c.execute(
            "SELECT * FROM positions WHERE mode=? ORDER BY opened DESC", (mode,)
        ).fetchall()]


def history(mode=None):
    mode = mode or current_mode()
    with db() as c:
        return [dict(r) for r in c.execute(
            "SELECT * FROM trades WHERE mode=? ORDER BY id DESC LIMIT 100", (mode,)
        ).fetchall()]


def get_position(symbol, mode=None):
    mode = mode or current_mode()
    with db() as c:
        r = c.execute(
            "SELECT * FROM positions WHERE mode=? AND symbol=?", (mode, symbol)
        ).fetchone()
        return dict(r) if r else None


def wallet(mode=None):
    mode = mode or current_mode()
    with db() as c:
        r = c.execute("SELECT * FROM wallets WHERE mode=?", (mode,)).fetchone()
        return dict(r)


def update_wallet(mode, **values):
    allowed = {"balance", "realized_pnl", "buys", "sells", "wins"}
    values = {k: v for k, v in values.items() if k in allowed}
    if not values:
        return
    with db() as c:
        c.execute(
            "UPDATE wallets SET " + ",".join(f"{k}=?" for k in values) + " WHERE mode=?",
            list(values.values()) + [mode]
        )


def save_position(symbol, qty, entry, spent, mode, order_id=""):
    stop = float(entry) * (1 - float(state["sl"]) / 100)
    take = float(entry) * (1 + float(state["tp"]) / 100)
    with db() as c:
        c.execute("""
            INSERT OR REPLACE INTO positions
            (mode,symbol,qty,entry,spent,opened,order_id,stop,take)
            VALUES(?,?,?,?,?,?,?,?,?)
        """, (mode, symbol, str(qty), str(entry), str(spent), utcnow(),
              str(order_id), str(stop), str(take)))


def remove_position(symbol, mode):
    with db() as c:
        c.execute("DELETE FROM positions WHERE mode=? AND symbol=?", (mode, symbol))


def log_trade(symbol, side, price, qty, quote, pnl, reason, mode, order_id=""):
    with db() as c:
        c.execute("""
            INSERT INTO trades
            (time,symbol,side,price,qty,quote,pnl,reason,mode,order_id)
            VALUES(?,?,?,?,?,?,?,?,?,?)
        """, (utcnow(), symbol, side, str(price), str(qty), str(quote),
              str(pnl), reason, mode, str(order_id)))


def telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message[:3900]},
            timeout=10
        )
        return r.ok and r.json().get("ok", False)
    except Exception:
        log.exception("Telegram send failed")
        return False


class Binance:
    def __init__(self):
        self.key = BINANCE_API_KEY
        self.secret = BINANCE_API_SECRET
        self.session = requests.Session()
        if self.key:
            self.session.headers.update({"X-MBX-APIKEY": self.key})

    def public(self, path, params=None):
        r = self.session.get(BASE + path, params=params or {}, timeout=15)
        r.raise_for_status()
        return r.json()

    def signed(self, method, path, params=None):
        if not self.key or not self.secret:
            raise RuntimeError("Binance API Key/Secret config.py অথবা environment-এ সেট করো")
        p = dict(params or {})
        p["timestamp"] = int(time.time() * 1000)
        p["recvWindow"] = 5000
        query = urlencode(p)
        signature = hmac.new(self.secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        r = self.session.request(
            method, BASE + path + "?" + query + "&signature=" + signature, timeout=20
        )
        if not r.ok:
            try:
                message = r.json().get("msg", r.text)
            except Exception:
                message = r.text
            raise RuntimeError("Binance API error: " + str(message)[:250])
        return r.json()

    def price(self, symbol):
        return float(self.public("/api/v3/ticker/price", {"symbol": symbol})["price"])

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
            "symbol": symbol, "side": "BUY", "type": "MARKET",
            "quoteOrderQty": f"{float(usdt):.2f}", "newOrderRespType": "FULL"
        })

    def market_sell(self, symbol, qty):
        if symbol not in exchange_filters:
            self.load_filters()
        f = exchange_filters[symbol]
        step = Decimal(f["step"])
        amount = Decimal(str(qty))
        if step > 0:
            amount = (amount / step).to_integral_value(rounding=ROUND_DOWN) * step
        if amount <= 0 or amount < Decimal(f["min_qty"]):
            raise RuntimeError("বিক্রির quantity Binance minimum/step অনুযায়ী বৈধ নয়")
        return self.signed("POST", "/api/v3/order", {
            "symbol": symbol, "side": "SELL", "type": "MARKET",
            "quantity": format(amount, "f"), "newOrderRespType": "FULL"
        })


bn = Binance()


def order_average(order):
    fills = order.get("fills") or []
    qty = sum(float(f.get("qty", 0)) for f in fills)
    quote = sum(float(f.get("qty", 0)) * float(f.get("price", 0)) for f in fills)
    if qty > 0 and quote > 0:
        return quote / qty, qty, quote
    qty = float(order.get("executedQty", 0))
    quote = float(order.get("cummulativeQuoteQty", 0))
    if qty > 0 and quote > 0:
        return quote / qty, qty, quote
    raise RuntimeError("Order fill পাওয়া যায়নি")


def live_account_balance():
    account = bn.account()
    balance = next((
        float(b["free"]) + float(b["locked"])
        for b in account.get("balances", []) if b["asset"] == "USDT"
    ), 0.0)
    live_balance_cache.update({"USDT": balance, "updated": utcnow(), "error": ""})
    return balance


def paper_buy(symbol, price, usdt, reason):
    w = wallet("DEMO")
    if float(w["balance"]) < usdt:
        raise RuntimeError("DEMO balance পর্যাপ্ত নয়")
    qty = usdt / price
    save_position(symbol, qty, price, usdt, "DEMO")
    update_wallet("DEMO", balance=float(w["balance"]) - usdt, buys=int(w["buys"]) + 1)
    log_trade(symbol, "BUY", price, qty, usdt, 0, reason, "DEMO")
    telegram(f"🟢 DEMO BUY\n{symbol}\nPrice: {price}\nAmount: {usdt:.2f} USDT")
    return {"mode": "DEMO", "qty": qty, "price": price, "quote": usdt}


def live_buy(symbol, usdt, reason):
    if not state["live_armed"]:
        raise RuntimeError("LIVE trading enable করা নেই")
    if symbol not in exchange_filters:
        bn.load_filters()
    f = exchange_filters[symbol]
    if usdt < float(f["min_notional"]):
        raise RuntimeError(f"{symbol} minimum order প্রায় {f['min_notional']} USDT")
    balance = live_account_balance()
    if balance < usdt:
        raise RuntimeError(f"LIVE USDT balance পর্যাপ্ত নয়: {balance:.4f} USDT")
    order = bn.market_buy(symbol, usdt)
    avg, qty, quote = order_average(order)
    save_position(symbol, qty, avg, quote, "LIVE", order.get("orderId", ""))
    w = wallet("LIVE")
    update_wallet("LIVE", buys=int(w["buys"]) + 1)
    log_trade(symbol, "BUY", avg, qty, quote, 0, reason, "LIVE", order.get("orderId", ""))
    telegram(f"🔴 LIVE BUY FILLED\n{symbol}\nAvg: {avg}\nQty: {qty}\nSpent: {quote:.4f} USDT")
    return {"mode": "LIVE", "qty": qty, "price": avg, "quote": quote}


def buy_symbol(symbol, reason="Manual buy"):
    if symbol not in SYMBOLS:
        raise ValueError("অজানা symbol")
    with trade_lock:
        mode = current_mode()
        if mode == "LIVE" and not state["live_armed"]:
            raise RuntimeError("LIVE trading enable করা নেই")
        if get_position(symbol, mode):
            raise RuntimeError(f"{mode} mode-এ {symbol} position আগে থেকেই আছে")
        if len(positions(mode)) >= int(state["maxpos"]):
            raise RuntimeError(f"{mode} mode-এর maximum position limit পূর্ণ")
        if reason != "Manual buy" and not state["symbols"][symbol]["enabled"]:
            raise RuntimeError(f"{symbol} disabled")
        price = bn.price(symbol)
        usdt = float(state["trade_usdt"])
        if usdt < 5:
            raise RuntimeError("Trade size অন্তত 5 USDT হতে হবে")
        return live_buy(symbol, usdt, reason) if mode == "LIVE" else paper_buy(symbol, price, usdt, reason)


def sell_position(symbol, price, reason, mode=None):
    with trade_lock:
        mode = mode or current_mode()
        pos = get_position(symbol, mode)
        if not pos:
            return {"ok": False, "error": f"{mode} position পাওয়া যায়নি"}
        entry = float(pos["entry"])
        qty = float(pos["qty"])

        if mode == "LIVE":
            if not state["live_armed"]:
                raise RuntimeError("LIVE trading enable করা নেই")
            account = bn.account()
            base_asset = symbol[:-4] if symbol.endswith("USDT") else symbol
            free = next((
                float(b["free"]) for b in account.get("balances", [])
                if b["asset"] == base_asset
            ), 0.0)
            qty = min(qty, free)
            if qty <= 0:
                raise RuntimeError("বিক্রির জন্য free balance পাওয়া যায়নি")
            order = bn.market_sell(symbol, qty)
            avg, sold_qty, quote = order_average(order)
            order_id = order.get("orderId", "")
        else:
            avg, sold_qty, quote, order_id = price, qty, qty * price, ""

        pnl = (avg - entry) * sold_qty
        pnl_pct = (avg - entry) / entry * 100 if entry else 0
        log_trade(symbol, "SELL", avg, sold_qty, quote, pnl, reason, mode, order_id)
        remove_position(symbol, mode)
        w = wallet(mode)
        updates = {
            "realized_pnl": float(w["realized_pnl"]) + pnl,
            "sells": int(w["sells"]) + 1,
            "wins": int(w["wins"]) + (1 if pnl > 0 else 0)
        }
        if mode == "DEMO":
            updates["balance"] = float(w["balance"]) + quote
        update_wallet(mode, **updates)
        telegram(
            f"🔻 {mode} SELL\n{symbol}\nPrice: {avg}\n"
            f"P/L before fees: {pnl:.4f} USDT ({pnl_pct:.2f}%)\nReason: {reason}"
        )
        return {"ok": True, "symbol": symbol, "price": avg, "qty": sold_qty, "pnl": pnl, "mode": mode}


def tick():
    for symbol in SYMBOLS:
        try:
            price = bn.price(symbol)
            with lock:
                state["symbols"][symbol]["price"] = price
                state["updated"] = utcnow()
            previous = last_prices.get(symbol)
            last_prices[symbol] = price

            for mode in ("DEMO", "LIVE"):
                pos = get_position(symbol, mode)
                if not pos:
                    continue
                if mode == "LIVE" and not state["live_armed"]:
                    continue
                entry = float(pos["entry"])
                pnl_pct = (price - entry) / entry * 100 if entry else 0
                if mode == current_mode():
                    state["symbols"][symbol]["status"] = f"{mode} Open: {pnl_pct:+.2f}%"
                if pnl_pct >= float(state["tp"]):
                    sell_position(symbol, price, "Take Profit", mode)
                elif pnl_pct <= -float(state["sl"]):
                    if state["no_loss"]:
                        if mode == current_mode():
                            state["symbols"][symbol]["status"] = "NO-LOSS HOLD: loss threshold reached"
                    else:
                        sell_position(symbol, price, "Stop Loss", mode)

            mode = current_mode()
            if (
                state["running"] and state["strategy"]
                and state["symbols"][symbol]["enabled"]
                and not get_position(symbol, mode)
                and (mode == "DEMO" or state["live_armed"])
                and previous and previous > 0
            ):
                change = (price - previous) / previous * 100
                if change >= 0.15 and time.time() - last_buy_time.get((mode, symbol), 0) > 300:
                    buy_symbol(symbol, "Momentum signal")
                    last_buy_time[(mode, symbol)] = time.time()

            if not get_position(symbol, current_mode()):
                state["symbols"][symbol]["status"] = "Watching" if state["symbols"][symbol]["enabled"] else "Paused"
        except Exception as e:
            log.warning("%s: %s", symbol, e)
            state["error"] = str(e)[:300]
            state["symbols"][symbol]["status"] = "Error"


def worker():
    while True:
        try:
            if state["running"]:
                tick()
            if state["mode"] == "LIVE" and state["live_armed"]:
                try:
                    live_account_balance()
                except Exception as e:
                    live_balance_cache["error"] = str(e)[:250]
        except Exception:
            log.exception("Worker error")
        time.sleep(max(5, int(state["poll"])))


HTML = r"""<!doctype html>
<html lang="bn">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Crypto Bot Pro</title>
<style>
*{box-sizing:border-box}
body{margin:0;background:#0b1020;color:#e8eefc;font:15px system-ui;padding:16px}
main{max-width:1100px;margin:auto}
.card{background:#151e33;border:1px solid #2d3b5b;border-radius:14px;padding:16px;margin:12px 0;overflow:auto}
button,input{padding:9px;border-radius:8px;border:1px solid #415477;margin:4px;background:#10192b;color:#fff}
button{cursor:pointer;background:#165dce}
table{width:100%;border-collapse:collapse}
td,th{text-align:left;padding:8px;border-bottom:1px solid #34415a;white-space:nowrap}
small{color:#a9b8d4}
.warn{color:#ffcb6b}
pre{white-space:pre-wrap;overflow-wrap:anywhere}
.mode-tabs{display:flex;gap:8px;flex-wrap:wrap}
.mode-tabs button{flex:1;min-width:120px;padding:12px}
.active-mode{outline:2px solid #53d8fb;background:#087f9b}
.metrics{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px}
.metric{background:#10192b;border:1px solid #2d3b5b;border-radius:10px;padding:13px}
.metric strong{display:block;font-size:21px;margin-top:6px;overflow-wrap:anywhere}
.green{color:#48e0a4}.red{color:#ff7b87}
</style>
</head>
<body>
<main>
<h1>⚡ Crypto Bot Pro</h1>
<p><small>Binance Spot · Telegram · Trading Dashboard</small></p>

<div class="card">
<h2>Trading Mode</h2>
<div class="mode-tabs">
<button id="demoTab" onclick="switchMode('DEMO')">DEMO MODE</button>
<button id="liveTab" onclick="switchMode('LIVE')">LIVE MODE</button>
</div>
<p id="modeNotice" class="warn"></p>
</div>

<div class="card">
<h2>Account Overview</h2>
<div class="metrics">
<div class="metric">Selected Mode<strong id="modeValue">DEMO</strong></div>
<div class="metric">Available USDT<strong id="balance">$10,000.00</strong></div>
<div class="metric">Realized P/L<strong id="realized">$0.00</strong></div>
<div class="metric">Open Positions<strong id="openCount">0</strong></div>
<div class="metric">Unrealized P/L<strong id="unrealized">$0.00</strong></div>
</div>
<p id="balanceNote"><small>DEMO balance is virtual.</small></p>
</div>

<div class="card">
<h2>Bot Status</h2>
<p id="status">Loading…</p>
<button onclick="post('/api/start')">Start Bot</button>
<button onclick="post('/api/stop')">Stop Bot</button>
</div>

<div class="card">
<h2>LIVE Trading Permission</h2>
<p class="warn">LIVE mode-এ real order হতে পারে। Binance API Key-তে withdrawal permission চালু কোরো না।</p>
<button onclick="toggleLive(true)">Enable LIVE Trading</button>
<button onclick="toggleLive(false)">Disable LIVE Trading</button>
<button onclick="post('/api/check-api')">Check Binance API</button>
<button onclick="post('/api/test-telegram')">Test Telegram</button>
<pre id="msg"></pre>
</div>

<div class="card">
<h2>Settings</h2>
<label>Trade USDT <input id="amount" type="number" value="10" min="5"></label>
<label>Stop Loss % <input id="sl" type="number" value="1.5" min=".1" step=".1"></label>
<label>Take Profit % <input id="tp" type="number" value="2" min=".1" step=".1"></label>
<label>Max Positions <input id="maxpos" type="number" value="3" min="1" max="8"></label>
<label>Poll Seconds <input id="poll" type="number" value="15" min="5"></label>
<p>
<label><input id="strategy" type="checkbox"> Automatic Strategy</label>
<label><input id="noloss" type="checkbox" checked> No-Loss Hold</label>
</p>
<button onclick="saveSettings()">Save Settings</button>
<p class="warn">No-Loss Hold চালু থাকলে Stop Loss threshold-এ পৌঁছালেও bot position ধরে রাখবে।</p>
</div>

<div class="card">
<h2>Markets</h2>
<table><thead><tr><th>Symbol</th><th>Price (USDT)</th><th>Status</th><th>Enabled</th><th>Actions</th></tr></thead><tbody id="markets"></tbody></table>
</div>

<div class="card">
<h2 id="positionsTitle">DEMO Open Positions</h2>
<table><thead><tr><th>Symbol</th><th>Qty</th><th>Entry</th><th>Spent</th><th>Mode</th><th>Action</th></tr></thead><tbody id="positions"></tbody></table>
</div>

<div class="card">
<h2 id="historyTitle">DEMO Trade History</h2>
<table><thead><tr><th>Time</th><th>Symbol</th><th>Side</th><th>Price</th><th>Qty</th><th>P/L</th><th>Reason</th><th>Mode</th></tr></thead><tbody id="history"></tbody></table>
</div>
</main>

<script>
let selectedMode='DEMO';
let settingsDirty=false;

async function post(url,data={}){
 try{
  const r=await fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});
  const d=await r.json();
  document.getElementById('msg').textContent=JSON.stringify(d,null,2);
  await refresh();
  return d;
 }catch(e){
  document.getElementById('msg').textContent=String(e);
  return null;
 }
}

async function switchMode(mode){
 const d=await post('/api/mode',{mode});
 if(d&&d.ok){selectedMode=mode;await refresh();}
}

async function toggleLive(enabled){
 if(enabled&&!confirm('LIVE trading চালু করবেন? বাস্তব টাকা দিয়ে order হতে পারে।'))return;
 await post('/api/live',{enabled});
}

async function saveSettings(){
 settingsDirty=false;
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

async function sym(symbol,enabled){await post('/api/symbol',{symbol,enabled});}

async function buy(symbol){
 const message=selectedMode==='LIVE'
  ?'LIVE BUY: বাস্তব টাকা দিয়ে '+symbol+' কিনবেন?'
  :'DEMO BUY: virtual '+symbol+' কিনবেন?';
 if(confirm(message))await post('/api/buy',{symbol});
}

async function sell(symbol){
 const message=selectedMode==='LIVE'
  ?'LIVE SELL: বাস্তব '+symbol+' position বিক্রি করবেন?'
  :'DEMO SELL: virtual '+symbol+' position বন্ধ করবেন?';
 if(confirm(message))await post('/api/sell',{symbol});
}

function money(n){
 return Number(n||0).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});
}
function esc(v){
 return String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

document.querySelectorAll('#amount,#sl,#tp,#maxpos,#poll,#strategy,#noloss').forEach(el=>{
 el.addEventListener('input',()=>{settingsDirty=true;});
 el.addEventListener('change',()=>{settingsDirty=true;});
});

async function refresh(){
 try{
  const r=await fetch('/api/state',{cache:'no-store'});
  const d=await r.json();
  const s=d.state;
  selectedMode=s.mode;
  document.getElementById('demoTab').className=selectedMode==='DEMO'?'active-mode':'';
  document.getElementById('liveTab').className=selectedMode==='LIVE'?'active-mode':'';
  document.getElementById('modeValue').textContent=selectedMode;
  document.getElementById('status').textContent=
   (s.running?'Running':'Stopped')+' · '+selectedMode+
   ' · LIVE Trading: '+(s.live_armed?'ENABLED':'DISABLED')+
   ' · Updated: '+(s.updated||'—')+(s.error?' · Error: '+s.error:'');
  if(selectedMode==='DEMO'){
   document.getElementById('balance').textContent='$'+money(d.demo_wallet.balance);
   document.getElementById('realized').textContent='$'+money(d.demo_wallet.realized_pnl);
   document.getElementById('balanceNote').innerHTML='<small>Virtual DEMO balance. Buy করলে কমবে, sell করলে proceeds যোগ হবে।</small>';
  }else{
   document.getElementById('balance').textContent=d.live_balance_error?'Unavailable':'$'+money(d.live_usdt_balance);
   document.getElementById('realized').textContent='$'+money(d.live_wallet.realized_pnl);
   document.getElementById('balanceNote').innerHTML='<small>Binance actual USDT balance. '+esc(d.live_balance_error||'')+'</small>';
  }
  document.getElementById('openCount').textContent=d.positions.length;
  document.getElementById('unrealized').textContent='$'+money(d.unrealized_pnl);
  document.getElementById('positionsTitle').textContent=selectedMode+' Open Positions';
  document.getElementById('historyTitle').textContent=selectedMode+' Trade History';

  if(!settingsDirty){
   document.getElementById('amount').value=s.trade_usdt;
   document.getElementById('sl').value=s.sl;
   document.getElementById('tp').value=s.tp;
   document.getElementById('maxpos').value=s.maxpos;
   document.getElementById('poll').value=s.poll;
   document.getElementById('strategy').checked=s.strategy;
   document.getElementById('noloss').checked=s.no_loss;
  }

  document.getElementById('modeNotice').textContent=selectedMode==='DEMO'
   ?'DEMO: virtual 10,000 USDT. Real orders disabled in this mode.'
   :(s.live_armed?'LIVE view selected. Real trading permission enabled.':'LIVE view selected. Real trading permission disabled.');

  document.getElementById('markets').innerHTML=Object.entries(s.symbols).map(([k,v])=>
   '<tr><td>'+esc(k)+'</td><td>'+Number(v.price).toPrecision(8)+'</td><td>'+esc(v.status)+'</td><td><input type="checkbox" '+(v.enabled?'checked':'')+' onchange="sym(\''+k+'\',this.checked)"></td><td><button onclick="buy(\''+k+'\')">BUY</button></td></tr>'
  ).join('');

  document.getElementById('positions').innerHTML=d.positions.map(p=>
   '<tr><td>'+esc(p.symbol)+'</td><td>'+esc(p.qty)+'</td><td>'+esc(p.entry)+'</td><td>'+esc(p.spent)+'</td><td>'+esc(p.mode)+'</td><td><button onclick="sell(\''+p.symbol+'\')">SELL/CLOSE</button></td></tr>'
  ).join('')||'<tr><td colspan="6">No open positions</td></tr>';

  document.getElementById('history').innerHTML=d.history.map(h=>
   '<tr><td>'+esc(h.time)+'</td><td>'+esc(h.symbol)+'</td><td>'+esc(h.side)+'</td><td>'+esc(h.price)+'</td><td>'+esc(h.qty)+'</td><td>'+esc(h.pnl)+'</td><td>'+esc(h.reason)+'</td><td>'+esc(h.mode)+'</td></tr>'
  ).join('')||'<tr><td colspan="8">No trade history</td></tr>';
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
    mode = current_mode()
    with lock:
        snapshot = {k: v for k, v in state.items() if k != "symbols"}
        snapshot["symbols"] = {k: dict(v) for k, v in state["symbols"].items()}

    pos = positions(mode)
    trades = history(mode)
    demo_wallet = wallet("DEMO")
    live_wallet = wallet("LIVE")

    unrealized = 0.0
    for p in pos:
        try:
            price = float(snapshot["symbols"][p["symbol"]]["price"])
            unrealized += (price - float(p["entry"])) * float(p["qty"])
        except Exception:
            pass

    live_error = live_balance_cache["error"]
    if mode == "LIVE" and state["live_armed"]:
        try:
            live_account_balance()
            live_error = ""
        except Exception as e:
            live_error = str(e)[:250]
            live_balance_cache["error"] = live_error

    return jsonify({
        "state": snapshot,
        "positions": pos,
        "history": trades,
        "demo_wallet": demo_wallet,
        "live_wallet": live_wallet,
        "live_usdt_balance": live_balance_cache["USDT"],
        "live_balance_error": live_error if mode == "LIVE" else "",
        "unrealized_pnl": unrealized
    })


@APP.post("/api/mode")
def api_mode():
    data = request.get_json(silent=True) or {}
    mode = str(data.get("mode", "")).upper()
    if mode not in ("DEMO", "LIVE"):
        return jsonify({"ok": False, "error": "Mode DEMO অথবা LIVE হতে হবে"}), 400
    with lock:
        state["mode"] = mode
    return jsonify({"ok": True, "mode": mode, "live_armed": state["live_armed"]})


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
                trade_usdt=amount, sl=sl, tp=tp, maxpos=maxpos, poll=poll,
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
            state["live_armed"] = False
        telegram("LIVE trading disabled. New live orders are blocked.")
        return jsonify({"ok": True, "live_armed": False, "message": "LIVE trading permission বন্ধ"})
    try:
        if not bn.key or not bn.secret:
            raise RuntimeError("Binance API Key/Secret সেট করা নেই")
        account = bn.account()
        if not account.get("canTrade", False):
            raise RuntimeError("Binance API account trading অনুমোদন করছে না")
        bn.load_filters()
        live_account_balance()
        with lock:
            state["live_armed"] = True
            state["error"] = ""
        telegram("LIVE TRADING ENABLED. Real orders may use real funds.")
        return jsonify({"ok": True, "live_armed": True, "message": "LIVE trading enabled"})
    except Exception as e:
        with lock:
            state["live_armed"] = False
            state["error"] = str(e)[:300]
        return jsonify({"ok": False, "live_armed": False, "error": str(e)}), 400


@APP.post("/api/start")
def api_start():
    with lock:
        state["running"] = True
    telegram("Bot started in " + current_mode() + " mode.")
    return jsonify({"ok": True, "running": True})


@APP.post("/api/stop")
def api_stop():
    with lock:
        state["running"] = False
    telegram("Bot stopped. Automatic strategy paused.")
    return jsonify({"ok": True, "running": False})


@APP.post("/api/buy")
def api_buy():
    data = request.get_json(silent=True) or {}
    try:
        result = buy_symbol(data.get("symbol"), "Manual buy")
        return jsonify({"ok": True, "result": result})
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
        result = sell_position(symbol, bn.price(symbol), "Manual sell", current_mode())
        if not result.get("ok", False):
            return jsonify(result), 400
        return jsonify(result)
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
        account = bn.account()
        usdt = next((
            float(b["free"]) + float(b["locked"])
            for b in account.get("balances", []) if b["asset"] == "USDT"
        ), 0.0)
        return jsonify({
            "ok": True,
            "canTrade": account.get("canTrade"),
            "canWithdraw": account.get("canWithdraw"),
            "usdt_balance": usdt
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
    print("DEMO mode selected. LIVE trading permission OFF.")
    APP.run(host=HOST, port=PORT, debug=False, use_reloader=False)
