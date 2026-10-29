#!/usr/bin/env python3
# Crypto Bot Pro — Binance Spot + Telegram
# DEMO and LIVE have separate pages, positions, histories and wallet views.

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
from flask import Flask, jsonify, request, render_template_string, redirect

APP = Flask(__name__)

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "").strip()
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "").strip()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

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

demo_wallet = {
    "initial": 1000.0,
    "cash": 1000.0,
    "realized": 0.0
}


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def db():
    c = sqlite3.connect(
        DB,
        timeout=20,
        check_same_thread=False
    )
    c.row_factory = sqlite3.Row
    return c


def init_db():
    with db() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS positions(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                qty TEXT NOT NULL,
                entry TEXT NOT NULL,
                spent TEXT NOT NULL,
                opened TEXT NOT NULL,
                mode TEXT NOT NULL,
                order_id TEXT,
                stop TEXT,
                take TEXT,
                UNIQUE(symbol, mode)
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS trades(
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

        cols = [
            r["name"]
            for r in c.execute(
                "PRAGMA table_info(positions)"
            ).fetchall()
        ]

        if "id" not in cols:
            c.execute("ALTER TABLE positions RENAME TO positions_old")

            c.execute("""
                CREATE TABLE positions(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    qty TEXT NOT NULL,
                    entry TEXT NOT NULL,
                    spent TEXT NOT NULL,
                    opened TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    order_id TEXT,
                    stop TEXT,
                    take TEXT,
                    UNIQUE(symbol, mode)
                )
            """)

            old = [
                r["name"]
                for r in c.execute(
                    "PRAGMA table_info(positions_old)"
                ).fetchall()
            ]

            names = [
                x for x in [
                    "symbol", "qty", "entry", "spent",
                    "opened", "mode", "order_id", "stop", "take"
                ]
                if x in old
            ]

            if names:
                cols_sql = ",".join(names)
                c.execute(
                    f"""
                    INSERT OR IGNORE INTO positions({cols_sql})
                    SELECT {cols_sql} FROM positions_old
                    """
                )

            c.execute("DROP TABLE positions_old")


def positions(mode=None):
    with db() as c:
        q = "SELECT * FROM positions"
        args = ()

        if mode:
            q += " WHERE mode=?"
            args = (mode,)

        return [
            dict(r)
            for r in c.execute(
                q + " ORDER BY opened DESC",
                args
            ).fetchall()
        ]


def history(mode=None):
    with db() as c:
        q = "SELECT * FROM trades"
        args = ()

        if mode:
            q += " WHERE mode=?"
            args = (mode,)

        return [
            dict(r)
            for r in c.execute(
                q + " ORDER BY id DESC LIMIT 100",
                args
            ).fetchall()
        ]


def get_position(symbol, mode):
    with db() as c:
        r = c.execute(
            "SELECT * FROM positions WHERE symbol=? AND mode=?",
            (symbol, mode)
        ).fetchone()

        return dict(r) if r else None


def save_position(symbol, qty, entry, spent, mode, order_id=""):
    with db() as c:
        c.execute("""
            INSERT INTO positions(
                symbol, qty, entry, spent, opened,
                mode, order_id, stop, take
            )
            VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(symbol,mode) DO UPDATE SET
                qty=excluded.qty,
                entry=excluded.entry,
                spent=excluded.spent,
                opened=excluded.opened,
                order_id=excluded.order_id,
                stop=excluded.stop,
                take=excluded.take
        """, (
            symbol,
            str(qty),
            str(entry),
            str(spent),
            utcnow(),
            mode,
            str(order_id),
            str(float(entry) * (1 - state["sl"] / 100)),
            str(float(entry) * (1 + state["tp"] / 100))
        ))


def remove_position(symbol, mode):
    with db() as c:
        c.execute(
            "DELETE FROM positions WHERE symbol=? AND mode=?",
            (symbol, mode)
        )


def log_trade(
    symbol, side, price, qty, quote,
    pnl, reason, mode, order_id=""
):
    with db() as c:
        c.execute("""
            INSERT INTO trades(
                time, symbol, side, price, qty,
                quote, pnl, reason, mode, order_id
            )
            VALUES(?,?,?,?,?,?,?,?,?,?)
        """, (
            utcnow(), symbol, side, str(price),
            str(qty), str(quote), str(pnl),
            reason, mode, str(order_id)
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
        self.key = BINANCE_API_KEY
        self.secret = BINANCE_API_SECRET
        self.session = requests.Session()

        if self.key:
            self.session.headers.update({
                "X-MBX-APIKEY": self.key
            })

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
            raise RuntimeError(
                "BINANCE_API_KEY এবং BINANCE_API_SECRET সেট করা নেই"
            )

        p = dict(params or {})
        p["timestamp"] = int(time.time() * 1000)
        p["recvWindow"] = 5000

        query = urlencode(p)

        sig = hmac.new(
            self.secret.encode(),
            query.encode(),
            hashlib.sha256
        ).hexdigest()

        r = self.session.request(
            method,
            BASE + path + "?" + query + "&signature=" + sig,
            timeout=20
        )

        if not r.ok:
            try:
                msg = r.json().get("msg", r.text)
            except Exception:
                msg = r.text

            raise RuntimeError(
                "Binance API error: " + str(msg)[:250]
            )

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
            if item["symbol"] in SYMBOLS:
                fs = {
                    f["filterType"]: f
                    for f in item["filters"]
                }

                lot = fs.get("LOT_SIZE", {})
                no = (
                    fs.get("NOTIONAL")
                    or fs.get("MIN_NOTIONAL", {})
                )

                exchange_filters[item["symbol"]] = {
                    "step": lot.get("stepSize", "0.000001"),
                    "min_qty": lot.get("minQty", "0"),
                    "min_notional": no.get("minNotional", "5")
                }

    def market_buy(self, symbol, usdt):
        return self.signed(
            "POST",
            "/api/v3/order",
            {
                "symbol": symbol,
                "side": "BUY",
                "type": "MARKET",
                "quoteOrderQty": f"{float(usdt):.2f}",
                "newOrderRespType": "FULL"
            }
        )

    def market_sell(self, symbol, qty):
        if symbol not in exchange_filters:
            self.load_filters()

        f = exchange_filters[symbol]
        step = Decimal(f["step"])
        amount = Decimal(str(qty))

        if step > 0:
            amount = (
                (amount / step).to_integral_value(
                    rounding=ROUND_DOWN
                ) * step
            )

        if amount <= 0 or amount < Decimal(f["min_qty"]):
            raise RuntimeError(
                "বিক্রির quantity Binance minimum/step অনুযায়ী বৈধ নয়"
            )

        return self.signed(
            "POST",
            "/api/v3/order",
            {
                "symbol": symbol,
                "side": "SELL",
                "type": "MARKET",
                "quantity": format(amount, "f"),
                "newOrderRespType": "FULL"
            }
        )


bn = Binance()


def order_average(order):
    fills = order.get("fills") or []

    qty = sum(
        float(f.get("qty", 0))
        for f in fills
    )

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


def active_mode():
    return "LIVE" if state["live"] else "DEMO"


def paper_buy(symbol, price, usdt, reason):
    if demo_wallet["cash"] < usdt:
        raise RuntimeError(
            "DEMO wallet-এ পর্যাপ্ত USDT নেই"
        )

    qty = usdt / price

    save_position(
        symbol, qty, price, usdt, "DEMO"
    )

    demo_wallet["cash"] -= usdt

    log_trade(
        symbol, "BUY", price, qty,
        usdt, 0, reason, "DEMO"
    )

    telegram(
        f"🟢 DEMO BUY\n{symbol}\n"
        f"Price: {price}\nAmount: {usdt:.2f} USDT"
    )

    return {
        "mode": "DEMO",
        "qty": qty,
        "price": price
    }


def live_buy(symbol, usdt, reason):
    if not state["live"]:
        raise RuntimeError("LIVE mode চালু নেই")

    if symbol not in exchange_filters:
        bn.load_filters()

    f = exchange_filters[symbol]

    if usdt < float(f["min_notional"]):
        raise RuntimeError(
            f"{symbol} minimum order প্রায় "
            f"{f['min_notional']} USDT"
        )

    order = bn.market_buy(symbol, usdt)
    avg, qty, quote = order_average(order)

    order_id = order.get("orderId", "")

    save_position(
        symbol, qty, avg, quote, "LIVE", order_id
    )

    log_trade(
        symbol, "BUY", avg, qty, quote,
        0, reason, "LIVE", order_id
    )

    telegram(
        f"🔴 LIVE BUY FILLED\n{symbol}\n"
        f"Avg: {avg}\nQty: {qty}\nSpent: {quote:.4f} USDT"
    )

    return {
        "mode": "LIVE",
        "qty": qty,
        "price": avg,
        "quote": quote
    }


def buy_symbol(symbol, reason="Manual buy", mode=None):
    if symbol not in SYMBOLS:
        raise ValueError("অজানা symbol")

    mode = (mode or active_mode()).upper()

    if mode not in ("DEMO", "LIVE"):
        raise ValueError("Invalid mode")

    with trade_lock:
        if get_position(symbol, mode):
            raise RuntimeError(
                f"{mode} position-এ {symbol} আগে থেকেই আছে"
            )

        if len(positions(mode)) >= int(state["maxpos"]):
            raise RuntimeError(
                f"{mode} সর্বোচ্চ open position limit পূর্ণ"
            )

        if (
            reason != "Manual buy"
            and not state["symbols"][symbol]["enabled"]
        ):
            raise RuntimeError(f"{symbol} disabled")

        price = bn.price(symbol)
        usdt = float(state["trade_usdt"])

        if usdt < 5:
            raise RuntimeError(
                "Trade size অন্তত 5 USDT হতে হবে"
            )

        if mode == "LIVE":
            return live_buy(symbol, usdt, reason)

        return paper_buy(symbol, price, usdt, reason)


def sell_position(symbol, price, reason, mode):
    with trade_lock:
        pos = get_position(symbol, mode)

        if not pos:
            return {
                "ok": False,
                "error": f"{mode} position পাওয়া যায়নি"
            }

        entry = float(pos["entry"])
        qty = float(pos["qty"])

        if mode == "LIVE":
            if not state["live"]:
                return {
                    "ok": False,
                    "error": "LIVE বন্ধ; বিক্রির আগে LIVE চালু করো"
                }

            account = bn.account()
            asset = symbol[:-4]

            free = next(
                (
                    float(b["free"])
                    for b in account.get("balances", [])
                    if b["asset"] == asset
                ),
                0.0
            )

            qty = min(qty, free)

            if qty <= 0:
                raise RuntimeError(
                    "বিক্রির জন্য free balance পাওয়া যায়নি"
                )

            order = bn.market_sell(symbol, qty)

            avg, sold_qty, quote = order_average(order)
            order_id = order.get("orderId", "")

        else:
            avg = price
            sold_qty = qty
            quote = qty * price
            order_id = ""
            demo_wallet["cash"] += quote

        pnl = (avg - entry) * sold_qty

        if mode == "DEMO":
            demo_wallet["realized"] += pnl

        log_trade(
            symbol, "SELL", avg, sold_qty, quote,
            pnl, reason, mode, order_id
        )

        remove_position(symbol, mode)

        telegram(
            f"🔻 {mode} SELL\n{symbol}\n"
            f"Price: {avg}\nP/L before fees: {pnl:.4f} USDT\n"
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


def wallet_snapshot(mode):
    if mode == "DEMO":
        value = 0.0
        unreal = 0.0

        for p in positions("DEMO"):
            price = (
                state["symbols"][p["symbol"]]["price"]
                or float(p["entry"])
            )

            qty = float(p["qty"])
            value += qty * price
            unreal += (
                price - float(p["entry"])
            ) * qty

        return {
            "mode": "DEMO",
            "cash": round(demo_wallet["cash"], 6),
            "position_value": round(value, 6),
            "equity": round(demo_wallet["cash"] + value, 6),
            "realized_pnl": round(demo_wallet["realized"], 6),
            "unrealized_pnl": round(unreal, 6),
            "initial": demo_wallet["initial"]
        }

    try:
        a = bn.account()

        balances = {
            b["asset"]: float(b["free"]) + float(b["locked"])
            for b in a.get("balances", [])
        }

        cash = balances.get("USDT", 0)
        value = 0

        for sym in SYMBOLS:
            amount = balances.get(sym[:-4], 0)

            if amount:
                value += amount * (
                    state["symbols"][sym]["price"]
                    or bn.price(sym)
                )

        return {
            "mode": "LIVE",
            "cash": round(cash, 6),
            "position_value": round(value, 6),
            "equity": round(cash + value, 6),
            "realized_pnl": None,
            "unrealized_pnl": None,
            "initial": None
        }

    except Exception as e:
        return {
            "mode": "LIVE",
            "cash": None,
            "position_value": None,
            "equity": None,
            "realized_pnl": None,
            "unrealized_pnl": None,
            "initial": None,
            "error": str(e)[:250]
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

            for mode in ("DEMO", "LIVE"):
                pos = get_position(symbol, mode)

                if pos:
                    pct = (
                        (price - float(pos["entry"]))
                        / float(pos["entry"]) * 100
                    )

                    if pct >= float(state["tp"]):
                        if mode == "DEMO" or state["live"]:
                            sell_position(
                                symbol, price, "Take Profit", mode
                            )

                    elif pct <= -float(state["sl"]):
                        if state["no_loss"]:
                            state["symbols"][symbol]["status"] = (
                                "NO-LOSS HOLD: loss threshold reached"
                            )
                        elif mode == "DEMO" or state["live"]:
                            sell_position(
                                symbol, price, "Stop Loss", mode
                            )

            if (
                state["running"]
                and state["strategy"]
                and state["symbols"][symbol]["enabled"]
                and previous
                and previous > 0
            ):
                change = (price - previous) / previous * 100

                if (
                    change >= 0.15
                    and time.time() - last_buy_time.get(symbol, 0) > 300
                ):
                    buy_symbol(
                        symbol,
                        "Momentum signal",
                        active_mode()
                    )
                    last_buy_time[symbol] = time.time()

            if (
                not get_position(symbol, "DEMO")
                and not get_position(symbol, "LIVE")
            ):
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
        try:
            if state["running"]:
                tick()

        except Exception:
            log.exception("Worker error")

        time.sleep(max(5, int(state["poll"])))


HTML = r"""<!doctype html>
<html lang="bn">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Crypto Bot Pro — {{mode}}</title>
<style>
:root{color-scheme:dark;--bg:#070b14;--panel:#111827;--line:#263247;--text:#e8eefc;--muted:#8fa2bf;--cyan:#22d3ee;--green:#22c55e;--red:#fb7185}
*{box-sizing:border-box}
body{margin:0;background:radial-gradient(ellipse at top,#12223c,var(--bg) 55%);color:var(--text);font:14px system-ui,Arial}
header{padding:20px;display:flex;gap:12px;align-items:center;justify-content:space-between;flex-wrap:wrap;border-bottom:1px solid var(--line)}
h1{font-size:22px;margin:0}
h2{font-size:16px;margin:0 0 14px}
.muted{color:var(--muted)}
nav{display:flex;gap:8px}
nav a{color:var(--text);text-decoration:none;padding:9px 14px;border:1px solid var(--line);border-radius:10px}
nav a.active{background:#164e63;border-color:var(--cyan)}
main{max-width:1200px;margin:auto;padding:16px;display:grid;gap:14px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px}
.panel,.metric{background:linear-gradient(145deg,#121c2e,#0d1422);border:1px solid var(--line);border-radius:14px;padding:16px;min-width:0}
.metric small{display:block;color:var(--muted);margin-bottom:8px}
.metric strong{font-size:22px;overflow-wrap:anywhere}
.tag{border:1px solid var(--line);border-radius:999px;padding:7px 12px}
button,input{font:inherit;color:var(--text);background:#18243a;border:1px solid #34445f;border-radius:8px;padding:9px}
button{cursor:pointer}
button:hover{border-color:var(--cyan)}
button.primary{background:#0e7490}
button.danger{background:#7f1d1d}
.controls{display:flex;gap:8px;flex-wrap:wrap}
.settings{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:10px}
.settings label{display:grid;gap:6px;color:var(--muted)}
.settings input{width:100%}
.tablewrap{overflow:auto}
table{width:100%;border-collapse:collapse;min-width:620px}
th,td{text-align:left;padding:10px;border-bottom:1px solid var(--line);white-space:nowrap}
th{color:var(--muted)}
td button{padding:6px 8px}
.check{width:18px;height:18px;accent-color:var(--cyan)}
#notice{padding:10px;border-radius:8px;background:#17243a;display:none;overflow-wrap:anywhere}
@media(max-width:600px){header{align-items:flex-start}h1{font-size:19px}main{padding:10px}}
</style>
</head>
<body>
<header>
<div>
<h1>⚡ Crypto Bot Pro</h1>
<div class="muted">Binance Spot · Telegram · পৃথক {{mode}} Dashboard</div>
</div>
<nav>
<a href="/demo" class="{{'active' if mode=='DEMO' else ''}}">DEMO Page</a>
<a href="/live" class="{{'active' if mode=='LIVE' else ''}}">LIVE Page</a>
</nav>
<div class="tag">{{mode}} MODE</div>
</header>

<main>
<div id="notice"></div>

<section class="grid">
<div class="metric"><small>Bot Status</small><strong id="botstatus">Loading…</strong></div>
<div class="metric"><small>{{mode}} Wallet / Equity (USDT)</small><strong id="equity">—</strong></div>
<div class="metric"><small>Available USDT</small><strong id="cash">—</strong></div>
<div class="metric"><small>Open Position Value</small><strong id="posvalue">—</strong></div>
<div class="metric"><small>Realized P/L</small><strong id="realized">—</strong></div>
<div class="metric"><small>Unrealized P/L</small><strong id="unrealized">—</strong></div>
</section>

<section class="panel">
<h2>Bot Controls</h2>
<div class="controls">
<button class="primary" onclick="post('/api/start',{})">Start Bot</button>
<button class="danger" onclick="post('/api/stop',{})">Stop Bot</button>

{% if mode=='LIVE' %}
<button class="danger" onclick="toggleLive(false)">Disable LIVE</button>
<button class="primary" onclick="toggleLive(true)">Enable LIVE</button>
<button onclick="post('/api/check-api',{}).then(x=>show(JSON.stringify(x)))">Check Binance API</button>
{% else %}
<span class="muted">DEMO page real order পাঠায় না।</span>
{% endif %}

<button onclick="post('/api/test-telegram',{}).then(x=>show(x.message||x.error))">Test Telegram</button>
</div>

<p class="muted">LIVE পেজে আসল অর্ডার হতে পারে। API key-তে withdrawal permission বন্ধ রাখুন।</p>
</section>

<section class="panel">
<h2>Settings</h2>

<div class="settings">
<label>Trade USDT<input id="trade_usdt" type="number" min="5" max="100000" step="1"></label>
<label>Stop Loss %<input id="sl" type="number" min=".1" max="50" step=".1"></label>
<label>Take Profit %<input id="tp" type="number" min=".1" max="100" step=".1"></label>
<label>Max positions<input id="maxpos" type="number" min="1" max="8"></label>
<label>Poll seconds<input id="poll" type="number" min="5" max="300"></label>
</div>

<p>
<label><input id="strategy" class="check" type="checkbox"> Automatic strategy</label>
&nbsp;
<label><input id="no_loss" class="check" type="checkbox"> No-Loss Hold</label>
</p>

<button class="primary" onclick="saveSettings()">Save settings</button>

<p class="muted">No-Loss Hold চালু থাকলে Stop Loss সীমায় পৌঁছালেও position ধরে রাখে।</p>
</section>

<section class="panel">
<h2>Markets</h2>
<div class="tablewrap">
<table>
<thead>
<tr><th>Symbol</th><th>Price (USDT)</th><th>Status</th><th>Enabled</th><th>Action</th></tr>
</thead>
<tbody id="markets"></tbody>
</table>
</div>
</section>

<section class="panel">
<h2>{{mode}} Open Positions</h2>
<div class="tablewrap">
<table>
<thead>
<tr><th>Symbol</th><th>Qty</th><th>Entry</th><th>Spent</th><th>Mode</th><th>Action</th></tr>
</thead>
<tbody id="positions"></tbody>
</table>
</div>
</section>

<section class="panel">
<h2>{{mode}} Trade History</h2>
<div class="tablewrap">
<table>
<thead>
<tr><th>Time</th><th>Symbol</th><th>Side</th><th>Price</th><th>P/L</th><th>Reason</th><th>Mode</th></tr>
</thead>
<tbody id="history"></tbody>
</table>
</div>
</section>
</main>

<script>
const MODE={{mode|tojson}},$=id=>document.getElementById(id);

function esc(v){
    return String(v??'').replace(/[&<>"']/g,c=>({
        '&':'&amp;',
        '<':'&lt;',
        '>':'&gt;',
        '"':'&quot;',
        "'":'&#39;'
    }[c]));
}

function num(v){
    return v===null||v===undefined
        ?'—'
        :Number(v).toLocaleString(undefined,{maximumFractionDigits:6});
}

function show(m){
    $('notice').style.display='block';
    $('notice').textContent=m;
    setTimeout(()=>$('notice').style.display='none',6000);
}

async function post(url,data){
    try{
        let r=await fetch(url,{
            method:'POST',
            headers:{'Content-Type':'application/json'},
            body:JSON.stringify(data)
        });

        let j=await r.json();

        if(!r.ok||j.ok===false)
            throw Error(j.error||j.message||'Request failed');

        show(j.message||'সফল');
        await refresh();
        return j;

    }catch(e){
        show(e.message);
        return {ok:false,error:e.message};
    }
}

async function toggleLive(enabled){
    if(confirm(enabled
        ?'LIVE চালু করলে আসল অর্ডার হতে পারে। নিশ্চিত?'
        :'LIVE বন্ধ করবেন?'
    )){
        await post('/api/live',{enabled});
    }
}

async function saveSettings(){
    await post('/api/settings',{
        trade_usdt:Number($('trade_usdt').value),
        sl:Number($('sl').value),
        tp:Number($('tp').value),
        maxpos:Number($('maxpos').value),
        poll:Number($('poll').value),
        strategy:$('strategy').checked,
        no_loss:$('no_loss').checked
    });
}

async function refresh(){
    try{
        let j=await (
            await fetch('/api/state?mode='+MODE)
        ).json();

        let s=j.state;
        let w=j.wallet;

        $('botstatus').textContent=s.running?'RUNNING':'STOPPED';
        $('equity').textContent=num(w.equity);
        $('cash').textContent=num(w.cash);
        $('posvalue').textContent=num(w.position_value);
        $('realized').textContent=num(w.realized_pnl);
        $('unrealized').textContent=num(w.unrealized_pnl);

        if(w.error) show('Wallet: '+w.error);

        ['trade_usdt','sl','tp','maxpos','poll'].forEach(k=>{
            if(document.activeElement!==$(k))
                $(k).value=s[k];
        });

        $('strategy').checked=!!s.strategy;
        $('no_loss').checked=!!s.no_loss;

        $('markets').innerHTML=Object.entries(s.symbols).map(([sym,x])=>`
            <tr>
                <td>${esc(sym)}</td>
                <td>${num(x.price)}</td>
                <td>${esc(x.status)}</td>
                <td>
                    <input class="check" type="checkbox"
                        ${x.enabled?'checked':''}
                        onchange="post('/api/symbol',{
                            symbol:'${sym}',enabled:this.checked
                        })">
                </td>
                <td>
                    <button onclick="post('/api/buy',{
                        symbol:'${sym}',mode:MODE
                    })">Buy</button>
                </td>
            </tr>
        `).join('');

        $('positions').innerHTML=j.positions.length
            ?j.positions.map(p=>`
                <tr>
                    <td>${esc(p.symbol)}</td>
                    <td>${num(p.qty)}</td>
                    <td>${num(p.entry)}</td>
                    <td>${num(p.spent)}</td>
                    <td>${esc(p.mode)}</td>
                    <td>
                        <button class="danger" onclick="post('/api/sell',{
                            symbol:'${esc(p.symbol)}',mode:MODE
                        })">Sell / Close</button>
                    </td>
                </tr>
            `).join('')
            :'<tr><td colspan="6">No open positions</td></tr>';

        $('history').innerHTML=j.history.length
            ?j.history.map(t=>`
                <tr>
                    <td>${esc(t.time)}</td>
                    <td>${esc(t.symbol)}</td>
                    <td>${esc(t.side)}</td>
                    <td>${num(t.price)}</td>
                    <td>${num(t.pnl)}</td>
                    <td>${esc(t.reason)}</td>
                    <td>${esc(t.mode)}</td>
                </tr>
            `).join('')
            :'<tr><td colspan="7">No trade history</td></tr>';

    }catch(e){
        show(e.message);
    }
}

refresh();
setInterval(refresh,5000);
</script>
</body>
</html>"""


@APP.get("/")
def home():
    return redirect("/demo")


@APP.get("/demo")
def demo_page():
    return render_template_string(HTML, mode="DEMO")


@APP.get("/live")
def live_page():
    return render_template_string(HTML, mode="LIVE")


@APP.get("/api/state")
def api_state():
    mode = request.args.get("mode", "DEMO").upper()

    if mode not in ("DEMO", "LIVE"):
        return jsonify({
            "ok": False,
            "error": "Invalid mode"
        }), 400

    with lock:
        snapshot = {
            k: v for k, v in state.items()
            if k != "symbols"
        }

        snapshot["symbols"] = {
            k: dict(v)
            for k, v in state["symbols"].items()
        }

    return jsonify({
        "state": snapshot,
        "wallet": wallet_snapshot(mode),
        "positions": positions(mode),
        "history": history(mode)
    })


@APP.post("/api/settings")
def api_settings():
    d = request.get_json(silent=True) or {}

    try:
        amount = float(d.get("trade_usdt", state["trade_usdt"]))
        sl = float(d.get("sl", state["sl"]))
        tp = float(d.get("tp", state["tp"]))
        maxpos = int(d.get("maxpos", state["maxpos"]))
        poll = int(d.get("poll", state["poll"]))

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
                no_loss=bool(
                    d.get("no_loss", state["no_loss"])
                ),
                strategy=bool(
                    d.get("strategy", state["strategy"])
                )
            )

        return jsonify({
            "ok": True,
            "message": "Settings saved"
        })

    except Exception as e:
        return jsonify({
            "ok": False,
            "error": str(e)
        }), 400


@APP.post("/api/symbol")
def api_symbol():
    d = request.get_json(silent=True) or {}
    symbol = d.get("symbol")

    if symbol not in SYMBOLS:
        return jsonify({
            "ok": False,
            "error": "Invalid symbol"
        }), 400

    with lock:
        state["symbols"][symbol]["enabled"] = bool(
            d.get("enabled")
        )

    return jsonify({"ok": True})


@APP.post("/api/live")
def api_live():
    enabled = bool(
        (request.get_json(silent=True) or {}).get("enabled")
    )

    if not enabled:
        with lock:
            state["live"] = False

        telegram(
            "LIVE mode disabled. New live orders are blocked."
        )

        return jsonify({
            "ok": True,
            "live": False,
            "message": "LIVE mode বন্ধ"
        })

    try:
        if not bn.key or not bn.secret:
            raise RuntimeError(
                "BINANCE_API_KEY এবং BINANCE_API_SECRET "
                "environment variable-এ সেট করো"
            )

        a = bn.account()

        if not a.get("canTrade", False):
            raise RuntimeError(
                "Binance API account trading অনুমোদন করছে না"
            )

        bn.load_filters()

        with lock:
            state["live"] = True
            state["error"] = ""

        telegram(
            "LIVE TRADING ENABLED. "
            "Real orders may use real funds."
        )

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
        "Bot started. Automatic strategy uses "
        + active_mode() + " mode."
    )

    return jsonify({
        "ok": True,
        "message": "Bot started"
    })


@APP.post("/api/stop")
def api_stop():
    with lock:
        state["running"] = False

    telegram("Bot stopped. Automatic strategy paused.")

    return jsonify({
        "ok": True,
        "message": "Bot stopped"
    })


@APP.post("/api/buy")
def api_buy():
    d = request.get_json(silent=True) or {}

    try:
        result = buy_symbol(
            d.get("symbol"),
            "Manual buy",
            str(d.get("mode", active_mode())).upper()
        )

        return jsonify({
            "ok": True,
            "result": result,
            "message": "Buy completed"
        })

    except Exception as e:
        log.exception("Buy failed")

        return jsonify({
            "ok": False,
            "error": str(e)
        }), 400


@APP.post("/api/sell")
def api_sell():
    d = request.get_json(silent=True) or {}

    symbol = d.get("symbol")
    mode = str(
        d.get("mode", active_mode())
    ).upper()

    if symbol not in SYMBOLS or mode not in ("DEMO", "LIVE"):
        return jsonify({
            "ok": False,
            "error": "Invalid symbol/mode"
        }), 400

    try:
        result = sell_position(
            symbol,
            bn.price(symbol),
            "Manual sell",
            mode
        )

        return (
            jsonify(result), 200
        ) if result.get("ok") else (
            jsonify(result), 400
        )

    except Exception as e:
        log.exception("Sell failed")

        return jsonify({
            "ok": False,
            "error": str(e)
        }), 400


@APP.post("/api/test-telegram")
def api_test_telegram():
    ok = telegram("Crypto Bot Pro Telegram test")

    return jsonify({
        "ok": ok,
        "message": (
            "Message sent"
            if ok
            else "Telegram credentials যাচাই করো"
        )
    }), (200 if ok else 400)


@APP.post("/api/check-api")
def api_check_api():
    try:
        a = bn.account()

        return jsonify({
            "ok": True,
            "canTrade": a.get("canTrade"),
            "canWithdraw": a.get("canWithdraw")
        })

    except Exception as e:
        return jsonify({
            "ok": False,
            "error": str(e)
        }), 400


if __name__ == "__main__":
    init_db()

    try:
        bn.load_filters()
    except Exception as e:
        log.warning(
            "Exchange filters unavailable at startup: %s",
            e
        )

    threading.Thread(
        target=worker,
        daemon=True
    ).start()

    print(
        f"Crypto Bot Pro: http://{HOST}:{PORT} | /demo | /live"
    )
    print("LIVE mode OFF at startup.")

    APP.run(
        host=HOST,
        port=PORT,
        debug=False,
        use_reloader=False
    )
