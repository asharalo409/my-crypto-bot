#!/usr/bin/env python3
import os, json, time, hmac, hashlib, sqlite3, threading, urllib.parse, urllib.request, urllib.error
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", "8080"))
DB = os.environ.get("BOT_DB_FILE", "trading_history.db")
BASE = "https://api.binance.com"
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT", "AVAXUSDT"]

try:
    import config as cfg
except Exception:
    cfg = object()

def cred(env, names):
    value = os.getenv(env, "")
    if value:
        return value
    for name in names:
        value = getattr(cfg, name, "")
        if value:
            return str(value)
    return ""

API_KEY = cred("BINANCE_API_KEY", ["BINANCE_API_KEY", "API_KEY", "BINANCE_KEY"])
API_SECRET = cred("BINANCE_API_SECRET", ["BINANCE_API_SECRET", "API_SECRET", "BINANCE_SECRET"])
TG_TOKEN = cred("TELEGRAM_BOT_TOKEN", ["TELEGRAM_BOT_TOKEN", "TELEGRAM_TOKEN", "BOT_TOKEN"])
TG_CHAT = cred("TELEGRAM_CHAT_ID", ["TELEGRAM_CHAT_ID", "CHAT_ID"])

lock = threading.RLock()
state = {
    "live_mode": False,
    "logs": [],
    "markets": {
        s: {
            "running": False,
            "price": 0,
            "amount": 10.0,
            "stop_loss": 50.0,
            "take_profit": 0.5,
            "no_loss_mode": True
        } for s in SYMBOLS
    }
}

def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def conn():
    c = sqlite3.connect(DB, timeout=20)
    c.row_factory = sqlite3.Row
    return c

def init_db():
    with conn() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS wallets(
                mode TEXT PRIMARY KEY,
                balance REAL NOT NULL,
                pnl REAL NOT NULL DEFAULT 0,
                buys INTEGER NOT NULL DEFAULT 0,
                sells INTEGER NOT NULL DEFAULT 0,
                wins INTEGER NOT NULL DEFAULT 0
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS positions(
                mode TEXT,
                symbol TEXT,
                qty REAL,
                cost REAL,
                entry REAL,
                opened TEXT,
                PRIMARY KEY(mode, symbol)
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS trades(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mode TEXT,
                time TEXT,
                symbol TEXT,
                side TEXT,
                price REAL,
                qty REAL,
                quote REAL,
                pnl REAL,
                reason TEXT
            )
        """)
        c.execute("INSERT OR IGNORE INTO wallets VALUES('DEMO',10000,0,0,0,0)")
        c.execute("INSERT OR IGNORE INTO wallets VALUES('LIVE',0,0,0,0,0)")

init_db()

def mode():
    return "LIVE" if state["live_mode"] else "DEMO"

def log(message):
    state["logs"].insert(0, f"[{time.strftime('%H:%M:%S')}] {message}")
    del state["logs"][25:]

def wallet(m=None):
    with conn() as c:
        return dict(c.execute(
            "SELECT * FROM wallets WHERE mode=?", (m or mode(),)
        ).fetchone())

def wallet_update(m, **updates):
    allowed = {"balance", "pnl", "buys", "sells", "wins"}
    keys = [k for k in updates if k in allowed]
    if keys:
        with conn() as c:
            c.execute(
                "UPDATE wallets SET " +
                ",".join(k + "=?" for k in keys) +
                " WHERE mode=?",
                [updates[k] for k in keys] + [m]
            )

def position(m, symbol):
    with conn() as c:
        row = c.execute(
            "SELECT * FROM positions WHERE mode=? AND symbol=?",
            (m, symbol)
        ).fetchone()
        return dict(row) if row else None

def positions(m=None):
    with conn() as c:
        return [dict(row) for row in c.execute(
            "SELECT * FROM positions WHERE mode=? ORDER BY opened DESC",
            (m or mode(),)
        )]

def history(m=None):
    with conn() as c:
        return [dict(row) for row in c.execute(
            "SELECT * FROM trades WHERE mode=? ORDER BY id DESC LIMIT 30",
            (m or mode(),)
        )]

def trade(m, symbol, side, price, qty, quote, pnl, reason):
    with conn() as c:
        c.execute("""
            INSERT INTO trades(mode,time,symbol,side,price,qty,quote,pnl,reason)
            VALUES(?,?,?,?,?,?,?,?,?)
        """, (m, now(), symbol, side, price, qty, quote, pnl, reason))

def telegram(message):
    if not TG_TOKEN or not TG_CHAT:
        return

    def send():
        try:
            data = urllib.parse.urlencode({
                "chat_id": TG_CHAT,
                "text": message
            }).encode()
            request = urllib.request.Request(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                data=data
            )
            urllib.request.urlopen(request, timeout=8).read()
        except Exception:
            pass

    threading.Thread(target=send, daemon=True).start()

def signed(path, method="GET", params=None):
    if not API_KEY or not API_SECRET:
        return None, "Binance API credentials config.py-তে পাওয়া যায়নি"

    p = dict(params or {})
    p["timestamp"] = int(time.time() * 1000)
    p["recvWindow"] = 10000
    query = urllib.parse.urlencode(p)
    signature = hmac.new(
        API_SECRET.encode(), query.encode(), hashlib.sha256
    ).hexdigest()

    request = urllib.request.Request(
        BASE + path + "?" + query + "&signature=" + signature,
        method=method,
        headers={"X-MBX-APIKEY": API_KEY}
    )

    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return json.loads(response.read().decode()), None
    except urllib.error.HTTPError as e:
        try:
            message = json.loads(e.read().decode()).get("msg", "Binance error")
        except Exception:
            message = f"HTTP {e.code}"
        return None, str(message)
    except Exception as e:
        return None, str(e)

def public(path, params=None):
    url = BASE + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=12) as response:
        return json.loads(response.read().decode())

def price(symbol):
    stored = float(state["markets"][symbol]["price"] or 0)
    if stored > 0:
        return stored
    return float(public(
        "/api/v3/ticker/price", {"symbol": symbol}
    )["price"])

def live_order(symbol, side, quote=None, qty=None):
    params = {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "newOrderRespType": "FULL"
    }
    if side == "BUY":
        params["quoteOrderQty"] = f"{quote:.2f}"
    else:
        params["quantity"] = f"{qty:.8f}".rstrip("0").rstrip(".")
    return signed("/api/v3/order", "POST", params)

def fill(order, fallback):
    fills = order.get("fills", [])
    qty = sum(float(x["qty"]) for x in fills)
    quote = sum(float(x["qty"]) * float(x["price"]) for x in fills)

    if qty and quote:
        return quote / qty, qty, quote

    qty = float(order.get("executedQty", 0))
    quote = float(order.get("cummulativeQuoteQty", 0))
    if qty and quote:
        return quote / qty, qty, quote

    raise RuntimeError("Binance order fill পাওয়া যায়নি")

def buy(symbol, price_now):
    if symbol not in SYMBOLS:
        raise ValueError("Invalid symbol")

    current_mode = mode()
    settings = state["markets"][symbol]
    amount = float(settings["amount"])

    if price_now <= 0:
        raise ValueError("দাম পাওয়া যায়নি")

    if position(current_mode, symbol):
        raise RuntimeError(
            f"{current_mode} mode-এ {symbol} position আগে থেকেই আছে"
        )

    if current_mode == "DEMO":
        w = wallet(current_mode)
        if w["balance"] < amount:
            raise RuntimeError("ডেমো ব্যালেন্স পর্যাপ্ত নয়")

        qty = amount / price_now
        quote = amount
        fill_price = price_now

        wallet_update(
            current_mode,
            balance=w["balance"] - quote,
            buys=w["buys"] + 1
        )
    else:
        order, error = live_order(symbol, "BUY", quote=amount)
        if error:
            raise RuntimeError(error)

        fill_price, qty, quote = fill(order, price_now)
        w = wallet(current_mode)
        wallet_update(current_mode, buys=w["buys"] + 1)

    with conn() as c:
        c.execute(
            "INSERT INTO positions VALUES(?,?,?,?,?,?)",
            (current_mode, symbol, qty, quote, fill_price, now())
        )

    trade(
        current_mode, symbol, "BUY",
        fill_price, qty, quote, 0, "Manual Buy"
    )
    log(f"{current_mode} BUY {symbol} @ {fill_price}")
    telegram(
        f"{current_mode} BUY {symbol} @ {fill_price}; "
        f"{quote:.2f} USDT"
    )

    return {
        "ok": True,
        "message": f"{current_mode} BUY সফল",
        "price": fill_price,
        "qty": qty,
        "quote": quote
    }

def sell(symbol, price_now):
    if symbol not in SYMBOLS:
        raise ValueError("Invalid symbol")

    current_mode = mode()
    pos = position(current_mode, symbol)

    if not pos:
        raise RuntimeError(
            f"{current_mode} mode-এ {symbol} position নেই"
        )

    qty = float(pos["qty"])
    cost = float(pos["cost"])

    if current_mode == "LIVE":
        account, error = signed("/api/v3/account")
        if error:
            raise RuntimeError(error)

        asset = symbol[:-4] if symbol.endswith("USDT") else symbol
        free = next(
            (
                float(x["free"])
                for x in account["balances"]
                if x["asset"] == asset
            ),
            0
        )
        qty = min(qty, free)

        if qty <= 0:
            raise RuntimeError("বিক্রির জন্য balance নেই")

        order, error = live_order(symbol, "SELL", qty=qty)
        if error:
            raise RuntimeError(error)

        fill_price, qty, quote = fill(order, price_now)
    else:
        fill_price = price_now
        quote = qty * fill_price

    pnl = quote - cost
    w = wallet(current_mode)

    wallet_update(
        current_mode,
        balance=w["balance"] + quote if current_mode == "DEMO" else w["balance"],
        pnl=w["pnl"] + pnl,
        sells=w["sells"] + 1,
        wins=w["wins"] + (1 if pnl > 0 else 0)
    )

    with conn() as c:
        c.execute(
            "DELETE FROM positions WHERE mode=? AND symbol=?",
            (current_mode, symbol)
        )

    trade(
        current_mode, symbol, "SELL",
        fill_price, qty, quote, pnl, "Manual Sell"
    )
    log(f"{current_mode} SELL {symbol}: P/L {pnl:+.4f}")
    telegram(
        f"{current_mode} SELL {symbol} @ {fill_price}; "
        f"P/L before fees {pnl:+.4f} USDT"
    )

    return {
        "ok": True,
        "message": f"{current_mode} SELL সফল; P/L {pnl:+.4f} USDT",
        "pnl": pnl
    }

def status():
    current_mode = mode()
    w = wallet(current_mode)
    live_balance = 0
    balance_error = ""

    if current_mode == "LIVE":
        account, error = signed("/api/v3/account")
        if error:
            balance_error = error
        else:
            live_balance = next(
                (
                    float(x["free"])
                    for x in account.get("balances", [])
                    if x["asset"] == "USDT"
                ),
                0
            )

    market_data = {}
    for symbol, settings in state["markets"].items():
        try:
            settings["price"] = price(symbol)
        except Exception:
            pass

        pos = position(current_mode, symbol)
        item = dict(settings)
        item["position"] = pos
        market_data[symbol] = item

    return {
        "mode": current_mode,
        "live_mode": state["live_mode"],
        "demo_balance": wallet("DEMO")["balance"],
        "live_balance": live_balance,
        "balance_error": balance_error,
        "total_buys": w["buys"],
        "total_sells": w["sells"],
        "wins": w["wins"],
        "realized_pnl": w["pnl"],
        "pnl_1h": w["pnl"],
        "pnl_24h": w["pnl"],
        "history": history(current_mode),
        "positions": positions(current_mode),
        "logs": state["logs"],
        "markets": market_data
    }

HTML = r"""<!doctype html><html lang="bn" data-theme="cyber"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>CYBERPUNK NEON TERMINAL</title>
<script src="https://cdn.jsdelivr.net/npm/lightweight-charts@4.1.1/dist/lightweight-charts.standalone.production.js"></script>
<style>
:root{--bg:#03000a;--panel:#0e051d;--cyan:#00f0ff;--pink:#ff0077;--green:#00ff66;--red:#ff0044;--text:#fff;--dim:#aaa}
[data-theme=white]{--bg:#f8fafc;--panel:#fff;--cyan:#0284c7;--pink:#7c3aed;--green:#16a34a;--red:#dc2626;--text:#111827;--dim:#64748b}
[data-theme=binance]{--bg:#121418;--panel:#1e2329;--cyan:#fcd535;--pink:#fcd535;--green:#0ecb81;--red:#f6465d}
*{box-sizing:border-box}body{margin:0;padding:8px;background:var(--bg);color:var(--text);font:12px system-ui}
.head,.box,.stat,.coin{background:var(--panel);border:1px solid var(--cyan);border-radius:9px;padding:10px;margin-bottom:8px}
.head{display:flex;justify-content:space-between;align-items:center;gap:6px;border-color:var(--pink);flex-wrap:wrap}
.brand{color:var(--cyan);font-weight:900}
.mode,button,select{padding:7px;border-radius:6px;cursor:pointer;background:#071c2c;color:var(--cyan);border:1px solid var(--cyan);font-weight:bold}
.mode{color:white;border-color:var(--red)}
.stats{display:grid;grid-template-columns:repeat(6,1fr);gap:4px}
.stat{text-align:center;border-color:var(--pink)}
.lbl{font-size:9px;color:var(--dim)}.val{color:var(--green);font-weight:bold;font-size:12px;margin-top:4px}
.coins{display:grid;grid-template-columns:repeat(8,1fr);gap:4px}
.coin{text-align:center;cursor:pointer;border-color:var(--pink);padding:7px 2px}
.coin.active{border:2px solid var(--cyan)}
.grid{display:grid;grid-template-columns:2fr 1fr;gap:8px}
.box{margin-bottom:8px}
.title{color:var(--cyan);font-weight:bold;margin-bottom:8px;display:flex;justify-content:space-between}
#chart{height:260px}
.row{display:flex;justify-content:space-between;align-items:center;margin:8px 0;gap:5px}
.row input{width:88px;padding:6px;background:#071020;color:var(--cyan);border:1px solid var(--cyan);border-radius:5px;text-align:center}
.btn{width:100%;margin-top:5px;padding:9px}
.buy{color:var(--green);border-color:var(--green)}
.sell{color:white;background:#520522;border-color:var(--red)}
table{width:100%;border-collapse:collapse;font-size:10px}
td,th{text-align:center;border-bottom:1px solid #ff007755;padding:5px 2px}
.logs{height:75px;overflow:auto;background:#070b18;padding:6px;color:var(--cyan);font-size:10px}
.orderbook{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.radar{display:flex;flex-wrap:wrap;gap:8px;justify-content:space-around}
.hud{display:grid;grid-template-columns:repeat(4,1fr);gap:4px;flex:1}
.hud .stat{border-color:var(--cyan)}
@media(max-width:650px){.stats{grid-template-columns:repeat(3,1fr)}.coins{grid-template-columns:repeat(4,1fr)}.grid{grid-template-columns:1fr}.hud{grid-template-columns:repeat(2,1fr)}}
</style></head><body>
<div class="head"><div class="brand">⚡ CYBERPUNK NEON TERMINAL</div>
<div><select onchange="document.documentElement.dataset.theme=this.value">
<option value="cyber">Cyberpunk Neon</option><option value="white">Pure White</option><option value="binance">Binance Dark</option>
</select> <button class="mode" id="mode" onclick="toggleMode()">DEMO MODE</button></div></div>
<div class="stats">
<div class="stat"><div class="lbl">মোট লাভ / ক্ষতি</div><div class="val" id="pnl">$0.00</div></div>
<div class="stat"><div class="lbl">১ ঘণ্টার লাভ</div><div class="val" id="pnl1">$0.00</div></div>
<div class="stat"><div class="lbl">২৪ ঘণ্টার লাভ</div><div class="val" id="pnl24">$0.00</div></div>
<div class="stat"><div class="lbl">BUY / SELL</div><div class="val" id="count">0 / 0</div></div>
<div class="stat"><div class="lbl">WIN RATE</div><div class="val" id="win">0%</div></div>
<div class="stat"><div class="lbl">USDT BALANCE</div><div class="val" id="bal">$10,000</div></div></div>
<div class="coins" id="coins"></div>
<div class="grid"><div>
<div class="box"><div class="title">লাইভ চার্ট: <span id="sym">BTCUSDT</span></div><div id="chart"></div>
<div class="orderbook"><div><b style="color:var(--green)">BIDS</b><table id="bids"></table></div><div><b style="color:var(--red)">ASKS</b><table id="asks"></table></div></div></div>
<div class="box radar"><canvas id="radar" width="170" height="170"></canvas><div class="hud" id="hud"></div></div></div>
<div><div class="box"><div class="title"><span id="cfgsym">BTCUSDT</span><span id="openpnl">ওপেন ট্রেড: নাই</span></div>
<div class="row">🛡️ No-Loss Mode <input id="noloss" type="checkbox" checked></div>
<div class="row">ট্রেড সাইজ ($)<input id="amount" type="number" value="10" min="5"></div>
<div class="row">স্টপ লস (%)<input id="sl" type="number" value="50"></div>
<div class="row">টেক প্রফিট (%)<input id="tp" type="number" value=".5"></div>
<button class="btn" onclick="settings()">সেটিংস সেভ করুন</button>
<button class="btn buy" onclick="buy()">তাৎক্ষণিক বাই</button>
<button class="btn sell" onclick="sell()">ট্রেড সেল/ক্লোজ</button>
<button class="btn" onclick="toggleBot()" id="run">অটো ট্রেড চালু/বন্ধ</button>
<div id="msg" class="logs"></div></div>
<div class="box"><div class="title">অ্যাকাউন্ট লগ</div><div id="logs" class="logs"></div></div>
<div class="box"><div class="title">ট্রেড হিস্ট্রি — বর্তমান মোড</div>
<table><thead><tr><th>সময়</th><th>কয়েন</th><th>অর্ডার</th><th>মূল্য</th><th>PnL</th></tr></thead><tbody id="hist"></tbody></table></div>
<div class="box"><div class="title">বর্তমান মোডের পজিশন</div>
<table><thead><tr><th>কয়েন</th><th>Qty</th><th>Entry</th><th>Mode</th></tr></thead><tbody id="pos"></tbody></table></div>
</div></div>
<script>
const S=["BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT","ADAUSDT","DOGEUSDT","AVAXUSDT"];
let active="BTCUSDT",price=0,prices={},ch,cs;
function fmt(x){return Number(x||0).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})}
function message(t){document.getElementById('msg').textContent=t}
async function post(url,data={}){try{let r=await fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)}),d=await r.json();if(!r.ok||d.ok===false)throw Error(d.error||'Request failed');message(d.message||'OK');await refresh()}catch(e){message('ত্রুটি: '+e.message)}}
function select(s){active=s;document.querySelectorAll('.coin').forEach(e=>e.classList.toggle('active',e.dataset.s===s));document.getElementById('sym').textContent=s;document.getElementById('cfgsym').textContent=s;chartLoad();refresh()}
function toggleMode(){if(confirm('DEMO/LIVE মোড বদলাবেন? দুই মোডের ব্যালেন্স ও ট্রেড আলাদা থাকবে।'))post('/api/toggle_mode')}
function buy(){if(confirm(active+' BUY করবেন?'))post('/api/buy',{symbol:active,price})}
function sell(){if(confirm(active+' SELL করবেন?'))post('/api/sell',{symbol:active,price})}
function toggleBot(){post('/api/toggle',{symbol:active})}
function settings(){post('/api/settings',{symbol:active,amount:+document.getElementById('amount').value,stop_loss:+document.getElementById('sl').value,take_profit:+document.getElementById('tp').value,no_loss_mode:document.getElementById('noloss').checked})}
async function refresh(){try{
let d=await(await fetch('/api/status')).json();
document.getElementById('mode').textContent=d.live_mode?'LIVE TRADING':'DEMO MODE';
document.getElementById('bal').textContent='$'+fmt(d.live_mode?d.live_balance:d.demo_balance);
document.getElementById('pnl').textContent=(d.realized_pnl>=0?'+$':'-$')+fmt(Math.abs(d.realized_pnl));
document.getElementById('pnl1').textContent=(d.pnl_1h>=0?'+$':'-$')+fmt(Math.abs(d.pnl_1h));
document.getElementById('pnl24').textContent=(d.pnl_24h>=0?'+$':'-$')+fmt(Math.abs(d.pnl_24h));
document.getElementById('count').textContent=d.total_buys+' BUY / '+d.total_sells+' SELL';
document.getElementById('win').textContent+(d.total_sells?(d.wins/d.total_sells*100).toFixed(1):'0')+'%';
document.getElementById('logs').innerHTML=d.logs.map(x=>'<div>'+esc(x)+'</div>').join('');
document.getElementById('hist').innerHTML=d.history.map(h=>`<tr><td>${h.time}</td><td>${h.symbol.replace('USDT','')}</td><td>${h.side}</td><td>${fmt(h.price)}</td><td>${fmt(h.pnl)}</td></tr>`).join('');
document.getElementById('pos').innerHTML=d.positions.map(p=>`<tr><td>${p.symbol.replace('USDT','')}</td><td>${Number(p.qty).toPrecision(5)}</td><td>${fmt(p.entry)}</td><td>${d.mode}</td></tr>`).join('');
for(let s of S){let m=d.markets[s];document.getElementById('pr-'+s).textContent='$'+fmt(m.price);document.getElementById('st-'+s).textContent=m.position?'OPEN':m.running?'ACTIVE':'STANDBY'}
let m=d.markets[active];document.getElementById('amount').value=m.amount;document.getElementById('sl').value=m.stop_loss;document.getElementById('tp').value=m.take_profit;document.getElementById('noloss').checked=m.no_loss_mode;document.getElementById('run').textContent=m.running?'অটো ট্রেড বন্ধ করুন':'অটো ট্রেড চালু করুন';document.getElementById('openpnl').textContent=m.position?'OPEN POSITION':'ওপেন ট্রেড: নাই'
}catch(e){}}
function esc(s){return String(s).replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]))}
function chartInit(){if(!window.LightweightCharts)return;ch=LightweightCharts.createChart(document.getElementById('chart'),{height:260,layout:{background:{color:'transparent'},textColor:'#aaa'},timeScale:{timeVisible:true}});cs=ch.addCandlestickSeries({upColor:'#00ff66',downColor:'#ff0044',borderVisible:false,wickUpColor:'#00ff66',wickDownColor:'#ff0044'});chartLoad()}
async function chartLoad(){try{let a=await(await fetch('https://api.binance.com/api/v3/klines?symbol='+active+'&interval=1m&limit=50')).json();if(Array.isArray(a)){let x=a.map(k=>({time:Math.floor(k[0]/1000),open:+k[1],high:+k[2],low:+k[3],close:+k[4]}));if(cs)cs.setData(x);if(x.length){price=x[x.length-1].close;fetch('/api/sync_price',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({symbol:active,price})})}}}catch(e){}}
async function pricesLoop(){try{
let a=await(await fetch('https://api.binance.com/api/v3/ticker/price')).json();
for(let x of a){if(S.includes(x.symbol))prices[x.symbol]=+x.price;if(x.symbol===active)price=+x.price}
for(let s of S){let el=document.getElementById('pr-'+s);if(el)el.textContent='$'+fmt(prices[s])}
let b=await(await fetch('https://api.binance.com/api/v3/depth?symbol='+active+'&limit=5')).json();
document.getElementById('bids').innerHTML=(b.bids||[]).map(x=>`<tr><td style="color:var(--green)">${fmt(x[0])}</td><td>${x[1]}</td></tr>`).join('');
document.getElementById('asks').innerHTML=(b.asks||[]).map(x=>`<tr><td style="color:var(--red)">${fmt(x[0])}</td><td>${x[1]}</td></tr>`).join('');
document.getElementById('hud').innerHTML=S.map(s=>`<div class="stat"><b>${s.replace('USDT','')}</b><div class="val">${fmt(prices[s])}</div></div>`).join('');
let c=document.getElementById('radar').getContext('2d');c.clearRect(0,0,170,170);c.strokeStyle='#00f0ff88';
for(let r=25;r<80;r+=25){c.beginPath();c.arc(85,85,r,0,7);c.stroke()}
S.forEach((s,i)=>{let a=i*6.283/S.length,x=85+Math.cos(a)*(25+i%3*20),y=85+Math.sin(a)*(25+i%3*20);c.fillStyle='#00ff66';c.beginPath();c.arc(x,y,3,0,7);c.fill();c.fillStyle='#fff';c.font='8px monospace';c.fillText(s.slice(0,-4),x+4,y)})
}catch(e){}}
document.getElementById('coins').innerHTML=S.map(s=>`<div class="coin ${s===active?'active':''}" data-s="${s}" onclick="select('${s}')"><b>${s.replace('USDT','')}</b><div id="pr-${s}" class="val">--</div><small id="st-${s}">STANDBY</small></div>`).join('');
chartInit();refresh();pricesLoop();setInterval(refresh,2500);setInterval(pricesLoop,2500);setInterval(chartLoad,20000);
</script></body></html>"""

class Handler(BaseHTTPRequestHandler):
    def respond(self, data, code=200):
        raw = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def readbody(self):
        size = int(self.headers.get("Content-Length", 0))
        return json.loads(self.rfile.read(size).decode()) if size else {}

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path
        if path in ("/", "/index.html"):
            raw = HTML.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
        elif path == "/api/status":
            try:
                self.respond(status())
            except Exception as e:
                self.respond({"error": str(e)}, 500)
        else:
            self.respond({"error": "Not found"}, 404)

    def do_POST(self):
        path = urllib.parse.urlparse(self.path).path
        try:
            data = self.readbody()

            if path == "/api/toggle_mode":
                with lock:
                    state["live_mode"] = not state["live_mode"]
                    log("Mode changed to " + mode())
                telegram("Trading mode: " + mode())
                self.respond({
                    "ok": True,
                    "live_mode": state["live_mode"],
                    "message": mode() + " mode"
                })

            elif path == "/api/sync_price":
                symbol = data.get("symbol")
                current_price = float(data.get("price", 0))
                if symbol in SYMBOLS and current_price > 0:
                    state["markets"][symbol]["price"] = current_price
                self.respond({"ok": True})

            elif path == "/api/buy":
                symbol = data.get("symbol", "BTCUSDT")
                current_price = float(data.get("price", 0) or price(symbol))
                self.respond(buy(symbol, current_price))

            elif path == "/api/sell":
                symbol = data.get("symbol", "BTCUSDT")
                current_price = float(data.get("price", 0) or price(symbol))
                self.respond(sell(symbol, current_price))

            elif path == "/api/toggle":
                symbol = data.get("symbol", "BTCUSDT")
                if symbol not in SYMBOLS:
                    raise ValueError("Invalid symbol")
                market = state["markets"][symbol]
                market["running"] = not market["running"]
                log(symbol + " auto " + ("ON" if market["running"] else "OFF"))
                self.respond({"ok": True, "running": market["running"]})

            elif path == "/api/settings":
                symbol = data.get("symbol", "BTCUSDT")
                if symbol not in SYMBOLS:
                    raise ValueError("Invalid symbol")
                amount = float(data.get("amount", 10))
                sl = float(data.get("stop_loss", 50))
                tp = float(data.get("take_profit", 0.5))

                if not 5 <= amount <= 100000:
                    raise ValueError("Trade size 5–100000 USDT হতে হবে")
                if not 0 <= sl <= 100 or not 0 < tp <= 100:
                    raise ValueError("Stop Loss/Take Profit range ভুল")

                state["markets"][symbol].update(
                    amount=amount,
                    stop_loss=sl,
                    take_profit=tp,
                    no_loss_mode=bool(data.get("no_loss_mode", True))
                )
                self.respond({"ok": True, "message": "সেটিংস সেভ হয়েছে"})

            elif path == "/api/check-api":
                account, error = signed("/api/v3/account")
                if error:
                    self.respond({"ok": False, "error": error}, 400)
                else:
                    self.respond({
                        "ok": True,
                        "canTrade": account.get("canTrade")
                    })

            elif path == "/api/test-telegram":
                if not TG_TOKEN or not TG_CHAT:
                    raise ValueError("Telegram credentials config.py-তে পাওয়া যায়নি")
                telegram("Crypto Bot Pro Telegram test")
                self.respond({"ok": True, "message": "Telegram test sent"})

            else:
                self.respond({"ok": False, "error": "Not found"}, 404)

        except Exception as e:
            self.respond({"ok": False, "error": str(e)}, 400)

    def log_message(self, *args):
        pass

if __name__ == "__main__":
    print(f"CYBERPUNK TERMINAL: http://0.0.0.0:{PORT}", flush=True)
    print("DEMO balance = 10000 USDT; DEMO/LIVE balances, positions and history are separate.", flush=True)
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
