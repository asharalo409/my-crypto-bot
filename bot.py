#!/usr/bin/env python3
import json
import time
import threading
import sqlite3
import urllib.parse
from datetime import datetime, timedelta
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from config import HOST, PORT, SYMBOLS, MAX_OPEN_POSITIONS, DB_FILE
from binance_api import (
    get_live_balance, execute_binance_order, fetch_klines,
    fetch_depth, send_telegram
)

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT, symbol TEXT, side TEXT,
        price REAL, pnl REAL, profit_usd REAL, reason TEXT
    )''')
    conn.commit()
    conn.close()

def log_trade_db(symbol, side, price, pnl, profit_usd, reason):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute('''INSERT INTO trades (timestamp, symbol, side, price, pnl, profit_usd, reason)
                     VALUES (?, ?, ?, ?, ?, ?, ?)''',
                  (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), symbol, side, price, pnl, profit_usd, reason))
        conn.commit()
        conn.close()
    except Exception:
        pass

init_db()
lock = threading.RLock()

bot = {
    "live_mode": True,  # ডিফল্টভাবে লাইভ মোড সক্রিয়
    "demo_balance": 1000.0,
    "live_balance": 0.0,
    "total_buys": 0,
    "total_sells": 0,
    "wins": 0,
    "realized_pnl": 0.0,
    "pnl_1h": 0.0,
    "pnl_24h": 0.0,
    "trades_1h": 0,
    "trades_24h": 0,
    "history": deque(maxlen=30),
    "logs": deque(maxlen=25),
    "markets": {},
    "klines_cache": {},
    "last_radar_event": None
}

for sym in SYMBOLS:
    bot["markets"][sym] = {
        "running": False, "price": 0.0, "buyer_ratio": 50.0,
        "amount": 10.0, "stop_loss": 50.0, "take_profit": 0.5,
        "no_loss_mode": True, "dca_orders": [], "position": None,
        "unrealized_pnl": 0.0, "cooldown_until": 0, "bids": [], "asks": []
    }
    bot["klines_cache"][sym] = []

def add_log(msg):
    bot["logs"].appendleft(f"[{time.strftime('%H:%M:%S')}] {msg}")

def balance_sync_loop():
    last_err = ""
    while True:
        try:
            bal, err = get_live_balance()
            with lock:
                if err:
                    if err != last_err:
                        add_log(f"[API লগ] {err}")
                        last_err = err
                else:
                    bot["live_balance"] = bal
                    if last_err:
                        add_log(f"[বাইনান্স লাইভ] ব্যালেন্স: ${bal:.2f}")
                        last_err = ""
            time.sleep(3)
        except Exception:
            time.sleep(3)

threading.Thread(target=balance_sync_loop, daemon=True).start()

def market_engine(symbol):
    while True:
        try:
            klines = fetch_klines(symbol)
            if not klines or len(klines) == 0:
                time.sleep(2)
                continue

            price = float(klines[-1][4])
            formatted_klines = [
                {"time": int(k[0] // 1000), "open": float(k[1]), "high": float(k[2]), "low": float(k[3]), "close": float(k[4])}
                for k in klines
            ]

            depth = fetch_depth(symbol)
            bids = depth.get("bids", [])
            asks = depth.get("asks", [])
            tot_bids = sum(float(b[1]) for b in bids) if bids else 1.0
            tot_asks = sum(float(a[1]) for a in asks) if asks else 1.0
            b_ratio = round((tot_bids / (tot_bids + tot_asks)) * 100, 1)

            with lock:
                m = bot["markets"][symbol]
                m["price"] = price
                m["buyer_ratio"] = b_ratio
                m["bids"] = bids
                m["asks"] = asks
                bot["klines_cache"][symbol] = formatted_klines

                now = time.time()
                orders = m["dca_orders"]

                if len(orders) > 0:
                    total_qty = sum(o["qty"] for o in orders)
                    total_cost = sum(o["cost"] for o in orders)
                    avg_price = total_cost / total_qty
                    diff = ((price - avg_price) / avg_price) * 100
                    m["unrealized_pnl"] = round(diff, 2)

                    is_tp = (diff >= m["take_profit"])
                    is_sl = (not m["no_loss_mode"]) and (diff <= -m["stop_loss"])

                    last_buy = orders[-1]["price"]
                    if (price <= last_buy * 0.985) and (len(orders) < 3) and (now > m["cooldown_until"]):
                        buy_amt = m["amount"]
                        if bot["live_mode"]:
                            execute_binance_order(symbol, "BUY", quote_order_qty=buy_amt)
                        orders.append({"price": price, "qty": buy_amt / price, "cost": buy_amt})
                        bot["total_buys"] += 1
                        m["cooldown_until"] = now + 15
                        bot["last_radar_event"] = {"symbol": symbol, "type": "BUY", "time": time.time()}
                        add_log(f"[{symbol}] 🟢 DCA ডিপ বাই #{len(orders)} @ ${price:.4f}")
                        threading.Thread(target=send_telegram, args=(f"⚡ [DCA BUY #{len(orders)}] {symbol} Price: ${price:.4f}",), daemon=True).start()

                    elif is_tp or is_sl:
                        revenue = total_qty * price
                        profit = revenue - total_cost
                        side_name = "TAKE PROFIT" if diff > 0 else "STOP LOSS"

                        if bot["live_mode"]:
                            execute_binance_order(symbol, "SELL", quantity=total_qty)

                        bot["realized_pnl"] += profit
                        bot["total_sells"] += 1
                        if profit > 0:
                            bot["wins"] += 1

                        bot["last_radar_event"] = {"symbol": symbol, "type": "SELL", "time": time.time()}
                        add_log(f"[{symbol}] 🔴 {side_name}: {profit:+.2f}$ ({diff:+.2f}%)")
                        threading.Thread(target=send_telegram, args=(f"🎯 [{side_name}] {symbol} Exit: ${price:.4f} PnL: {diff:+.2f}%",), daemon=True).start()
                        log_trade_db(symbol, "SELL", price, round(diff, 2), round(profit, 4), side_name)
                        
                        bot["history"].appendleft({
                            "time": time.strftime("%H:%M:%S"),
                            "symbol": symbol,
                            "side": f"SELL({len(orders)}x)",
                            "price": round(price, 4),
                            "pnl": round(diff, 2)
                        })
                        m["dca_orders"] = []
                        m["position"] = None
                        m["unrealized_pnl"] = 0.0
                        m["cooldown_until"] = now + 10

                    m["position"] = {"buy_price": avg_price, "amount": total_cost} if len(orders) > 0 else None

                elif m["running"]:
                    active_pos = sum(1 for k in bot["markets"].values() if len(k["dca_orders"]) > 0)
                    if (now > m["cooldown_until"]) and (active_pos < MAX_OPEN_POSITIONS):
                        buy_amt = m["amount"]
                        if bot["live_mode"]:
                            execute_binance_order(symbol, "BUY", quote_order_qty=buy_amt)

                        m["dca_orders"] = [{"price": price, "qty": buy_amt / price, "cost": buy_amt}]
                        m["position"] = {"buy_price": price, "amount": buy_amt}
                        bot["total_buys"] += 1
                        bot["last_radar_event"] = {"symbol": symbol, "type": "BUY", "time": time.time()}
                        add_log(f"[{symbol}] 🟢 বেস বাই @ ${price:.4f}")
                        threading.Thread(target=send_telegram, args=(f"⚡ [BASE BUY] {symbol} Price: ${price:.4f}",), daemon=True).start()
                        log_trade_db(symbol, "BUY", price, 0.0, 0.0, "Base Entry")
                        bot["history"].appendleft({
                            "time": time.strftime("%H:%M:%S"),
                            "symbol": symbol,
                            "side": "BUY",
                            "price": round(price, 4),
                            "pnl": 0.0
                        })

            time.sleep(1.2)
        except Exception:
            time.sleep(2)

for sym in SYMBOLS:
    threading.Thread(target=market_engine, args=(sym,), daemon=True).start()

HTML_PAGE = """<!DOCTYPE html>
<html lang="bn" data-theme="cyberpunk">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>CYBERPUNK NEON TERMINAL ULTRA</title>
<script src="https://cdn.jsdelivr.net/npm/lightweight-charts@4.1.1/dist/lightweight-charts.standalone.production.js"></script>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@500;700;800&family=Plus+Jakarta+Sans:wght@600;700;800;900&display=swap" rel="stylesheet">
<style>
:root {
  --bg: #03000a; --panel: rgba(14, 5, 29, 0.96); --cyan: #00f0ff; --purple: #ff0077;
  --green: #00ff66; --red: #ff0044; --border-cyan: #00f0ff; --border-pink: #ff0077;
  --text: #ffffff; --text-dim: #a1a1aa; --card-bg: #081026;
}
[data-theme="white"] {
  --bg: #f4f6f9; --panel: #ffffff; --cyan: #0066cc; --purple: #8b5cf6;
  --green: #10b981; --red: #ef4444; --border-cyan: #cbd5e1; --border-pink: #e2e8f0;
  --text: #0f172a; --text-dim: #64748b; --card-bg: #f8fafc;
}
[data-theme="binance"] { --bg: #121418; --panel: #1e2329; --cyan: #fcd535; --purple: #fcd535; --green: #0ecb81; --red: #f6465d; --border-cyan: #2b313a; --border-pink: #fcd535; --card-bg: #181a20; }
[data-theme="oled"] { --bg: #000000; --panel: #0d0d0d; --cyan: #3b82f6; --purple: #3b82f6; --green: #22c55e; --red: #ef4444; --border-cyan: #262626; --border-pink: #3b82f6; --card-bg: #111111; }

* { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Plus Jakarta Sans', system-ui, sans-serif; }
body { background-color: var(--bg); color: var(--text); padding: 8px; min-height: 100vh; }
.mono { font-family: 'JetBrains Mono', monospace; }

.header { display: flex; justify-content: space-between; align-items: center; background: var(--panel); border: 2px solid var(--border-pink); padding: 8px 12px; border-radius: 12px; margin-bottom: 8px; }
.header-left { display: flex; align-items: center; gap: 8px; }
.brand { font-size: 13px; font-weight: 900; color: var(--cyan); letter-spacing: 0.5px; }

select { background: var(--panel); color: var(--text); border: 1px solid var(--border-cyan); padding: 4px 6px; border-radius: 6px; font-size: 10px; }
.mode-badge { padding: 5px 12px; border-radius: 8px; font-weight: 800; font-size: 10px; cursor: pointer; user-select: none; }
.mode-demo { background: rgba(0, 240, 255, 0.2); color: var(--cyan); border: 1.5px solid var(--cyan); }
.mode-live { background: rgba(255, 0, 68, 0.35); color: #fff; border: 1.5px solid var(--red); box-shadow: 0 0 10px rgba(255,0,68,0.5); }

.analytics-panel { display: grid; grid-template-columns: repeat(6, 1fr); gap: 4px; margin-bottom: 8px; }
@media(max-width: 600px) { .analytics-panel { grid-template-columns: repeat(3, 1fr); } }
.an-card { background: var(--panel); border: 1px solid var(--border-pink); border-radius: 8px; padding: 6px 2px; text-align: center; }
.an-title { font-size: 8px; color: var(--text-dim); text-transform: uppercase; margin-bottom: 2px; font-weight: 700; }
.an-val { font-size: 12px; font-weight: 800; }

.market-nav { display: grid; grid-template-columns: repeat(8, 1fr); gap: 4px; margin-bottom: 8px; overflow-x: auto; }
@media(max-width: 600px) { .market-nav { grid-template-columns: repeat(4, 1fr); } }
.market-tab { background: var(--panel); border: 1.5px solid var(--border-pink); border-radius: 6px; padding: 6px 2px; cursor: pointer; text-align: center; }
.market-tab.active { background: rgba(0, 240, 255, 0.15); border-color: var(--cyan); box-shadow: 0 0 8px rgba(0,240,255,0.3); }
.tab-name { font-size: 10px; font-weight: 900; }
.tab-price { font-size: 8.5px; font-weight: 700; color: var(--cyan); margin-top: 1px; }
.tab-status { font-size: 7.5px; margin-top: 1px; font-weight: bold; }
.tab-dca-badge { font-size: 7px; color: var(--cyan); margin-top: 1px; font-weight: bold; }

.main-grid { display: grid; grid-template-columns: 2fr 1fr; gap: 8px; }
@media(max-width: 860px) { .main-grid { grid-template-columns: 1fr; } }

.box { background: var(--panel); border: 1.5px solid var(--border-cyan); border-radius: 10px; padding: 10px; margin-bottom: 8px; }
.box-head { font-size: 12px; font-weight: 800; margin-bottom: 8px; display: flex; justify-content: space-between; align-items: center; color: var(--cyan); }

#chart { height: 280px; width: 100%; border-radius: 6px; }

.sentiment-box { margin-top: 8px; padding-top: 6px; border-top: 1px solid var(--border-pink); }
.bar-wrap { height: 6px; width: 100%; background: var(--red); border-radius: 4px; overflow: hidden; margin: 4px 0; display: flex; }
.bar-fill { height: 100%; background: var(--green); transition: width 0.4s ease; }

.ob-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-top: 8px; font-size: 10px; }
.ob-table { width: 100%; border-collapse: collapse; }
.ob-table td { padding: 2px 0; }
.ob-table td:last-child { text-align: right; color: var(--text-dim); }

.row { display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px; font-size: 11px; }
.row input { width: 80px; text-align: center; background: var(--card-bg); color: var(--cyan); border: 1.5px solid var(--border-cyan); padding: 4px; border-radius: 4px; font-weight: bold; }

.toggle-switch-row {
  display: flex; justify-content: space-between; align-items: center;
  background: rgba(0, 255, 102, 0.1); border: 1px solid rgba(0, 255, 102, 0.4);
  padding: 6px 8px; border-radius: 6px; margin-bottom: 8px;
}
.switch { position: relative; display: inline-block; width: 34px; height: 18px; }
.switch input { opacity: 0; width: 0; height: 0; }
.slider { position: absolute; cursor: pointer; top: 0; left: 0; right: 0; bottom: 0; background-color: #333; transition: .3s; border-radius: 18px; }
.slider:before { position: absolute; content: ""; height: 12px; width: 12px; left: 3px; bottom: 3px; background-color: white; transition: .3s; border-radius: 50%; }
input:checked + .slider { background-color: var(--green); }
input:checked + .slider:before { transform: translateX(16px); }

.btn { width: 100%; padding: 8px; border: none; border-radius: 6px; font-weight: 800; cursor: pointer; font-size: 11px; margin-top: 4px; }
.btn-save { background: rgba(0, 240, 255, 0.3); color: var(--cyan); border: 1.5px solid var(--cyan); margin-bottom: 5px; }
.btn-manual-buy { background: rgba(0, 255, 102, 0.25); color: var(--green); border: 1.5px solid var(--green); }
.btn-manual-sell { background: rgba(255, 0, 68, 0.3); color: var(--red); border: 1.5px solid var(--red); }
.btn-on { background: linear-gradient(135deg, var(--green), #00f0ff); color: #021208; font-weight: 900; }
.btn-off { background: linear-gradient(135deg, var(--red), #ff0077); color: #fff; font-weight: 900; }

.log-box { background: var(--card-bg); border: 1.5px solid var(--border-pink); border-radius: 6px; padding: 6px; height: 90px; overflow-y: hidden; font-size: 10px; line-height: 1.5; color: var(--cyan); }
.hist-table { width: 100%; border-collapse: collapse; font-size: 10px; margin-top: 4px; }
.hist-table th, .hist-table td { border-bottom: 1px solid var(--border-pink); padding: 4px 2px; text-align: center; }

.radar-panel {
  display: flex; flex-wrap: wrap; align-items: center; justify-content: space-around;
  background: var(--panel); border: 1.5px solid var(--cyan);
  border-radius: 10px; padding: 12px; margin-top: 10px;
}
.radar-canvas-box { text-align: center; }
#masterRadar { background: var(--card-bg); border-radius: 50%; border: 2px solid var(--cyan); }
.radar-hud-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 6px; flex: 1; min-width: 260px; margin-left: 12px; }
@media(max-width: 600px) { .radar-hud-grid { grid-template-columns: repeat(2, 1fr); margin-left: 0; margin-top: 10px; } }
.hud-coin-card { background: var(--card-bg); border: 1px solid var(--border-cyan); border-radius: 6px; padding: 6px; text-align: center; }
</style>
</head>
<body>

<div class="header">
  <div class="header-left">
    <div class="brand">⚡ CYBERPUNK TRADING TERMINAL</div>
  </div>
  <div style="display:flex; align-items:center; gap:5px;">
    <select onchange="applyTheme(this.value)">
      <option value="cyberpunk">Cyberpunk Neon</option>
      <option value="white">Pure White Minimal</option>
      <option value="binance">Binance Dark</option>
      <option value="oled">OLED Black</option>
    </select>
    <div class="mode-badge mode-live" id="mode-toggle" onclick="toggleTradingMode()">LIVE TRADING</div>
  </div>
</div>

<div class="analytics-panel">
  <div class="an-card"><div class="an-title">মোট লাভ / ক্ষতি</div><div class="an-val mono" id="v-all-pnl" style="color:var(--green);">$0.00</div></div>
  <div class="an-card"><div class="an-title">১ ঘণ্টার লাভ</div><div class="an-val mono" id="v-1h-pnl">$0.00</div></div>
  <div class="an-card"><div class="an-title">২৪ ঘণ্টার লাভ</div><div class="an-val mono" id="v-24h-pnl">$0.00</div></div>
  <div class="an-card"><div class="an-title">বাই / সেল</div><div class="an-val mono"><span style="color:var(--green);" id="v-tot-buy">0</span> / <span style="color:var(--red);" id="v-tot-sell">0</span></div></div>
  <div class="an-card"><div class="an-title">উইন রেট</div><div class="an-val mono" id="v-winrate" style="color:var(--cyan);">0%</div></div>
  <div class="an-card"><div class="an-title">USDT ব্যালেন্স</div><div class="an-val mono" id="v-bal" style="color:var(--green);">$0.00</div></div>
</div>

<div class="market-nav">
  <div class="market-tab active" onclick="selectMarket('BTCUSDT', this)"><div class="tab-name">BTC</div><div class="tab-price mono" id="tab-price-BTCUSDT">$0.00</div><div class="tab-status" id="tab-st-BTCUSDT">OFF</div><div class="tab-dca-badge" id="tab-dca-BTCUSDT">DCA: 0/3</div></div>
  <div class="market-tab" onclick="selectMarket('ETHUSDT', this)"><div class="tab-name">ETH</div><div class="tab-price mono" id="tab-price-ETHUSDT">$0.00</div><div class="tab-status" id="tab-st-ETHUSDT">OFF</div><div class="tab-dca-badge" id="tab-dca-ETHUSDT">DCA: 0/3</div></div>
  <div class="market-tab" onclick="selectMarket('SOLUSDT', this)"><div class="tab-name">SOL</div><div class="tab-price mono" id="tab-price-SOLUSDT">$0.00</div><div class="tab-status" id="tab-st-SOLUSDT">OFF</div><div class="tab-dca-badge" id="tab-dca-SOLUSDT">DCA: 0/3</div></div>
  <div class="market-tab" onclick="selectMarket('BNBUSDT', this)"><div class="tab-name">BNB</div><div class="tab-price mono" id="tab-price-BNBUSDT">$0.00</div><div class="tab-status" id="tab-st-BNBUSDT">OFF</div><div class="tab-dca-badge" id="tab-dca-BNBUSDT">DCA: 0/3</div></div>
  <div class="market-tab" onclick="selectMarket('XRPUSDT', this)"><div class="tab-name">XRP</div><div class="tab-price mono" id="tab-price-XRPUSDT">$0.00</div><div class="tab-status" id="tab-st-XRPUSDT">OFF</div><div class="tab-dca-badge" id="tab-dca-XRPUSDT">DCA: 0/3</div></div>
  <div class="market-tab" onclick="selectMarket('ADAUSDT', this)"><div class="tab-name">ADA</div><div class="tab-price mono" id="tab-price-ADAUSDT">$0.00</div><div class="tab-status" id="tab-st-ADAUSDT">OFF</div><div class="tab-dca-badge" id="tab-dca-ADAUSDT">DCA: 0/3</div></div>
  <div class="market-tab" onclick="selectMarket('DOGEUSDT', this)"><div class="tab-name">DOGE</div><div class="tab-price mono" id="tab-price-DOGEUSDT">$0.00</div><div class="tab-status" id="tab-st-DOGEUSDT">OFF</div><div class="tab-dca-badge" id="tab-dca-DOGEUSDT">DCA: 0/3</div></div>
  <div class="market-tab" onclick="selectMarket('AVAXUSDT', this)"><div class="tab-name">AVAX</div><div class="tab-price mono" id="tab-price-AVAXUSDT">$0.00</div><div class="tab-status" id="tab-st-AVAXUSDT">OFF</div><div class="tab-dca-badge" id="tab-dca-AVAXUSDT">DCA: 0/3</div></div>
</div>

<div class="main-grid">
  <div>
    <div class="box">
      <div class="box-head">
        <span>লাইভ চার্ট: <b style="color:var(--cyan);" id="chart-sym">BTCUSDT</b></span>
      </div>
      <div id="chart"></div>

      <div class="sentiment-box">
        <div style="display:flex; justify-content:space-between; font-size:10px;">
          <span style="color:var(--green); font-weight:800;">ক্রেতা শক্তি: <span id="buy-pct">50%</span></span>
          <span style="color:var(--red); font-weight:800;">বিক্রেতা শক্তি: <span id="sell-pct">50%</span></span>
        </div>
        <div class="bar-wrap"><div class="bar-fill" id="buy-bar" style="width: 50%;"></div></div>
      </div>

      <div class="ob-grid mono">
        <div><b style="color:var(--green);">BIDS (বায়ার)</b><table class="ob-table" id="bids-list"></table></div>
        <div><b style="color:var(--red);">ASKS (সেলার)</b><table class="ob-table" id="asks-list"></table></div>
      </div>
    </div>

    <div class="radar-panel">
      <div class="radar-canvas-box">
        <canvas id="masterRadar" width="200" height="200"></canvas>
        <div id="radarEventText" class="mono" style="font-size:10px; color:var(--green); margin-top:6px;">RADAR: SCANNING</div>
      </div>
      <div class="radar-hud-grid mono" id="hudGrid"></div>
    </div>
  </div>

  <div>
    <div class="box">
      <div class="box-head">
        <span id="cfg-sym">BTCUSDT</span>
        <span id="active-pnl-label" class="mono" style="font-size:10px; color:var(--text-dim);">ওপেন ট্রেড: নাই</span>
      </div>

      <div class="toggle-switch-row">
        <span style="color:var(--green); font-weight:bold;">🛡️ নো-লস মোড:</span>
        <label class="switch">
          <input type="checkbox" id="no-loss-chk" checked onchange="toggleNoLoss(this.checked)">
          <span class="slider"></span>
        </label>
      </div>

      <div class="row"><span>ট্রেড সাইজ ($):</span><input class="mono" id="amt" type="number" value="10"></div>
      <div class="row"><span>স্টপ লস (%):</span><input class="mono" id="sl" type="number" step="0.01" value="50.0"></div>
      <div class="row"><span>টেক প্রফিট (%):</span><input class="mono" id="tp" type="number" step="0.01" value="0.5"></div>
      <button class="btn btn-save" onclick="saveCurrentConfig()">সেটিংস সেভ করুন</button>
      
      <div style="display:flex; gap:5px; margin-bottom:5px;">
        <button class="btn btn-manual-buy" onclick="instantBuy()">বাই টেস্ট</button>
        <button class="btn btn-manual-sell" onclick="instantSell()">সেল / ক্লোজ</button>
      </div>
      <button id="pwr-btn" class="btn btn-on" onclick="toggleCurrentMarket()">চালু করুন (BTC)</button>
    </div>

    <div class="box">
      <div class="box-head">অ্যাকশন ও এরর লগ</div>
      <div class="log-box mono" id="logs"></div>
    </div>

    <div class="box">
      <div class="box-head">লাইভ ট্রেড হিস্ট্রি</div>
      <table class="hist-table mono">
        <thead><tr><th>সময়</th><th>কয়েন</th><th>অর্ডার</th><th>PnL</th></tr></thead>
        <tbody id="hist"></tbody>
      </table>
    </div>
  </div>
</div>

<script>
let activeSymbol = "BTCUSDT";
let chartEl = document.getElementById('chart');

function applyTheme(t) {
  document.documentElement.setAttribute('data-theme', t);
  const isWhite = (t === 'white');
  ch.applyOptions({
    layout: {
      background: { color: isWhite ? '#ffffff' : 'transparent' },
      textColor: isWhite ? '#0f172a' : '#a1a1aa'
    }
  });
}

function toggleTradingMode() {
  fetch('/api/toggle_mode', {method:'POST'}).then(r=>r.json()).then(d=>{
    const b = document.getElementById('mode-toggle');
    b.className = d.live_mode ? "mode-badge mode-live" : "mode-badge mode-demo";
    b.innerText = d.live_mode ? "LIVE TRADING" : "DEMO MODE";
  });
}

function selectMarket(sym, tabEl) {
  activeSymbol = sym;
  document.querySelectorAll('.market-tab').forEach(t => t.classList.remove('active'));
  tabEl.classList.add('active');
  document.getElementById('chart-sym').innerText = sym;
  document.getElementById('cfg-sym').innerText = sym;
  loadChartData(sym);
  updateActiveControls();
}

function updateActiveControls() {
  fetch('/api/status').then(r=>r.json()).then(d=>{
    const m = d.markets[activeSymbol];
    if(m) {
      const b = document.getElementById('pwr-btn');
      b.className = m.running ? "btn btn-off" : "btn btn-on";
      b.innerText = m.running ? "বন্ধ করুন (" + activeSymbol.replace('USDT','') + ")" : "চালু করুন (" + activeSymbol.replace('USDT','') + ")";
      document.getElementById('no-loss-chk').checked = !!m.no_loss_mode;
      document.getElementById('amt').value = m.amount;
      document.getElementById('sl').value = m.stop_loss;
      document.getElementById('tp').value = m.take_profit;
    }
  });
}

function toggleNoLoss(val) {
  fetch('/api/toggle_no_loss?symbol=' + encodeURIComponent(activeSymbol) + '&val=' + val, {method:'POST'});
}

const ch = LightweightCharts.createChart(chartEl, {
  layout: { background: { color: 'transparent' }, textColor: '#a1a1aa', fontSize: 10 },
  grid: { vertLines: { color: 'rgba(255,0,119,0.08)' }, horzLines: { color: 'rgba(0,240,255,0.08)' } },
  timeScale: { timeVisible: true, rightOffset: 5 }
});
let series = ch.addCandlestickSeries({ upColor: '#00ff66', downColor: '#ff0044', borderVisible: false, wickUpColor: '#00ff66', wickDownColor: '#ff0044' });

function loadChartData(sym) {
  fetch('/api/klines?symbol=' + sym)
    .then(r => r.json())
    .then(d => {
      if(d && d.length > 0) {
        series.setData(d);
        ch.timeScale().fitContent();
      }
    }).catch(e=>{});
}
loadChartData("BTCUSDT");

function toggleCurrentMarket() {
  fetch('/api/toggle?symbol=' + encodeURIComponent(activeSymbol), {method:'POST'}).then(r=>r.json()).then(d=>{
    const b = document.getElementById('pwr-btn');
    b.className = d.running ? "btn btn-off" : "btn btn-on";
    b.innerText = d.running ? "বন্ধ করুন (" + activeSymbol.replace('USDT','') + ")" : "চালু করুন (" + activeSymbol.replace('USDT','') + ")";
  });
}

function instantBuy() { fetch('/api/instant_buy?symbol=' + encodeURIComponent(activeSymbol), {method:'POST'}); }
function instantSell() { fetch('/api/instant_sell?symbol=' + encodeURIComponent(activeSymbol), {method:'POST'}); }

function saveCurrentConfig() {
  const p = {
    symbol: activeSymbol,
    amount: +document.getElementById('amt').value,
    stop_loss: +document.getElementById('sl').value,
    take_profit: +document.getElementById('tp').value,
    no_loss_mode: document.getElementById('no-loss-chk').checked
  };
  fetch('/api/settings', {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify(p)
  }).then(()=>{ alert(activeSymbol + ' সেটিংস সেভ হয়েছে!'); });
}

const rCvs = document.getElementById('masterRadar');
const rCtx = rCvs.getContext('2d');
let rAngle = 0;
let radarBlips = [];

function drawMasterRadar() {
  rCtx.clearRect(0,0,200,200);
  const cx = 100, cy = 100, r = 85;
  rCtx.strokeStyle = 'rgba(0, 240, 255, 0.4)';
  rCtx.lineWidth = 1;
  for(let i=1; i<=3; i++) {
    rCtx.beginPath();
    rCtx.arc(cx, cy, (r/3)*i, 0, Math.PI*2);
    rCtx.stroke();
  }
  rCtx.save();
  rCtx.translate(cx, cy);
  rCtx.rotate(rAngle);
  rCtx.beginPath();
  rCtx.moveTo(0,0);
  rCtx.arc(0,0,r,0,Math.PI/3.5);
  rCtx.fillStyle = 'rgba(0, 240, 255, 0.22)';
  rCtx.fill();
  rCtx.restore();

  radarBlips.forEach(b => {
    rCtx.beginPath();
    rCtx.arc(b.x, b.y, b.size, 0, Math.PI*2);
    rCtx.fillStyle = b.color;
    rCtx.fill();
    rCtx.fillStyle = '#fff';
    rCtx.font = '8px "JetBrains Mono"';
    rCtx.fillText(b.name, b.x + 4, b.y + 3);
  });
  rAngle += 0.04;
  requestAnimationFrame(drawMasterRadar);
}
drawMasterRadar();

setInterval(()=>{
  fetch('/api/status').then(r=>r.json()).then(d=>{
    const b = document.getElementById('mode-toggle');
    b.className = d.live_mode ? "mode-badge mode-live" : "mode-badge mode-demo";
    b.innerText = d.live_mode ? "LIVE TRADING" : "DEMO MODE";

    const curBal = d.live_mode ? d.live_balance : d.demo_balance;
    document.getElementById('v-bal').innerText = '$' + curBal.toFixed(2);
    
    const pnl = d.realized_pnl;
    const allEl = document.getElementById('v-all-pnl');
    allEl.innerText = (pnl >= 0 ? '+$' : '-$') + Math.abs(pnl).toFixed(2);
    allEl.style.color = pnl >= 0 ? "var(--green)" : "var(--red)";

    document.getElementById('v-1h-pnl').innerText = (d.pnl_1h >= 0 ? '+$' : '-$') + Math.abs(d.pnl_1h).toFixed(2);
    document.getElementById('v-24h-pnl').innerText = (d.pnl_24h >= 0 ? '+$' : '-$') + Math.abs(d.pnl_24h).toFixed(2);
    document.getElementById('v-tot-buy').innerText = d.total_buys;
    document.getElementById('v-tot-sell').innerText = d.total_sells;

    const winRate = d.total_sells > 0 ? ((d.wins / d.total_sells) * 100).toFixed(1) : "0";
    document.getElementById('v-winrate').innerText = winRate + '%';

    let hudHtml = '';
    let newBlips = [];
    const symList = Object.keys(d.markets);

    symList.forEach((sym, idx) => {
      const m = d.markets[sym];
      const pEl = document.getElementById('tab-price-' + sym);
      if(pEl) pEl.innerText = '$' + (m.price >= 1 ? m.price.toFixed(2) : m.price.toFixed(4));
      
      const ords = m.dca_orders || [];
      const inTrade = ords.length > 0;

      const sEl = document.getElementById('tab-st-' + sym);
      if(sEl) {
        sEl.innerText = inTrade ? "TRADING" : (m.running ? "ACTIVE" : "OFF");
        sEl.style.color = inTrade ? "var(--cyan)" : (m.running ? "var(--green)" : "var(--text-dim)");
      }
      const dcaEl = document.getElementById('tab-dca-' + sym);
      if(dcaEl) dcaEl.innerText = 'DCA: ' + ords.length + '/3';

      const a = (idx / symList.length) * Math.PI * 2;
      const dist = 28 + (idx % 3) * 22;
      newBlips.push({
        x: 100 + Math.cos(a) * dist,
        y: 100 + Math.sin(a) * dist,
        size: inTrade ? 4.5 : 3,
        color: inTrade ? '#00ff66' : '#00f0ff',
        name: sym.replace('USDT','')
      });

      hudHtml += `<div class="hud-coin-card">
        <div style="font-size:10px; font-weight:800; color:var(--cyan);">${sym.replace('USDT','')}</div>
        <div style="font-size:10px; color:#fff; margin:2px 0;">$${m.price < 1 ? m.price.toFixed(4) : m.price.toFixed(2)}</div>
        <div style="font-size:8px; color:${inTrade ? 'var(--green)' : 'var(--text-dim)'};">${inTrade ? 'DCA ('+ords.length+'/3)' : 'STANDBY'}</div>
      </div>`;
    });

    radarBlips = newBlips;
    document.getElementById('hudGrid').innerHTML = hudHtml;

    const cur = d.markets[activeSymbol];
    if(cur) {
      document.getElementById('buy-pct').innerText = cur.buyer_ratio + '%';
      document.getElementById('sell-pct').innerText = (100 - cur.buyer_ratio).toFixed(1) + '%';
      document.getElementById('buy-bar').style.width = cur.buyer_ratio + '%';

      const pTag = document.getElementById('active-pnl-label');
      const curOrds = cur.dca_orders || [];
      if(curOrds.length > 0) {
        const sign = cur.unrealized_pnl >= 0 ? '+' : '';
        pTag.innerText = `DCA (${curOrds.length}x): ${sign}${cur.unrealized_pnl}%`;
        pTag.style.color = cur.unrealized_pnl >= 0 ? "var(--green)" : "var(--red)";
      } else {
        pTag.innerText = "ওপেন ট্রেড: নাই";
        pTag.style.color = "var(--text-dim)";
      }

      if(cur.price > 0) {
        series.update({time: Math.floor(Date.now()/1000), open: cur.price, high: cur.price, low: cur.price, close: cur.price});
      }

      document.getElementById('bids-list').innerHTML = cur.bids.map(b=>`<tr><td style="color:var(--green)">$${(+b[0]).toFixed(b[0]>=1?2:4)}</td><td>${(+b[1]).toFixed(2)}</td></tr>`).join('');
      document.getElementById('asks-list').innerHTML = cur.asks.map(a=>`<tr><td style="color:var(--red)">$${(+a[0]).toFixed(a[0]>=1?2:4)}</td><td>${(+a[1]).toFixed(2)}</td></tr>`).join('');
    }

    document.getElementById('logs').innerHTML = d.logs.map(l=>`<div>${l}</div>`).join('');
    document.getElementById('hist').innerHTML = d.history.map(h=>`<tr><td>${h.time}</td><td style="color:var(--cyan);font-weight:bold">${h.symbol.replace('USDT','')}</td><td style="color:${h.side.includes('BUY')?'var(--green)':'var(--red)'}">${h.side}</td><td>${h.pnl}%</td></tr>`).join('');
  });
}, 1500);
</script>
</body>
</html>"""

class ReusableThreadingServer(ThreadingHTTPServer):
    allow_reuse_address = True

class CompleteRouter(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ['/', '/index.html']:
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(HTML_PAGE.encode('utf-8'))
        elif self.path == '/api/status':
            with lock:
                res = {
                    "live_mode": bot["live_mode"],
                    "demo_balance": bot["demo_balance"],
                    "live_balance": bot["live_balance"],
                    "total_buys": bot["total_buys"],
                    "total_sells": bot["total_sells"],
                    "wins": bot["wins"],
                    "realized_pnl": bot["realized_pnl"],
                    "pnl_1h": bot["pnl_1h"],
                    "pnl_24h": bot["pnl_24h"],
                    "history": list(bot["history"]),
                    "logs": list(bot["logs"]),
                    "markets": bot["markets"],
                    "last_radar_event": bot["last_radar_event"]
                }
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(res).encode('utf-8'))
        elif self.path.startswith('/api/klines'):
            query = self.path.split('?')
            sym = "BTCUSDT"
            if len(query) > 1 and "symbol=" in query[1]:
                sym = query[1].split('symbol=')[1].split('&')[0]
            with lock:
                klines = bot["klines_cache"].get(sym, [])
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(klines).encode('utf-8'))

    def do_POST(self):
        if self.path.startswith('/api/instant_buy'):
            query = self.path.split('?')
            sym = "BTCUSDT"
            if len(query) > 1 and "symbol=" in query[1]:
                sym = query[1].split('symbol=')[1].split('&')[0]
            with lock:
                if sym in bot["markets"]:
                    m = bot["markets"][sym]
                    p = m["price"]
                    amt = m["amount"]
                    if bot["live_mode"]:
                        execute_binance_order(sym, "BUY", quote_order_qty=amt)
                    m["dca_orders"].append({"price": p, "qty": amt / p, "cost": amt})
                    m["position"] = {"buy_price": p, "amount": amt}
                    bot["total_buys"] += 1
                    add_log(f"[{sym}] ম্যানুয়াল বাই @ ${p:.2f}")
                    threading.Thread(target=send_telegram, args=(f"⚡ [MANUAL BUY] {sym} @ ${p:.4f}",), daemon=True).start()
                    log_trade_db(sym, "BUY", p, 0.0, 0.0, "Manual Buy")
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"ok":true}')

        elif self.path.startswith('/api/instant_sell'):
            query = self.path.split('?')
            sym = "BTCUSDT"
            if len(query) > 1 and "symbol=" in query[1]:
                sym = query[1].split('symbol=')[1].split('&')[0]
            with lock:
                if sym in bot["markets"]:
                    m = bot["markets"][sym]
                    orders = m["dca_orders"]
                    if len(orders) > 0:
                        p = m["price"]
                        total_qty = sum(o["qty"] for o in orders)
                        total_cost = sum(o["cost"] for o in orders)
                        profit = (total_qty * p) - total_cost
                        diff = ((p - (total_cost / total_qty)) / (total_cost / total_qty)) * 100
                        if bot["live_mode"]:
                            execute_binance_order(sym, "SELL", quantity=total_qty)
                        bot["realized_pnl"] += profit
                        bot["total_sells"] += 1
                        add_log(f"[{sym}] ম্যানুয়াল সেল: {profit:+.2f}$")
                        threading.Thread(target=send_telegram, args=(f"🎯 [MANUAL SELL] {sym} @ ${p:.4f} PnL: {diff:+.2f}%",), daemon=True).start()
                        log_trade_db(sym, "SELL", p, round(diff, 2), round(profit, 4), "Manual Sell")
                        m["dca_orders"] = []
                        m["position"] = None
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"ok":true}')

        elif self.path.startswith('/api/toggle_no_loss'):
            query = self.path.split('?')
            sym = "BTCUSDT"
            val = True
            if len(query) > 1:
                params = urllib.parse.parse_qs(query[1])
                sym = params.get("symbol", ["BTCUSDT"])[0]
                val = params.get("val", ["true"])[0].lower() == "true"
            with lock:
                if sym in bot["markets"]:
                    bot["markets"][sym]["no_loss_mode"] = val
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"ok":true}')

        elif self.path.startswith('/api/toggle'):
            query = self.path.split('?')
            sym = "BTCUSDT"
            if len(query) > 1 and "symbol=" in query[1]:
                sym = query[1].split('symbol=')[1].split('&')[0]
            with lock:
                if sym in bot["markets"]:
                    bot["markets"][sym]["running"] = not bot["markets"][sym]["running"]
                    state = "চালু" if bot["markets"][sym]["running"] else "বন্ধ"
                    add_log(f"{sym} {state}")
                    res = {"running": bot["markets"][sym]["running"]}
                else:
                    res = {"running": False}
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(res).encode('utf-8'))

        elif self.path == '/api/toggle_mode':
            with lock:
                bot["live_mode"] = not bot["live_mode"]
                mode = "LIVE" if bot["live_mode"] else "DEMO"
                add_log(f"ট্রেডিং মোড: {mode}")
                res = {"live_mode": bot["live_mode"]}
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(res).encode('utf-8'))

        elif self.path == '/api/settings':
            sz = int(self.headers.get('Content-Length', 0))
            data = json.loads(self.rfile.read(sz).decode('utf-8'))
            sym = data.get("symbol", "BTCUSDT")
            with lock:
                if sym in bot["markets"]:
                    m = bot["markets"][sym]
                    m["amount"] = float(data.get("amount", m["amount"]))
                    m["stop_loss"] = float(data.get("stop_loss", m["stop_loss"]))
                    m["take_profit"] = float(data.get("take_profit", m["take_profit"]))
                    if "no_loss_mode" in data:
                        m["no_loss_mode"] = bool(data["no_loss_mode"])
                    add_log(f"[{sym}] সেটিংস সংরক্ষিত")
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"ok":true}')

    def log_message(self, *a):
        return

def run_server():
    print(f"\nTerminal Listening on 0.0.0.0:{PORT}\n", flush=True)
    srv = ReusableThreadingServer((HOST, PORT), CompleteRouter)
    srv.serve_forever()

if __name__ == "__main__":
    run_server()
