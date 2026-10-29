#!/usr/bin/env python3
import json
import time
import threading
import sqlite3
import urllib.parse
from datetime import datetime
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
    "live_mode": True,
    "demo_balance": 1000.0,
    "live_balance": 0.0,
    "total_buys": 0,
    "total_sells": 0,
    "wins": 0,
    "realized_pnl": 0.0,
    "pnl_1h": 0.0,
    "pnl_24h": 0.0,
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
                        add_log(f"[API ত্রুটি] {err}")
                        last_err = err
                else:
                    bot["live_balance"] = bal
                    if last_err:
                        add_log(f"[লাইভ ব্যালেন্স] ${bal:.2f}")
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
                        add_log(f"[{symbol}] 🟢 DCA বাই #{len(orders)} @ ${price:.4f}")
                        send_telegram(f"⚡ [DCA BUY #{len(orders)}] {symbol} Price: ${price:.4f}")

                    elif is_tp or is_sl:
                        revenue = total_qty * price
                        profit = revenue - total_cost
                        side_name = "TAKE PROFIT" if diff > 0 else "STOP LOSS"

                        if bot["live_mode"]:
                            execute_binance_order(symbol, "SELL", quantity=total_qty)
                        else:
                            bot["demo_balance"] += profit

                        bot["realized_pnl"] += profit
                        bot["total_sells"] += 1
                        if profit > 0:
                            bot["wins"] += 1

                        add_log(f"[{symbol}] 🔴 {side_name}: {profit:+.2f}$ ({diff:+.2f}%)")
                        send_telegram(f"🎯 [{side_name}] {symbol} Exit: ${price:.4f} PnL: {diff:+.2f}%")
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
                        add_log(f"[{symbol}] 🟢 বেস বাই @ ${price:.4f}")
                        send_telegram(f"⚡ [BASE BUY] {symbol} Price: ${price:.4f}")
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

class ReusableThreadingServer(ThreadingHTTPServer):
    allow_reuse_address = True

class CompleteRouter(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ['/', '/index.html']:
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            try:
                with open("index.html", "rb") as f:
                    self.wfile.write(f.read())
            except Exception:
                self.wfile.write(b"Error loading index.html")
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
                    "markets": bot["markets"]
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
                    send_telegram(f"⚡ [MANUAL BUY] {sym} @ ${p:.4f}")
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
                        else:
                            bot["demo_balance"] += profit
                        bot["realized_pnl"] += profit
                        bot["total_sells"] += 1
                        add_log(f"[{sym}] ম্যানুয়াল সেল: {profit:+.2f}$")
                        send_telegram(f"🎯 [MANUAL SELL] {sym} @ ${p:.4f} PnL: {diff:+.2f}%")
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
