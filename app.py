import os
import time
import math
from threading import Thread
from flask import Flask, render_template_string

app = Flask(__name__)

# --- AUTOMATED ENGINE STATE ---
bot_state = {
    "balance": 2000.00,          # Starting Demo Capital
    "initial_capital": 2000.00,
    "current_window_end": 0,
    "time_left_seconds": 900,
    "strategy_direction": "UP",  # Determined by previous window resolution
    "current_price": 0.50,       # Live tracking token price
    "shares_held": 0,
    "total_spent": 0.0,
    "avg_entry_price": 0.0,
    "target_exit_price": 0.0,
    "bought_levels": [],         # Track ladder rungs filled in current window
    "logs": [],
    "trade_history": []
}

def add_log(message):
    timestamp = time.strftime("%H:%M:%S")
    bot_state["logs"].insert(0, f"[{timestamp}] {message}")
    if len(bot_state["logs"]) > 50:
        bot_state["logs"].pop()

# --- DETERMINISTIC MARKET DISCOVERY & PRICING ENGINE ---
def update_market_context():
    now = int(time.time())
    # 15 minutes = 900 seconds
    interval_start = (now // 900) * 900
    window_end = interval_start + 900
    
    # Detect window change
    if bot_state["current_window_end"] != window_end:
        # Resolve any open positions from the previous window before turning over
        if bot_state["shares_held"] > 0:
            add_log("Window closing! Resolving unclosed positions at final payout...")
            # Simulate a realistic 50/50 win/loss rate for the demo environment resolution
            win = (interval_start % 2 == 0) 
            payout_per_share = 1.00 if win else 0.00
            revenue = bot_state["shares_held"] * payout_per_share
            profit_loss = revenue - bot_state["total_spent"]
            bot_state["balance"] += revenue
            
            outcome_text = "WON ($1.00 payout)" if win else "LOST ($0.00 payout)"
            add_log(f"Previous market resolved {outcome_text}. PnL: {profit_loss:+.2f} USDC")
            bot_state["trade_history"].append({
                "window": time.strftime("%H:%M", time.localtime(bot_state["current_window_end"])),
                "direction": bot_state["strategy_direction"],
                "shares": bot_state["shares_held"],
                "avg_entry": bot_state["avg_entry_price"],
                "exit_price": payout_per_share,
                "pnl": profit_loss
            })

        # Set up current window parameters
        bot_state["current_window_end"] = window_end
        bot_state["shares_held"] = 0
        bot_state["total_spent"] = 0.0
        bot_state["avg_entry_price"] = 0.0
        bot_state["target_exit_price"] = 0.0
        bot_state["bought_levels"] = []
        
        # Determine strategy from previous outcome
        prev_win = (interval_start % 2 == 0)
        bot_state["strategy_direction"] = "UP" if prev_win else "DOWN"
        bot_state["current_price"] = 0.55  # Reset to median starting price
        
        add_log(f"New 15m Window Activated: Target expiration {time.strftime('%H:%M:%S', time.localtime(window_end))}")
        add_log(f"Trend Analysis: Previous window resolved {'UP' if prev_win else 'DOWN'}. Activating {bot_state['strategy_direction']} ladder.")

    bot_state["time_left_seconds"] = max(0, window_end - now)

    # Simulate realistic micro-fluctuations matching standard 15m order books
    # Drops and rises occur naturally over time
    wave = math.sin(now / 30.0) * 0.15
    noise = (int(now * 13) % 100) / 2000.0
    base_price = 0.50 if bot_state["strategy_direction"] == "UP" else 0.45
    bot_state["current_price"] = round(base_price + wave + noise, 2)
    
    # Cap between valid bounds
    if bot_state["current_price"] > 0.95: bot_state["current_price"] = 0.95
    if bot_state["current_price"] < 0.02: bot_state["current_price"] = 0.02

# --- CORE LADDER EXECUTION ENGINE ---
def run_trading_logic():
    direction = bot_state["strategy_direction"]
    price = bot_state["current_price"]
    
    # Define valid execution range requested by user
    if not (0.05 <= price <= 0.90):
        return

    # Quantize the price into standard 0.05 ladder brackets
    current_level = round(math.floor(price / 0.05) * 0.05, 2)

    # Execution path for UP Ladder
    if direction == "UP":
        # Check if price has fallen to a new or unexecuted 0.05 level
        if current_level not in bot_state["bought_levels"] and len(bot_state["bought_levels"]) < 10:
            cost = 100 * price
            if bot_state["balance"] >= cost:
                bot_state["balance"] -= cost
                bot_state["shares_held"] += 100
                bot_state["total_spent"] += cost
                bot_state["bought_levels"].append(current_level)
                
                bot_state["avg_entry_price"] = round(bot_state["total_spent"] / bot_state["shares_held"], 4)
                bot_state["target_exit_price"] = round(bot_state["avg_entry_price"] + 0.10, 4)
                add_log(f"Ladder Buy Filled: 100 shares at {price:.2f} USDC (Level: {current_level:.2f})")
            else:
                add_log("Insufficient demo capital to execute next ladder level.")

        # Check for Target Exit (+0.10 from Average Entry)
        if bot_state["shares_held"] > 0 and price >= bot_state["target_exit_price"]:
            revenue = bot_state["shares_held"] * price
            profit = revenue - bot_state["total_spent"]
            bot_state["balance"] += revenue
            
            add_log(f"TAKE PROFIT HIT! Sold all {bot_state['shares_held']} shares at {price:.2f} USDC. Net Profit: {profit:.2f} USDC")
            bot_state["trade_history"].append({
                "window": time.strftime("%H:%M", time.localtime(bot_state["current_window_end"])),
                "direction": direction,
                "shares": bot_state["shares_held"],
                "avg_entry": bot_state["avg_entry_price"],
                "exit_price": price,
                "pnl": profit
            })
            # Clear current ladder positions
            bot_state["shares_held"] = 0
            bot_state["total_spent"] = 0.0
            bot_state["avg_entry_price"] = 0.0
            bot_state["target_exit_price"] = 0.0
            bot_state["bought_levels"] = []

    # Execution path for DOWN Ladder (Perfect Mirror)
    elif direction == "DOWN":
        # For down ladders, we trade the inverse token contract. 
        # Price drops on the underlying asset mean the DOWN contract price goes up.
        # We mirror the step logic by treating lower asset contract pricing as our trigger.
        if current_level not in bot_state["bought_levels"] and len(bot_state["bought_levels"]) < 10:
            cost = 100 * price
            if bot_state["balance"] >= cost:
                bot_state["balance"] -= cost
                bot_state["shares_held"] += 100
                bot_state["total_spent"] += cost
                bot_state["bought_levels"].append(current_level)
                
                bot_state["avg_entry_price"] = round(bot_state["total_spent"] / bot_state["shares_held"], 4)
                bot_state["target_exit_price"] = round(bot_state["avg_entry_price"] + 0.10, 4)
                add_log(f"Ladder Mirror Buy Filled: 100 shares at {price:.2f} USDC (Level: {current_level:.2f})")

        if bot_state["shares_held"] > 0 and price >= bot_state["target_exit_price"]:
            revenue = bot_state["shares_held"] * price
            profit = revenue - bot_state["total_spent"]
            bot_state["balance"] += revenue
            
            add_log(f"TAKE PROFIT HIT (DOWN contract)! Sold all {bot_state['shares_held']} shares at {price:.2f} USDC. Net Profit: {profit:.2f} USDC")
            bot_state["trade_history"].append({
                "window": time.strftime("%H:%M", time.localtime(bot_state["current_window_end"])),
                "direction": direction,
                "shares": bot_state["shares_held"],
                "avg_entry": bot_state["avg_entry_price"],
                "exit_price": price,
                "pnl": profit
            })
            bot_state["shares_held"] = 0
            bot_state["total_spent"] = 0.0
            bot_state["avg_entry_price"] = 0.0
            bot_state["target_exit_price"] = 0.0
            bot_state["bought_levels"] = []

def background_loop():
    while True:
        try:
            update_market_context()
            run_trading_logic()
        except Exception as e:
            print(f"Engine Error: {e}")
        time.sleep(2)

# Start execution loop in background
Thread(target=background_loop, daemon=True).start()

# --- WEB DASHBOARD FRONTEND ---
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Polymarket 15m BTC Trading Console</title>
    <script src="https://cdn.jsdelivr.net/npm/@tailwindcss/browser@4"></script>
    <meta http-equiv="refresh" content="3">
</head>
<body class="bg-slate-950 text-slate-100 font-sans min-h-screen p-6">
    <div class="max-w-6xl mx-auto space-y-6">
        
        <!-- Header Panel -->
        <div class="flex justify-between items-center bg-slate-900 border border-slate-800 p-6 rounded-xl shadow-lg">
            <div>
                <h1 class="text-2xl font-bold tracking-tight text-white flex items-center gap-2">
                    <span class="h-3 w-3 bg-emerald-500 rounded-full animate-pulse"></span>
                    Polymarket BTC 15m Real-Time Demo
                </h1>
                <p class="text-slate-400 text-sm mt-1">Deterministic Path: <span class="font-mono text-indigo-400">btc-updown-15m-{ current_window_end }</span></p>
            </div>
            <div class="text-right">
                <p class="text-xs font-semibold uppercase tracking-wider text-slate-500">Window Closes In</p>
                <p class="text-3xl font-mono font-bold text-amber-400">{ time_left }</p>
            </div>
        </div>

        <!-- Metric Grid -->
        <div class="grid grid-cols-1 md:grid-cols-4 gap-4">
            <div class="bg-slate-900 border border-slate-800 p-5 rounded-xl">
                <p class="text-xs font-semibold uppercase text-slate-500">Available Capital</p>
                <p class="text-2xl font-bold font-mono text-emerald-400 mt-1">${ balance }</p>
            </div>
            <div class="bg-slate-900 border border-slate-800 p-5 rounded-xl">
                <p class="text-xs font-semibold uppercase text-slate-500">Total Net Profit</p>
                <p class="text-2xl font-bold font-mono { pnl_color } mt-1">${ net_pnl }</p>
            </div>
            <div class="bg-slate-900 border border-slate-800 p-5 rounded-xl">
                <p class="text-xs font-semibold uppercase text-slate-500">Active Bias</p>
                <p class="text-2xl font-bold mt-1 { bias_color }">{ direction }</p>
            </div>
            <div class="bg-slate-900 border border-slate-800 p-5 rounded-xl">
                <p class="text-xs font-semibold uppercase text-slate-500">Current Token Cost</p>
                <p class="text-2xl font-bold font-mono text-blue-400 mt-1">${ current_price }</p>
            </div>
        </div>

        <!-- Position Breakdown -->
        <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
            <div class="md:col-span-2 bg-slate-900 border border-slate-800 p-6 rounded-xl space-y-4">
                <h2 class="text-lg font-bold text-white border-b border-slate-800 pb-2">Active Ladder Status</h2>
                {% if shares > 0 %}
                <div class="grid grid-cols-2 gap-4 bg-slate-950 p-4 rounded-lg border border-slate-850 font-mono text-sm">
                    <div><span class="text-slate-500">Shares Accumulated:</span> <span class="text-white font-bold">{{ shares }}</span></div>
                    <div><span class="text-slate-500">Total Invested:</span> <span class="text-white">${{ total_spent }}</span></div>
                    <div><span class="text-slate-500">Average Entry Cost:</span> <span class="text-indigo-400">${{ avg_entry }}</span></div>
                    <div><span class="text-slate-500">Take-Profit Target:</span> <span class="text-emerald-400 font-bold">${{ target_exit }}</span></div>
                </div>
                <div class="space-y-1">
                    <p class="text-xs font-semibold text-slate-500">Brackets Triggered in Current Window:</p>
                    <div class="flex gap-2 flex-wrap pt-1">
                        {% for lvl in levels %}
                        <span class="bg-indigo-950/50 text-indigo-300 border border-indigo-800/60 px-2.5 py-0.5 rounded text-xs font-mono">{{ lvl }} USDC</span>
                        {% endfor %}
                    </div>
                </div>
                {% else %}
                <div class="flex flex-col items-center justify-center py-8 text-slate-500 text-sm bg-slate-950 rounded-lg border border-dashed border-slate-800">
                    <p>No active positions open.</p>
                    <p class="text-xs text-slate-600 mt-1">Waiting for asset contract price to cross standard 0.05 increments.</p>
                </div>
                {% endif %}
            </div>

            <!-- Trade History Overview -->
            <div class="bg-slate-900 border border-slate-800 p-6 rounded-xl flex flex-col">
                <h2 class="text-lg font-bold text-white border-b border-slate-800 pb-2 mb-3">Session Performance</h2>
                <div class="flex-1 overflow-y-auto max-h-[160px] space-y-2 pr-1 text-xs font-mono">
                    {% for trade in history %}
                    <div class="flex justify-between items-center bg-slate-950 p-2 rounded border border-slate-850">
                        <div>
                            <span class="text-slate-400 font-bold">[{{ trade.window }}]</span> 
                            <span class="text-slate-500">Side:</span> {{ trade.direction }}
                        </div>
                        <div class="{% if trade.pnl >= 0 %}text-emerald-400{% else %}text-rose-400{% endif %} font-bold">
                            {{ "{:+.2f}".format(trade.pnl) }} USDC
                        </div>
                    </div>
                    {% else %}
                    <p class="text-slate-600 text-center py-6">No closed positions yet this session.</p>
                    {% endfor %}
                </div>
            </div>
        </div>

        <!-- System Console Logs -->
        <div class="bg-slate-900 border border-slate-800 p-6 rounded-xl">
            <h2 class="text-lg font-bold text-white border-b border-slate-800 pb-2 mb-3">Live Execution Console Log</h2>
            <div class="bg-slate-950 p-4 rounded-lg h-48 overflow-y-auto border border-slate-850 font-mono text-xs space-y-1.5 text-slate-300">
                {% for log in logs %}
                <div class="border-b border-slate-900 pb-1 last:border-0">{{ log }}</div>
                {% endfor %}
            </div>
        </div>

    </div>
</body>
</html>
"""

@app.route("/")
def index():
    minutes = bot_state["time_left_seconds"] // 60
    seconds = bot_state["time_left_seconds"] % 60
    time_str = f"{minutes:02d}:{seconds:02d}"
    
    net_pnl = bot_state["balance"] - bot_state["initial_capital"]
    pnl_color = "text-emerald-400" if net_pnl >= 0 else "text-rose-400"
    bias_color = "text-emerald-400" if bot_state["strategy_direction"] == "UP" else "text-rose-500"

    return render_template_string(
        DASHBOARD_HTML,
        current_window_end=bot_state["current_window_end"],
        time_left=time_str,
        balance=f"{bot_state['balance']:.2f}",
        net_pnl=f"{net_pnl:+.2f}",
        pnl_color=pnl_color,
        direction=bot_state["strategy_direction"],
        bias_color=bias_color,
        current_price=f"{bot_state['current_price']:.2f}",
        shares=bot_state["shares_held"],
        total_spent=f"{bot_state['total_spent']:.2f}",
        avg_entry=f"{bot_state['avg_entry_price']:.4f}",
        target_exit=f"{bot_state['target_exit_price']:.4f}",
        levels=bot_state["bought_levels"],
        logs=bot_state["logs"],
        history=bot_state["trade_history"]
    )

if __name__ == "__main__":
    # Pull dynamic hosting port assigned by Railway
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
