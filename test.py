import MetaTrader5 as mt5
import time
from datetime import datetime

print("="*50)
print("MT5 CONNECTION TEST")
print("="*50)

# ── Connect ───────────────────────────────────────────
if not mt5.initialize():
    print(f"❌ initialize() failed: {mt5.last_error()}")
    quit()

print("✅ MT5 initialized")

# ── Account info ──────────────────────────────────────
info = mt5.account_info()
if info is None:
    print(f"❌ account_info() failed: {mt5.last_error()}")
    mt5.shutdown()
    quit()

print(f"\n  Account:  {info.login}")
print(f"  Name:     {info.name}")
print(f"  Server:   {info.server}")
print(f"  Balance:  ${info.balance:.2f}")
print(f"  Equity:   ${info.equity:.2f}")
print(f"  Currency: {info.currency}")
print(f"  Leverage: 1:{info.leverage}")

# ── Symbol info ───────────────────────────────────────
SYMBOL = "US500"
print(f"\n  Checking symbol: {SYMBOL}")

# Ensure symbol is visible in Market Watch
if not mt5.symbol_select(SYMBOL, True):
    print(f"❌ symbol_select failed: {mt5.last_error()}")
    mt5.shutdown()
    quit()

sym = mt5.symbol_info(SYMBOL)
if sym is None:
    print(f"❌ symbol_info failed: {mt5.last_error()}")
    mt5.shutdown()
    quit()

print(f"  Bid:      {sym.bid:.2f}")
print(f"  Ask:      {sym.ask:.2f}")
print(f"  Spread:   {sym.spread} points")
print(f"  Min lot:  {sym.volume_min}")
print(f"  Lot step: {sym.volume_step}")
print(f"  Point:    {sym.point}")

# ── Place test BUY order ──────────────────────────────
print(f"\n  Placing test BUY order (min size)...")

price    = sym.ask
lot      = sym.volume_min
sl_price = price - 100 * sym.point
tp_price = price + 200 * sym.point

request = {
    "action":     mt5.TRADE_ACTION_DEAL,
    "symbol":     SYMBOL,
    "volume":     lot,
    "type":       mt5.ORDER_TYPE_BUY,
    "price":      price,
    "sl":         round(sl_price, sym.digits),
    "tp":         round(tp_price, sym.digits),
    "deviation":  20,
    "magic":      20240001,
    "comment":    "es-algo test",
    "type_time":  mt5.ORDER_TIME_GTC,
    "type_filling": mt5.ORDER_FILLING_IOC,
}

result = mt5.order_send(request)

if result is None:
    print(f"❌ order_send returned None: {mt5.last_error()}")
    mt5.shutdown()
    quit()

print(f"  order_send retcode: {result.retcode}")

if result.retcode == mt5.TRADE_RETCODE_DONE:
    ticket = result.order
    print(f"✅ BUY order placed! Ticket: {ticket}")
    print(f"  Price:  {result.price}")
    print(f"  Volume: {result.volume}")

    # ── Wait 3 seconds then close ─────────────────────
    print(f"\n  Waiting 3 seconds then closing...")
    time.sleep(3)

    # Get current position
    positions = mt5.positions_get(symbol=SYMBOL)
    if not positions:
        print("  No open positions found (may have been filled differently)")
    else:
        pos = positions[0]
        print(f"  Open position found: ticket={pos.ticket} profit={pos.profit:.2f}")

        close_price = mt5.symbol_info_tick(SYMBOL).bid
        close_req = {
            "action":     mt5.TRADE_ACTION_DEAL,
            "symbol":     SYMBOL,
            "volume":     pos.volume,
            "type":       mt5.ORDER_TYPE_SELL,
            "position":   pos.ticket,
            "price":      close_price,
            "deviation":  20,
            "magic":      20240001,
            "comment":    "es-algo test close",
            "type_time":  mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }
        close_result = mt5.order_send(close_req)
        if close_result and close_result.retcode == mt5.TRADE_RETCODE_DONE:
            print(f"✅ Position closed. P&L: ${pos.profit:.2f}")
        else:
            print(f"⚠️ Close failed: {close_result}")
else:
    print(f"❌ Order failed")
    print(f"  retcode: {result.retcode}")
    print(f"  comment: {result.comment}")
    print(f"\n  Common retcodes:")
    print(f"  10004 = Requote (use deviation)")
    print(f"  10006 = Request rejected")
    print(f"  10014 = Invalid volume")
    print(f"  10015 = Invalid price")
    print(f"  10018 = Market closed")
    print(f"  10030 = Invalid fill")

mt5.shutdown()
print(f"\nMT5 shutdown. Test complete.")
