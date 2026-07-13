import MetaTrader5 as mt5

if not mt5.initialize():
    print(f"❌ init failed: {mt5.last_error()}")
    quit()

mt5.symbol_select("US500", True)
sym  = mt5.symbol_info("US500")
tick = mt5.symbol_info_tick("US500")

print(f"ask={tick.ask} bid={tick.bid} point={sym.point} digits={sym.digits}")

filling_modes = [
    (mt5.ORDER_FILLING_IOC,    "IOC"),
    (mt5.ORDER_FILLING_RETURN, "RETURN"),
    (mt5.ORDER_FILLING_FOK,    "FOK"),
]

for mode, name in filling_modes:
    req = {
        "action":       mt5.TRADE_ACTION_DEAL,
        "symbol":       "US500",
        "volume":       sym.volume_min,
        "type":         mt5.ORDER_TYPE_BUY,
        "price":        tick.ask,
        "sl":           round(tick.ask - 100 * sym.point, sym.digits),
        "tp":           round(tick.ask + 200 * sym.point, sym.digits),
        "deviation":    20,
        "magic":        20240001,
        "comment":      f"test {name}",
        "type_time":    mt5.ORDER_TIME_GTC,
        "type_filling": mode,
    }
    result = mt5.order_send(req)
    if result and result.retcode == mt5.TRADE_RETCODE_DONE:
        print(f"✅ {name} works! ticket={result.order}")
        # Close it immediately
        import time; time.sleep(1)
        pos = mt5.positions_get(symbol="US500")
        if pos:
            cr = {
                "action":       mt5.TRADE_ACTION_DEAL,
                "symbol":       "US500",
                "volume":       pos[0].volume,
                "type":         mt5.ORDER_TYPE_SELL,
                "position":     pos[0].ticket,
                "price":        mt5.symbol_info_tick("US500").bid,
                "deviation":    20,
                "magic":        20240001,
                "comment":      "test close",
                "type_time":    mt5.ORDER_TIME_GTC,
                "type_filling": mode,
            }
            cr2 = mt5.order_send(cr)
            print(f"  Closed: retcode={cr2.retcode}")
        break
    else:
        rc = result.retcode if result else "None"
        cm = result.comment if result else mt5.last_error()
        print(f"❌ {name}: retcode={rc} comment={cm}")

mt5.shutdown()
