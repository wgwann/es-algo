import MetaTrader5 as mt5
import time
import threading
from datetime import datetime, timezone

SYMBOL        = "US500"
LOT           = 0.01
SL_POINTS     = 100
TP_POINTS     = 200
MAGIC         = 20240001
COOLDOWN_SECS = 60
DEVIATION     = 20

_lock     = threading.Lock()
_last_ts  = 0.0
_last_dir = None
_last_res = None
_mt5_ready= False


def _now():
    return datetime.now(timezone.utc).strftime("%H:%M:%S UTC")


def init_mt5():
    global _mt5_ready
    if not mt5.initialize():
        print(f"[{_now()}] ❌ MT5 init failed: {mt5.last_error()}", flush=True)
        _mt5_ready = False
        return False
    info = mt5.account_info()
    if info is None:
        print(f"[{_now()}] ❌ account_info failed", flush=True)
        _mt5_ready = False
        return False
    mt5.symbol_select(SYMBOL, True)
    print(f"[{_now()}] ✅ MT5 connected | "
          f"Account: {info.login} | Balance: ${info.balance:.2f} | "
          f"Server: {info.server}", flush=True)
    _mt5_ready = True
    return True


def _ensure_mt5():
    global _mt5_ready
    if not _mt5_ready:
        return init_mt5()
    if mt5.account_info() is None:
        print(f"[{_now()}] MT5 dropped — reconnecting...", flush=True)
        mt5.shutdown()
        time.sleep(1)
        return init_mt5()
    return True


def can_fire():
    return (time.time() - _last_ts) >= COOLDOWN_SECS


def cooldown_remaining():
    return max(0, int(COOLDOWN_SECS - (time.time() - _last_ts)))


def _send_order(request):
    """
    Try all three filling modes in sequence.
    FBS typically uses FILLING_IOC or FILLING_RETURN.
    Returns the first successful result or the last failure.
    """
    filling_modes = [
        mt5.ORDER_FILLING_IOC,
        mt5.ORDER_FILLING_RETURN,
        mt5.ORDER_FILLING_FOK,
    ]
    filling_names = ["IOC", "RETURN", "FOK"]

    last_result = None
    for mode, name in zip(filling_modes, filling_names):
        request["type_filling"] = mode
        result = mt5.order_send(request)
        if result is None:
            print(f"  [{name}] order_send returned None: "
                  f"{mt5.last_error()}", flush=True)
            last_result = None
            continue
        if result.retcode == mt5.TRADE_RETCODE_DONE:
            print(f"  ✅ [{name}] filling mode works!", flush=True)
            return result
        print(f"  [{name}] retcode={result.retcode} "
              f"comment={result.comment}", flush=True)
        last_result = result
        # Only retry on filling-related errors
        if result.retcode not in (
            mt5.TRADE_RETCODE_INVALID_FILL,
            10030,   # unsupported filling mode
        ):
            return result   # different error — no point retrying modes
    return last_result


def fire(action: str, reason: str) -> dict:
    global _last_ts, _last_dir, _last_res

    with _lock:
        if not can_fire():
            return {"ok": False, "skipped": True,
                    "reason": f"cooldown {cooldown_remaining()}s"}
        if action not in ("buy", "sell"):
            return {"ok": False, "error": f"invalid action: {action}"}
        _last_ts  = time.time()
        _last_dir = action

    print(f"[{_now()}] FIRING {action.upper()} | {reason}", flush=True)

    if not _ensure_mt5():
        res = {"ok": False, "error": "MT5 not connected"}
        _last_res = res
        return res

    tick = mt5.symbol_info_tick(SYMBOL)
    sym  = mt5.symbol_info(SYMBOL)
    if tick is None or sym is None:
        res = {"ok": False,
               "error": f"symbol info failed: {mt5.last_error()}"}
        _last_res = res
        return res

    if action == "buy":
        order_type = mt5.ORDER_TYPE_BUY
        price      = tick.ask
        sl_price   = round(price - SL_POINTS * sym.point, sym.digits)
        tp_price   = round(price + TP_POINTS * sym.point, sym.digits)
    else:
        order_type = mt5.ORDER_TYPE_SELL
        price      = tick.bid
        sl_price   = round(price + SL_POINTS * sym.point, sym.digits)
        tp_price   = round(price - TP_POINTS * sym.point, sym.digits)

    request = {
        "action":    mt5.TRADE_ACTION_DEAL,
        "symbol":    SYMBOL,
        "volume":    LOT,
        "type":      order_type,
        "price":     price,
        "sl":        sl_price,
        "tp":        tp_price,
        "deviation": DEVIATION,
        "magic":     MAGIC,
        "comment":   f"es-algo {action}",
        "type_time": mt5.ORDER_TIME_GTC,
        # type_filling set inside _send_order
    }

    print(f"  price={price} sl={sl_price} tp={tp_price} "
          f"lot={LOT}", flush=True)

    result = _send_order(request)

    if result is None:
        res = {"ok": False,
               "error": f"all filling modes failed: {mt5.last_error()}"}
        _last_res = res
        print(f"  ❌ {res['error']}", flush=True)
        return res

    if result.retcode == mt5.TRADE_RETCODE_DONE:
        res = {
            "ok":       True,
            "action":   action,
            "ticket":   result.order,
            "price":    result.price,
            "volume":   result.volume,
            "retcode":  result.retcode,
            "fired_at": _now(),
        }
        print(f"  ✅ {action.upper()} ticket={result.order} "
              f"price={result.price}", flush=True)
    else:
        res = {
            "ok":       False,
            "retcode":  result.retcode,
            "comment":  result.comment,
            "fired_at": _now(),
        }
        print(f"  ❌ All modes failed. Last: "
              f"retcode={result.retcode} "
              f"comment={result.comment}", flush=True)

    _last_res = res
    return res


def last_info():
    return {
        "last_ts":            _last_ts,
        "last_dir":           _last_dir,
        "last_result":        _last_res,
        "cooldown_remaining": cooldown_remaining(),
        "can_fire":           can_fire(),
        "mt5_ready":          _mt5_ready,
    }


def shutdown():
    mt5.shutdown()
    print(f"[{_now()}] MT5 shutdown", flush=True)
