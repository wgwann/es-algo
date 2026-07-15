import MetaTrader5 as mt5
import time
import threading
import json
import os
from datetime import datetime, timezone

# ── Per-symbol config ─────────────────────────────────────────
SYMBOL_CONFIG = {
    "US100": {
        "symbol":    "US100",
        "lot":       float(os.environ.get("LOT_US100", "0.01")),
        "sl_points": 1500,
        "tp_points": 2000,
        "be_points": 1000,   # breakeven trigger
    },
    "XAUUSD": {
        "symbol":    "XAUUSD",
        "lot":       float(os.environ.get("LOT_XAUUSD", "0.01")),
        "sl_points": 250,
        "tp_points": 300,
        "be_points": 100,
    },
}

MAGIC         = 20240001
COOLDOWN_SECS = 60
DEVIATION     = 20
LOG_FILE      = "trades.log"

_lock      = threading.Lock()
_last_ts   = 0.0
_last_dir  = None
_last_res  = None
_mt5_ready = False


def _now():
    return datetime.now(timezone.utc).strftime("%H:%M:%S UTC")


def _log_trade(entry: dict):
    line = json.dumps(entry)
    print(f"  TRADE_LOG: {line}", flush=True)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass


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
    for cfg in SYMBOL_CONFIG.values():
        mt5.symbol_select(cfg["symbol"], True)
    print(f"[{_now()}] ✅ MT5 connected | "
          f"Account: {info.login} | "
          f"Balance: ${info.balance:.2f} | "
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


def get_open_position(symbol: str):
    """Returns the first open position for this symbol, or None."""
    positions = mt5.positions_get(symbol=symbol)
    if positions:
        return positions[0]
    return None


def close_position(pos, symbol_cfg: dict) -> dict:
    """Close an existing position immediately."""
    sym  = mt5.symbol_info(pos.symbol)
    tick = mt5.symbol_info_tick(pos.symbol)
    if sym is None or tick is None:
        return {"ok": False, "error": "symbol info failed on close"}

    # Opposite action to close
    if pos.type == mt5.ORDER_TYPE_BUY:
        close_type  = mt5.ORDER_TYPE_SELL
        close_price = tick.bid
    else:
        close_type  = mt5.ORDER_TYPE_BUY
        close_price = tick.ask

    req = {
        "action":       mt5.TRADE_ACTION_DEAL,
        "symbol":       pos.symbol,
        "volume":       pos.volume,
        "type":         close_type,
        "position":     pos.ticket,
        "price":        close_price,
        "deviation":    DEVIATION,
        "magic":        MAGIC,
        "comment":      "es-algo reverse close",
        "type_time":    mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_FOK,
    }
    result = mt5.order_send(req)
    if result and result.retcode == mt5.TRADE_RETCODE_DONE:
        print(f"[{_now()}] ✅ Closed position ticket={pos.ticket} "
              f"profit={pos.profit:.2f}", flush=True)
        _log_trade({
            "event":   "close",
            "ticket":  pos.ticket,
            "symbol":  pos.symbol,
            "profit":  pos.profit,
            "reason":  "direction_reverse",
            "time":    _now(),
        })
        return {"ok": True, "ticket": pos.ticket, "profit": pos.profit}
    rc = result.retcode if result else mt5.last_error()
    cm = result.comment if result else ""
    print(f"[{_now()}] ❌ Close failed retcode={rc} {cm}", flush=True)
    return {"ok": False, "retcode": rc, "comment": cm}


def fire(action: str, reason: str, mt5_symbol: str) -> dict:
    global _last_ts, _last_dir, _last_res

    with _lock:
        if not can_fire():
            return {"ok": False, "skipped": True,
                    "reason": f"cooldown {cooldown_remaining()}s"}
        if action not in ("buy", "sell"):
            return {"ok": False, "error": f"invalid action: {action}"}
        _last_ts  = time.time()
        _last_dir = action

    print(f"[{_now()}] SIGNAL {action.upper()} | {mt5_symbol} | {reason}",
          flush=True)

    if not _ensure_mt5():
        res = {"ok": False, "error": "MT5 not connected"}
        _last_res = res
        return res

    cfg  = SYMBOL_CONFIG.get(mt5_symbol)
    if cfg is None:
        res = {"ok": False, "error": f"unknown symbol: {mt5_symbol}"}
        _last_res = res
        return res

    # ── Check existing position ───────────────────────────────
    existing = get_open_position(mt5_symbol)
    if existing:
        existing_is_buy = existing.type == mt5.ORDER_TYPE_BUY
        signal_is_buy   = action == "buy"

        if existing_is_buy == signal_is_buy:
            # Same direction — ignore
            msg = (f"ignored: already have {'buy' if existing_is_buy else 'sell'} "
                   f"on {mt5_symbol}")
            print(f"[{_now()}] ⏭  {msg}", flush=True)
            res = {"ok": False, "skipped": True, "reason": msg}
            _last_res = res
            return res
        else:
            # Opposite direction — close existing first
            print(f"[{_now()}] 🔄 Reversing position on {mt5_symbol}", flush=True)
            close_result = close_position(existing, cfg)
            if not close_result["ok"]:
                res = {"ok": False, "error": "failed to close existing position"}
                _last_res = res
                return res
            time.sleep(0.5)   # brief pause before opening new

    # ── Open new position ─────────────────────────────────────
    tick = mt5.symbol_info_tick(mt5_symbol)
    sym  = mt5.symbol_info(mt5_symbol)
    if tick is None or sym is None:
        res = {"ok": False,
               "error": f"symbol info failed: {mt5.last_error()}"}
        _last_res = res
        return res

    spread      = sym.ask - sym.bid
    lot         = cfg["lot"]
    sl_pts      = cfg["sl_points"]
    tp_pts      = cfg["tp_points"]

    if action == "buy":
        order_type = mt5.ORDER_TYPE_BUY
        price      = tick.ask
        sl_price   = round(price - sl_pts * sym.point, sym.digits)
        tp_price   = round(price + tp_pts * sym.point, sym.digits)
    else:
        order_type = mt5.ORDER_TYPE_SELL
        price      = tick.bid
        sl_price   = round(price + sl_pts * sym.point, sym.digits)
        tp_price   = round(price - tp_pts * sym.point, sym.digits)

    req = {
        "action":       mt5.TRADE_ACTION_DEAL,
        "symbol":       mt5_symbol,
        "volume":       lot,
        "type":         order_type,
        "price":        price,
        "sl":           sl_price,
        "tp":           tp_price,
        "deviation":    DEVIATION,
        "magic":        MAGIC,
        "comment": f"fabs {action}",  # MT5 comment max 31 chars
        "type_time":    mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_FOK,
    }

    print(f"  {mt5_symbol} {action.upper()} "
          f"price={price} sl={sl_price} tp={tp_price} lot={lot}",
          flush=True)

    result = mt5.order_send(req)

    if result is None:
        res = {"ok": False,
               "error": f"order_send None: {mt5.last_error()}"}
        _last_res = res
        print(f"  ❌ {res['error']}", flush=True)
        return res

    if result.retcode == mt5.TRADE_RETCODE_DONE:
        res = {
            "ok":       True,
            "action":   action,
            "symbol":   mt5_symbol,
            "ticket":   result.order,
            "price":    result.price,
            "volume":   result.volume,
            "sl":       sl_price,
            "tp":       tp_price,
            "spread":   spread,
            "retcode":  result.retcode,
            "reason":   reason,
            "fired_at": _now(),
        }
        print(f"  ✅ {action.upper()} {mt5_symbol} "
              f"ticket={result.order} price={result.price}", flush=True)
        _log_trade(res)
    else:
        res = {
            "ok":       False,
            "symbol":   mt5_symbol,
            "retcode":  result.retcode,
            "comment":  result.comment,
            "fired_at": _now(),
        }
        print(f"  ❌ retcode={result.retcode} "
              f"comment={result.comment}", flush=True)
        _log_trade(res)

    _last_res = res
    return res


def check_breakeven():
    """
    Called periodically. For each open position, if price has
    moved be_points in trade direction, move SL to entry + spread
    to ensure breakeven after spread cost.
    """
    if not _mt5_ready:
        return
    for mt5_symbol, cfg in SYMBOL_CONFIG.items():
        positions = mt5.positions_get(symbol=mt5_symbol)
        if not positions:
            continue
        for pos in positions:
            if pos.magic != MAGIC:
                continue

            sym  = mt5.symbol_info(mt5_symbol)
            tick = mt5.symbol_info_tick(mt5_symbol)
            if sym is None or tick is None:
                continue

            be_pts = cfg["be_points"]
            spread = sym.ask - sym.bid

            if pos.type == mt5.ORDER_TYPE_BUY:
                current_price = tick.bid
                entry         = pos.price_open
                profit_pts    = (current_price - entry) / sym.point
                # Breakeven SL = entry + spread (so we don't lose to spread)
                be_sl = round(entry + spread, sym.digits)
                # Only move if: profit threshold reached AND
                # current SL is still below breakeven SL
                if profit_pts >= be_pts and pos.sl < be_sl:
                    _move_sl(pos, be_sl, mt5_symbol)

            else:  # SELL
                current_price = tick.ask
                entry         = pos.price_open
                profit_pts    = (entry - current_price) / sym.point
                # Breakeven SL = entry - spread
                be_sl = round(entry - spread, sym.digits)
                if profit_pts >= be_pts and (pos.sl == 0 or pos.sl > be_sl):
                    _move_sl(pos, be_sl, mt5_symbol)


def _move_sl(pos, new_sl: float, mt5_symbol: str):
    req = {
        "action":   mt5.TRADE_ACTION_SLTP,
        "symbol":   mt5_symbol,
        "position": pos.ticket,
        "sl":       new_sl,
        "tp":       pos.tp,
    }
    result = mt5.order_send(req)
    if result and result.retcode == mt5.TRADE_RETCODE_DONE:
        print(f"[{_now()}] 🔒 Breakeven set for ticket={pos.ticket} "
              f"new_sl={new_sl}", flush=True)
        _log_trade({
            "event":  "breakeven",
            "ticket": pos.ticket,
            "symbol": mt5_symbol,
            "new_sl": new_sl,
            "time":   _now(),
        })
    else:
        rc = result.retcode if result else mt5.last_error()
        print(f"[{_now()}] ⚠️ Breakeven move failed: {rc}", flush=True)


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
