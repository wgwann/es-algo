import websocket
import json
import time
import threading
import uuid
import os
from collections import deque
from datetime import datetime, timezone, timedelta

INFOWAY_KEY = os.environ.get("INFOWAY_API_KEY", "")

# ── Session schedule (all times ET = UTC-4 in summer) ─────────
# London open → 10:00 AM ET  : trade GC1!  → XAUUSD
# 10:00 AM ET gap (30 min)
# 10:30 AM ET → 3:30 PM ET   : trade ES1!  → US100
# Outside these windows       : no trading
GC_SESSION_START_ET  = (3,  0)   # 3:00 AM ET (London open)
GC_SESSION_END_ET    = (9, 55)   # 9:55 AM ET
ES_SESSION_START_ET  = (10, 0)  # 10:00 AM ET
ES_SESSION_END_ET    = (15, 30)  # 3:30 PM ET

# Warmup period — no signals for first 60s after connect
WARMUP_SECS    = 60
MIN_TRADE_BUF  = 30   # minimum trades in buffer before signals allowed

# Signal thresholds
SPEED_MULT   = 2.0
IMB_THRESH   = 20.0

_lock      = threading.Lock()
_trade_buf = deque(maxlen=5000)
_depth_buf = deque(maxlen=200)

_state = {
    "last_price":    0.0,
    "prev_price":    0.0,
    "prev_dir":      0,
    "cum_delta":     0.0,
    "buy_vol":       0.0,
    "sell_vol":      0.0,
    "speed_5s":      0.0,
    "speed_60s_avg": 0.0,
    "speed_ratio":   0.0,
    "speed_fired":   False,
    "imb_pct":       50.0,
    "imb_bid_px":    0.0,
    "imb_ask_px":    0.0,
    "imb_bid_sz":    0.0,
    "imb_ask_sz":    0.0,
    "imb_fired":     False,
    "imb_dir":       0,
    "signal_dir":    0,
    "signal_reason": "",
    "signal_ts":     0.0,
    "ws_status":     "offline",
    "last_update":   "",
    "active_symbol": "none",
    "session":       "none",
    "log":           deque(maxlen=60),
    "warmup":        True,
}

_start_time   = 0.0
_current_sym  = None   # "GC1!" or "ES1!" — Infoway symbol
_ws_instance  = None

import mt5_exec as exe

MIN_RECONNECT_GAP    = 5.0
_last_connect_ts     = 0.0
_connect_lock        = threading.Lock()


def _now():
    return datetime.now(timezone.utc).strftime("%H:%M:%S UTC")


def _log(msg):
    entry = f"[{_now()}] {msg}"
    print(entry, flush=True)
    with _lock:
        _state["log"].append(entry)


def get_state():
    with _lock:
        s = dict(_state)
        s["log"] = list(_state["log"])
    info = exe.last_info()
    s["cooldown_remaining"] = info["cooldown_remaining"]
    s["can_fire"]           = info["can_fire"]
    s["last_dir"]           = info["last_dir"]
    s["last_result"]        = info["last_result"]
    s["mt5_ready"]          = info["mt5_ready"]
    return s


def set_thresholds(speed_mult=None, imb_thresh=None):
    global SPEED_MULT, IMB_THRESH
    if speed_mult is not None:
        SPEED_MULT = float(speed_mult)
    if imb_thresh is not None:
        IMB_THRESH = float(imb_thresh)
    _log(f"Thresholds updated: speed={SPEED_MULT}x imb={IMB_THRESH}%")


# ── Session time logic ────────────────────────────────────────
def _et_now():
    """Current time in ET (UTC-4 during EDT, UTC-5 during EST).
    Simple approach: use UTC-4 for summer (EDT).
    For production adjust for EST in winter months."""
    utc = datetime.now(timezone.utc)
    # EDT = UTC - 4
    et  = utc - timedelta(hours=4)
    return et


def _get_session():
    """
    Returns:
      ("GC", "GC1!", "XAUUSD") during gold session
      ("ES", "ES1!", "US100")  during ES session
      (None, None, None)        outside trading hours
    """
    et   = _et_now()
    h, m = et.hour, et.minute
    mins = h * 60 + m

    gc_start = GC_SESSION_START_ET[0] * 60 + GC_SESSION_START_ET[1]
    gc_end   = GC_SESSION_END_ET[0]   * 60 + GC_SESSION_END_ET[1]
    es_start = ES_SESSION_START_ET[0] * 60 + ES_SESSION_START_ET[1]
    es_end   = ES_SESSION_END_ET[0]   * 60 + ES_SESSION_END_ET[1]

    if gc_start <= mins < gc_end:
        return ("GC", "GC1!", "XAUUSD")
    elif es_start <= mins < es_end:
        return ("ES", "ES1!", "US100")
    else:
        return (None, None, None)


# ── Tick rule ─────────────────────────────────────────────────
def _classify(price, td):
    with _lock:
        n = int(td) if td is not None else 0
        if n == 1:
            _state["prev_price"] = price
            _state["prev_dir"]   = 1
            return 1
        if n == 2:
            _state["prev_price"] = price
            _state["prev_dir"]   = -1
            return -1
        prev = _state["prev_price"]
        if price > prev:
            _state["prev_dir"] = 1
        elif price < prev:
            _state["prev_dir"] = -1
        _state["prev_price"] = price
        return _state["prev_dir"] if _state["prev_dir"] != 0 else 1


# ── Signal evaluation ─────────────────────────────────────────
def _compute_and_evaluate(mt5_symbol):
    now   = time.time()

    # Warmup check
    with _lock:
        in_warmup = _state["warmup"]
        buf_size  = len(_trade_buf)

    if in_warmup:
        elapsed = now - _start_time
        if elapsed >= WARMUP_SECS and buf_size >= MIN_TRADE_BUF:
            with _lock:
                _state["warmup"] = False
            _log(f"Warmup complete — signals now active "
                 f"({buf_size} trades in buffer)")
        else:
            with _lock:
                _state["signal_dir"] = 0
            return

    cut5  = now - 5.0
    cut60 = now - 60.0
    cut30 = now - 30.0

    while _trade_buf and _trade_buf[0]["ts"] < cut60:
        _trade_buf.popleft()

    v5    = sum(t["vol"] for t in _trade_buf if t["ts"] >= cut5)
    v60   = sum(t["vol"] for t in _trade_buf)
    avg60 = v60 / 12.0
    ratio = (v5 / avg60) if avg60 > 0 else 0.0
    sf    = ratio >= SPEED_MULT

    r5    = [t for t in _trade_buf if t["ts"] >= cut5]
    rbuy  = sum(t["vol"] for t in r5 if t["dir"] ==  1)
    rsell = sum(t["vol"] for t in r5 if t["dir"] == -1)
    sdir  = 1 if rbuy >= rsell else -1

    while _depth_buf and _depth_buf[0]["ts"] < cut30:
        _depth_buf.popleft()

    avg_imb = (sum(d["bid_pct"] for d in _depth_buf) / len(_depth_buf)
               if _depth_buf else 50.0)
    bull  = avg_imb >= (50 + IMB_THRESH / 2)
    bear  = avg_imb <= (50 - IMB_THRESH / 2)
    imf   = bull or bear
    idir  = 1 if bull else -1

    with _lock:
        _state["speed_5s"]      = round(v5, 0)
        _state["speed_60s_avg"] = round(avg60, 1)
        _state["speed_ratio"]   = round(ratio, 2)
        _state["speed_fired"]   = sf
        _state["imb_pct"]       = round(avg_imb, 1)
        _state["imb_fired"]     = imf
        _state["imb_dir"]       = idir if imf else 0

    if not sf or not imf or sdir != idir:
        with _lock:
            _state["signal_dir"] = 0
        return

    if not exe.can_fire():
        with _lock:
            _state["signal_dir"] = sdir
        return

    action = "buy" if sdir == 1 else "sell"
    reason = (f"speed {ratio:.2f}x | "
              f"imb {avg_imb:.0f}% | {action.upper()}")

    with _lock:
        _state["signal_dir"]    = sdir
        _state["signal_reason"] = reason
        _state["signal_ts"]     = time.time()

    _log(f"SIGNAL: {action.upper()} | {mt5_symbol} | {reason}")
    result = exe.fire(action, reason, mt5_symbol)
    _log(f"Execution: {json.dumps(result)}")


# ── Subscribe to a symbol on an open WS ──────────────────────
def _subscribe(ws, infoway_sym):
    ws.send(json.dumps({
        "code":  10000,
        "trace": uuid.uuid4().hex,
        "data":  {"codes": infoway_sym, "includeTy": True},
    }))
    time.sleep(0.3)
    ws.send(json.dumps({
        "code":  10003,
        "trace": uuid.uuid4().hex,
        "data":  {"codes": infoway_sym},
    }))
    _log(f"Subscribed to {infoway_sym}")


def _wait_for_connect_slot():
    global _last_connect_ts
    with _connect_lock:
        elapsed = time.time() - _last_connect_ts
        if elapsed < MIN_RECONNECT_GAP:
            time.sleep(MIN_RECONNECT_GAP - elapsed)
        _last_connect_ts = time.time()


# ── WebSocket thread ──────────────────────────────────────────
def _ws_thread():
    global _current_sym, _ws_instance

    while True:
        _wait_for_connect_slot()

        session, infoway_sym, mt5_symbol = _get_session()

        if session is None:
            with _lock:
                _state["ws_status"]     = "outside hours"
                _state["active_symbol"] = "none"
                _state["session"]       = "none"
            _log("Outside trading hours — waiting...")
            time.sleep(30)
            continue

        _log(f"Session: {session} | "
             f"Infoway: {infoway_sym} | MT5: {mt5_symbol}")

        with _lock:
            _state["active_symbol"] = infoway_sym
            _state["session"]       = session
            _state["warmup"]        = True

        # Reset buffers on symbol switch
        _trade_buf.clear()
        _depth_buf.clear()
        with _lock:
            _state["cum_delta"]  = 0.0
            _state["buy_vol"]    = 0.0
            _state["sell_vol"]   = 0.0
            _state["prev_price"] = 0.0
            _state["prev_dir"]   = 0

        global _start_time
        _start_time = time.time()

        def on_open(ws):
            _ws_instance = ws
            with _lock:
                _state["ws_status"] = "live"
            _log("WebSocket connected")
            _subscribe(ws, infoway_sym)

            def hb():
                while True:
                    time.sleep(15)
                    try:
                        if ws.sock and ws.sock.connected:
                            ws.send(json.dumps({
                                "code":  10006,
                                "trace": uuid.uuid4().hex,
                                "data":  {
                                    "arr": [{"type": 1,
                                             "codes": infoway_sym}]
                                },
                            }))
                    except Exception:
                        break
            threading.Thread(target=hb, daemon=True).start()

        def on_message(ws, raw):
            try:
                msg  = json.loads(raw)
                code = msg.get("code")

                if code == 10002:
                    d      = msg["data"]
                    price  = float(d["p"])
                    vol    = float(d["v"])
                    dirn   = _classify(price, d.get("td", 0))
                    is_buy = dirn == 1
                    with _lock:
                        _state["last_price"]  = price
                        _state["cum_delta"]  += vol if is_buy else -vol
                        _state["buy_vol"]    += vol if is_buy else 0
                        _state["sell_vol"]   += 0   if is_buy else vol
                        _state["last_update"] = _now()
                    _trade_buf.append({
                        "ts": time.time(), "vol": vol, "dir": dirn
                    })

                elif code == 10005:
                    d      = msg["data"]
                    ask_sz = float(d["a"][1][0]) if d.get("a") else 0
                    bid_sz = float(d["b"][1][0]) if d.get("b") else 0
                    ask_px = float(d["a"][0][0]) if d.get("a") else 0
                    bid_px = float(d["b"][0][0]) if d.get("b") else 0
                    total  = ask_sz + bid_sz
                    bp     = (bid_sz/total*100) if total > 0 else 50.0
                    _depth_buf.append({"ts": time.time(), "bid_pct": bp})
                    with _lock:
                        _state["imb_bid_px"] = bid_px
                        _state["imb_ask_px"] = ask_px
                        _state["imb_bid_sz"] = bid_sz
                        _state["imb_ask_sz"] = ask_sz

            except Exception:
                pass

        def on_error(ws, e):
            _log(f"WS error: {e}")
            with _lock:
                _state["ws_status"] = "error"

        def on_close(ws, c, m):
            with _lock:
                _state["ws_status"] = "offline"
            _log("WS closed — reconnecting in 5s")

        try:
            url = (f"wss://data.infoway.io/ws?business=common"
                   f"&apikey={INFOWAY_KEY}")
            ws  = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=25, ping_timeout=10)
        except Exception as e:
            _log(f"WS exception: {e}")

        # Check if session changed after disconnect
        new_session, _, _ = _get_session()
        if new_session != session:
            _log(f"Session changed — switching symbol")


def _eval_thread():
    while True:
        time.sleep(2)
        try:
            _, _, mt5_symbol = _get_session()
            if mt5_symbol:
                _compute_and_evaluate(mt5_symbol)
        except Exception as e:
            _log(f"Eval error: {e}")


def _breakeven_thread():
    """Check breakeven every 5 seconds."""
    while True:
        time.sleep(5)
        try:
            exe.check_breakeven()
        except Exception as e:
            _log(f"Breakeven error: {e}")


def start():
    if not INFOWAY_KEY:
        print("ERROR: INFOWAY_API_KEY not set", flush=True)
        return
    threading.Thread(target=_ws_thread,      daemon=True).start()
    threading.Thread(target=_eval_thread,    daemon=True).start()
    threading.Thread(target=_breakeven_thread, daemon=True).start()
    _log("Algo started — all threads running")
