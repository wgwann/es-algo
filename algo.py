import websocket
import json
import time
import threading
import uuid
import os
from collections import deque
from datetime import datetime, timezone

INFOWAY_KEY = os.environ.get("INFOWAY_API_KEY", "")
SYM         = "ES1!"
SPEED_MULT  = 2.0
IMB_THRESH  = 20.0

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
    "trade_ws":      "offline",
    "depth_ws":      "offline",
    "last_update":   "",
    "log":           deque(maxlen=40),
}

import mt5_exec as exe

MIN_RECONNECT_GAP    = 5.0
_last_connect_attempt= 0.0
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
    _log(f"Thresholds: speed_mult={SPEED_MULT} imb_thresh={IMB_THRESH}")


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


def _compute_and_evaluate():
    now   = time.time()
    cut5  = now - 5.0
    cut60 = now - 60.0

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

    cut30  = now - 30.0
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

    _log(f"SIGNAL: {reason}")
    result = exe.fire(action, reason)
    _log(f"Execution: {json.dumps(result)}")


def _wait_for_connect_slot():
    global _last_connect_attempt
    with _connect_lock:
        elapsed = time.time() - _last_connect_attempt
        if elapsed < MIN_RECONNECT_GAP:
            time.sleep(MIN_RECONNECT_GAP - elapsed)
        _last_connect_attempt = time.time()


def _ws_thread():
    while True:
        _wait_for_connect_slot()

        def on_open(ws):
            _log("WebSocket connected")
            with _lock:
                _state["trade_ws"] = "live"
                _state["depth_ws"] = "live"

            ws.send(json.dumps({
                "code":  10000,
                "trace": uuid.uuid4().hex,
                "data":  {"codes": SYM, "includeTy": True},
            }))
            time.sleep(0.3)
            ws.send(json.dumps({
                "code":  10003,
                "trace": uuid.uuid4().hex,
                "data":  {"codes": SYM},
            }))

            def hb():
                while True:
                    time.sleep(20)
                    try:
                        if ws.sock and ws.sock.connected:
                            ws.send(json.dumps({
                                "code":  10006,
                                "trace": uuid.uuid4().hex,
                                "data":  {
                                    "arr": [{"type": 1, "codes": SYM}]
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
                _state["trade_ws"] = "error"
                _state["depth_ws"] = "error"

        def on_close(ws, c, m):
            with _lock:
                _state["trade_ws"] = "offline"
                _state["depth_ws"] = "offline"
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
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            _log(f"WS exception: {e}")


def _eval_thread():
    while True:
        time.sleep(2)
        try:
            _compute_and_evaluate()
        except Exception as e:
            _log(f"Eval error: {e}")


def start():
    if not INFOWAY_KEY:
        print("ERROR: INFOWAY_API_KEY not set", flush=True)
        return
    threading.Thread(target=_ws_thread,  daemon=True).start()
    threading.Thread(target=_eval_thread, daemon=True).start()
    _log("Algo started")
