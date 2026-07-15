import os
import sys
import time
import subprocess
import threading
import re
import mt5_exec as exe
import algo
import server


def start_tunnel():
    try:
        if not os.path.exists("cloudflared.exe"):
            print("Downloading cloudflared...", flush=True)
            import urllib.request
            urllib.request.urlretrieve(
                "https://github.com/cloudflare/cloudflared/releases/"
                "latest/download/cloudflared-windows-amd64.exe",
                "cloudflared.exe"
            )

        proc = subprocess.Popen(
            ["cloudflared.exe", "tunnel", "--url",
             "http://localhost:8080", "--no-autoupdate"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue
            urls = re.findall(
                r"https://[^\s]+trycloudflare\.com", line
            )
            if urls:
                print(f"\n{'='*55}", flush=True)
                print(f"  UI URL (open on phone):", flush=True)
                print(f"  {urls[0]}", flush=True)
                print(f"{'='*55}\n", flush=True)
                return
            print(f"[tunnel] {line}", flush=True)

    except Exception as e:
        print(f"Tunnel error: {e}", flush=True)
        print("UI at: http://localhost:8080", flush=True)


if __name__ == "__main__":
    missing = [k for k in ["INFOWAY_API_KEY"]
               if not os.environ.get(k)]
    if missing:
        print(f"ERROR: Missing: {missing}", flush=True)
        print(
            'Run: $env:INFOWAY_API_KEY="your_key_here"',
            flush=True
        )
        sys.exit(1)

    print("="*55, flush=True)
    print("  ES1!/GC1! ALGO", flush=True)
    print("="*55, flush=True)

    if not exe.init_mt5():
        print("ERROR: MT5 not available — open MT5 and log in first",
              flush=True)
        sys.exit(1)

    server.start()
    threading.Thread(target=start_tunnel, daemon=True).start()
    algo.start()

    print("Running. Ctrl+C to stop.", flush=True)
    print("Lot sizes override via env vars:", flush=True)
    print('  $env:LOT_US100="0.01"', flush=True)
    print('  $env:LOT_XAUUSD="0.01"', flush=True)

    try:
        while True:
            time.sleep(30)
            s = algo.get_state()
            et = algo._et_now()
            print(
                f"[{s.get('last_update','--:--')}] "
                f"ET={et.strftime('%H:%M')} "
                f"session={s['session']} "
                f"sym={s['active_symbol']} "
                f"price={s['last_price']:.2f} "
                f"ws={s['ws_status']} "
                f"warmup={s['warmup']} "
                f"ratio={s['speed_ratio']:.2f}x "
                f"imb={s['imb_pct']:.0f}%",
                flush=True
            )
    except KeyboardInterrupt:
        exe.shutdown()
        print("Stopped.", flush=True)
