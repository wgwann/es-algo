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
        # Download cloudflared for Windows if not present
        if not os.path.exists("cloudflared.exe"):
            print("Downloading cloudflared...", flush=True)
            import urllib.request
            urllib.request.urlretrieve(
                "https://github.com/cloudflare/cloudflared/releases/"
                "latest/download/cloudflared-windows-amd64.exe",
                "cloudflared.exe"
            )
            print("Downloaded cloudflared.exe", flush=True)

        proc = subprocess.Popen(
            ["cloudflared.exe", "tunnel", "--url",
             "http://localhost:8080", "--no-autoupdate"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        print("Starting tunnel...", flush=True)
        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue
            urls = re.findall(
                r"https://[^\s]+trycloudflare\.com", line
            )
            if urls:
                print(f"\n{'='*55}", flush=True)
                print(f"  OPEN ON YOUR PHONE (UI):", flush=True)
                print(f"  {urls[0]}", flush=True)
                print(f"{'='*55}\n", flush=True)
                return
            print(f"[tunnel] {line}", flush=True)

    except Exception as e:
        print(f"Tunnel error: {e}", flush=True)
        print("Access UI at: http://localhost:8080", flush=True)


if __name__ == "__main__":
    # Check credentials
    missing = [k for k in ["INFOWAY_API_KEY"]
               if not os.environ.get(k)]
    if missing:
        print(f"ERROR: Missing env vars: {missing}", flush=True)
        print("Run: set INFOWAY_API_KEY=your_key", flush=True)
        sys.exit(1)

    print("="*55, flush=True)
    print("  ES1! ALGO — Windows MT5 mode", flush=True)
    print("="*55, flush=True)

    # Init MT5 first — fail fast if not connected
    if not exe.init_mt5():
        print("ERROR: MT5 not available. "
              "Make sure MT5 is open and logged in.", flush=True)
        sys.exit(1)

    # Start web server
    server.start()

    # Start tunnel in background
    threading.Thread(target=start_tunnel, daemon=True).start()

    # Start algo
    algo.start()

    print("Running. Ctrl+C to stop.", flush=True)

    try:
        while True:
            time.sleep(30)
            s = algo.get_state()
            print(
                f"[{s.get('last_update','--:--')}] "
                f"price={s['last_price']:.2f} "
                f"delta={s['cum_delta']:+.0f} "
                f"ws={s['trade_ws']} "
                f"ratio={s['speed_ratio']:.2f}x "
                f"imb={s['imb_pct']:.0f}% "
                f"mt5={s.get('mt5_ready','?')}",
                flush=True
            )
    except KeyboardInterrupt:
        exe.shutdown()
        print("Stopped.", flush=True)
