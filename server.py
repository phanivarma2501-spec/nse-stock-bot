"""
server.py - Combined bot + web dashboard for Railway deployment
Runs the scan loop in background + serves the dashboard on PORT.
"""

import sys
import os
import io

print(f"[BOOT] server.py starting, Python {sys.version}", flush=True)
print(f"[BOOT] PORT={os.environ.get('PORT', 'not set')}", flush=True)

# Fix Windows emoji encoding
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import asyncio
import time
import threading
import uvicorn
from loguru import logger

print("[BOOT] importing web_dashboard...", flush=True)
from web_dashboard import app
print("[BOOT] importing stock_bot...", flush=True)
from stock_bot import StockBotEngine
print("[BOOT] all imports OK", flush=True)


def run_bot():
    """Run the stock bot scan loop with auto-restart."""
    while True:
        try:
            logger.info("[BOT-THREAD] Starting NSE Stock Bot engine...")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            engine = StockBotEngine()
            loop.run_until_complete(engine.run())
        except BaseException as e:
            import traceback
            logger.error(f"[BOT-THREAD] ERROR — restarting in 60s: {type(e).__name__}: {e}")
            traceback.print_exc()
            time.sleep(60)


if __name__ == "__main__":
    # Start bot in background thread
    bot_thread = threading.Thread(target=run_bot, daemon=True)
    bot_thread.start()

    # Start web dashboard (Railway provides PORT env var)
    port = int(os.environ.get("PORT", 8060))
    logger.info(f"[SERVER] Starting NSE dashboard on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
