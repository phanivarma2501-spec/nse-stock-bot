"""
server.py - Combined bot + web dashboard for Railway deployment
"""

import sys
import os

# Ensure clean stdout on all platforms
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

print(f"[BOOT] Python {sys.version}", flush=True)
print(f"[BOOT] PORT={os.environ.get('PORT', 'not set')}", flush=True)

import asyncio
import time
import threading
import uvicorn
from loguru import logger


def run_bot():
    """Run the stock bot scan loop with auto-restart."""
    while True:
        try:
            from stock_bot import StockBotEngine
            logger.info("[BOT-THREAD] Starting NSE Stock Bot engine...")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            engine = StockBotEngine()
            loop.run_until_complete(engine.run())
        except BaseException as e:
            import traceback
            logger.error(f"[BOT-THREAD] ERROR: {type(e).__name__}: {e}")
            traceback.print_exc()
            time.sleep(60)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8060))
    logger.info(f"[SERVER] Starting on port {port}")

    # Import app late to catch import errors
    try:
        from web_dashboard import app
        logger.info("[SERVER] Dashboard imported OK")
    except Exception as e:
        logger.error(f"[SERVER] Dashboard import FAILED: {e}")
        # Create minimal fallback app
        from fastapi import FastAPI
        app = FastAPI()

        @app.get("/")
        def health():
            return {"status": "ok", "error": str(e)}

    # Start bot thread
    bot_thread = threading.Thread(target=run_bot, daemon=True)
    bot_thread.start()
    logger.info("[SERVER] Bot thread started")

    # Start web server
    uvicorn.run(app, host="0.0.0.0", port=port)
