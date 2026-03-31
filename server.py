"""
server.py - Combined bot + web dashboard for Railway deployment
Runs the scan loop in background + serves the dashboard on PORT.
"""

import asyncio
import os
import threading
import uvicorn
from web_dashboard import app
from stock_bot import StockBotEngine


def run_bot():
    """Run the stock bot scan loop in a separate thread."""
    engine = StockBotEngine()
    asyncio.run(engine.run())


if __name__ == "__main__":
    # Start bot in background thread
    bot_thread = threading.Thread(target=run_bot, daemon=True)
    bot_thread.start()

    # Start web dashboard (Railway provides PORT env var)
    port = int(os.environ.get("PORT", 8060))
    uvicorn.run(app, host="0.0.0.0", port=port)
