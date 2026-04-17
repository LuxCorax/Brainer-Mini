# Brainer Mini App — Backend

FastAPI backend for the Brainer Mini Telegram App.

## What it does
- Fetches OHLCV data from Binance (free, no auth)
- Computes BrainWaves, RSI, EMA, Fibonacci, S/R, VWAP, Volume
- Receives TradingView webhook alerts for Market Manipulation signals
- Serves analysis API for the Mini App frontend
- Runs Telegram bot with commands + alert delivery
- Stores users, waitlist, signals, events in SQLite

## Files
```
main.py           — FastAPI app, routes, startup
config.py         — Settings and constants
database.py       — SQLite tables and queries
binance_client.py — Binance REST API data fetcher
indicators.py     — All indicator math (from PineScript)
analysis.py       — Orchestrates indicators into API response
bot.py            — Telegram bot commands + alerts
```

## Local Development
```bash
pip install -r requirements.txt
BOT_TOKEN=your_token python main.py
```

## Deploy to Render.com
1. Push this folder to a GitHub repo
2. Connect repo to Render.com → New Web Service
3. Set environment variables:
   - `BOT_TOKEN` — from @BotFather
   - `OWNER_CHAT_ID` — your Telegram user ID
   - `WEBHOOK_SECRET` — any secret string (match in TradingView alerts)
4. Deploy — Render auto-detects `render.yaml`

## API Endpoints
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/analysis/{symbol}` | Full analysis (price, MTF, BW, signals, levels) |
| GET | `/api/brainwaves/{symbol}` | BrainWaves chart data only |
| GET | `/api/pairs` | List of supported pairs |
| GET | `/api/health` | Health check + stats |
| POST | `/api/waitlist` | Join NoBrainer waitlist |
| POST | `/api/events` | Track analytics events |
| POST | `/api/webhook/tradingview` | Receive TradingView alerts |

## TradingView Webhook Setup
Set alert URL to: `https://your-render-url.onrender.com/api/webhook/tradingview`

Alert message JSON:
```json
{
    "secret": "your-webhook-secret",
    "type": "Market Manipulation",
    "pair": "{{ticker}}",
    "timeframe": "{{interval}}",
    "direction": "bull",
    "price": {{close}},
    "message": "{{strategy.order.alert_message}}"
}
```

## Connecting Frontend
In the Mini App `index.html`, set `API_BASE`:
```javascript
const API_BASE = 'https://your-render-url.onrender.com';
```
