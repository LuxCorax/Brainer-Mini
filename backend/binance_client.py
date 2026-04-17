"""
Binance REST API client for fetching OHLCV candle data.
No authentication needed — uses free public endpoints.
"""
import asyncio
import time
import logging
from typing import Dict, List, Optional, Set, Tuple
import httpx

from config import BINANCE_BASE, CANDLE_LIMIT, FALLBACK_PAIRS

logger = logging.getLogger(__name__)

# In-memory cache: {(symbol, interval): {"data": {...}, "ts": float}}
_cache: Dict[Tuple[str, str], Dict] = {}
CACHE_TTL = 30  # seconds

# Dynamic pair list — populated on startup from Binance exchangeInfo
_supported_pairs: Set[str] = set()


async def load_all_usdt_pairs(client: Optional[httpx.AsyncClient] = None) -> int:
    """
    Fetch all active USDT spot trading pairs from Binance.
    Called once on startup. Returns count of pairs loaded.
    """
    global _supported_pairs
    url = f"{BINANCE_BASE}/api/v3/exchangeInfo"

    should_close = False
    if client is None:
        client = httpx.AsyncClient(timeout=20.0)
        should_close = True

    try:
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()
        pairs = set()
        for sym in data.get("symbols", []):
            if (
                sym.get("quoteAsset") == "USDT"
                and sym.get("status") == "TRADING"
                and sym.get("isSpotTradingAllowed", False)
            ):
                pairs.add(sym["symbol"])
        _supported_pairs = pairs
        logger.info(f"Loaded {len(pairs)} USDT pairs from Binance")
        return len(pairs)
    except Exception as e:
        logger.warning(f"Could not fetch Binance pairs, using fallback: {e}")
        _supported_pairs = set(FALLBACK_PAIRS)
        return len(_supported_pairs)
    finally:
        if should_close:
            await client.aclose()


async def fetch_klines(
    symbol: str,
    interval: str = "5m",
    limit: int = CANDLE_LIMIT,
    client: Optional[httpx.AsyncClient] = None,
) -> Optional[Dict]:
    """
    Fetch OHLCV candles from Binance.
    Returns dict with lists: timestamps, opens, highs, lows, closes, volumes.
    Uses cache to avoid hammering the API.
    """
    cache_key = (symbol.upper(), interval)

    # Check cache
    cached = _cache.get(cache_key)
    if cached and (time.time() - cached["ts"]) < CACHE_TTL:
        return cached["data"]

    url = f"{BINANCE_BASE}/api/v3/klines"
    params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}

    should_close = False
    if client is None:
        client = httpx.AsyncClient(timeout=15.0)
        should_close = True

    try:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        raw = resp.json()
    except Exception as e:
        logger.error(f"Binance fetch error for {symbol} {interval}: {e}")
        return None
    finally:
        if should_close:
            await client.aclose()

    if not raw:
        return None

    data = {
        "timestamps": [],
        "opens": [],
        "highs": [],
        "lows": [],
        "closes": [],
        "volumes": [],
    }

    for candle in raw:
        # Binance kline format: [open_time, open, high, low, close, volume, ...]
        data["timestamps"].append(int(candle[0]))
        data["opens"].append(float(candle[1]))
        data["highs"].append(float(candle[2]))
        data["lows"].append(float(candle[3]))
        data["closes"].append(float(candle[4]))
        data["volumes"].append(float(candle[5]))

    # Cache it
    _cache[cache_key] = {"data": data, "ts": time.time()}

    return data


async def fetch_ticker_price(
    symbol: str,
    client: Optional[httpx.AsyncClient] = None,
) -> Optional[Dict]:
    """Fetch current price and 24h change."""
    url = f"{BINANCE_BASE}/api/v3/ticker/24hr"
    params = {"symbol": symbol.upper()}

    should_close = False
    if client is None:
        client = httpx.AsyncClient(timeout=10.0)
        should_close = True

    try:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
        return {
            "price": float(data["lastPrice"]),
            "change_pct": float(data["priceChangePercent"]),
            "high_24h": float(data["highPrice"]),
            "low_24h": float(data["lowPrice"]),
            "volume_24h": float(data["quoteVolume"]),
        }
    except Exception as e:
        logger.error(f"Binance ticker error for {symbol}: {e}")
        return None
    finally:
        if should_close:
            await client.aclose()


async def fetch_multi_tf(
    symbol: str,
    timeframes: List[str],
    client: Optional[httpx.AsyncClient] = None,
) -> Dict[str, Optional[Dict]]:
    """Fetch candles for multiple timeframes in parallel."""
    should_close = False
    if client is None:
        client = httpx.AsyncClient(timeout=15.0)
        should_close = True

    try:
        tasks = [fetch_klines(symbol, tf, client=client) for tf in timeframes]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        output = {}
        for tf, result in zip(timeframes, results):
            if isinstance(result, Exception):
                logger.error(f"Error fetching {symbol} {tf}: {result}")
                output[tf] = None
            else:
                output[tf] = result
        return output
    finally:
        if should_close:
            await client.aclose()


def clear_cache():
    """Clear the candle cache."""
    _cache.clear()


def validate_symbol(symbol: str) -> bool:
    """Check if symbol is in supported USDT pairs."""
    if not _supported_pairs:
        # Not loaded yet — accept anything ending in USDT
        return symbol.upper().endswith("USDT")
    return symbol.upper() in _supported_pairs


def get_supported_pairs() -> List[str]:
    """Return sorted list of all supported pairs."""
    if not _supported_pairs:
        return sorted(FALLBACK_PAIRS)
    return sorted(_supported_pairs)
