"""
Indicator calculations translated from Brainer Pro PineScript.
All computations use raw OHLCV data from Binance.

SCOPE: Crypto only, 1m / 5m / 15m timeframes.
Every formula verified line-by-line against Brainer_Pro_2026_v4.txt.
"""
import math
from typing import List, Dict, Tuple, Optional


# ═══════════════════════════════════════════════════════════════
#  BASIC TA FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def ema(data: List[float], period: int) -> List[float]:
    """Exponential Moving Average."""
    if not data or period < 1:
        return []
    result = [None] * len(data)
    if period > len(data):
        return result
    k = 2.0 / (period + 1)
    valid = [v for v in data[:period] if v is not None]
    if not valid:
        return result
    result[period - 1] = sum(valid) / len(valid)
    for i in range(period, len(data)):
        if data[i] is not None and result[i - 1] is not None:
            result[i] = data[i] * k + result[i - 1] * (1 - k)
    return result


def sma(data: List[float], period: int) -> List[float]:
    """Simple Moving Average."""
    if not data or period < 1:
        return []
    result = [None] * len(data)
    for i in range(period - 1, len(data)):
        window = data[i - period + 1 : i + 1]
        vals = [v for v in window if v is not None]
        result[i] = sum(vals) / len(vals) if vals else None
    return result


def rsi(closes: List[float], period: int = 13) -> List[float]:
    """RSI using Wilder's smoothing. PineScript uses hl2 with period 13."""
    if len(closes) < period + 1:
        return [None] * len(closes)
    result = [None] * len(closes)
    gains = []
    losses = []
    for i in range(1, len(closes)):
        if closes[i] is not None and closes[i - 1] is not None:
            diff = closes[i] - closes[i - 1]
            gains.append(max(diff, 0))
            losses.append(max(-diff, 0))
        else:
            gains.append(0)
            losses.append(0)
    if len(gains) < period:
        return result
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    if avg_loss == 0:
        result[period] = 100.0
    else:
        rs = avg_gain / avg_loss
        result[period] = 100 - (100 / (1 + rs))
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            result[i + 1] = 100.0
        else:
            rs = avg_gain / avg_loss
            result[i + 1] = 100 - (100 / (1 + rs))
    return result


def atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> List[float]:
    """Average True Range using RMA (Wilder's smoothing)."""
    if len(highs) < 2:
        return [None] * len(highs)
    tr = [None] * len(highs)
    tr[0] = highs[0] - lows[0] if highs[0] and lows[0] else None
    for i in range(1, len(highs)):
        if all(v is not None for v in [highs[i], lows[i], closes[i - 1]]):
            tr[i] = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
    result = [None] * len(highs)
    if period > len(highs):
        return result
    vals = [v for v in tr[:period] if v is not None]
    if not vals:
        return result
    result[period - 1] = sum(vals) / len(vals)
    alpha = 1.0 / period
    for i in range(period, len(tr)):
        if tr[i] is not None and result[i - 1] is not None:
            result[i] = tr[i] * alpha + result[i - 1] * (1 - alpha)
    return result


def get_atr_period(tf_minutes: int) -> int:
    """PineScript: atrPeriod = (intraday and < 60m) ? 5 : 8. For 1m/5m/15m -> 5."""
    if tf_minutes < 60:
        return 5
    return 8


# ═══════════════════════════════════════════════════════════════
#  BRAINWAVES OSCILLATOR
# ═══════════════════════════════════════════════════════════════

def get_wave_settings(tf_minutes: int, asset_type: str = "Crypto") -> Tuple[int, int]:
    """Asset-specific BrainWaves parameters. Returns (n1, n2)."""
    base_n1 = 8 if tf_minutes < 60 else 13
    base_n2 = 13 if tf_minutes < 60 else 21
    multipliers = {"Crypto": 0.8, "Forex": 1.2, "Index": 1.1, "Commodity": 1.0, "Stock": 1.0}
    m = multipliers.get(asset_type, 1.0)
    n1 = max(5, round(base_n1 * m))
    n2 = max(8, round(base_n2 * m))
    return n1, n2


def get_market_regime(tf_minutes: int) -> str:
    if tf_minutes < 5:
        return "SCALP TRADE"
    elif tf_minutes <= 60:
        return "DAY TRADE"
    elif tf_minutes <= 720:
        return "SWING TRADE"
    else:
        return "POSITION TRADE"


def compute_brainwaves(highs: List[float], lows: List[float], tf_minutes: int = 5, asset_type: str = "Crypto") -> Dict:
    """Full BrainWaves oscillator computation."""
    n1, n2 = get_wave_settings(tf_minutes, asset_type)
    regime = get_market_regime(tf_minutes)
    signal_len = 3 if regime == "SCALP TRADE" else 4
    sensitivity = 0.015
    hl2 = [(h + l) / 2 for h, l in zip(highs, lows)]
    esa = ema(hl2, n1)
    abs_diff = []
    for i in range(len(hl2)):
        if hl2[i] is not None and esa[i] is not None:
            abs_diff.append(abs(hl2[i] - esa[i]))
        else:
            abs_diff.append(None)
    d = ema(abs_diff, n1)
    ci = []
    for i in range(len(hl2)):
        if all(v is not None for v in [hl2[i], esa[i], d[i]]) and d[i] != 0:
            ci.append((hl2[i] - esa[i]) / (sensitivity * d[i]))
        else:
            ci.append(None)
    wt1 = ema(ci, n2)
    wt2 = sma(wt1, signal_len)
    histogram = []
    for i in range(len(wt1)):
        if wt1[i] is not None and wt2[i] is not None:
            val = (wt1[i] - wt2[i]) * 3
            histogram.append(max(-100, min(100, val)))
        else:
            histogram.append(None)
    crosses = []
    for i in range(1, len(wt1)):
        if all(v is not None for v in [wt1[i], wt2[i], wt1[i - 1], wt2[i - 1]]):
            diff = wt1[i] - wt2[i]
            prev_diff = wt1[i - 1] - wt2[i - 1]
            cross_over = diff > 0 and prev_diff <= 0
            cross_under = diff < 0 and prev_diff >= 0
            if cross_over or cross_under:
                crosses.append({
                    "index": i,
                    "type": "bullish" if cross_over else "bearish",
                    "wt1": round(wt1[i], 2),
                    "extreme": abs(wt1[i]) > (80 if asset_type == "Crypto" else 70),
                })
    thresholds = _get_bw_thresholds(asset_type)
    # Ribbon (midline) — single source of truth for all BW signal threshold checks
    # per the ribbon rule (BrainWaves_2026 line 121: `ribbon = (wt1 + wt2) / 2.0`).
    ribbon = []
    for w1, w2 in zip(wt1, wt2):
        if w1 is not None and w2 is not None:
            ribbon.append((w1 + w2) / 2.0)
        else:
            ribbon.append(None)
    return {
        "wt1": wt1, "wt2": wt2, "ribbon": ribbon,
        "histogram": histogram, "crosses": crosses,
        "n1": n1, "n2": n2, "signal_len": signal_len, "thresholds": thresholds,
    }


def get_ribbon_state(wt1: List[Optional[float]], wt2: List[Optional[float]]) -> str:
    """
    BrainWaves ribbon state from PineScript:
    width = abs(wt1 - wt2), prev = abs(wt1[1] - wt2[1])
    Expanding / Contracting / Flat
    """
    if len(wt1) < 2 or len(wt2) < 2:
        return "Flat"
    w1_cur, w2_cur, w1_prev, w2_prev = wt1[-1], wt2[-1], wt1[-2], wt2[-2]
    if any(v is None for v in [w1_cur, w2_cur, w1_prev, w2_prev]):
        return "Flat"
    width = abs(w1_cur - w2_cur)
    width_prev = abs(w1_prev - w2_prev)
    if width > width_prev:
        return "Expanding"
    elif width < width_prev:
        return "Contracting"
    return "Flat"


def _get_bw_thresholds(asset_type: str) -> Dict:
    if asset_type == "Crypto":
        return {"ob": 55, "os": -55, "eob": 80, "eos": -80}
    elif asset_type == "Forex":
        return {"ob": 50, "os": -50, "eob": 70, "eos": -70}
    elif asset_type == "Index":
        return {"ob": 50, "os": -50, "eob": 75, "eos": -75}
    else:
        return {"ob": 53, "os": -53, "eob": 75, "eos": -75}


# ═══════════════════════════════════════════════════════════════
#  VWAP (Daily)
# ═══════════════════════════════════════════════════════════════

def compute_vwap(highs, lows, closes, volumes, timestamps):
    """Daily VWAP. Resets each UTC day. Timestamps in ms."""
    result = [None] * len(closes)
    cum_vol = 0.0
    cum_tp_vol = 0.0
    prev_day = None
    for i in range(len(closes)):
        if any(v is None for v in [highs[i], lows[i], closes[i], volumes[i]]):
            continue
        day = timestamps[i] // 86400000
        if prev_day is not None and day != prev_day:
            cum_vol = 0.0
            cum_tp_vol = 0.0
        prev_day = day
        tp = (highs[i] + lows[i] + closes[i]) / 3
        cum_tp_vol += tp * volumes[i]
        cum_vol += volumes[i]
        result[i] = cum_tp_vol / cum_vol if cum_vol > 0 else None
    return result


# ═══════════════════════════════════════════════════════════════
#  FIBONACCI LEVELS
# ═══════════════════════════════════════════════════════════════

def compute_fibonacci(highs, lows, closes, lookback=144):
    """
    Auto swing detection + Fibonacci levels.
    PineScript direction: fib 0.0 = swing_low, fib 1.0 = swing_high.
    Levels = swing_low + range * ratio.
    Extended lookback when price near swing low (findFibonacciSwings).
    """
    if len(highs) < lookback:
        lookback = len(highs)
    if lookback < 10:
        return {}
    recent_highs = highs[-lookback:]
    recent_lows = lows[-lookback:]
    swing_high = max(recent_highs)
    swing_low = min(recent_lows)
    price_range = swing_high - swing_low
    if price_range <= 0:
        return {}
    # Extended lookback when price near swing low (PineScript findFibonacciSwings)
    current_close = closes[-1] if closes else swing_low
    price_near_zero = current_close <= swing_low + price_range * 0.1
    if price_near_zero:
        extended_lookback = min(lookback * 2, 300, len(lows))
        if extended_lookback > lookback:
            extended_low = min(lows[-extended_lookback:])
            if extended_low < swing_low:
                swing_low = extended_low
                try:
                    low_idx = len(lows) - 1 - list(reversed(lows[-extended_lookback:])).index(extended_low)
                    bars_since = len(lows) - 1 - low_idx
                    if bars_since > 0:
                        swing_high = max(highs[-bars_since:])
                except (ValueError, IndexError):
                    pass
                price_range = swing_high - swing_low
                if price_range <= 0:
                    return {}
    # PineScript: swing_low + range * ratio (0.0=bottom, 1.0=top)
    levels = {}
    for ratio in [0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0]:
        levels[str(ratio)] = round(swing_low + price_range * ratio, 8)
    return {"swing_high": swing_high, "swing_low": swing_low, "levels": levels}


def get_visible_fib_levels(fib_data, current_price, close=None, trend_lines=None):
    """Only 4 visible Fibonacci levels for S/R: 2 closest above + 2 closest below price.
    PineScript dedup (lines 1117-1124): skip Fib 0.0 if within 0.01% of Lower trend
    line (l1); skip Fib 1.0 if within 0.01% of Upper trend line (h1).
    `close` defaults to `current_price` (same value in normal usage); `trend_lines`
    is the dict from compute_trend_lines — only `lower` and `upper` are read here.
    """
    if not fib_data or "levels" not in fib_data:
        return []
    ref_close = close if close is not None else current_price
    fib_tolerance = ref_close * 0.0001 if ref_close else 0
    l1 = trend_lines.get("lower") if trend_lines else None
    h1 = trend_lines.get("upper") if trend_lines else None

    above = []
    below = []
    fib_ratios = [0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0]
    for ratio in fib_ratios:
        key = str(ratio)
        if key in fib_data["levels"]:
            price = fib_data["levels"][key]
            # PineScript dedup
            if ratio == 0.0 and l1 is not None and abs(price - l1) < fib_tolerance:
                continue
            if ratio == 1.0 and h1 is not None and abs(price - h1) < fib_tolerance:
                continue
            if price > current_price and len(above) < 2:
                above.append({"price": price, "source": f"Fib {key}", "type": "fib"})
    for ratio in reversed(fib_ratios):
        key = str(ratio)
        if key in fib_data["levels"]:
            price = fib_data["levels"][key]
            if ratio == 0.0 and l1 is not None and abs(price - l1) < fib_tolerance:
                continue
            if ratio == 1.0 and h1 is not None and abs(price - h1) < fib_tolerance:
                continue
            if price < current_price and len(below) < 2:
                below.append({"price": price, "source": f"Fib {key}", "type": "fib"})
    return above + below


# ═══════════════════════════════════════════════════════════════
#  TREND LINES (h1/l1/m1 = Upper/Mid/Lower)
#  PRIMARY S/R levels in PineScript. NOT Supertrend bands.
# ═══════════════════════════════════════════════════════════════

def compute_trend_lines(highs, lows, tf_minutes=5):
    """
    PineScript trend line system (lines 932-1031):
      lenTrend = SCALP?8 : DAY?10 : 13
      if highest(lenTrend)==high: trend=up; if lowest(lenTrend)==low: trend=down
      On trend change: bars := 1, else bars += 1
      h1/l1/m1 computed from highest/lowest over `bars`.
      When bars == 1 (trend just flipped), PineScript nulls h1/l1/m1 (line 1028-1031)
      — a single bar isn't a trend range yet. Nulling also prevents Middle Line
      Break signals from firing against a single-bar midpoint.
    Returns {upper, mid, lower, mid_series} for S/R and Middle Line Break.
    """
    n = len(highs)
    regime = get_market_regime(tf_minutes)
    len_trend = 8 if regime == "SCALP TRADE" else 10 if regime == "DAY TRADE" else 13
    if n < len_trend:
        return {"upper": None, "mid": None, "lower": None, "mid_series": [None] * n}
    trend = False
    bars = 1
    h1_val = None
    l1_val = None
    m1_val = None
    mid_series = [None] * n
    for i in range(len_trend - 1, n):
        window_h = highs[i - len_trend + 1 : i + 1]
        window_l = lows[i - len_trend + 1 : i + 1]
        hh = max(window_h)
        ll = min(window_l)
        prev_trend = trend
        if hh == highs[i]:
            trend = True
        if ll == lows[i]:
            trend = False
        if trend != prev_trend:
            bars = 1
        else:
            bars += 1
        if bars == 1:
            # PineScript line 1028-1031 — null on trend-change bar
            h1_val = None
            l1_val = None
            m1_val = None
            mid_series[i] = None
        else:
            start_idx = max(0, i - bars + 1)
            h1_val = max(highs[start_idx : i + 1])
            l1_val = min(lows[start_idx : i + 1])
            m1_val = (h1_val + l1_val) / 2
            mid_series[i] = m1_val
    return {"upper": h1_val, "mid": m1_val, "lower": l1_val, "mid_series": mid_series}


# ═══════════════════════════════════════════════════════════════
#  SUPPORT / RESISTANCE with CLUSTERING
# ═══════════════════════════════════════════════════════════════

def compute_support_resistance(highs, lows, closes, volumes, timestamps,
                                fib_levels, ema_values, vwap_values, atr_val,
                                trend_lines=None):
    """
    S/R from: trend lines (Upper/Mid/Lower), Fib (4 closest), EMAs, VWAP,
    PDH/PDL/PDC.

    D1 (Mini App scope): NO CLUSTERING. At 5m crypto on a phone screen, the
    cluster ranges (ATR(13)*0.382 wide) read as bands not levels. We
    deliberately deviate from Brainer Pro PineScript here for readability:
    individual levels shown, sorted by distance from close, capped at 4 per
    side. Output shape preserved (price/price_low/price_high/sources/
    cluster_size/is_range) so the frontend renderer needs no change —
    cluster_size is always 1, is_range always False, sources always a
    single-element list.

    `_cluster_levels` is left in place (dead code in this scope) in case the
    decision reverses for a future desktop variant.
    """
    raw_levels = []
    current_price = closes[-1] if closes else 0
    # 1. Trend line levels (PRIMARY)
    if trend_lines:
        for name, key in [("Trend Top", "upper"), ("Trend Mid", "mid"), ("Trend Low", "lower")]:
            val = trend_lines.get(key)
            if val is not None:
                raw_levels.append({"price": val, "source": name, "type": "trend"})
    # 2. Fibonacci (4 closest only) — dedup vs trend lines per PineScript
    raw_levels.extend(get_visible_fib_levels(fib_levels, current_price,
                                              close=current_price,
                                              trend_lines=trend_lines))
    # 3. EMAs
    for ema_name, ema_data in ema_values.items():
        val = _last_valid(ema_data)
        if val is not None:
            raw_levels.append({"price": val, "source": ema_name, "type": "ema"})
    # 4. VWAP
    vwap_val = _last_valid(vwap_values)
    if vwap_val is not None:
        raw_levels.append({"price": vwap_val, "source": "VWAP", "type": "vwap"})
    # 5. Previous day H/L/C
    pdh, pdl, pdc = _prev_day_hld(highs, lows, closes, timestamps)
    if pdh is not None:
        raw_levels.append({"price": pdh, "source": "PDH", "type": "prev_day"})
    if pdl is not None:
        raw_levels.append({"price": pdl, "source": "PDL", "type": "prev_day"})
    if pdc is not None:
        raw_levels.append({"price": pdc, "source": "PDC", "type": "prev_day"})
    if not raw_levels:
        return {"resistance": [], "support": []}
    # Split BEFORE selection (resistance > close, support <= close)
    raw_resistance = [l for l in raw_levels if l["price"] > current_price]
    raw_support = [l for l in raw_levels if l["price"] <= current_price]
    # Sort each side by distance from close: resistance ASC (nearest above
    # first), support DESC (nearest below first)
    raw_resistance.sort(key=lambda x: x["price"])
    raw_support.sort(key=lambda x: x["price"], reverse=True)

    # D1: no clustering — wrap each individual level into the existing
    # display shape so the frontend renderer is unaffected.
    def _to_entry(level):
        p = round(level["price"], 8)
        return {
            "price": p,
            "price_low": p,
            "price_high": p,
            "sources": [level["source"]],
            "cluster_size": 1,
            "is_range": False,
        }
    resistance = [_to_entry(l) for l in raw_resistance[:4]]
    support = [_to_entry(l) for l in raw_support[:4]]
    return {"resistance": resistance, "support": support}


def _cluster_levels(levels, tolerance):
    """Group levels within tolerance. Max 3 per cluster (PineScript clusterCount < 3).

    NOTE: D1 disabled clustering for the Mini App — this function is currently
    UNUSED. Kept in place in case a future desktop variant needs to revert
    to the PineScript-parity clustered S/R presentation.
    """
    if not levels:
        return []
    sorted_levels = sorted(levels, key=lambda x: x["price"])
    clusters = [[sorted_levels[0]]]
    for level in sorted_levels[1:]:
        cluster_low = clusters[-1][0]["price"]
        cluster_high = clusters[-1][-1]["price"]
        if (abs(level["price"] - cluster_low) <= tolerance or
            abs(level["price"] - cluster_high) <= tolerance):
            if len(clusters[-1]) < 3:
                clusters[-1].append(level)
            else:
                clusters.append([level])
        else:
            clusters.append([level])
    return clusters


def _prev_day_hld(highs, lows, closes, timestamps):
    """Get previous day high, low, close."""
    if not timestamps:
        return None, None, None
    current_day = timestamps[-1] // 86400000
    prev_highs, prev_lows, prev_closes = [], [], []
    for i in range(len(timestamps) - 1, -1, -1):
        day = timestamps[i] // 86400000
        if day == current_day - 1:
            prev_highs.append(highs[i])
            prev_lows.append(lows[i])
            prev_closes.append(closes[i])
        elif day < current_day - 1:
            break
    if not prev_highs:
        return None, None, None
    return max(prev_highs), min(prev_lows), prev_closes[0]


def _last_valid(data):
    for v in reversed(data):
        if v is not None:
            return v
    return None


# ═══════════════════════════════════════════════════════════════
#  VOLUME ANALYSIS
# ═══════════════════════════════════════════════════════════════

def compute_volume_analysis(opens, closes, highs, lows, volumes, avg_period=21):
    """Volume trend, relative-to-average percent, and candle type.

    PineScript refs:
      line 1537-1538: volAvgRecent = ta.sma(volume, 5); volAvgMedium = ta.sma(volume, 21)
      line 2581:      volTrend = volAvgRecent > volAvgMedium*1.1 ? "Increasing"
                              : volAvgRecent < volAvgMedium*0.9 ? "Decreasing" : "Stable"
      line 2582:      candleType = close > open ? "Bullish" : close < open ? "Bearish" : "Doji"
      Mini App deviation: c==o labeled "Neutral" (matches line 1560 convictionDir
      convention). "Doji" belongs to candlestick pattern signals, not to this
      display field. Keeps the volume row binary+flat and visually unambiguous.

    M6: conviction_pct is NOT part of the Volume row — it belongs to Delta Analysis
    in Brainer Pro, which the Mini App dashboard doesn't render.
    """
    if len(volumes) < avg_period:
        return {"of_average_pct": 100, "trend": "Stable", "candle": "Neutral"}
    o, c, v = opens[-1], closes[-1], volumes[-1]
    candle_type = "Bullish" if c > o else "Bearish" if c < o else "Neutral"
    avg_vol = sum(volumes[-avg_period:]) / avg_period  # = volAvgMedium (21-bar SMA incl. current)
    vol_pct = round((v / avg_vol) * 100) if avg_vol > 0 else 100
    recent = sum(volumes[-5:]) / 5  # = volAvgRecent
    # #10: trend = sma(vol, 5) vs sma(vol, 21) with ±10% bands (PineScript line 2581)
    trend = "Increasing" if recent > avg_vol * 1.1 else "Decreasing" if recent < avg_vol * 0.9 else "Stable"
    return {"of_average_pct": vol_pct, "trend": trend, "candle": candle_type}


# ═══════════════════════════════════════════════════════════════
#  SUPERTREND (for bias detection)
# ═══════════════════════════════════════════════════════════════

def compute_supertrend(highs, lows, closes, atr_values, factor=3.0):
    """
    Supertrend: returns list of 1 (bullish) or 0 (bearish).
    `factor` may be a scalar OR a per-bar list (matches PineScript
    `combinedFactor` being re-evaluated every bar — line 873).
    """
    n = len(closes)
    result = [None] * n
    upper = [None] * n
    lower = [None] * n
    is_list = isinstance(factor, list)
    for i in range(n):
        if atr_values[i] is None:
            continue
        f = factor[i] if is_list else factor
        if f is None:
            continue
        hlc3 = (highs[i] + lows[i] + closes[i]) / 3
        up = hlc3 + f * atr_values[i]
        dn = hlc3 - f * atr_values[i]
        if i == 0 or upper[i - 1] is None:
            upper[i] = up
            lower[i] = dn
            result[i] = 1 if closes[i] > up else 0
            continue
        upper[i] = min(up, upper[i - 1]) if closes[i - 1] < upper[i - 1] else up
        lower[i] = max(dn, lower[i - 1]) if closes[i - 1] > lower[i - 1] else dn
        prev_os = result[i - 1] if result[i - 1] is not None else 0
        if closes[i] > upper[i]:
            result[i] = 1
        elif closes[i] < lower[i]:
            result[i] = 0
        else:
            result[i] = prev_os
    return result


def compute_adaptive_factor(atr_values, tf_minutes, asset_type="Crypto"):
    """
    PineScript `calculateAIFactor()` — evaluated PER BAR (lines 502-503, 848-873).
      atrAvg[i]            = sma(atrVal, 20)[i]
      volatilityFactor[i]  = atrVal[i] / atrAvg[i]   (1.0 if atrAvg is 0)
      adaptiveMult[i]      = clamp(1, 5, 1 + (volatilityFactor[i] - 1))
      target_factor[i]     = clamp(1, 5, adaptiveMult[i] * assetMult * tfMult)

    Returns a per-bar list aligned to atr_values. Entries are None where the
    20-bar SMA of ATR hasn't warmed up yet (matches PineScript na propagation).
    """
    n = len(atr_values)
    asset_mult = {"Crypto": 1.2, "Forex": 0.8, "Index": 0.9, "Commodity": 1.1}.get(asset_type, 1.0)
    regime = get_market_regime(tf_minutes)
    tf_mult = {"SCALP TRADE": 0.9, "DAY TRADE": 1.0, "SWING TRADE": 1.1, "POSITION TRADE": 1.2}.get(regime, 1.0)
    atr_avg_series = sma(atr_values, 20)
    out = [None] * n
    for i in range(n):
        atr_val = atr_values[i]
        atr_avg = atr_avg_series[i]
        if atr_val is None or atr_avg is None:
            continue
        if atr_avg > 0:
            volatility_factor = atr_val / atr_avg
            adaptive_mult = max(1.0, min(5.0, 1.0 + (volatility_factor - 1)))
        else:
            adaptive_mult = 1.0
        out[i] = max(1.0, min(5.0, adaptive_mult * asset_mult * tf_mult))
    return out


def compute_volatility_factor(atr_values):
    """PineScript lines 502-503: volatilityFactor = atrVal / sma(atrVal, 20).
    Returns the scalar value for the LATEST bar. 1.0 if warmup not ready
    or if atrAvg is zero (matches PineScript `atrAvg != 0 ? ... : 1.0`).
    """
    if len(atr_values) < 20:
        return 1.0
    atr_avg_series = sma(atr_values, 20)
    last_atr = atr_values[-1]
    last_avg = atr_avg_series[-1]
    if last_atr is None or last_avg is None or last_avg == 0:
        return 1.0
    return last_atr / last_avg


def is_near_key_level(price, high, low, recent_high, recent_low,
                      trend_lines, atr_val,
                      fib_382=None, fib_500=None, fib_618=None):
    """PineScript isNearKeyLevel (lines 173-186).
      nearUpper/Middle/Lower = |price - h1/m1/l1|       <= atrVal * 0.5
      nearSwingHigh/Low      = |high/low - recent*|     <= atrVal * 0.5
      nearFib382/500/618     = |price - fib*|           <= atrVal * 0.3  (tighter)
      nearRound              = |price - nearest100|     <= atrVal * 0.5
    Returns True if ANY check passes. Returns False if atr_val invalid
    (matches PineScript behavior — each nearX would fail individually).
    """
    if not atr_val or atr_val <= 0:
        return False
    tol_large = atr_val * 0.5
    tol_fib = atr_val * 0.3
    # Trend lines — upper / middle / lower
    if trend_lines:
        h1 = trend_lines.get("upper")
        m1 = trend_lines.get("mid")
        l1 = trend_lines.get("lower")
        if h1 is not None and abs(price - h1) <= tol_large:
            return True
        if m1 is not None and abs(price - m1) <= tol_large:
            return True
        if l1 is not None and abs(price - l1) <= tol_large:
            return True
    # 20-bar swing high/low — checks CURRENT bar's high/low vs the extremes
    if recent_high is not None and abs(high - recent_high) <= tol_large:
        return True
    if recent_low is not None and abs(low - recent_low) <= tol_large:
        return True
    # Fib levels — tighter tolerance (atrVal * 0.3)
    if fib_382 is not None and abs(price - fib_382) <= tol_fib:
        return True
    if fib_500 is not None and abs(price - fib_500) <= tol_fib:
        return True
    if fib_618 is not None and abs(price - fib_618) <= tol_fib:
        return True
    # Nearest 100 (PineScript: math.round(closePrice / 100) * 100)
    round_number = round(price / 100) * 100
    if abs(price - round_number) <= tol_large:
        return True
    return False


# ═══════════════════════════════════════════════════════════════
#  SIGNAL DETECTION
# ═══════════════════════════════════════════════════════════════

def detect_signals(closes, highs, lows, opens, bw, rsi_values,
                   ema_21, ema_50, ema_200, vwap_values,
                   fib_levels=None, supertrend_values=None, atr_val=0,
                   tf_minutes=5, trend_lines=None, volatility_factor=1.0):
    """Detect all signals matching PineScript exactly."""
    signals = []
    n = len(closes)
    if n < 3:
        return signals
    wt1 = bw["wt1"]
    wt2 = bw["wt2"]
    ribbon = bw["ribbon"]   # ribbon rule: threshold checks against midline, not wt1
    thresholds = bw["thresholds"]
    # #3-corrected: PineScript signalWindow (line 1780) is the binding age cap.
    # Signals older than signalWindow bars are purged (line 2026).
    # maxSignals (line 139-145) caps total count. Live dashboard shows <= 10 on 5m.
    regime = get_market_regime(tf_minutes)
    signal_window = {"SCALP TRADE": 3, "DAY TRADE": 5, "SWING TRADE": 8, "POSITION TRADE": 13}[regime]
    max_sigs = {"SCALP TRADE": 6, "DAY TRADE": 10, "SWING TRADE": 18, "POSITION TRADE": 30}[regime]
    scan_range = min(signal_window, n - 1)
    fib_382 = fib_levels["levels"].get("0.382") if fib_levels and "levels" in fib_levels else None
    fib_500 = fib_levels["levels"].get("0.5") if fib_levels and "levels" in fib_levels else None
    fib_618 = fib_levels["levels"].get("0.618") if fib_levels and "levels" in fib_levels else None
    adaptive_lookback = 34  # PineScript FIB_34
    mid_series = trend_lines.get("mid_series", [None] * n) if trend_lines else [None] * n

    def _fmt_price(p):
        if p >= 1000: return f"${p:,.0f}"
        elif p >= 1: return f"${p:,.2f}"
        else: return f"${p:.6f}"

    for i in range(n - 1, max(n - scan_range - 1, 0), -1):
        age = n - 1 - i
        price_at = closes[i]

        # BrainWaves crosses — PineScript detection (lines 899-910).
        #   CROSS_TOL = 0.01, TOUCH_TOL = 0.5.
        #   cross_over = (diff > -TOL and prev_diff <= TOL and diff > prev_diff)
        #                or (|diff| < TOUCH_TOL and diff > 0 and prev_diff <= 0)   [touch fallback]
        #   cross_under = mirror.
        #   Gated by `not regimeChanged` in PineScript — always false in single-TF
        #   Python call, so gate is a no-op here.
        # Ribbon rule: post-cross classification checks `ribbon`, not `wt1`
        # (BrainWaves_2026 lines 126-129).
        # S3: no "Cross Up"/"Cross Down" fallback — Brainer Pro main (source of
        # truth) emits nothing for crosses outside OS/OB zones.
        if i > 0 and all(v is not None for v in [wt1[i], wt2[i], wt1[i-1], wt2[i-1]]):
            diff = wt1[i] - wt2[i]
            prev_diff = wt1[i-1] - wt2[i-1]
            CROSS_TOL = 0.01
            TOUCH_TOL = 0.5
            cross_over = (
                (diff > -CROSS_TOL and prev_diff <= CROSS_TOL and diff > prev_diff)
                or (abs(diff) < TOUCH_TOL and diff > 0 and prev_diff <= 0)
            )
            cross_under = (
                (diff < CROSS_TOL and prev_diff >= -CROSS_TOL and diff < prev_diff)
                or (abs(diff) < TOUCH_TOL and diff < 0 and prev_diff >= 0)
            )
            rib = ribbon[i]   # guard above guarantees wt1[i] and wt2[i] valid → ribbon[i] valid
            if cross_over:
                if rib < thresholds["eos"]:
                    signals.append({"name": "BrainWaves Extreme Oversold Cross", "bullish": True, "age": age, "priority": 1})
                elif rib < thresholds["os"]:
                    signals.append({"name": "BrainWaves Bullish Cross", "bullish": True, "age": age, "priority": 2})
            if cross_under:
                if rib > thresholds["eob"]:
                    signals.append({"name": "BrainWaves Extreme Overbought Cross", "bullish": False, "age": age, "priority": 1})
                elif rib > thresholds["ob"]:
                    signals.append({"name": "BrainWaves Bearish Cross", "bullish": False, "age": age, "priority": 2})

        # BrainWaves zone entries / zero crosses / Broke Back — ribbon rule.
        # Matches BrainWaves_2026 alertconditions (lines 248-257): all threshold
        # checks and crossings evaluated on `ribbon`, not on `wt1`.
        # #5: Extreme zone entries tagged "(Reversal Possible)" to match main
        #     PineScript emissions (lines 2149, 2155).
        # #4: Broke Back has four variants (main PineScript lines 927-930 +
        #     emissions 2158-2165) — Below OB, Below EOB, Above OS, Above EOS.
        if i > 0 and ribbon[i] is not None and ribbon[i-1] is not None:
            rib = ribbon[i]
            rib_prev = ribbon[i-1]
            if rib > thresholds["eob"] and rib_prev <= thresholds["eob"]:
                signals.append({"name": f"BrainWaves Extreme Overbought {thresholds['eob']} (Reversal Possible)", "bullish": False, "age": age, "priority": 3})
            if rib < thresholds["eos"] and rib_prev >= thresholds["eos"]:
                signals.append({"name": f"BrainWaves Extreme Oversold {thresholds['eos']} (Reversal Possible)", "bullish": True, "age": age, "priority": 3})
            if rib > thresholds["ob"] and rib_prev <= thresholds["ob"]:
                signals.append({"name": f"BrainWaves Overbought {thresholds['ob']} (Reversal Possible)", "bullish": False, "age": age, "priority": 5})
            if rib < thresholds["os"] and rib_prev >= thresholds["os"]:
                signals.append({"name": f"BrainWaves Oversold {thresholds['os']} (Reversal Possible)", "bullish": True, "age": age, "priority": 5})
            if rib > 0 and rib_prev <= 0:
                signals.append({"name": "BrainWaves Crossed Zero Up", "bullish": True, "age": age, "priority": 6})
            if rib < 0 and rib_prev >= 0:
                signals.append({"name": "BrainWaves Crossed Zero Down", "bullish": False, "age": age, "priority": 6})
            if rib < thresholds["ob"] and rib_prev >= thresholds["ob"]:
                signals.append({"name": f"BrainWaves Broke Back Below {thresholds['ob']}", "bullish": False, "age": age, "priority": 5})
            if rib < thresholds["eob"] and rib_prev >= thresholds["eob"]:
                signals.append({"name": f"BrainWaves Broke Back Below {thresholds['eob']}", "bullish": False, "age": age, "priority": 5})
            if rib > thresholds["os"] and rib_prev <= thresholds["os"]:
                signals.append({"name": f"BrainWaves Broke Back Above {thresholds['os']}", "bullish": True, "age": age, "priority": 5})
            if rib > thresholds["eos"] and rib_prev <= thresholds["eos"]:
                signals.append({"name": f"BrainWaves Broke Back Above {thresholds['eos']}", "bullish": True, "age": age, "priority": 5})

        # EMA breaks
        # B6: emit display names with space ("EMA 21" not "EMA21") for
        # readable signal text. Cancel/suppression checks below match.
        if i > 0:
            for ema_name, ema_data, prio in [("EMA 21", ema_21, 5), ("EMA 50", ema_50, 5), ("EMA 200", ema_200, 3)]:
                if ema_data[i] is not None and ema_data[i-1] is not None:
                    if closes[i] > ema_data[i] and closes[i-1] <= ema_data[i-1]:
                        signals.append({"name": f"{ema_name} BO", "bullish": True, "age": age, "priority": prio})
                    if closes[i] < ema_data[i] and closes[i-1] >= ema_data[i-1]:
                        signals.append({"name": f"{ema_name} BD", "bullish": False, "age": age, "priority": prio})

        # EMA crosses
        if i > 0:
            if all(v is not None for v in [ema_21[i], ema_50[i], ema_21[i-1], ema_50[i-1]]):
                if ema_21[i] > ema_50[i] and ema_21[i-1] <= ema_50[i-1]:
                    signals.append({"name": "21/50 EMA Bullish Cross", "bullish": True, "age": age, "priority": 4})
                if ema_21[i] < ema_50[i] and ema_21[i-1] >= ema_50[i-1]:
                    signals.append({"name": "21/50 EMA Bearish Cross", "bullish": False, "age": age, "priority": 4})
            if all(v is not None for v in [ema_50[i], ema_200[i], ema_50[i-1], ema_200[i-1]]):
                if ema_50[i] > ema_200[i] and ema_50[i-1] <= ema_200[i-1]:
                    signals.append({"name": "Golden Cross (50/200)", "bullish": True, "age": age, "priority": 2})
                if ema_50[i] < ema_200[i] and ema_50[i-1] >= ema_200[i-1]:
                    signals.append({"name": "Death Cross (50/200)", "bullish": False, "age": age, "priority": 2})

        # VWAP breaks
        if i > 0 and vwap_values[i] is not None and vwap_values[i-1] is not None:
            if closes[i] > vwap_values[i] and closes[i-1] <= vwap_values[i-1]:
                signals.append({"name": "VWAP BO", "bullish": True, "age": age, "priority": 4})
            if closes[i] < vwap_values[i] and closes[i-1] >= vwap_values[i-1]:
                signals.append({"name": "VWAP BD", "bullish": False, "age": age, "priority": 4})

        # Middle Line Break (price crossing trend midline m1, NOT Supertrend flip)
        if i > 0 and i < len(mid_series) and mid_series[i] is not None and mid_series[i-1] is not None:
            if closes[i] > mid_series[i] and closes[i-1] <= mid_series[i-1]:
                signals.append({"name": "Middle Line BO", "bullish": True, "age": age, "priority": 3})
            if closes[i] < mid_series[i] and closes[i-1] >= mid_series[i-1]:
                signals.append({"name": "Middle Line BD", "bullish": False, "age": age, "priority": 3})

        # Fibonacci level breaks (all 7 for signals)
        if fib_levels and "levels" in fib_levels and i > 0:
            for fib_name, fib_price in fib_levels["levels"].items():
                if closes[i] > fib_price and closes[i-1] <= fib_price:
                    signals.append({"name": f"Fib {fib_name} BO", "bullish": True, "age": age, "priority": 5})
                if closes[i] < fib_price and closes[i-1] >= fib_price:
                    signals.append({"name": f"Fib {fib_name} BD", "bullish": False, "age": age, "priority": 5})

        # ═══ CANDLESTICK PATTERNS — MATCH PINESCRIPT EXACTLY ═══
        if i >= 1:
            o0, c0, h0, l0 = opens[i], closes[i], highs[i], lows[i]
            o1, c1, h1, l1 = opens[i-1], closes[i-1], highs[i-1], lows[i-1]
            body0 = abs(c0 - o0)
            body1 = abs(c1 - o1)
            range1 = h1 - l1
            hl2_1 = (h1 + l1) / 2
            # S12-replaced: isNearKeyLevel now needs bar high/low + 20-bar swing extremes
            sw_start = max(0, i - 19)
            recent_high = max(highs[sw_start:i+1])
            recent_low = min(lows[sw_start:i+1])
            near_key = is_near_key_level(c0, h0, l0, recent_high, recent_low,
                                          trend_lines, atr_val,
                                          fib_382, fib_500, fib_618)

            if near_key:
                # Bullish Engulfing
                if c0 > o0 and c1 < o1 and o0 <= c1 and c0 >= o1 and body0 > body1:
                    signals.append({"name": "Bullish Engulfing", "bullish": True, "age": age, "priority": 4})
                # Bearish Engulfing
                if c0 < o0 and c1 > o1 and o0 >= c1 and c0 <= o1 and body0 > body1:
                    signals.append({"name": "Bearish Engulfing", "bullish": False, "age": age, "priority": 4})
                # Piercing Line: open < low[1], close > hl2[1], close < open[1], body > body[1]*0.5
                if c1 < o1 and c0 > o0 and o0 < l1 and c0 > hl2_1 and c0 < o1 and body0 > body1 * 0.5:
                    signals.append({"name": "Piercing Line", "bullish": True, "age": age, "priority": 5})
                # Dark Cloud Cover: open > high[1], close < hl2[1], close > open[1], body > body[1]*0.5
                if c1 > o1 and c0 < o0 and o0 > h1 and c0 < hl2_1 and c0 > o1 and body0 > body1 * 0.5:
                    signals.append({"name": "Dark Cloud Cover", "bullish": False, "age": age, "priority": 5})

            # Star Pattern: 3-bar, NO nearKeyLevel
            if i >= 2:
                o2, c2, h2, l2 = opens[i-2], closes[i-2], highs[i-2], lows[i-2]
                body_mid = abs(c1 - o1)
                range_mid = h1 - l1
                lb_start = max(0, i - 1 - adaptive_lookback + 1)
                lb_end = i
                if lb_end > lb_start:
                    lowest_low = min(lows[lb_start:lb_end])
                    highest_high = max(highs[lb_start:lb_end])
                else:
                    lowest_low = l0
                    highest_high = h0
                # Bull star: close>open, mid body small, close[1]<open[2], close>close[2], low<lowestLow
                if c0 > o0 and range_mid > 0 and body_mid < range_mid * 0.3 and c1 < o2 and c0 > c2 and l0 < lowest_low:
                    signals.append({"name": "Bullish Star Pattern", "bullish": True, "age": age, "priority": 5})
                # Bear star: close<open, mid body small, close[1]>open[2], close<close[2], high>highestHigh
                if c0 < o0 and range_mid > 0 and body_mid < range_mid * 0.3 and c1 > o2 and c0 < c2 and h0 > highest_high:
                    signals.append({"name": "Bearish Star Pattern", "bullish": False, "age": age, "priority": 5})

            # Three Soldiers/Crows: requires nearKeyLevel + open within prev body
            if near_key and i >= 2:
                o2, c2 = opens[i-2], closes[i-2]
                if (c2 > o2 and c1 > o1 and c0 > o0 and c0 > c1 and c1 > c2 and
                    o0 > o1 and o1 > o2 and o0 <= c1 and o1 <= c2):
                    signals.append({"name": "Three White Soldiers", "bullish": True, "age": age, "priority": 3})
                if (c2 < o2 and c1 < o1 and c0 < o0 and c0 < c1 and c1 < c2 and
                    o0 < o1 and o1 < o2 and o0 >= c1 and o1 >= c2):
                    signals.append({"name": "Three Black Crows", "bullish": False, "age": age, "priority": 3})

    # #7: dynamic pivot length from volatility (PineScript line 826).
    # basePivotLen = 2, dynamicPivotLen = max(1, round(2 * volatilityFactor)).
    dynamic_pivot_len = max(1, round(2 * volatility_factor))
    # #3-corrected: divergence age cap = signal_window (pivot search window stays
    # wider at 80 bars so we can still find pivot pairs that have one pivot old
    # enough to be meaningful but the second within signal_window of current).
    rsi_div = _detect_divergence(closes, lows, highs, rsi_values, "RSI",
                                  scan_range=80, pivot_len=dynamic_pivot_len,
                                  max_age=signal_window)
    signals.extend(rsi_div)
    # BrainWaves Divergence — source is `ribbon` (midline = canonical BW oscillator).
    # wt1/wt2 are components; all BW readings (threshold checks AND pivot-based
    # divergence) evaluate against the ribbon by definition. Matches visualizer's
    # ribbon definition. PineScript Brainer Pro main line 1507-1508 still uses
    # `wt1` — owner to update later to match.
    bw_div = _detect_divergence(closes, lows, highs, ribbon, "BrainWaves",
                                 scan_range=80, pivot_len=dynamic_pivot_len,
                                 max_age=signal_window)
    signals.extend(bw_div)
    # S10: consecutive-bar suppression (PineScript filterSignal/shouldShowPattern).
    # Must run BEFORE cancellation, matching PineScript order: filterSignal runs
    # at detection time; addSignalToTracking -> cancelOpposingSignals runs after.
    signals = _apply_consecutive_bar_suppression(signals, n)
    # Signal cancellation
    signals = _cancel_opposing_signals(signals)
    signals.sort(key=lambda s: (s["priority"], s["age"]))
    seen = set()
    unique = []
    for s in signals:
        base = s["name"].split(" @ ")[0] if " @ " in s["name"] else s["name"]
        if base not in seen:
            seen.add(base)
            unique.append(s)
    return unique[:max_sigs]


def _applies_shouldshowpattern(name):
    """Scope of PineScript filterSignal (lines 1633-1682, 1647-1654).
    APPLIES to: divergences, BW main crosses, EMA breaks/crosses, Fib breaks,
    all 8 candlestick patterns.
    DOES NOT apply to: BW zone entries, zero crosses, Broke Back, Middle Line,
    D-VWAP (these fire freely — no consecutive-bar suppression).
    """
    # Divergences (RSI + BrainWaves, regular + hidden — all now end with "Divergence")
    if "Divergence" in name:
        return True
    # BrainWaves main crosses (distinguished from zone entries by suffix "Cross")
    if name in ("BrainWaves Bullish Cross", "BrainWaves Bearish Cross",
                "BrainWaves Extreme Oversold Cross", "BrainWaves Extreme Overbought Cross"):
        return True
    # EMA breaks (EMA 21 BO / EMA 50 BD / EMA 200 BO ...) — B6: names have space
    if (name.startswith("EMA 21 ") or name.startswith("EMA 50 ") or name.startswith("EMA 200 ")) \
       and (" BO" in name or " BD" in name):
        return True
    # EMA crosses
    if name in ("21/50 EMA Bullish Cross", "21/50 EMA Bearish Cross",
                "Golden Cross (50/200)", "Death Cross (50/200)"):
        return True
    # Fib breaks (Fib X.Y BO / BD)
    if name.startswith("Fib ") and (" BO" in name or " BD" in name):
        return True
    # Candlestick patterns
    if name in ("Bullish Engulfing", "Bearish Engulfing",
                "Three White Soldiers", "Three Black Crows",
                "Piercing Line", "Dark Cloud Cover",
                "Bullish Star Pattern", "Bearish Star Pattern"):
        return True
    return False


def _apply_consecutive_bar_suppression(signals, n):
    """PineScript shouldShowPattern (lines 1614-1631): suppress a signal if it
    fires on the bar immediately after the same signal last fired. Tracking is
    per-pattern-name. lastBar updates EVEN when suppressing — a chain of
    consecutive-bar firings keeps only the first; a gap of 2+ bars resets.
    """
    groups = {}
    for idx, s in enumerate(signals):
        base = s["name"].split(" @ ")[0] if " @ " in s["name"] else s["name"]
        if _applies_shouldshowpattern(base):
            groups.setdefault(base, []).append((idx, s))
    suppressed = set()
    for base, pairs in groups.items():
        pairs_sorted = sorted(pairs, key=lambda p: -p[1]["age"])
        last_bar = None
        for idx, s in pairs_sorted:
            cur_bar = n - 1 - s["age"]
            if last_bar is not None and cur_bar - last_bar == 1:
                suppressed.add(idx)
            last_bar = cur_bar  # updates even on suppression — PineScript semantics
    return [s for i, s in enumerate(signals) if i not in suppressed]


def _detect_divergence(closes, lows, highs, indicator_values, indicator_name="RSI", scan_range=80, pivot_len=5, max_age=5):
    """Divergence detection matching PineScript `detectWaveDivergences` (lines 1492-1529).

    Price pivots are detected on `close` for BOTH high and low (PineScript lines
    1505-1506). Indicator pivots are detected on `indicator_values` with the same
    pivot length. A divergence is only considered if the price pivot AND the
    indicator pivot both occur at the same bar (lines 1513, 1521).

    `lows` and `highs` params are kept for call-site compat but unused —
    PineScript divergence does not reference them.
    """
    signals = []
    n = len(closes)
    if n < pivot_len * 2 + 5:
        return signals
    start = max(pivot_len, n - scan_range)
    pivot_lows = []   # bars where close AND indicator both bottom
    pivot_highs = []  # bars where close AND indicator both top
    for i in range(start, n - pivot_len):
        if indicator_values[i] is None:
            continue
        # Close pivots
        close_pivot_low = all(
            closes[i] < closes[i-j] and closes[i] < closes[i+j]
            for j in range(1, pivot_len + 1)
        )
        close_pivot_high = all(
            closes[i] > closes[i-j] and closes[i] > closes[i+j]
            for j in range(1, pivot_len + 1)
        )
        if not (close_pivot_low or close_pivot_high):
            continue
        # Indicator pivot at same bar — require indicator values on both sides
        ind_window_ok = all(
            indicator_values[i-j] is not None and indicator_values[i+j] is not None
            for j in range(1, pivot_len + 1)
        )
        if not ind_window_ok:
            continue
        ind_pivot_low = all(
            indicator_values[i] < indicator_values[i-j] and indicator_values[i] < indicator_values[i+j]
            for j in range(1, pivot_len + 1)
        )
        ind_pivot_high = all(
            indicator_values[i] > indicator_values[i-j] and indicator_values[i] > indicator_values[i+j]
            for j in range(1, pivot_len + 1)
        )
        if close_pivot_low and ind_pivot_low:
            pivot_lows.append(i)
        if close_pivot_high and ind_pivot_high:
            pivot_highs.append(i)
    # Pivot-low pairs → bullish / hidden-bull divergence
    for k in range(len(pivot_lows) - 1):
        a, b = pivot_lows[k], pivot_lows[k + 1]
        age = n - 1 - b
        if age > max_age:
            continue
        if closes[b] < closes[a] and indicator_values[b] > indicator_values[a]:
            signals.append({"name": f"{indicator_name} Bullish Divergence", "bullish": True, "age": age, "priority": 2})
        if closes[b] > closes[a] and indicator_values[b] < indicator_values[a]:
            signals.append({"name": f"{indicator_name} Hidden Bullish Divergence", "bullish": True, "age": age, "priority": 3})
    # Pivot-high pairs → bearish / hidden-bear divergence
    for k in range(len(pivot_highs) - 1):
        a, b = pivot_highs[k], pivot_highs[k + 1]
        age = n - 1 - b
        if age > max_age:
            continue
        if closes[b] > closes[a] and indicator_values[b] < indicator_values[a]:
            signals.append({"name": f"{indicator_name} Bearish Divergence", "bullish": False, "age": age, "priority": 2})
        if closes[b] < closes[a] and indicator_values[b] > indicator_values[a]:
            signals.append({"name": f"{indicator_name} Hidden Bearish Divergence", "bullish": False, "age": age, "priority": 3})
    return signals


def _cancel_opposing_signals(signals):
    """PineScript cancelOpposingSignals (lines 1836-1981).

    Model: each (predicate_a, predicate_b) pair — when BOTH match some signal(s)
    in the list, keep only the newest (lowest age) and drop all others matching
    either side. Approximates PineScript's "new signal added -> remove existing
    opposed signals" semantics in a single post-scan pass.

    Scope (Crypto thresholds 55/80 hardcoded — dashboard is Crypto-only):
      - Direct opposites: Middle Line, 7 Fib levels, 3 EMA levels, 2 EMA crosses,
        D-VWAP, BW main crosses, BW Extreme Cross, BW Zero crosses, 4 candlestick
        pattern pairs (21 pairs total).
      - Broke Back cancels matching zone entry (4 pairs).
      - Escalation: Extreme zone entry cancels regular zone entry of same side
        (2 pairs).

    Dropped from previous Python: `BrainWaves OB<->OS` (bogus — different zones,
    not opposites) and 4 divergence-opposite pairs (PineScript doesn't cancel
    divergences).
    """
    def base(s):
        return s["name"].split(" @ ")[0] if " @ " in s["name"] else s["name"]

    # Each tuple: (predicate_a, predicate_b) — both on base signal name (string).
    # When at least one signal matches A and at least one matches B, keep only
    # the newest (lowest age) across the union; drop the rest.
    cancel_pairs = [
        # ─── Direct opposites (symmetric) ───
        # Middle Line BO <-> BD
        (lambda n: "Middle Line BO" in n,              lambda n: "Middle Line BD" in n),
        # Fib breaks — 7 levels, same level opposite direction
        (lambda n: "Fib 0.0 BO" in n,                  lambda n: "Fib 0.0 BD" in n),
        (lambda n: "Fib 0.236 BO" in n,                lambda n: "Fib 0.236 BD" in n),
        (lambda n: "Fib 0.382 BO" in n,                lambda n: "Fib 0.382 BD" in n),
        (lambda n: "Fib 0.5 BO" in n,                  lambda n: "Fib 0.5 BD" in n),
        (lambda n: "Fib 0.618 BO" in n,                lambda n: "Fib 0.618 BD" in n),
        (lambda n: "Fib 0.786 BO" in n,                lambda n: "Fib 0.786 BD" in n),
        (lambda n: "Fib 1.0 BO" in n,                  lambda n: "Fib 1.0 BD" in n),
        # EMA breaks — 3 EMAs, same EMA opposite direction (B6: names have space)
        (lambda n: "EMA 21 BO" in n,                    lambda n: "EMA 21 BD" in n),
        (lambda n: "EMA 50 BO" in n,                    lambda n: "EMA 50 BD" in n),
        (lambda n: "EMA 200 BO" in n,                   lambda n: "EMA 200 BD" in n),
        # EMA crosses
        (lambda n: n == "21/50 EMA Bullish Cross",     lambda n: n == "21/50 EMA Bearish Cross"),
        (lambda n: n == "Golden Cross (50/200)",       lambda n: n == "Death Cross (50/200)"),
        # VWAP BO <-> BD
        (lambda n: "VWAP BO" in n,                     lambda n: "VWAP BD" in n),
        # BrainWaves main crosses
        (lambda n: n == "BrainWaves Bullish Cross",    lambda n: n == "BrainWaves Bearish Cross"),
        (lambda n: n == "BrainWaves Extreme Oversold Cross",
         lambda n: n == "BrainWaves Extreme Overbought Cross"),
        # BrainWaves zero crosses
        (lambda n: n == "BrainWaves Crossed Zero Up",  lambda n: n == "BrainWaves Crossed Zero Down"),
        # Candlestick opposites
        (lambda n: n == "Bullish Engulfing",           lambda n: n == "Bearish Engulfing"),
        (lambda n: n == "Three White Soldiers",        lambda n: n == "Three Black Crows"),
        (lambda n: n == "Piercing Line",               lambda n: n == "Dark Cloud Cover"),
        (lambda n: n == "Bullish Star Pattern",        lambda n: n == "Bearish Star Pattern"),
        # ─── Broke Back cancels matching zone entry ───
        (lambda n: "Broke Back Below 55" in n,
         lambda n: "BrainWaves Overbought 55" in n and "Reversal Possible" in n and "Extreme" not in n),
        (lambda n: "Broke Back Below 80" in n,
         lambda n: "BrainWaves Extreme Overbought 80" in n and "Reversal Possible" in n),
        (lambda n: "Broke Back Above -55" in n,
         lambda n: "BrainWaves Oversold -55" in n and "Reversal Possible" in n and "Extreme" not in n),
        (lambda n: "Broke Back Above -80" in n,
         lambda n: "BrainWaves Extreme Oversold -80" in n and "Reversal Possible" in n),
        # ─── Escalation: Extreme entry cancels regular entry of same side ───
        (lambda n: "BrainWaves Extreme Overbought 80" in n and "Reversal Possible" in n,
         lambda n: "BrainWaves Overbought 55" in n and "Reversal Possible" in n and "Extreme" not in n),
        (lambda n: "BrainWaves Extreme Oversold -80" in n and "Reversal Possible" in n,
         lambda n: "BrainWaves Oversold -55" in n and "Reversal Possible" in n and "Extreme" not in n),
    ]

    to_remove = set()
    for pred_a, pred_b in cancel_pairs:
        sigs_a = [s for s in signals if pred_a(base(s))]
        sigs_b = [s for s in signals if pred_b(base(s))]
        if sigs_a and sigs_b:
            all_in_pair = sigs_a + sigs_b
            newest = min(all_in_pair, key=lambda s: s["age"])
            for s in all_in_pair:
                if id(s) != id(newest):
                    to_remove.add(id(s))
    return [s for s in signals if id(s) not in to_remove]


# ═══════════════════════════════════════════════════════════════
#  TIMEFRAME HELPERS
# ═══════════════════════════════════════════════════════════════

def tf_to_minutes(tf):
    mapping = {"1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30, "1h": 60, "2h": 120, "4h": 240, "6h": 360, "8h": 480, "12h": 720, "1d": 1440, "3d": 4320, "1w": 10080, "1M": 43200}
    return mapping.get(tf, 5)


def tf_display_name(tf):
    mapping = {"1m": "1m", "3m": "3m", "5m": "5m", "15m": "15m", "30m": "30m", "1h": "1H", "2h": "2H", "4h": "4H", "6h": "6H", "8h": "8H", "12h": "12H", "1d": "1D", "3d": "3D", "1w": "1W", "1M": "1M"}
    return mapping.get(tf, tf)
