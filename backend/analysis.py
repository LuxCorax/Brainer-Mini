"""
Analysis module: orchestrates indicator computation for a given pair.
Returns the exact JSON shape the Mini App frontend expects.
"""
import asyncio
import logging
from typing import Dict, Optional

from binance_client import fetch_klines, fetch_ticker_price, fetch_multi_tf
from indicators import (
    ema, sma, rsi, atr,
    compute_brainwaves, compute_vwap, compute_fibonacci,
    compute_support_resistance, compute_volume_analysis,
    compute_supertrend, compute_trend_lines, detect_signals,
    get_ribbon_state,
    compute_adaptive_factor, compute_volatility_factor, get_atr_period,
    tf_to_minutes, tf_display_name, _last_valid,
)
from mtf_analysis import analyze_mtf
from config import CTF, LTF, HTF

logger = logging.getLogger(__name__)


async def get_full_analysis(symbol: str) -> Optional[Dict]:
    """
    Compute full analysis for a symbol.
    Returns JSON matching the frontend's expected shape.
    """
    symbol = symbol.upper()
    ctf = CTF   # 5m
    ltf = LTF   # 1m
    htf = HTF   # 15m

    # Fetch data in parallel
    import httpx
    async with httpx.AsyncClient(timeout=15.0) as client:
        ticker_task = fetch_ticker_price(symbol, client=client)
        multi_tf_task = fetch_multi_tf(symbol, [ctf, ltf, htf], client=client)
        ticker, tf_data = await asyncio.gather(ticker_task, multi_tf_task)

    if ticker is None:
        logger.error(f"No ticker data for {symbol}")
        return None

    ctf_data = tf_data.get(ctf)
    ltf_data = tf_data.get(ltf)
    htf_data = tf_data.get(htf)

    if ctf_data is None:
        logger.error(f"No CTF data for {symbol}")
        return None

    # Analyze each timeframe
    ctf_analysis = _analyze_timeframe(ctf_data, ctf)
    ltf_analysis = _analyze_timeframe(ltf_data, ltf) if ltf_data else None
    htf_analysis = _analyze_timeframe(htf_data, htf) if htf_data else None

    # BrainWaves (CTF only, for chart rendering)
    bw = ctf_analysis["brainwaves"] if ctf_analysis else None

    # MTF Alignment
    ctf_bias = ctf_analysis["bias"] if ctf_analysis else "NEUTRAL"
    ltf_bias = ltf_analysis["bias"] if ltf_analysis else "NEUTRAL"
    htf_bias = htf_analysis["bias"] if htf_analysis else "NEUTRAL"
    mtf_summary = analyze_mtf(ctf_analysis, ltf_analysis, htf_analysis)

    # Signals (CTF)
    signals_list = []
    if ctf_analysis:
        ca = ctf_analysis
        signals_list = detect_signals(
            ctf_data["closes"], ctf_data["highs"], ctf_data["lows"],
            ctf_data["opens"], ca["bw_raw"], ca["rsi_values"],
            ca["ema_21"], ca["ema_50"], ca["ema_200"],
            ca["vwap_values"],
            fib_levels=ca.get("fib"),
            supertrend_values=ca.get("supertrend"),
            atr_val=ca.get("atr_val", 0),
            tf_minutes=tf_to_minutes(ctf),
            trend_lines=ca.get("trend_lines"),
            volatility_factor=ca.get("volatility_factor", 1.0),
        )

    # Levels (CTF)
    levels = ctf_analysis["levels"] if ctf_analysis else {"resistance": [], "support": []}

    # Volume (CTF)
    volume = ctf_analysis["volume"] if ctf_analysis else {}

    # BrainWaves chart data
    bw_chart = _format_bw_chart(bw) if bw else None

    return {
        "symbol": symbol,
        "price": ticker["price"],
        "change": ticker["change_pct"],
        "high_24h": ticker["high_24h"],
        "low_24h": ticker["low_24h"],
        "brainwaves": bw_chart,
        "mtf": {
            "ctf": _format_mtf_card(ctf_analysis, ctf, ctf_bias, is_ctf=True),
            "ltf": _format_mtf_card(ltf_analysis, ltf, ltf_bias, is_ctf=False),
            "htf": _format_mtf_card(htf_analysis, htf, htf_bias, is_ctf=False),
            "alignment": mtf_summary,
        },
        "volume": volume,
        "signals": [
            {"name": s["name"], "bullish": s["bullish"], "age": s["age"]}
            for s in signals_list
        ],
        "levels": levels,
    }


def _analyze_timeframe(data: Dict, tf: str) -> Optional[Dict]:
    """Run all indicators on a single timeframe's candle data."""
    if not data or not data.get("closes"):
        return None

    closes = data["closes"]
    highs = data["highs"]
    lows = data["lows"]
    opens = data["opens"]
    volumes = data["volumes"]
    timestamps = data["timestamps"]
    tf_min = tf_to_minutes(tf)

    # HL2 for RSI (PineScript uses hl2)
    hl2 = [(h + l) / 2 for h, l in zip(highs, lows)]

    # RSI
    rsi_values = rsi(hl2, 13)

    # EMAs
    ema_21 = ema(closes, 21)
    ema_50 = ema(closes, 50)
    ema_200 = ema(closes, 200)

    # ATR — PineScript: period 5 for <60m, 8 for >=60m (FIX #1)
    atr_period = get_atr_period(tf_min)
    atr_values = atr(highs, lows, closes, atr_period)
    atr_val = _last_valid(atr_values) or 0

    # Cluster tolerance uses ATR(13) (PineScript getClusterTolerance)
    atr_values_13 = atr(highs, lows, closes, 13)
    cluster_atr = _last_valid(atr_values_13) or atr_val

    # #7: volatility factor for dynamic divergence pivot length
    volatility_factor = compute_volatility_factor(atr_values)

    # BrainWaves
    bw = compute_brainwaves(highs, lows, tf_min, "Crypto")
    bw_raw = bw

    # VWAP
    vwap_values = compute_vwap(highs, lows, closes, volumes, timestamps)
    vwap_val = _last_valid(vwap_values)

    # Adaptive Supertrend factor (FIX #10: use atrVal/sma(atrVal,20), not percentile)
    combined_factor = compute_adaptive_factor(atr_values, tf_min, "Crypto")
    st = compute_supertrend(highs, lows, closes, atr_values, factor=combined_factor)
    last_st = _last_valid(st)
    bias = "BULLISH" if last_st == 1 else "BEARISH"

    # Trend lines — Upper/Mid/Lower (FIX #3: new computation for S/R + Middle Line Break)
    trend_lines_data = compute_trend_lines(highs, lows, tf_min)

    # Fibonacci — timeframe-specific lookback (matches PineScript getFibLookbackEarly)
    fib_lookback = {1: 120, 5: 144, 15: 96, 30: 96, 60: 120, 240: 84}.get(tf_min, 100)
    fib = compute_fibonacci(highs, lows, closes, lookback=fib_lookback)

    # S/R levels — with trend lines as PRIMARY source (FIX #3, #5)
    ema_dict = {"EMA 21": ema_21, "EMA 50": ema_50, "EMA 200": ema_200}
    levels = compute_support_resistance(
        highs, lows, closes, volumes, timestamps,
        fib, ema_dict, vwap_values, cluster_atr,
        trend_lines=trend_lines_data,
    )

    # Volume
    volume = compute_volume_analysis(opens, closes, highs, lows, volumes)

    # RSI trend
    last_rsi = _last_valid(rsi_values)
    prev_rsi = rsi_values[-2] if len(rsi_values) > 1 and rsi_values[-2] is not None else None
    rsi_trend = "RISING" if (last_rsi and prev_rsi and last_rsi > prev_rsi) else "FALLING" if (last_rsi and prev_rsi and last_rsi < prev_rsi) else "FLAT"

    # B5: BW trend reading uses `ribbon` (the canonical BW oscillator), not
    # `wt1` (a component). All other BW signals already use ribbon — this
    # closes the last orphan. `bw_val` shown in the MTF card also switches
    # to ribbon for consistency.
    last_ribbon = _last_valid(bw["ribbon"])
    prev_ribbon = bw["ribbon"][-2] if len(bw["ribbon"]) > 1 and bw["ribbon"][-2] is not None else None
    bw_trend = "RISING" if (last_ribbon and prev_ribbon and last_ribbon > prev_ribbon) else "FALLING" if (last_ribbon and prev_ribbon and last_ribbon < prev_ribbon) else "STABLE"
    ribbon_state = get_ribbon_state(bw["wt1"], bw["wt2"])

    return {
        "bias": bias,
        "rsi": round(last_rsi, 1) if last_rsi else None,
        "rsi_trend": rsi_trend,
        "rsi_values": rsi_values,
        "bw_val": round(last_ribbon, 1) if last_ribbon else None,
        "bw_trend": bw_trend,
        "ribbon_state": ribbon_state,
        "brainwaves": bw,
        "bw_raw": bw_raw,
        "vwap_values": vwap_values,
        "ema_21": ema_21,
        "ema_50": ema_50,
        "ema_200": ema_200,
        "levels": levels,
        "volume": volume,
        "fib": fib,
        "supertrend": st,
        "trend_lines": trend_lines_data,
        "atr_val": atr_val,
        "volatility_factor": volatility_factor,
    }


def _format_mtf_card(analysis: Optional[Dict], tf: str, bias: str, is_ctf: bool = False) -> Dict:
    """Format a single MTF card for the frontend."""
    if analysis is None:
        # B4: fallback must honor is_ctf — CTF shows "Neutral", LTF/HTF "Stable".
        flat_label = "Neutral" if is_ctf else "Stable"
        return {
            "name": tf_display_name(tf),
            "tf": tf,
            "bias": "NEUTRAL",
            "rsi": None,
            "rsi_trend": flat_label,
            "bw_val": None,
            "bw_trend": flat_label,
            "ribbon_state": "Flat",
        }

    # RSI trend label: CTF uses "Neutral", LTF/HTF use "Stable" (FIX #8)
    rsi_trend = analysis["rsi_trend"]
    if rsi_trend == "FLAT":
        rsi_trend_label = "Neutral" if is_ctf else "Stable"
    elif rsi_trend == "RISING":
        rsi_trend_label = "Rising"
    else:
        rsi_trend_label = "Falling"

    # BW trend label: CTF uses "Neutral", LTF/HTF use "Stable" (same pattern)
    bw_trend = analysis.get("bw_trend", "STABLE")
    if bw_trend in ("STABLE", "FLAT"):
        bw_trend_label = "Neutral" if is_ctf else "Stable"
    elif bw_trend == "RISING":
        bw_trend_label = "Rising"
    else:
        bw_trend_label = "Falling"

    return {
        "name": tf_display_name(tf),
        "tf": tf,
        "bias": bias,
        "rsi": analysis["rsi"],
        "rsi_trend": rsi_trend_label,
        "bw_val": analysis.get("bw_val"),
        "bw_trend": bw_trend_label,
        "ribbon_state": analysis.get("ribbon_state", "Flat"),
    }


def _format_bw_chart(bw: Dict, points: int = 100) -> Dict:
    """Format BrainWaves data for canvas rendering (last N points).

    B8: ribbon and crosses are already computed by `compute_brainwaves` —
    reuse them instead of recomputing. Crosses get index translation from
    full-series space into the 100-point slice space; out-of-window crosses
    are dropped.
    """
    full_len = len(bw["wt1"])
    wt1 = bw["wt1"][-points:]
    wt2 = bw["wt2"][-points:]
    hist = bw["histogram"][-points:]
    ribbon = bw["ribbon"][-points:]   # B8: reuse upstream ribbon, no recompute

    clean = lambda lst: [round(v, 2) if v is not None else 0 for v in lst]

    # B8: translate full-series cross indices into the slice's 0..points-1
    # space; drop any cross outside the visible window. Frontend keys
    # preserved (x/y/type/extreme).
    offset = max(0, full_len - points)
    crosses = []
    for c in bw.get("crosses", []):
        slice_idx = c["index"] - offset
        if slice_idx < 0 or slice_idx >= len(wt1):
            continue
        crosses.append({
            "x": slice_idx,
            "y": c["wt1"],
            "type": c["type"],
            "extreme": c["extreme"],
        })

    return {
        "wt1": clean(wt1),
        "wt2": clean(wt2),
        "histogram": clean(hist),
        "ribbon": clean(ribbon),
        "crosses": crosses,
        "thresholds": bw["thresholds"],
    }
