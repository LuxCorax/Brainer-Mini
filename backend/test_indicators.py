"""
Unit tests for indicators.py — aligned to current signatures after the
alignment pass + S/R fixes (#15/#16/#17) + Fib dedup + BW-div-on-ribbon.

Run: python3 test_indicators.py
Target: "ALL TESTS PASSED".
"""
import sys
import random

sys.path.insert(0, "/home/claude/test")

from indicators import (
    ema, sma, rsi, atr,
    get_atr_period, get_market_regime,
    compute_brainwaves, get_ribbon_state,
    compute_fibonacci, get_visible_fib_levels,
    compute_trend_lines,
    compute_supertrend, compute_adaptive_factor,
    compute_support_resistance,
    is_near_key_level,
    detect_signals,
    _last_valid,
)

passed = 0
failed = 0


def check(name, condition, detail=""):
    global passed, failed
    if condition:
        passed += 1
        print(f"  ✓ {name}")
    else:
        failed += 1
        print(f"  ✗ {name} — {detail}")


# ═══════════════════════════════════════════════════════════════
#  Synthetic BTC-like OHLCV data for reuse
# ═══════════════════════════════════════════════════════════════
random.seed(42)
N = 300
base_price = 85000.0
_prices = [base_price]
for _ in range(1, N):
    _prices.append(_prices[-1] * (1 + random.uniform(-0.003, 0.003)))
HIGHS = [p * random.uniform(1.001, 1.005) for p in _prices]
LOWS = [p * random.uniform(0.995, 0.999) for p in _prices]
CLOSES = _prices[:]
OPENS = [_prices[0]] + _prices[:-1]
VOLUMES = [random.uniform(100, 500) for _ in range(N)]
# 5m candles, one UTC day's worth of timestamps
_base_ts = 1713100800000
TIMESTAMPS = [_base_ts + i * 300_000 for i in range(N)]


# ═══════════════════════════════════════════════════════════════
print("═══ ATR period dispatch ═══")
# ═══════════════════════════════════════════════════════════════
check("ATR(5) for 1m", get_atr_period(1) == 5)
check("ATR(5) for 5m", get_atr_period(5) == 5)
check("ATR(5) for 15m", get_atr_period(15) == 5)
check("ATR(5) for 30m", get_atr_period(30) == 5)
check("ATR(8) for 60m", get_atr_period(60) == 8)
check("ATR(8) for 240m", get_atr_period(240) == 8)
atr5 = atr(HIGHS, LOWS, CLOSES, 5)
atr14 = atr(HIGHS, LOWS, CLOSES, 14)
check("ATR(5) has value at index 4", atr5[4] is not None)
check("ATR(14) has no value at index 4", atr14[4] is None)
check("ATR(14) has value at index 13", atr14[13] is not None)


# ═══════════════════════════════════════════════════════════════
print("\n═══ Adaptive factor (per-bar list — #10 / M1) ═══")
# ═══════════════════════════════════════════════════════════════
# Current API returns a list aligned to atr_values, with None during the
# 20-bar SMA warmup. Last entry is the meaningful scalar for current bar.
constant_atr = [100.0] * 30
factor_list = compute_adaptive_factor(constant_atr, 5, "Crypto")
check("Adaptive factor returns a list", isinstance(factor_list, list))
check("Adaptive factor list length == atr list length",
      len(factor_list) == len(constant_atr))
check("Adaptive factor None during 20-bar warmup",
      factor_list[0] is None and factor_list[18] is None)
check("Adaptive factor value present at bar 19 (first full SMA)",
      factor_list[19] is not None)
# With constant ATR: volatilityFactor = 1.0, adaptiveMult = 1.0,
# final = 1.0 * 1.2 (Crypto) * 1.0 (DAY) = 1.2
baseline = factor_list[-1]
check("Constant ATR → factor ~1.2 (Crypto DAY)",
      1.1 <= baseline <= 1.3, f"got {baseline}")

# Spike ATR → factor jumps
spike_atr = [100.0] * 20 + [500.0]
spike_list = compute_adaptive_factor(spike_atr, 5, "Crypto")
check("Spike ATR last factor > baseline",
      spike_list[-1] > baseline, f"spike={spike_list[-1]}, base={baseline}")

# SCALP (1m) tfMult 0.9 < DAY tfMult 1.0
scalp_list = compute_adaptive_factor(constant_atr, 1, "Crypto")
check("SCALP TF last factor < DAY TF last factor",
      scalp_list[-1] < baseline, f"scalp={scalp_list[-1]}, day={baseline}")


# ═══════════════════════════════════════════════════════════════
print("\n═══ Trend lines (null on bars==1) ═══")
# ═══════════════════════════════════════════════════════════════
tl = compute_trend_lines(HIGHS, LOWS, tf_minutes=5)
check("Trend lines dict has all 4 keys",
      set(tl.keys()) == {"upper", "mid", "lower", "mid_series"})
check("mid_series length == N", len(tl["mid_series"]) == N)
# Force a trend flip on the last bar → bars resets to 1 → upper/mid/lower null.
# Construction: 30 flat bars keep trend False (lows always equal window min).
# Last bar prints a new window-high AND its low sits above all prior lows,
# so `hh == highs[i]` fires (trend=True) but `ll == lows[i]` does not →
# trend flips False→True on the final bar → bars resets to 1 → null branch.
flip_h = [100.0] * 30 + [105.0]
flip_l = [99.0] * 30 + [100.5]
tl_flip = compute_trend_lines(flip_h, flip_l, tf_minutes=5)
check("On trend-flip bar: upper is None",
      tl_flip["upper"] is None, f"got {tl_flip['upper']}")
check("On trend-flip bar: mid is None",
      tl_flip["mid"] is None)
check("On trend-flip bar: lower is None",
      tl_flip["lower"] is None)
check("On trend-flip bar: mid_series[-1] is None",
      tl_flip["mid_series"][-1] is None)
# Normal case: all three values present, Upper >= Mid >= Lower, Mid = avg
if tl["upper"] is not None:
    check("Upper >= Mid >= Lower",
          tl["upper"] >= tl["mid"] >= tl["lower"],
          f"U={tl['upper']}, M={tl['mid']}, L={tl['lower']}")
    check("Mid == (Upper + Lower) / 2",
          abs(tl["mid"] - (tl["upper"] + tl["lower"]) / 2) < 1e-6)


# ═══════════════════════════════════════════════════════════════
print("\n═══ Ribbon state (Expanding / Contracting / Flat + None safety) ═══")
# ═══════════════════════════════════════════════════════════════
check("Expanding when |wt1-wt2| grows",
      get_ribbon_state([10.0, 20.0, 30.0], [5.0, 10.0, 12.0]) == "Expanding")
check("Contracting when |wt1-wt2| shrinks",
      get_ribbon_state([30.0, 20.0, 15.0], [10.0, 12.0, 13.0]) == "Contracting")
check("Flat when width equals prev",
      get_ribbon_state([10.0, 20.0], [5.0, 15.0]) == "Flat")
check("Flat when trailing value None",
      get_ribbon_state([None, 10.0], [None, 5.0]) == "Flat")
check("Flat when series too short",
      get_ribbon_state([10.0], [5.0]) == "Flat")


# ═══════════════════════════════════════════════════════════════
print("\n═══ Supertrend (scalar factor + list factor) ═══")
# ═══════════════════════════════════════════════════════════════
atr5_vals = atr(HIGHS, LOWS, CLOSES, 5)
st_scalar = compute_supertrend(HIGHS, LOWS, CLOSES, atr5_vals, factor=2.0)
valid_scalar = [v for v in st_scalar if v is not None]
check("Supertrend (scalar) produces values", len(valid_scalar) > 0)
check("Supertrend (scalar) outputs 0 or 1",
      all(v in (0, 1) for v in valid_scalar))

# List-factor path: per-bar list from compute_adaptive_factor
adaptive = compute_adaptive_factor(atr5_vals, 5, "Crypto")
st_list = compute_supertrend(HIGHS, LOWS, CLOSES, atr5_vals, factor=adaptive)
valid_list = [v for v in st_list if v is not None]
check("Supertrend (list factor) produces values", len(valid_list) > 0)
check("Supertrend (list factor) outputs 0 or 1",
      all(v in (0, 1) for v in valid_list))


# ═══════════════════════════════════════════════════════════════
print("\n═══ is_near_key_level (new 7+3 arg signature) ═══")
# ═══════════════════════════════════════════════════════════════
tl_mock = {"upper": 101.0, "mid": 100.0, "lower": 99.0}
# price sitting on the middle line, atr_val=1.0 → tol_large=0.5
check("Near when |price - mid| <= atr*0.5",
      is_near_key_level(100.0, 100.0, 100.0, 105.0, 95.0, tl_mock, 1.0))
# price 0.4 away from mid → still within tol_large (0.5)
check("Near when price just inside tol_large of mid",
      is_near_key_level(100.4, 100.4, 100.4, 105.0, 95.0, tl_mock, 1.0))
# Far from everything, no trend lines, no fib — only nearest-100 check left
check("Not near when far from all anchors",
      not is_near_key_level(150.3, 150.3, 150.3, 200.0, 50.0,
                            None, 1.0))
# Near a fib level, tol_fib = atr*0.3 = 0.3
check("Near fib_500 within tighter tol",
      is_near_key_level(99.7, 99.7, 99.7, 200.0, 50.0,
                        None, 1.0, fib_500=99.5))
# Just outside fib tolerance
check("Not near fib when outside tol_fib",
      not is_near_key_level(99.0, 99.0, 99.0, 200.0, 50.0,
                            None, 1.0, fib_500=99.5))
# Near recent swing high
check("Near when high == recent_high",
      is_near_key_level(104.8, 105.0, 104.5, 105.0, 95.0, None, 1.0))
# ATR invalid → always False
check("Returns False when atr_val is 0",
      not is_near_key_level(100.0, 100.0, 100.0, 100.0, 100.0, tl_mock, 0))
check("Returns False when atr_val is None",
      not is_near_key_level(100.0, 100.0, 100.0, 100.0, 100.0, tl_mock, None))


# ═══════════════════════════════════════════════════════════════
print("\n═══ Fibonacci basics + dedup vs trend lines (#1117-1124) ═══")
# ═══════════════════════════════════════════════════════════════
fib = compute_fibonacci(HIGHS, LOWS, CLOSES, lookback=144)
check("Fib computed", bool(fib))
check("Fib 0.0 == swing_low", abs(fib["levels"]["0.0"] - fib["swing_low"]) < 1e-6)
check("Fib 1.0 == swing_high", abs(fib["levels"]["1.0"] - fib["swing_high"]) < 1e-6)
# Levels must be ascending: low → high
_prev = float("-inf")
_ascending = True
for r in ["0.0", "0.236", "0.382", "0.5", "0.618", "0.786", "1.0"]:
    if fib["levels"][r] <= _prev:
        _ascending = False
    _prev = fib["levels"][r]
check("Fib levels ascending (low→high)", _ascending)

# Dedup vs trend lines — fib 0.0 within close*0.0001 of lower should drop
close_price = 100.0
fib_tol = close_price * 0.0001   # 0.01
fake_fib = {
    "levels": {
        "0.0": 99.0,        # == lower exactly → within tol → DROP
        "0.236": 99.5,
        "0.382": 99.7,
        "0.5": 100.2,
        "0.618": 100.4,
        "0.786": 100.8,
        "1.0": 101.0,       # == upper exactly → within tol → DROP
    },
    "swing_low": 99.0,
    "swing_high": 101.0,
}
tl_for_dedup = {"upper": 101.0, "mid": 100.0, "lower": 99.0}
visible_with_dedup = get_visible_fib_levels(
    fake_fib, close_price, close=close_price, trend_lines=tl_for_dedup
)
vis_sources = [v["source"] for v in visible_with_dedup]
check("Fib 0.0 dropped when coincides with Lower trend line",
      "Fib 0.0" not in vis_sources, f"sources: {vis_sources}")
check("Fib 1.0 dropped when coincides with Upper trend line",
      "Fib 1.0" not in vis_sources, f"sources: {vis_sources}")

# Outside tolerance → kept. Shift 0.0 and 1.0 to sit beyond close*0.0001.
fake_fib_far = dict(fake_fib)
fake_fib_far["levels"] = dict(fake_fib["levels"])
fake_fib_far["levels"]["0.0"] = 99.0 - 10 * fib_tol   # clearly outside tol
fake_fib_far["levels"]["1.0"] = 101.0 + 10 * fib_tol
visible_no_dedup = get_visible_fib_levels(
    fake_fib_far, close_price, close=close_price, trend_lines=tl_for_dedup
)
# At most 4 visible (2 above + 2 below)
check("Visible fib cap (<= 4 entries)", len(visible_no_dedup) <= 4)
above = [v for v in visible_no_dedup if v["price"] > close_price]
below = [v for v in visible_no_dedup if v["price"] < close_price]
check("At most 2 fibs above price", len(above) <= 2)
check("At most 2 fibs below price", len(below) <= 2)

# Without trend_lines (None) — 0.0 and 1.0 are kept regardless, subject to cap.
vis_no_tl = get_visible_fib_levels(fake_fib, close_price)
all_fib_sources = [v["source"] for v in vis_no_tl]
check("Without trend_lines, dedup does not happen",
      len(all_fib_sources) <= 4 and len(all_fib_sources) > 0)


# ═══════════════════════════════════════════════════════════════
print("\n═══ S/R split-first (#15), sort (#17), cluster display (#16) ═══")
# ═══════════════════════════════════════════════════════════════
atr13 = _last_valid(atr(HIGHS, LOWS, CLOSES, 13)) or 100.0
ema_dict = {
    "EMA 21": ema(CLOSES, 21),
    "EMA 50": ema(CLOSES, 50),
    "EMA 200": ema(CLOSES, 200),
}
vwap_vals_flat = [CLOSES[-1]] * N
sr = compute_support_resistance(
    HIGHS, LOWS, CLOSES, VOLUMES, TIMESTAMPS,
    fib, ema_dict, vwap_vals_flat, atr13,
    trend_lines=tl,
)
close_now = CLOSES[-1]
# Compare against the same 8-decimal rounding the entries use, so display
# rounding (round(price, 8)) doesn't push a true support entry 1e-8 above
# the unrounded close and falsely fail the <= check.
close_now_rounded = round(close_now, 8)

# #15: all resistance prices > close, all support prices <= close
res_all_above = all(r["price"] > close_now_rounded for r in sr["resistance"])
sup_all_at_or_below = all(s["price"] <= close_now_rounded for s in sr["support"])
check("All resistance entries have price > close (#15)",
      res_all_above,
      f"close={close_now_rounded}, prices={[r['price'] for r in sr['resistance']]}")
check("All support entries have price <= close (#15)",
      sup_all_at_or_below,
      f"close={close_now_rounded}, prices={[s['price'] for s in sr['support']]}")

# #17: resistance ASC, support DESC
res_prices = [r["price"] for r in sr["resistance"]]
sup_prices = [s["price"] for s in sr["support"]]
check("Resistance sorted ASC (nearest above first, #17)",
      res_prices == sorted(res_prices),
      f"got {res_prices}")
check("Support sorted DESC (nearest below first, #17)",
      sup_prices == sorted(sup_prices, reverse=True),
      f"got {sup_prices}")

# #16: cluster display shape
required_keys = {"price", "price_low", "price_high", "sources", "cluster_size", "is_range"}
all_entries = sr["resistance"] + sr["support"]
check("Every S/R entry has full display shape (#16)",
      all(required_keys.issubset(e.keys()) for e in all_entries),
      f"first entry keys: {set(all_entries[0].keys()) if all_entries else 'none'}")
check("Every entry: price == price_low (#16)",
      all(e["price"] == e["price_low"] for e in all_entries))

# is_range logic: True iff cluster_size > 1 AND spread_pct >= 0.2
def _compute_is_range(e):
    if e["cluster_size"] <= 1:
        return False
    if e["price_low"] <= 0:
        return False
    spread_pct = (e["price_high"] - e["price_low"]) / e["price_low"] * 100
    return spread_pct >= 0.2

check("is_range matches cluster_size>1 AND spread_pct>=0.2 for all entries (#16)",
      all(e["is_range"] == _compute_is_range(e) for e in all_entries),
      "mismatch on at least one entry")

# Trend-line sources should appear somewhere in S/R (primary sources)
flat_sources = [s for e in all_entries for s in e["sources"]]
check("Trend-line sources (Upper/Mid/Lower) appear in S/R output",
      any(name in flat_sources for name in ("Upper", "Mid", "Lower")),
      f"sources found: {set(flat_sources)}")


# ═══════════════════════════════════════════════════════════════
print("\n═══ BrainWaves divergence fires on ribbon (not wt1) ═══")
# ═══════════════════════════════════════════════════════════════
# Construct a custom bw dict with:
#   - wt1 / wt2 held constant → no cross / threshold / zone signals
#   - ribbon crafted with a pivot-low pair that diverges from price pivots
#
# If the detector were still keyed to wt1 (constant, no pivots), no BW
# divergence would fire. Firing "BrainWaves Bullish Divergence" proves
# ribbon is the source for divergence, per the alignment fix.
n = 30
closes_div = [100.0] * n
highs_div = [100.5] * n
lows_div = [99.5] * n
opens_div = [100.0] * n

# Pivot A at bar 15, pivot B at bar 25 — both pivot_len >= 2 deep
closes_div[13] = 100.0; closes_div[14] = 99.0; closes_div[15] = 98.0
closes_div[16] = 99.0; closes_div[17] = 100.0
closes_div[23] = 100.0; closes_div[24] = 97.0; closes_div[25] = 95.0
closes_div[26] = 97.0; closes_div[27] = 100.0
# Price forms a LOWER low (95 < 98). Classic bullish divergence setup.

# Ribbon forms a HIGHER low (60 > 50) at the same pivot bars
ribbon_div = [70.0] * n
ribbon_div[13] = 70.0; ribbon_div[14] = 55.0; ribbon_div[15] = 50.0
ribbon_div[16] = 55.0; ribbon_div[17] = 70.0
ribbon_div[23] = 80.0; ribbon_div[24] = 65.0; ribbon_div[25] = 60.0
ribbon_div[26] = 65.0; ribbon_div[27] = 80.0

bw_custom = {
    "wt1": [0.0] * n,
    "wt2": [0.0] * n,
    "ribbon": ribbon_div,
    "thresholds": {"ob": 55, "os": -55, "eob": 80, "eos": -80},
}

# Put EMAs / VWAP far from closes so no break signals fire accidentally
far_const = [500.0] * n
rsi_vals = rsi([(h + l) / 2 for h, l in zip(highs_div, lows_div)], 13)

sigs_ribbon = detect_signals(
    closes_div, highs_div, lows_div, opens_div,
    bw_custom, rsi_vals,
    far_const, far_const, far_const, far_const,
    fib_levels=None, atr_val=1.0, tf_minutes=5,
    trend_lines=None, volatility_factor=1.0,
)
sig_names_ribbon = {s["name"] for s in sigs_ribbon}
check("BrainWaves Bullish Divergence fires when ribbon diverges",
      "BrainWaves Bullish Divergence" in sig_names_ribbon,
      f"signals: {sorted(sig_names_ribbon)}")

# Negative control: identical price pivots, but ribbon ALSO makes a lower low
# (no divergence in ribbon). Signal must NOT fire.
ribbon_no_div = [70.0] * n
ribbon_no_div[13] = 70.0; ribbon_no_div[14] = 55.0; ribbon_no_div[15] = 50.0
ribbon_no_div[16] = 55.0; ribbon_no_div[17] = 70.0
ribbon_no_div[23] = 60.0; ribbon_no_div[24] = 50.0; ribbon_no_div[25] = 40.0
ribbon_no_div[26] = 50.0; ribbon_no_div[27] = 60.0   # ribbon lower-low too

bw_no_div = dict(bw_custom)
bw_no_div["ribbon"] = ribbon_no_div
sigs_no_div = detect_signals(
    closes_div, highs_div, lows_div, opens_div,
    bw_no_div, rsi_vals,
    far_const, far_const, far_const, far_const,
    fib_levels=None, atr_val=1.0, tf_minutes=5,
    trend_lines=None, volatility_factor=1.0,
)
sig_names_no_div = {s["name"] for s in sigs_no_div}
check("BrainWaves Bullish Divergence does NOT fire when ribbon also makes a lower low",
      "BrainWaves Bullish Divergence" not in sig_names_no_div,
      f"signals: {sorted(sig_names_no_div)}")


# ═══════════════════════════════════════════════════════════════
print("\n═══ Smoke: detect_signals on N=300 synthetic data ═══")
# ═══════════════════════════════════════════════════════════════
bw_full = compute_brainwaves(HIGHS, LOWS, 5, "Crypto")
rsi_full = rsi([(h + l) / 2 for h, l in zip(HIGHS, LOWS)], 13)
ema21 = ema(CLOSES, 21)
ema50 = ema(CLOSES, 50)
ema200 = ema(CLOSES, 200)

# Wrap in try/except so an unexpected exception fails the test loudly
smoke_exception = None
smoke_signals = []
try:
    smoke_signals = detect_signals(
        CLOSES, HIGHS, LOWS, OPENS, bw_full, rsi_full,
        ema21, ema50, ema200, vwap_vals_flat,
        fib_levels=fib, supertrend_values=None,
        atr_val=atr13, tf_minutes=5, trend_lines=tl,
        volatility_factor=1.0,
    )
except Exception as e:
    smoke_exception = e

check("Smoke: detect_signals does not raise",
      smoke_exception is None,
      f"exception: {smoke_exception}")

# DAY regime max_sigs = 10 (indicators.py line 731)
check("Smoke: returns <= max_sigs (10 for DAY 5m)",
      len(smoke_signals) <= 10,
      f"got {len(smoke_signals)} signals")

# Every signal has the expected shape
required_sig_keys = {"name", "age"}
check("Smoke: every signal has required keys (name, age)",
      all(required_sig_keys.issubset(s.keys()) for s in smoke_signals))


# ═══════════════════════════════════════════════════════════════
print("\n" + "═" * 50)
print(f"Results: {passed} passed, {failed} failed")
if failed:
    print("SOME TESTS FAILED!")
    sys.exit(1)
else:
    print("ALL TESTS PASSED!")
