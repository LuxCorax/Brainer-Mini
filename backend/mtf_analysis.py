"""
MTF Analysis Engine — implements the scenario catalog's decision flow.

Pure function: analyze_mtf(ctf_dict, ltf_dict, htf_dict) -> dict.

Returns one of 42 catalog entries selected by priority-ordered routing:
  Priority 1 — Overrides (capitulation top, capitulation bottom, all neutral)
  Priority 2 — State tier (stuck, compression, mostly-neutral, partial-neutral)
  Priority 3 — Scenario tier (8 bias codes, variants test most-specific-first)

Inputs are the full _analyze_timeframe output dicts (NOT the reduced
_format_mtf_card shape). Reads only these per-TF fields:
    bias, rsi, rsi_trend, bw_val, bw_trend, ribbon_state.

Output shape (catalog spec):
    {
      "scenario_code": "BRB",         # literal 3-letter B/R/N from biases
      "sentiment":     "opportunity_long",
      "confidence":    "moderate",    # hard-coded per catalog entry
      "paragraph":     "...",         # 3 sentences, ~40-65 words
      "conflicts":     [],            # backup channel; empty by default
      "capitulation":  False,         # True only when 5m capitulation fires
    }

Standalone module — no external dependencies beyond typing.
"""
from typing import Dict, Optional, Any


# ══════════════════════════════════════════════════════════════════════
#  ENTRIES — all 42 catalog entries, paragraph + sentiment + confidence.
#  Keys are catalog IDs (used internally only — output uses scenario_code).
#  Prose transcribed verbatim from mtf_engine_scenario_catalog.md.
# ══════════════════════════════════════════════════════════════════════

ENTRIES: Dict[str, Dict[str, Any]] = {
    # ── §1 OVERRIDE TIER ──
    "1.1": {
        "paragraph": "The 5m is in blow-off territory — deeply overbought and already starting to roll over. Momentum has stretched into the upper extreme and turned, the kind of reading that's hard to sustain on its own. Reads as exhaustion at the top: typically marks at minimum a meaningful pause, often the start of a sharper unwind.",
        "sentiment": "caution",
        "confidence": "low",
        "capitulation": True,
    },
    "1.2": {
        "paragraph": "The 5m is in capitulation territory — deeply oversold with the first signs of a turn already there. Momentum has pushed into the lower extreme and started to lift, though the larger frame hasn't confirmed the flip yet. Reads as exhaustion at the lows: typically marks at minimum a meaningful bounce, sometimes a more lasting turn.",
        "sentiment": "opportunity_long",
        "confidence": "low",
        "capitulation": True,
    },
    "1.3": {
        "paragraph": "All three frames are sitting in indecision with no side holding control. The stack is waiting — momentum isn't committed in either direction across any timeframe. Reads as a market between moves: direction will come from outside the current picture rather than from anything already on the chart.",
        "sentiment": "neutral",
        "confidence": "low",
    },

    # ── §2 STATE TIER ──
    "2.1": {
        "paragraph": "All three frames are contracting and the bias signals don't agree. Nothing is being pushed with energy — momentum is cooling across every layer even as direction stays uncommitted. Reads as a compressed pause: waiting for one side to force a move, unlikely to produce anything decisive from here without a fresh impulse.",
        "sentiment": "neutral",
        "confidence": "low",
    },
    "2.2": {
        "paragraph": "Momentum is coiling across multiple frames with readings hovering near their midline. The stack is pent up rather than moving — conditions like this tend to resolve into expansion once one frame releases. Reads as coiled pressure: direction is unresolved, but the squeeze itself signals one is coming.",
        "sentiment": "neutral",
        "confidence": "low",
    },
    "2.3a-agree": {
        "paragraph": "The short-term is sitting flat without taking a side, even though the 5m and the bigger trend are both pointing the same way. The outer frames have a clean read, but the 1m is holding back rather than confirming it. Reads as a setup where the broader picture is clear but short-term engagement is missing: timing usually catches up once the 1m joins in.",
        "sentiment": "neutral",
        "confidence": "low",
    },
    "2.3a-oppose": {
        "paragraph": "The 5m and the bigger trend are pulling in opposite directions, and the short-term isn't taking a side either way. There's an active disagreement between frames without the 1m to mediate. Reads as a conflicted setup: direction needs either the 1m to pick a side or one of the outer frames to capitulate.",
        "sentiment": "neutral",
        "confidence": "low",
    },
    "2.3b-agree": {
        "paragraph": "The 5m has gone neutral while the short-term and the bigger trend are both pointing the same way. The surrounding frames have agreement but the current chart itself is uncommitted. Reads as a setup with direction waiting on the 5m: clarity arrives once the current chart confirms what the neighbors already show.",
        "sentiment": "neutral",
        "confidence": "low",
    },
    "2.3b-oppose": {
        "paragraph": "The 5m is sitting neutral while the short-term and the bigger trend are pushing in opposite directions. Without the 5m committing, there's nothing mediating the conflict between the fast and slow frames. Reads as a conflicted setup: direction needs the 5m to re-engage and pick a side.",
        "sentiment": "neutral",
        "confidence": "low",
    },
    "2.3c-agree": {
        "paragraph": "The 5m and short-term are pushing in the same direction, but the bigger trend hasn't committed to either side. The lower frames have agreement without the macro behind them. Reads as a setup missing its macro anchor: stronger moves typically wait until the larger frame joins in.",
        "sentiment": "neutral",
        "confidence": "low",
    },
    "2.3c-oppose": {
        "paragraph": "The 5m and short-term are pushing in opposite directions while the bigger trend is sitting neutral. The lower frames are already in conflict without the macro to anchor either side. Reads as a fragmented setup: direction comes from whichever frame wins the near-term fight, or from the bigger trend finally committing.",
        "sentiment": "neutral",
        "confidence": "low",
    },
    "2.4a": {
        "paragraph": "The 5m is the only frame taking a side, with both the short-term and the bigger trend sitting flat. The 5m carries the read alone — no macro confirmation and no short-term timing behind it. Reads as a weak, isolated signal: direction like this often fails when both neighboring frames refuse to commit.",
        "sentiment": "neutral",
        "confidence": "low",
    },
    "2.4b": {
        "paragraph": "The short-term is the only frame moving while both the 5m and the bigger trend sit flat. The 1m is pushing without any context from the surrounding frames to confirm it. Reads as an isolated short-term pulse: rarely holds on its own when neither the 5m nor the macro will join in.",
        "sentiment": "neutral",
        "confidence": "low",
    },
    "2.4c": {
        "paragraph": "The bigger trend holds a read but the 5m and the short-term are both sitting flat. The macro has direction while the immediate picture stays uncommitted across the near frames. Reads as a macro-only signal waiting for engagement: the lower frames need to wake up before direction actually expresses in price action.",
        "sentiment": "neutral",
        "confidence": "low",
    },

    # ── §3 SCENARIO TIER — BBB family ──
    "3.1": {
        "paragraph": "The 5m is bullish with bulls firmly in control, and the bigger trend is still actively pushing higher underneath. The short-term is rising in step — no frame is fading. Reads as a clean continuation setup: alignment this deep typically extends rather than stalls.",
        "sentiment": "strong_bull",
        "confidence": "high",
    },
    "3.1b": {
        "paragraph": "All three frames are bullish and pushing in the right direction, but momentum is cooling across every frame at once. Nothing is accelerating — the trend is decelerating even as it holds its shape. Reads as a tired continuation: direction is still intact, but the force behind it is fading across the stack.",
        "sentiment": "weak_bull",
        "confidence": "moderate",
    },
    "3.2": {
        "paragraph": "The 5m and short-term are still pushing higher but the bigger trend has already stretched and started to fade. Momentum on the larger frame has reached extreme territory and turned, leaving the lower-frame strength running into a wobbling ceiling. Reads as a stretched continuation: moves like this often give back ground sharply once the bigger frame finishes its turn.",
        "sentiment": "caution",
        "confidence": "low",
    },
    "3.2b": {
        "paragraph": "The 5m is bullish but already stretched at the top — momentum has pushed into the overextended zone and started to cool. The bigger trend is still holding underneath, ready to catch the 5m if it needs to reset. Reads as a leading warning inside a still-intact trend: the immediate push is tiring, though the macro hasn't confirmed a turn.",
        "sentiment": "weak_bull",
        "confidence": "moderate",
    },
    "3.3": {
        "paragraph": "All three frames are bullish on the surface but the bigger trend is quietly weakening underneath. The 5m and short-term still push up, yet the 15m's momentum has begun to fade even as bias holds. Reads as a continuation on a thinner foundation than it appears: the bigger frame's turn often arrives before the surface bias flips.",
        "sentiment": "weak_bull",
        "confidence": "low",
    },
    "3.4": {
        "paragraph": "All three frames read bullish but every timeframe's momentum has started to fade at the same time. The move is hollow — bias still points up but no frame is actively pushing anymore. Reads as an early reversal dressed as a trend: the bias label is usually the last thing to flip.",
        "sentiment": "weak_bull",
        "confidence": "low",
    },
    "3.5": {
        "paragraph": "All three frames read bullish but none of them are actively moving. Momentum is flat across the stack, the trend marker coasting without participation. Reads as consolidation dressed as a trend: direction isn't being defended, and the bias holds only until something breaks the drift.",
        "sentiment": "neutral",
        "confidence": "low",
    },
    "3.6": {
        "paragraph": "The 5m is bullish with both the short-term and the bigger trend on side. Alignment is clean across the stack though not every frame is pushing with equal energy. Reads as a broadly bullish setup: direction isn't contested, even if the force behind it varies by frame.",
        "sentiment": "strong_bull",
        "confidence": "moderate",
    },

    # ── §3 SCENARIO TIER — RRR family ──
    "3.7": {
        "paragraph": "The 5m is bearish with bears firmly in control, and the bigger trend is still actively pushing lower underneath. The short-term is falling in step — no frame is fading. Reads as a clean continuation lower: alignment this deep typically extends rather than stalls.",
        "sentiment": "strong_bear",
        "confidence": "high",
    },
    "3.7b": {
        "paragraph": "All three frames are bearish and pushing in the right direction, but momentum is cooling across every frame at once. Nothing is accelerating — the decline is decelerating even as it holds its shape. Reads as a tired continuation lower: direction is still intact, but the force behind it is fading across the stack.",
        "sentiment": "weak_bear",
        "confidence": "moderate",
    },
    "3.8": {
        "paragraph": "The 5m and short-term are still pushing lower but the bigger trend has already stretched into extreme oversold territory and started to lift. Lower-frame downward momentum is running into a floor that's already flexing. Reads as a stretched decline: moves like this often bounce sharply once the bigger frame finishes its turn.",
        "sentiment": "caution",
        "confidence": "low",
    },
    "3.8b": {
        "paragraph": "The 5m is bearish but already stretched at the lows — momentum has pushed into the overextended zone and started to lift. The bigger trend is still holding underneath, providing pressure if the 5m needs to reset. Reads as a leading warning inside a still-intact downtrend: the immediate push is tiring, though the macro hasn't confirmed a turn.",
        "sentiment": "weak_bear",
        "confidence": "moderate",
    },
    "3.9": {
        "paragraph": "All three frames are bearish on the surface but the bigger trend is quietly firming underneath. The 5m and short-term still push down, yet the 15m's momentum has begun to lift even as bias holds. Reads as a continuation on a weakening foundation: the bigger frame's turn often arrives before the surface bias flips.",
        "sentiment": "weak_bear",
        "confidence": "low",
    },
    "3.10": {
        "paragraph": "All three frames read bearish but every timeframe's momentum has started to firm at the same time. The move is hollow — bias still points down but no frame is actively pushing anymore. Reads as an early turn dressed as a trend: the bias label is usually the last thing to flip.",
        "sentiment": "weak_bear",
        "confidence": "low",
    },
    "3.11": {
        "paragraph": "All three frames read bearish but none of them are actively moving. Momentum is flat across the stack, the trend marker coasting without participation. Reads as consolidation dressed as a trend: direction isn't being defended, and the bias holds only until something breaks the drift.",
        "sentiment": "neutral",
        "confidence": "low",
    },
    "3.12": {
        "paragraph": "The 5m is bearish with both the short-term and the bigger trend on side. Alignment is clean across the stack though not every frame is pushing with equal energy. Reads as a broadly bearish setup: direction isn't contested, even if the force behind it varies by frame.",
        "sentiment": "strong_bear",
        "confidence": "moderate",
    },

    # ── §3 SCENARIO TIER — BRB family ──
    "3.13": {
        "paragraph": "The 5m remains bullish and the current weakness reads as a short-term dip rather than a trend change. The bigger trend is still actively pushing up, so the pullback is being absorbed rather than driving a real reversal. Reads as a shallow retracement inside a healthy uptrend: typically resolves back in the trend's favor.",
        "sentiment": "opportunity_long",
        "confidence": "moderate",
    },
    "3.13b": {
        "paragraph": "The 5m is bullish but the short-term pullback is still extending rather than stalling. The 1m is actively pushing lower with momentum expanding, so the dip hasn't bottomed yet. Reads as a pullback mid-move rather than bottoming: the bigger trend remains intact, but the dip needs to finish releasing before the trend can reassert.",
        "sentiment": "caution",
        "confidence": "moderate",
    },
    "3.14": {
        "paragraph": "The 5m is bullish and the bigger trend backs it, but the short-term has pulled back against the direction. The 1m dip is fighting the larger move rather than being confirmed by it. Reads as a pullback inside a broader uptrend: shape is right, but the dip needs to stall before the trend reasserts.",
        "sentiment": "opportunity_long",
        "confidence": "moderate",
    },

    # ── §3 SCENARIO TIER — RBR family ──
    "3.15": {
        "paragraph": "The 5m remains bearish and the current bounce reads as a short-term relief rather than a trend change. The bigger trend is still actively pushing down, so the bounce is being absorbed rather than turning the move. Reads as a shallow relief inside a healthy downtrend: typically fades back in the trend's favor.",
        "sentiment": "opportunity_short",
        "confidence": "moderate",
    },
    "3.15b": {
        "paragraph": "The 5m is bearish but the short-term bounce is still extending rather than stalling. The 1m is actively pushing higher with momentum expanding, so the relief hasn't topped out yet. Reads as a relief rally mid-move rather than exhausting: the bigger trend remains intact, but the bounce needs to finish releasing before the trend can reassert.",
        "sentiment": "caution",
        "confidence": "moderate",
    },
    "3.16": {
        "paragraph": "The 5m is bearish and the bigger trend backs it, but the short-term has bounced against the direction. The 1m relief is fighting the larger move rather than being confirmed by it. Reads as a relief rally inside a broader downtrend: shape is right, but the bounce needs to stall before the trend reasserts.",
        "sentiment": "opportunity_short",
        "confidence": "moderate",
    },

    # ── §3 SCENARIO TIER — BBR family ──
    "3.17": {
        "paragraph": "The 5m and short-term are pushing higher but the bigger trend remains bearish underneath. The lower frames look like a counter-rally inside a larger downtrend rather than the start of a real reversal. Reads as a relief move pressing into the bigger trend's weight: typically struggles to extend much further before the macro re-exerts.",
        "sentiment": "caution",
        "confidence": "moderate",
    },
    "3.18": {
        "paragraph": "The 5m and short-term are bullish but the bigger trend hasn't confirmed the move. The 15m still leans down even though its push has faded, leaving the lower frames trying to turn a tide that hasn't released. Reads as a counter-move waiting on the larger frame: direction at the macro level hasn't flipped yet.",
        "sentiment": "caution",
        "confidence": "moderate",
    },

    # ── §3 SCENARIO TIER — RRB family ──
    "3.19": {
        "paragraph": "The 5m and short-term are pushing lower but the bigger trend remains bullish underneath. The lower frames look like a decline inside a larger uptrend rather than the start of a real reversal. Reads as a dip pressing into the bigger trend's lift: typically struggles to extend much further before the macro re-exerts.",
        "sentiment": "caution",
        "confidence": "moderate",
    },
    "3.20": {
        "paragraph": "The 5m and short-term are bearish but the bigger trend hasn't confirmed the move. The 15m still leans up even though its push has faded, leaving the lower frames trying to turn a tide that hasn't released. Reads as a counter-move waiting on the larger frame: direction at the macro level hasn't flipped yet.",
        "sentiment": "caution",
        "confidence": "moderate",
    },

    # ── §3 SCENARIO TIER — BRR / RBB ──
    "3.21": {
        "paragraph": "Only the 5m is bullish while both the short-term and the bigger trend are pointing down. The larger frame is actively pushing lower with momentum still expanding, putting weight on the bear side. Reads as a delayed signal: the 5m is often last to flip when the surrounding frames are this aligned, and the bullish pulse here typically fades into the larger pressure.",
        "sentiment": "weak_bull",
        "confidence": "low",
    },
    "3.22": {
        "paragraph": "Only the 5m is bearish while both the short-term and the bigger trend are pointing up. The larger frame is actively pushing higher with momentum still expanding, putting weight on the bull side. Reads as a delayed signal: the 5m is often last to flip when the surrounding frames are this aligned, and the bearish pulse here typically fades into the larger pressure.",
        "sentiment": "weak_bear",
        "confidence": "low",
    },
}


# ══════════════════════════════════════════════════════════════════════
#  FIELD NORMALIZATION
#  Analysis dicts use UPPERCASE for trend fields (RISING/FALLING/FLAT/STABLE).
#  Catalog triggers use Title Case (Rising/Falling/Stable/Neutral).
#  Normalize once at entry so trigger expressions read 1:1 with the catalog.
# ══════════════════════════════════════════════════════════════════════

_NEUTRAL_TF: Dict[str, Any] = {
    "bias": "NEUTRAL",
    "rsi": None,
    "rsi_trend": None,
    "bw_val": None,
    "bw_trend": None,
    "ribbon_state": None,
}


def _norm_trend(v: Optional[str]) -> Optional[str]:
    """Map trend label to title case. UPPERCASE -> TitleCase. Returns None
    when input is None. FLAT -> Stable (catalog uses Stable for LTF/HTF
    flat state; CTF Neutral never appears as a positive trigger in any
    catalog entry, so collapsing to Stable is safe)."""
    if v is None:
        return None
    s = str(v).strip().lower()
    if s == "rising":
        return "Rising"
    if s == "falling":
        return "Falling"
    if s in ("flat", "stable", "neutral"):
        return "Stable"
    return v


def _norm_ribbon(v: Optional[str]) -> Optional[str]:
    """Ribbon state already comes title-cased from get_ribbon_state, but
    accept lowercase too for safety. Returns Expanding/Contracting/Flat
    or None."""
    if v is None:
        return None
    s = str(v).strip().lower()
    if s == "expanding":
        return "Expanding"
    if s == "contracting":
        return "Contracting"
    if s == "flat":
        return "Flat"
    return v


def _coalesce(tf_dict: Optional[Dict]) -> Dict[str, Any]:
    """Convert a TF analysis dict (or None) into a normalized 6-field dict.
    Missing TF -> all-NEUTRAL sentinel so triggers don't crash. Trend and
    ribbon fields are normalized here so callers can use catalog phrasing."""
    if tf_dict is None:
        return dict(_NEUTRAL_TF)
    out = {
        "bias": tf_dict.get("bias") or "NEUTRAL",
        "rsi": tf_dict.get("rsi"),
        "rsi_trend": _norm_trend(tf_dict.get("rsi_trend")),
        "bw_val": tf_dict.get("bw_val"),
        "bw_trend": _norm_trend(tf_dict.get("bw_trend")),
        "ribbon_state": _norm_ribbon(tf_dict.get("ribbon_state")),
    }
    return out


# ══════════════════════════════════════════════════════════════════════
#  HELPERS — None-safe comparators and bias predicates.
# ══════════════════════════════════════════════════════════════════════

def _gt(a, b) -> bool:
    return a is not None and a > b


def _lt(a, b) -> bool:
    return a is not None and a < b


def _ge(a, b) -> bool:
    return a is not None and a >= b


def _le(a, b) -> bool:
    return a is not None and a <= b


def _abs_ge(a, b) -> bool:
    return a is not None and abs(a) >= b


def _abs_lt(a, b) -> bool:
    return a is not None and abs(a) < b


def _bias_letter(bias: str) -> str:
    """B / R / N from BULLISH / BEARISH / NEUTRAL."""
    if bias == "BULLISH":
        return "B"
    if bias == "BEARISH":
        return "R"
    return "N"


def _scenario_code(c: Dict, l: Dict, h: Dict) -> str:
    """3-letter B/R/N concat in CTF/LTF/HTF order. Always emitted in output."""
    return _bias_letter(c["bias"]) + _bias_letter(l["bias"]) + _bias_letter(h["bias"])


def _count_neutral(c: Dict, l: Dict, h: Dict) -> int:
    return sum(1 for tf in (c, l, h) if tf["bias"] == "NEUTRAL")


# ══════════════════════════════════════════════════════════════════════
#  PRIORITY 1 — OVERRIDE TIER
# ══════════════════════════════════════════════════════════════════════

def _is_capitulation_top(c: Dict) -> bool:
    """1.1 — bias=BULLISH AND rsi>80 AND rsi_trend=Falling on CTF."""
    return (c["bias"] == "BULLISH"
            and _gt(c["rsi"], 80)
            and c["rsi_trend"] == "Falling")


def _is_capitulation_bottom(c: Dict) -> bool:
    """1.2 — bias=BEARISH AND rsi<20 AND rsi_trend=Rising on CTF."""
    return (c["bias"] == "BEARISH"
            and _lt(c["rsi"], 20)
            and c["rsi_trend"] == "Rising")


def _is_all_neutral(c: Dict, l: Dict, h: Dict) -> bool:
    """1.3 — all three biases NEUTRAL."""
    return c["bias"] == "NEUTRAL" and l["bias"] == "NEUTRAL" and h["bias"] == "NEUTRAL"


# ══════════════════════════════════════════════════════════════════════
#  PRIORITY 2 — STATE TIER
# ══════════════════════════════════════════════════════════════════════

def _is_stuck(c: Dict, l: Dict, h: Dict) -> bool:
    """2.1 — all three ribbon_state=Contracting AND NOT (all biases equal)."""
    all_contracting = (c["ribbon_state"] == "Contracting"
                       and l["ribbon_state"] == "Contracting"
                       and h["ribbon_state"] == "Contracting")
    biases_equal = c["bias"] == l["bias"] == h["bias"]
    return all_contracting and not biases_equal


def _is_compression(c: Dict, l: Dict, h: Dict) -> bool:
    """2.2 — >=2 TFs Contracting AND |bw_val|<20 on those TFs AND NOT stuck.
    Per catalog: count Contracting TFs first, then check that EACH of those
    TFs has |bw_val|<20."""
    if _is_stuck(c, l, h):
        return False
    contracting_tfs = [tf for tf in (c, l, h) if tf["ribbon_state"] == "Contracting"]
    if len(contracting_tfs) < 2:
        return False
    return all(_abs_lt(tf["bw_val"], 20) for tf in contracting_tfs)


def _route_partial_neutral(c: Dict, l: Dict, h: Dict) -> str:
    """2.3 — exactly one TF is NEUTRAL. Route by which TF, then agree/oppose
    of the other two."""
    if l["bias"] == "NEUTRAL":
        # 1m is the neutral one; check whether 5m and 15m agree or oppose.
        return "2.3a-agree" if c["bias"] == h["bias"] else "2.3a-oppose"
    if c["bias"] == "NEUTRAL":
        # 5m is the neutral one.
        return "2.3b-agree" if l["bias"] == h["bias"] else "2.3b-oppose"
    # h["bias"] == "NEUTRAL" — 15m is the neutral one.
    return "2.3c-agree" if c["bias"] == l["bias"] else "2.3c-oppose"


def _route_mostly_neutral(c: Dict, l: Dict, h: Dict) -> str:
    """2.4 — exactly two TFs are NEUTRAL. Route by which TF still holds direction."""
    if c["bias"] != "NEUTRAL":
        return "2.4a"
    if l["bias"] != "NEUTRAL":
        return "2.4b"
    return "2.4c"


# ══════════════════════════════════════════════════════════════════════
#  PRIORITY 3 — SCENARIO TIER
#  Each family routes variants in catalog-specified order, most-specific first.
# ══════════════════════════════════════════════════════════════════════

# ── BBB family ──
# Routing order: 3.2 → 3.4 → 3.2b → 3.3 → 3.1 → 3.1b → 3.5 → 3.6

def _bbb_3_2(c: Dict, l: Dict, h: Dict) -> bool:
    """HTF Stretched: (|htf.bw_val|>=55 OR htf.rsi>70) AND
    (htf.bw_trend=Falling OR htf.rsi_trend=Falling OR htf.ribbon_state=Contracting)."""
    extreme = _abs_ge(h["bw_val"], 55) or _gt(h["rsi"], 70)
    fading = (h["bw_trend"] == "Falling"
              or h["rsi_trend"] == "Falling"
              or h["ribbon_state"] == "Contracting")
    return extreme and fading


def _bbb_3_4(c: Dict, l: Dict, h: Dict) -> bool:
    """Surface Turn: ctf.bw_trend AND ltf.bw_trend AND htf.bw_trend all Falling."""
    return (c["bw_trend"] == "Falling"
            and l["bw_trend"] == "Falling"
            and h["bw_trend"] == "Falling")


def _bbb_3_2b(c: Dict, l: Dict, h: Dict) -> bool:
    """CTF Stretched: (ctf.rsi>70 OR |ctf.bw_val|>=55) AND
    (ctf.rsi_trend=Falling OR ctf.bw_trend=Falling) AND NOT 3.2.
    The NOT 3.2 check happens via routing order — caller already ruled it out."""
    extreme = _gt(c["rsi"], 70) or _abs_ge(c["bw_val"], 55)
    fading = c["rsi_trend"] == "Falling" or c["bw_trend"] == "Falling"
    return extreme and fading


def _bbb_3_3(c: Dict, l: Dict, h: Dict) -> bool:
    """Hidden HTF Weakness: htf.rsi_trend=Falling OR htf.bw_trend=Falling.
    NOT 3.2 enforced by routing order."""
    return h["rsi_trend"] == "Falling" or h["bw_trend"] == "Falling"


def _bbb_3_1(c: Dict, l: Dict, h: Dict) -> bool:
    """Strong Clean: all RSI trend Rising AND all BW trend Rising AND
    htf.ribbon_state=Expanding."""
    return (c["rsi_trend"] == "Rising" and l["rsi_trend"] == "Rising" and h["rsi_trend"] == "Rising"
            and c["bw_trend"] == "Rising" and l["bw_trend"] == "Rising" and h["bw_trend"] == "Rising"
            and h["ribbon_state"] == "Expanding")


def _bbb_3_1b(c: Dict, l: Dict, h: Dict) -> bool:
    """Cooling Alignment: all BW trend Rising AND all three ribbon Contracting.
    NOT (3.2 OR 3.2b OR 3.3 OR 3.4) enforced by routing order."""
    return (c["bw_trend"] == "Rising" and l["bw_trend"] == "Rising" and h["bw_trend"] == "Rising"
            and c["ribbon_state"] == "Contracting"
            and l["ribbon_state"] == "Contracting"
            and h["ribbon_state"] == "Contracting")


def _bbb_3_5(c: Dict, l: Dict, h: Dict) -> bool:
    """Quiet Alignment: all BW trends Stable AND all ribbon Flat."""
    return (c["bw_trend"] == "Stable" and l["bw_trend"] == "Stable" and h["bw_trend"] == "Stable"
            and c["ribbon_state"] == "Flat"
            and l["ribbon_state"] == "Flat"
            and h["ribbon_state"] == "Flat")


def _route_bbb(c, l, h) -> str:
    if _bbb_3_2(c, l, h):  return "3.2"
    if _bbb_3_4(c, l, h):  return "3.4"
    if _bbb_3_2b(c, l, h): return "3.2b"
    if _bbb_3_3(c, l, h):  return "3.3"
    if _bbb_3_1(c, l, h):  return "3.1"
    if _bbb_3_1b(c, l, h): return "3.1b"
    if _bbb_3_5(c, l, h):  return "3.5"
    return "3.6"


# ── RRR family ──
# Routing order: 3.8 → 3.10 → 3.8b → 3.9 → 3.7 → 3.7b → 3.11 → 3.12

def _rrr_3_8(c: Dict, l: Dict, h: Dict) -> bool:
    """HTF Stretched: (|htf.bw_val|>=55 OR htf.rsi<30) AND
    (htf.bw_trend=Rising OR htf.rsi_trend=Rising OR htf.ribbon_state=Contracting)."""
    extreme = _abs_ge(h["bw_val"], 55) or _lt(h["rsi"], 30)
    firming = (h["bw_trend"] == "Rising"
               or h["rsi_trend"] == "Rising"
               or h["ribbon_state"] == "Contracting")
    return extreme and firming


def _rrr_3_10(c: Dict, l: Dict, h: Dict) -> bool:
    """Surface Turn: all three bw_trend Rising."""
    return (c["bw_trend"] == "Rising"
            and l["bw_trend"] == "Rising"
            and h["bw_trend"] == "Rising")


def _rrr_3_8b(c: Dict, l: Dict, h: Dict) -> bool:
    """CTF Stretched: (ctf.rsi<30 OR |ctf.bw_val|>=55) AND
    (ctf.rsi_trend=Rising OR ctf.bw_trend=Rising). NOT 3.8 by routing order."""
    extreme = _lt(c["rsi"], 30) or _abs_ge(c["bw_val"], 55)
    firming = c["rsi_trend"] == "Rising" or c["bw_trend"] == "Rising"
    return extreme and firming


def _rrr_3_9(c: Dict, l: Dict, h: Dict) -> bool:
    """Hidden HTF Strengthening: htf.rsi_trend=Rising OR htf.bw_trend=Rising.
    NOT 3.8 by routing order."""
    return h["rsi_trend"] == "Rising" or h["bw_trend"] == "Rising"


def _rrr_3_7(c: Dict, l: Dict, h: Dict) -> bool:
    """Strong Clean: all RSI trend Falling AND all BW trend Falling AND
    htf.ribbon_state=Expanding."""
    return (c["rsi_trend"] == "Falling" and l["rsi_trend"] == "Falling" and h["rsi_trend"] == "Falling"
            and c["bw_trend"] == "Falling" and l["bw_trend"] == "Falling" and h["bw_trend"] == "Falling"
            and h["ribbon_state"] == "Expanding")


def _rrr_3_7b(c: Dict, l: Dict, h: Dict) -> bool:
    """Cooling Decline: all BW trend Falling AND all three ribbon Contracting."""
    return (c["bw_trend"] == "Falling" and l["bw_trend"] == "Falling" and h["bw_trend"] == "Falling"
            and c["ribbon_state"] == "Contracting"
            and l["ribbon_state"] == "Contracting"
            and h["ribbon_state"] == "Contracting")


def _rrr_3_11(c: Dict, l: Dict, h: Dict) -> bool:
    """Quiet Alignment: all BW trends Stable AND all ribbon Flat."""
    return (c["bw_trend"] == "Stable" and l["bw_trend"] == "Stable" and h["bw_trend"] == "Stable"
            and c["ribbon_state"] == "Flat"
            and l["ribbon_state"] == "Flat"
            and h["ribbon_state"] == "Flat")


def _route_rrr(c, l, h) -> str:
    if _rrr_3_8(c, l, h):  return "3.8"
    if _rrr_3_10(c, l, h): return "3.10"
    if _rrr_3_8b(c, l, h): return "3.8b"
    if _rrr_3_9(c, l, h):  return "3.9"
    if _rrr_3_7(c, l, h):  return "3.7"
    if _rrr_3_7b(c, l, h): return "3.7b"
    if _rrr_3_11(c, l, h): return "3.11"
    return "3.12"


# ── BRB family ──
# Routing order: 3.13 → 3.13b → 3.14

def _brb_3_13(c, l, h) -> bool:
    """Quality Pullback: ltf.ribbon_state=Contracting AND ltf.rsi<=40 AND
    htf.ribbon_state=Expanding."""
    return (l["ribbon_state"] == "Contracting"
            and _le(l["rsi"], 40)
            and h["ribbon_state"] == "Expanding")


def _brb_3_13b(c, l, h) -> bool:
    """Accelerating Pullback: ltf.ribbon_state=Expanding AND ltf.rsi<35."""
    return l["ribbon_state"] == "Expanding" and _lt(l["rsi"], 35)


def _route_brb(c, l, h) -> str:
    if _brb_3_13(c, l, h):  return "3.13"
    if _brb_3_13b(c, l, h): return "3.13b"
    return "3.14"


# ── RBR family ──
# Routing order: 3.15 → 3.15b → 3.16

def _rbr_3_15(c, l, h) -> bool:
    """Quality Bounce: ltf.ribbon_state=Contracting AND ltf.rsi>=60 AND
    htf.ribbon_state=Expanding."""
    return (l["ribbon_state"] == "Contracting"
            and _ge(l["rsi"], 60)
            and h["ribbon_state"] == "Expanding")


def _rbr_3_15b(c, l, h) -> bool:
    """Accelerating Bounce: ltf.ribbon_state=Expanding AND ltf.rsi>65."""
    return l["ribbon_state"] == "Expanding" and _gt(l["rsi"], 65)


def _route_rbr(c, l, h) -> str:
    if _rbr_3_15(c, l, h):  return "3.15"
    if _rbr_3_15b(c, l, h): return "3.15b"
    return "3.16"


# ── BBR / RRB / BRR / RBB families ──

def _route_bbr(c, l, h) -> str:
    """3.17 if htf.bw_trend=Falling, else 3.18."""
    return "3.17" if h["bw_trend"] == "Falling" else "3.18"


def _route_rrb(c, l, h) -> str:
    """3.19 if htf.bw_trend=Rising, else 3.20."""
    return "3.19" if h["bw_trend"] == "Rising" else "3.20"


def _route_brr(c, l, h) -> str:
    return "3.21"


def _route_rbb(c, l, h) -> str:
    return "3.22"


_SCENARIO_ROUTERS = {
    "BBB": _route_bbb,
    "RRR": _route_rrr,
    "BRB": _route_brb,
    "RBR": _route_rbr,
    "BBR": _route_bbr,
    "RRB": _route_rrb,
    "BRR": _route_brr,
    "RBB": _route_rbb,
}


# ══════════════════════════════════════════════════════════════════════
#  SELECT — walk priority-ordered decision flow, return entry id.
# ══════════════════════════════════════════════════════════════════════

def _select_entry(c: Dict, l: Dict, h: Dict) -> str:
    # PRIORITY 1 — Overrides
    if _is_capitulation_top(c):
        return "1.1"
    if _is_capitulation_bottom(c):
        return "1.2"
    if _is_all_neutral(c, l, h):
        return "1.3"

    # PRIORITY 2 — State tier
    if _is_stuck(c, l, h):
        return "2.1"
    if _is_compression(c, l, h):
        return "2.2"

    n = _count_neutral(c, l, h)
    if n == 2:
        return _route_mostly_neutral(c, l, h)
    if n == 1:
        return _route_partial_neutral(c, l, h)

    # PRIORITY 3 — Scenario tier (no NEUTRAL biases at this point)
    code = _scenario_code(c, l, h)
    router = _SCENARIO_ROUTERS.get(code)
    if router is None:
        # Defensive: should be unreachable since count_neutral==0 here means
        # all biases are B/R, giving exactly the 8 codes covered above.
        return "1.3"
    return router(c, l, h)


# ══════════════════════════════════════════════════════════════════════
#  PUBLIC API
# ══════════════════════════════════════════════════════════════════════

def analyze_mtf(ctf: Optional[Dict],
                ltf: Optional[Dict],
                htf: Optional[Dict]) -> Dict[str, Any]:
    """Select one of 42 catalog entries by priority-ordered routing and
    return the output dict per the catalog spec.

    Inputs are full _analyze_timeframe output dicts (or None for missing TF).
    Reads only: bias, rsi, rsi_trend, bw_val, bw_trend, ribbon_state.

    Output:
        {
          "scenario_code": "BRB",
          "sentiment":     "opportunity_long",
          "confidence":    "moderate",
          "paragraph":     "...",
          "conflicts":     [],
          "capitulation":  False,
        }
    """
    c = _coalesce(ctf)
    l = _coalesce(ltf)
    h = _coalesce(htf)

    entry_id = _select_entry(c, l, h)
    entry = ENTRIES[entry_id]

    return {
        "scenario_code": _scenario_code(c, l, h),
        "sentiment": entry["sentiment"],
        "confidence": entry["confidence"],
        "paragraph": entry["paragraph"],
        "conflicts": [],
        "capitulation": bool(entry.get("capitulation", False)),
    }
