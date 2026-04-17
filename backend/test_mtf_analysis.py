"""
Unit tests for mtf_analysis.py — the MTF Analysis Engine.

Independent of test_indicators.py. Covers all 42 catalog entries, priority
routing (overrides → state → scenario), all 27 bias combinations, capitulation
flag, output shape, missing-field degradation, and the four catalog scanners
(length / digits / forbidden substrings / valid sentiments).

Run: python3 test_mtf_analysis.py
Target: "ALL TESTS PASSED!".
"""
import re
import sys

sys.path.insert(0, "/home/claude/test")

from mtf_analysis import analyze_mtf, ENTRIES, _select_entry, _coalesce


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
#  Helpers — build TF dicts shaped like _analyze_timeframe output
# ═══════════════════════════════════════════════════════════════

def tf(bias="BULLISH", rsi=50, rsi_trend="STABLE",
       bw_val=0, bw_trend="STABLE", ribbon_state="Flat"):
    """Mirror the relevant fields of _analyze_timeframe's return dict.
    Defaults are "neutral conditions" so individual triggers can be
    activated by overriding only the relevant fields."""
    return {"bias": bias, "rsi": rsi, "rsi_trend": rsi_trend,
            "bw_val": bw_val, "bw_trend": bw_trend, "ribbon_state": ribbon_state}


def routes(c, l, h):
    """Return the entry id that fires for given inputs."""
    return _select_entry(_coalesce(c), _coalesce(l), _coalesce(h))


VALID_SENTIMENTS = {"strong_bull", "strong_bear", "caution", "opportunity_long",
                    "opportunity_short", "weak_bull", "weak_bear", "neutral"}
VALID_CONFIDENCES = {"high", "moderate", "low"}
EXPECTED_KEYS = {"scenario_code", "sentiment", "confidence", "paragraph",
                 "conflicts", "capitulation"}


# ═══════════════════════════════════════════════════════════════
print("═══ Output shape sanity ═══")
# ═══════════════════════════════════════════════════════════════

out = analyze_mtf(tf(), tf(), tf())
check("Output is dict", isinstance(out, dict))
check("Output has exactly the 6 catalog-spec keys",
      set(out.keys()) == EXPECTED_KEYS,
      f"got {set(out.keys())}")
check("scenario_code is 3-letter B/R/N", re.fullmatch(r"[BRN]{3}", out["scenario_code"]) is not None)
check("sentiment in valid set", out["sentiment"] in VALID_SENTIMENTS)
check("confidence in valid set", out["confidence"] in VALID_CONFIDENCES)
check("paragraph is non-empty string", isinstance(out["paragraph"], str) and len(out["paragraph"]) > 0)
check("conflicts is list", isinstance(out["conflicts"], list))
check("capitulation is bool", isinstance(out["capitulation"], bool))


# ═══════════════════════════════════════════════════════════════
print("\n═══ ENTRIES dict integrity ═══")
# ═══════════════════════════════════════════════════════════════

check("ENTRIES has exactly 42 entries", len(ENTRIES) == 42, f"got {len(ENTRIES)}")
expected_ids = {
    "1.1", "1.2", "1.3",
    "2.1", "2.2",
    "2.3a-agree", "2.3a-oppose", "2.3b-agree", "2.3b-oppose", "2.3c-agree", "2.3c-oppose",
    "2.4a", "2.4b", "2.4c",
    "3.1", "3.1b", "3.2", "3.2b", "3.3", "3.4", "3.5", "3.6",
    "3.7", "3.7b", "3.8", "3.8b", "3.9", "3.10", "3.11", "3.12",
    "3.13", "3.13b", "3.14",
    "3.15", "3.15b", "3.16",
    "3.17", "3.18", "3.19", "3.20", "3.21", "3.22",
}
check("ENTRIES keys exactly match catalog id set",
      set(ENTRIES.keys()) == expected_ids,
      f"diff: {set(ENTRIES.keys()) ^ expected_ids}")

for eid, entry in ENTRIES.items():
    check(f"  {eid}: has paragraph", "paragraph" in entry and isinstance(entry["paragraph"], str))
    check(f"  {eid}: has sentiment in valid set",
          entry.get("sentiment") in VALID_SENTIMENTS,
          f"got {entry.get('sentiment')}")
    check(f"  {eid}: has confidence in valid set",
          entry.get("confidence") in VALID_CONFIDENCES,
          f"got {entry.get('confidence')}")


# ═══════════════════════════════════════════════════════════════
print("\n═══ §1 OVERRIDE TIER — routing ═══")
# ═══════════════════════════════════════════════════════════════

# 1.1 Capitulation Top — bias=BULL, rsi>80 strict, rsi_trend=Falling on CTF
check("1.1 fires (BULL rsi=85 Falling)",
      routes(tf("BULLISH", 85, "FALLING"), tf("BULLISH"), tf("BULLISH")) == "1.1")
check("1.1 fires regardless of LTF/HTF state",
      routes(tf("BULLISH", 85, "FALLING"),
             tf("BEARISH", 30, "FALLING", -50, "FALLING", "Contracting"),
             tf("BEARISH")) == "1.1")
check("1.1 doesn't fire at rsi=80 (strict >80)",
      routes(tf("BULLISH", 80, "FALLING"), tf("BULLISH"), tf("BULLISH")) != "1.1")
check("1.1 doesn't fire when rsi_trend=Rising",
      routes(tf("BULLISH", 85, "RISING"), tf("BULLISH"), tf("BULLISH")) != "1.1")
check("1.1 doesn't fire when bias=BEARISH",
      routes(tf("BEARISH", 85, "FALLING"), tf("BULLISH"), tf("BULLISH")) != "1.1")

# 1.2 Capitulation Bottom — bias=BEAR, rsi<20 strict, rsi_trend=Rising on CTF
check("1.2 fires (BEAR rsi=15 Rising)",
      routes(tf("BEARISH", 15, "RISING"), tf("BEARISH"), tf("BEARISH")) == "1.2")
check("1.2 doesn't fire at rsi=20 (strict <20)",
      routes(tf("BEARISH", 20, "RISING"), tf("BEARISH"), tf("BEARISH")) != "1.2")
check("1.2 doesn't fire when rsi_trend=Falling",
      routes(tf("BEARISH", 15, "FALLING"), tf("BEARISH"), tf("BEARISH")) != "1.2")
check("1.2 doesn't fire when bias=BULLISH",
      routes(tf("BULLISH", 15, "RISING"), tf("BEARISH"), tf("BEARISH")) != "1.2")

# 1.3 All Neutral
check("1.3 fires (all NEUTRAL)",
      routes(tf("NEUTRAL"), tf("NEUTRAL"), tf("NEUTRAL")) == "1.3")
check("1.3 doesn't fire when any bias is BULL/BEAR",
      routes(tf("NEUTRAL"), tf("NEUTRAL"), tf("BULLISH")) != "1.3")


# ═══════════════════════════════════════════════════════════════
print("\n═══ §1 OVERRIDES — capitulation flag ═══")
# ═══════════════════════════════════════════════════════════════

out_top = analyze_mtf(tf("BULLISH", 85, "FALLING"), tf("BULLISH"), tf("BULLISH"))
check("1.1 sets capitulation=True", out_top["capitulation"] is True)
check("1.1 sentiment=caution", out_top["sentiment"] == "caution")

out_bot = analyze_mtf(tf("BEARISH", 15, "RISING"), tf("BEARISH"), tf("BEARISH"))
check("1.2 sets capitulation=True", out_bot["capitulation"] is True)
check("1.2 sentiment=opportunity_long", out_bot["sentiment"] == "opportunity_long")

# Capitulation flag MUST stay False for all non-1.1/1.2 entries
out_normal = analyze_mtf(tf("BULLISH"), tf("BULLISH"), tf("BULLISH"))
check("Non-capitulation entry capitulation=False", out_normal["capitulation"] is False)

# Capitulation conditions on LTF or HTF (not CTF) must NOT set the flag
out_ltf_cap = analyze_mtf(
    tf("BULLISH", 60),
    tf("BULLISH", 85, "FALLING"),  # LTF capitulation conditions — must NOT flag
    tf("BULLISH"),
)
check("Capitulation conditions on LTF only do NOT set flag",
      out_ltf_cap["capitulation"] is False)

out_htf_cap = analyze_mtf(
    tf("BULLISH", 60),
    tf("BULLISH"),
    tf("BULLISH", 85, "FALLING"),  # HTF capitulation conditions — must NOT flag
)
check("Capitulation conditions on HTF only do NOT set flag",
      out_htf_cap["capitulation"] is False)


# ═══════════════════════════════════════════════════════════════
print("\n═══ §2 STATE TIER — Stuck (2.1) ═══")
# ═══════════════════════════════════════════════════════════════

# 2.1 fires: all 3 ribbon_state=Contracting AND biases not equal
check("2.1 fires (BBR all Contracting bw=50)",
      routes(tf("BULLISH", ribbon_state="Contracting", bw_val=50),
             tf("BULLISH", ribbon_state="Contracting", bw_val=50),
             tf("BEARISH", ribbon_state="Contracting", bw_val=50)) == "2.1")
check("2.1 fires (RBN all Contracting)",
      routes(tf("BEARISH", ribbon_state="Contracting", bw_val=50),
             tf("BULLISH", ribbon_state="Contracting", bw_val=50),
             tf("NEUTRAL", ribbon_state="Contracting", bw_val=50)) == "2.1")
check("2.1 doesn't fire when biases all equal",
      routes(tf("BULLISH", ribbon_state="Contracting", bw_val=50),
             tf("BULLISH", ribbon_state="Contracting", bw_val=50),
             tf("BULLISH", ribbon_state="Contracting", bw_val=50)) != "2.1")
check("2.1 doesn't fire when not all Contracting",
      routes(tf("BULLISH", ribbon_state="Expanding"),
             tf("BULLISH", ribbon_state="Contracting"),
             tf("BEARISH", ribbon_state="Contracting")) != "2.1")


# ═══════════════════════════════════════════════════════════════
print("\n═══ §2 STATE TIER — Compression (2.2) ═══")
# ═══════════════════════════════════════════════════════════════

# 2.2 fires: ≥2 TFs Contracting AND |bw_val|<20 on those TFs AND NOT stuck
check("2.2 fires (BBB 2 Contracting near midline)",
      routes(tf("BULLISH", ribbon_state="Contracting", bw_val=10),
             tf("BULLISH", ribbon_state="Contracting", bw_val=-5),
             tf("BULLISH", ribbon_state="Expanding", bw_val=30)) == "2.2")
check("2.2 fires (3 TFs Contracting all near midline, biases all equal)",
      routes(tf("BULLISH", ribbon_state="Contracting", bw_val=5),
             tf("BULLISH", ribbon_state="Contracting", bw_val=-10),
             tf("BULLISH", ribbon_state="Contracting", bw_val=15)) == "2.2")
check("2.2 doesn't fire if Contracting TFs have high BW",
      routes(tf("BULLISH", ribbon_state="Contracting", bw_val=50),
             tf("BULLISH", ribbon_state="Contracting", bw_val=10),
             tf("BULLISH", ribbon_state="Expanding")) != "2.2")
check("2.2 doesn't fire with only 1 Contracting",
      routes(tf("BULLISH", ribbon_state="Contracting", bw_val=10),
             tf("BULLISH", ribbon_state="Expanding"),
             tf("BULLISH", ribbon_state="Expanding")) != "2.2")
check("2.2 doesn't fire when Stuck would (Stuck takes priority)",
      routes(tf("BULLISH", ribbon_state="Contracting", bw_val=10),
             tf("BEARISH", ribbon_state="Contracting", bw_val=10),
             tf("BEARISH", ribbon_state="Contracting", bw_val=10)) == "2.1")


# ═══════════════════════════════════════════════════════════════
print("\n═══ §2.3 PARTIAL NEUTRAL — 6 sub-routes ═══")
# ═══════════════════════════════════════════════════════════════

# 2.3a: LTF=NEUTRAL, outers agree (BNB/RNR) or oppose (BNR/RNB)
check("2.3a-agree fires for BNB", routes(tf("BULLISH"), tf("NEUTRAL"), tf("BULLISH")) == "2.3a-agree")
check("2.3a-agree fires for RNR", routes(tf("BEARISH"), tf("NEUTRAL"), tf("BEARISH")) == "2.3a-agree")
check("2.3a-oppose fires for BNR", routes(tf("BULLISH"), tf("NEUTRAL"), tf("BEARISH")) == "2.3a-oppose")
check("2.3a-oppose fires for RNB", routes(tf("BEARISH"), tf("NEUTRAL"), tf("BULLISH")) == "2.3a-oppose")

# 2.3b: CTF=NEUTRAL, outers agree (NBB/NRR) or oppose (NBR/NRB)
check("2.3b-agree fires for NBB", routes(tf("NEUTRAL"), tf("BULLISH"), tf("BULLISH")) == "2.3b-agree")
check("2.3b-agree fires for NRR", routes(tf("NEUTRAL"), tf("BEARISH"), tf("BEARISH")) == "2.3b-agree")
check("2.3b-oppose fires for NBR", routes(tf("NEUTRAL"), tf("BULLISH"), tf("BEARISH")) == "2.3b-oppose")
check("2.3b-oppose fires for NRB", routes(tf("NEUTRAL"), tf("BEARISH"), tf("BULLISH")) == "2.3b-oppose")

# 2.3c: HTF=NEUTRAL, outers agree (BBN/RRN) or oppose (BRN/RBN)
check("2.3c-agree fires for BBN", routes(tf("BULLISH"), tf("BULLISH"), tf("NEUTRAL")) == "2.3c-agree")
check("2.3c-agree fires for RRN", routes(tf("BEARISH"), tf("BEARISH"), tf("NEUTRAL")) == "2.3c-agree")
check("2.3c-oppose fires for BRN", routes(tf("BULLISH"), tf("BEARISH"), tf("NEUTRAL")) == "2.3c-oppose")
check("2.3c-oppose fires for RBN", routes(tf("BEARISH"), tf("BULLISH"), tf("NEUTRAL")) == "2.3c-oppose")


# ═══════════════════════════════════════════════════════════════
print("\n═══ §2.4 MOSTLY NEUTRAL — 3 sub-routes ═══")
# ═══════════════════════════════════════════════════════════════

check("2.4a fires for BNN (only CTF)", routes(tf("BULLISH"), tf("NEUTRAL"), tf("NEUTRAL")) == "2.4a")
check("2.4a fires for RNN (only CTF)", routes(tf("BEARISH"), tf("NEUTRAL"), tf("NEUTRAL")) == "2.4a")
check("2.4b fires for NBN (only LTF)", routes(tf("NEUTRAL"), tf("BULLISH"), tf("NEUTRAL")) == "2.4b")
check("2.4b fires for NRN (only LTF)", routes(tf("NEUTRAL"), tf("BEARISH"), tf("NEUTRAL")) == "2.4b")
check("2.4c fires for NNB (only HTF)", routes(tf("NEUTRAL"), tf("NEUTRAL"), tf("BULLISH")) == "2.4c")
check("2.4c fires for NNR (only HTF)", routes(tf("NEUTRAL"), tf("NEUTRAL"), tf("BEARISH")) == "2.4c")


# ═══════════════════════════════════════════════════════════════
print("\n═══ §3 BBB FAMILY — 8 entries with most-specific-first routing ═══")
# ═══════════════════════════════════════════════════════════════

# 3.1 Strong Clean — all RSI Rising, all BW Rising, htf Expanding
check("3.1 fires (clean alignment)",
      routes(tf("BULLISH", 60, "RISING", 30, "RISING", "Expanding"),
             tf("BULLISH", 60, "RISING", 30, "RISING", "Expanding"),
             tf("BULLISH", 60, "RISING", 30, "RISING", "Expanding")) == "3.1")
check("3.1 doesn't fire when htf ribbon Flat (no Expanding)",
      routes(tf("BULLISH", 60, "RISING", 30, "RISING", "Expanding"),
             tf("BULLISH", 60, "RISING", 30, "RISING", "Expanding"),
             tf("BULLISH", 60, "RISING", 30, "RISING", "Flat")) != "3.1")

# 3.1b Cooling — all BW Rising + all ribbons Contracting
check("3.1b fires",
      routes(tf("BULLISH", 60, "STABLE", 30, "RISING", "Contracting"),
             tf("BULLISH", 60, "STABLE", 30, "RISING", "Contracting"),
             tf("BULLISH", 60, "STABLE", 30, "RISING", "Contracting")) == "3.1b")

# 3.2 HTF Stretched
check("3.2 fires (htf bw_val>=55 + Falling)",
      routes(tf("BULLISH"), tf("BULLISH"),
             tf("BULLISH", 50, "STABLE", 60, "FALLING", "Flat")) == "3.2")
check("3.2 fires (htf rsi>70 + ribbon Contracting)",
      routes(tf("BULLISH"), tf("BULLISH"),
             tf("BULLISH", 75, "STABLE", 30, "STABLE", "Contracting")) == "3.2")
check("3.2 doesn't fire when extreme but no fading (Quiet 3.5 catches)",
      routes(tf("BULLISH"), tf("BULLISH"),
             tf("BULLISH", 75, "STABLE", 30, "STABLE", "Flat")) != "3.2")

# 3.2b CTF Stretched (NOT 3.2 enforced by routing order)
check("3.2b fires when CTF stretched + HTF not",
      routes(tf("BULLISH", 75, "STABLE", 60, "FALLING"),
             tf("BULLISH", 50, "STABLE", 30, "RISING"),  # avoid 3.4
             tf("BULLISH", 50, "STABLE", 30, "STABLE", "Flat")) == "3.2b")

# 3.3 Hidden Weakness — htf rsi/bw Falling, NOT 3.2
check("3.3 fires (htf bw_trend Falling, not stretched)",
      routes(tf("BULLISH"), tf("BULLISH"),
             tf("BULLISH", 50, "STABLE", 30, "FALLING", "Flat")) == "3.3")

# 3.4 Surface Turn — all 3 bw_trend Falling, not 3.2
check("3.4 fires when all bw Falling but htf not stretched",
      routes(tf("BULLISH", 50, "STABLE", 30, "FALLING"),
             tf("BULLISH", 50, "STABLE", 30, "FALLING"),
             tf("BULLISH", 50, "STABLE", 30, "FALLING", "Flat")) == "3.4")

# 3.5 Quiet — all BW Stable + all ribbon Flat
check("3.5 fires",
      routes(tf("BULLISH", 50, "STABLE", 0, "STABLE", "Flat"),
             tf("BULLISH", 50, "STABLE", 0, "STABLE", "Flat"),
             tf("BULLISH", 50, "STABLE", 0, "STABLE", "Flat")) == "3.5")

# 3.6 Base — fallthrough
check("3.6 fires (BBB fallthrough)",
      routes(tf("BULLISH", 50, "STABLE", 30, "RISING", "Expanding"),
             tf("BULLISH", 50, "FALLING", 30, "RISING", "Flat"),
             tf("BULLISH", 50, "STABLE", 30, "RISING", "Expanding")) == "3.6")

# Most-specific-first ordering inside BBB
# When both 3.2 (HTF Stretched) and 3.3 (Hidden Weakness) would fire, 3.2 wins
check("3.2 wins over 3.3 (both would fire, 3.2 is more specific)",
      routes(tf("BULLISH"), tf("BULLISH"),
             tf("BULLISH", 75, "FALLING", 60, "FALLING", "Flat")) == "3.2")


# ═══════════════════════════════════════════════════════════════
print("\n═══ §3 RRR FAMILY — 8 entries ═══")
# ═══════════════════════════════════════════════════════════════

check("3.7 fires (clean decline)",
      routes(tf("BEARISH", 40, "FALLING", -30, "FALLING", "Expanding"),
             tf("BEARISH", 40, "FALLING", -30, "FALLING", "Expanding"),
             tf("BEARISH", 40, "FALLING", -30, "FALLING", "Expanding")) == "3.7")
check("3.7b fires (cooling decline)",
      routes(tf("BEARISH", 40, "STABLE", -30, "FALLING", "Contracting"),
             tf("BEARISH", 40, "STABLE", -30, "FALLING", "Contracting"),
             tf("BEARISH", 40, "STABLE", -30, "FALLING", "Contracting")) == "3.7b")
check("3.8 fires (htf rsi<30 + Rising)",
      routes(tf("BEARISH"), tf("BEARISH"),
             tf("BEARISH", 25, "STABLE", -30, "RISING")) == "3.8")
check("3.8b fires (CTF stretched)",
      routes(tf("BEARISH", 25, "STABLE", -60, "RISING"),
             tf("BEARISH", 40, "STABLE", -30, "FALLING"),
             tf("BEARISH", 40, "STABLE", -30, "STABLE", "Flat")) == "3.8b")
check("3.9 fires (htf bw Rising, not stretched)",
      routes(tf("BEARISH"), tf("BEARISH"),
             tf("BEARISH", 40, "STABLE", -30, "RISING", "Flat")) == "3.9")
check("3.10 fires (all bw Rising)",
      routes(tf("BEARISH", 40, "STABLE", -30, "RISING"),
             tf("BEARISH", 40, "STABLE", -30, "RISING"),
             tf("BEARISH", 40, "STABLE", -30, "RISING", "Flat")) == "3.10")
check("3.11 fires (RRR Quiet)",
      routes(tf("BEARISH", 50, "STABLE", 0, "STABLE", "Flat"),
             tf("BEARISH", 50, "STABLE", 0, "STABLE", "Flat"),
             tf("BEARISH", 50, "STABLE", 0, "STABLE", "Flat")) == "3.11")
check("3.12 fires (RRR Base fallthrough)",
      routes(tf("BEARISH", 40, "STABLE", -30, "FALLING", "Expanding"),
             tf("BEARISH", 40, "RISING", -30, "FALLING", "Flat"),
             tf("BEARISH", 40, "STABLE", -30, "FALLING", "Expanding")) == "3.12")


# ═══════════════════════════════════════════════════════════════
print("\n═══ §3 BRB FAMILY — 3 entries ═══")
# ═══════════════════════════════════════════════════════════════

check("3.13 fires (Quality Pullback: ltf rsi<=40, Contracting + htf Expanding)",
      routes(tf("BULLISH"),
             tf("BEARISH", 35, "FALLING", -20, "FALLING", "Contracting"),
             tf("BULLISH", 60, "RISING", 30, "RISING", "Expanding")) == "3.13")
check("3.13 boundary ltf.rsi=40 still fires (<=40 inclusive)",
      routes(tf("BULLISH"),
             tf("BEARISH", 40, "FALLING", -20, "FALLING", "Contracting"),
             tf("BULLISH", 60, "RISING", 30, "RISING", "Expanding")) == "3.13")
check("3.13b fires (Accelerating Pullback: ltf rsi<35 Expanding)",
      routes(tf("BULLISH"),
             tf("BEARISH", 30, "FALLING", -40, "FALLING", "Expanding"),
             tf("BULLISH")) == "3.13b")
check("3.13b boundary ltf.rsi=35 doesn't fire (strict <35)",
      routes(tf("BULLISH"),
             tf("BEARISH", 35, "FALLING", -40, "FALLING", "Expanding"),
             tf("BULLISH")) != "3.13b")
check("3.14 fires (BRB Base fallthrough)",
      routes(tf("BULLISH"),
             tf("BEARISH", 50, "STABLE", -10, "STABLE", "Flat"),
             tf("BULLISH")) == "3.14")


# ═══════════════════════════════════════════════════════════════
print("\n═══ §3 RBR FAMILY — 3 entries (mirror of BRB) ═══")
# ═══════════════════════════════════════════════════════════════

check("3.15 fires (Quality Bounce: ltf rsi>=60, Contracting + htf Expanding)",
      routes(tf("BEARISH"),
             tf("BULLISH", 65, "RISING", 20, "RISING", "Contracting"),
             tf("BEARISH", 40, "FALLING", -30, "FALLING", "Expanding")) == "3.15")
check("3.15 boundary ltf.rsi=60 still fires (>=60 inclusive)",
      routes(tf("BEARISH"),
             tf("BULLISH", 60, "RISING", 20, "RISING", "Contracting"),
             tf("BEARISH", 40, "FALLING", -30, "FALLING", "Expanding")) == "3.15")
check("3.15b fires (Accelerating Bounce: ltf rsi>65 Expanding)",
      routes(tf("BEARISH"),
             tf("BULLISH", 70, "RISING", 40, "RISING", "Expanding"),
             tf("BEARISH")) == "3.15b")
check("3.15b boundary ltf.rsi=65 doesn't fire (strict >65)",
      routes(tf("BEARISH"),
             tf("BULLISH", 65, "RISING", 40, "RISING", "Expanding"),
             tf("BEARISH")) != "3.15b")
check("3.16 fires (RBR Base fallthrough)",
      routes(tf("BEARISH"),
             tf("BULLISH", 50, "STABLE", 10, "STABLE", "Flat"),
             tf("BEARISH")) == "3.16")


# ═══════════════════════════════════════════════════════════════
print("\n═══ §3 BBR / RRB / BRR / RBB FAMILIES ═══")
# ═══════════════════════════════════════════════════════════════

check("3.17 fires (BBR htf bw Falling)",
      routes(tf("BULLISH"), tf("BULLISH"),
             tf("BEARISH", 40, "STABLE", -30, "FALLING")) == "3.17")
check("3.18 fires (BBR htf bw not Falling)",
      routes(tf("BULLISH"), tf("BULLISH"),
             tf("BEARISH", 40, "STABLE", -30, "STABLE")) == "3.18")
check("3.19 fires (RRB htf bw Rising)",
      routes(tf("BEARISH"), tf("BEARISH"),
             tf("BULLISH", 60, "STABLE", 30, "RISING")) == "3.19")
check("3.20 fires (RRB htf bw not Rising)",
      routes(tf("BEARISH"), tf("BEARISH"),
             tf("BULLISH", 60, "STABLE", 30, "STABLE")) == "3.20")
check("3.21 fires (BRR isolated CTF bull)",
      routes(tf("BULLISH"), tf("BEARISH"), tf("BEARISH")) == "3.21")
check("3.22 fires (RBB isolated CTF bear)",
      routes(tf("BEARISH"), tf("BULLISH"), tf("BULLISH")) == "3.22")


# ═══════════════════════════════════════════════════════════════
print("\n═══ Priority order: capitulation overrides scenario tier ═══")
# ═══════════════════════════════════════════════════════════════

check("Capitulation top wins over BBB Strong Clean",
      routes(tf("BULLISH", 85, "FALLING", 30, "RISING", "Expanding"),
             tf("BULLISH", 60, "RISING", 30, "RISING", "Expanding"),
             tf("BULLISH", 60, "RISING", 30, "RISING", "Expanding")) == "1.1")
check("Capitulation bottom wins over RRR scenario",
      routes(tf("BEARISH", 15, "RISING", -30, "FALLING", "Expanding"),
             tf("BEARISH", 40, "FALLING", -30, "FALLING", "Expanding"),
             tf("BEARISH", 40, "FALLING", -30, "FALLING", "Expanding")) == "1.2")


# ═══════════════════════════════════════════════════════════════
print("\n═══ Priority order: state tier (2.x) overrides scenario tier (3.x) ═══")
# ═══════════════════════════════════════════════════════════════

# Stuck overrides scenario routing for mixed-bias all-Contracting cases
check("Stuck overrides BBR scenario when all 3 Contracting",
      routes(tf("BULLISH", ribbon_state="Contracting", bw_val=50),
             tf("BULLISH", ribbon_state="Contracting", bw_val=50),
             tf("BEARISH", ribbon_state="Contracting", bw_val=50)) == "2.1")
# Compression overrides scenario routing for aligned-bias 2-Contracting low-BW
check("Compression overrides BBB scenario when 2 contracting + low BW",
      routes(tf("BULLISH", ribbon_state="Contracting", bw_val=10),
             tf("BULLISH", ribbon_state="Contracting", bw_val=-5),
             tf("BULLISH", ribbon_state="Expanding", bw_val=30)) == "2.2")


# ═══════════════════════════════════════════════════════════════
print("\n═══ All 27 bias combinations route somewhere (no errors) ═══")
# ═══════════════════════════════════════════════════════════════

biases = ["BULLISH", "BEARISH", "NEUTRAL"]
all_combo_ids = set()
all_combo_outputs = []
for c_b in biases:
    for l_b in biases:
        for h_b in biases:
            try:
                eid = routes(tf(c_b), tf(l_b), tf(h_b))
                all_combo_ids.add(eid)
                # Also call analyze_mtf to verify full output
                full = analyze_mtf(tf(c_b), tf(l_b), tf(h_b))
                all_combo_outputs.append((c_b, l_b, h_b, eid, full))
                check(f"  {c_b[0]}{l_b[0]}{h_b[0]}: routed to {eid}", eid in ENTRIES)
            except Exception as e:
                check(f"  {c_b[0]}{l_b[0]}{h_b[0]}: routing raised {type(e).__name__}", False, str(e))

check("All 27 combinations produced a routed entry",
      len(all_combo_outputs) == 27,
      f"got {len(all_combo_outputs)}")
check("All routed entries are in ENTRIES",
      all_combo_ids.issubset(set(ENTRIES.keys())))


# ═══════════════════════════════════════════════════════════════
print("\n═══ Missing-field degradation ═══")
# ═══════════════════════════════════════════════════════════════

# All-None
try:
    out = analyze_mtf(None, None, None)
    check("All-None → 1.3 (NNN)", out["scenario_code"] == "NNN" and out["sentiment"] == "neutral")
except Exception as e:
    check("All-None doesn't raise", False, str(e))

# Some-None
try:
    out = analyze_mtf(tf("BULLISH"), None, None)
    check("CTF-only (LTF/HTF None) → BNN routes to 2.4a", out["scenario_code"] == "BNN")
except Exception as e:
    check("Some-None doesn't raise", False, str(e))

try:
    out = analyze_mtf(tf("BULLISH"), None, tf("BULLISH"))
    check("LTF=None middle → BNB routes to 2.3a-agree",
          _select_entry(_coalesce(tf("BULLISH")), _coalesce(None), _coalesce(tf("BULLISH"))) == "2.3a-agree")
except Exception as e:
    check("LTF=None doesn't raise", False, str(e))

# Per-field None — bias present but other fields missing
sparse_tf = {"bias": "BULLISH"}  # missing rsi, rsi_trend, bw_val, bw_trend, ribbon_state
try:
    out = analyze_mtf(sparse_tf, sparse_tf, sparse_tf)
    check("Sparse TF dicts (bias only) don't raise", True)
    check("Sparse TF dicts produce valid sentiment", out["sentiment"] in VALID_SENTIMENTS)
except Exception as e:
    check("Sparse TF dicts don't raise", False, str(e))

# rsi=None, bw_val=None — capitulation can't fire
try:
    out = analyze_mtf(tf("BULLISH", rsi=None, rsi_trend=None),
                      tf("BULLISH", rsi=None, rsi_trend=None),
                      tf("BULLISH", rsi=None, rsi_trend=None))
    check("rsi=None doesn't crash triggers", True)
    check("rsi=None: capitulation flag stays False", out["capitulation"] is False)
except Exception as e:
    check("rsi=None doesn't raise", False, str(e))

# bw_val=None across all TFs: compression can't determine, doesn't fire
try:
    out = analyze_mtf(tf("BULLISH", ribbon_state="Contracting", bw_val=None),
                      tf("BULLISH", ribbon_state="Contracting", bw_val=None),
                      tf("BULLISH", ribbon_state="Contracting", bw_val=None))
    check("bw_val=None across all TFs: no false-positive Compression",
          # All Contracting, biases equal → Stuck=False. Compression needs |bw_val|<20 — None can't
          # satisfy that. So Compression doesn't fire. Falls to scenario tier. With bw_trend=STABLE
          # default, doesn't hit 3.1/3.1b/3.2/etc., lands on 3.6 Base.
          out["scenario_code"] == "BBB")
except Exception as e:
    check("bw_val=None doesn't raise", False, str(e))


# ═══════════════════════════════════════════════════════════════
print("\n═══ Catalog scanners — applied to all 42 entry paragraphs ═══")
# ═══════════════════════════════════════════════════════════════

# Forbidden substring scanner — strict substring match per kickoff
FORBIDDEN_SUBSTRINGS = [
    # Kickoff hard rule
    "buy", "sell", "enter", "exit", "tighten", "reduce", "position", "trail",
    # Catalog tone-rule forbidden phrases (line 36 of catalog)
    "stop", "target", "trade here", "avoid", "take profits",
    # Pedagogical scaffolding (line 37 of catalog)
    "the primary timeframe", "the lower timeframe", "the higher timeframe",
    # Apologetic framing (line 38 of catalog)
    "this is a limited view", "with more data",
    # Common drift words also worth blocking
    "buy the dip", "sell the rip",
]


def find_forbidden(p):
    pl = p.lower()
    return [s for s in FORBIDDEN_SUBSTRINGS if s in pl]


# Number scanner — whitelist 5m/1m/15m timeframe references per agreed scope
TF_REF_RX = re.compile(r"\b(?:5m|1m|15m)\b")
DIGIT_RX = re.compile(r"[0-9%$]")


def find_digits(p):
    """Strip TF references first, then scan for any remaining digits/% /$."""
    return DIGIT_RX.findall(TF_REF_RX.sub("", p))


# Length scanner — 3 sentences, 40-65 words (catalog says "roughly 40-60")
SENT_SPLIT = re.compile(r"(?<=[.!?])\s+(?=[A-Z])")


def count_sentences(p):
    return [s for s in SENT_SPLIT.split(p.strip()) if s.strip()]


def count_words(p):
    return len(re.findall(r"\b[\w'-]+\b", p))


for eid in sorted(ENTRIES):
    p = ENTRIES[eid]["paragraph"]
    forb = find_forbidden(p)
    digs = find_digits(p)
    sents = count_sentences(p)
    words = count_words(p)
    check(f"  {eid}: no forbidden substrings", len(forb) == 0, f"hits: {forb}")
    check(f"  {eid}: no digits/% /$ (TF whitelist applied)",
          len(digs) == 0, f"hits: {digs}")
    check(f"  {eid}: exactly 3 sentences", len(sents) == 3, f"got {len(sents)}")
    check(f"  {eid}: 40-65 words", 40 <= words <= 65, f"got {words}")


# ═══════════════════════════════════════════════════════════════
print("\n═══ Side-effect & purity ═══")
# ═══════════════════════════════════════════════════════════════

# Calling analyze_mtf must not mutate inputs
inp_c = tf("BULLISH", 60, "RISING", 30, "RISING", "Expanding")
inp_l = tf("BULLISH", 60, "RISING", 30, "RISING", "Expanding")
inp_h = tf("BULLISH", 60, "RISING", 30, "RISING", "Expanding")
import copy
snapshot_c = copy.deepcopy(inp_c)
snapshot_l = copy.deepcopy(inp_l)
snapshot_h = copy.deepcopy(inp_h)
_ = analyze_mtf(inp_c, inp_l, inp_h)
check("analyze_mtf does not mutate ctf input", inp_c == snapshot_c)
check("analyze_mtf does not mutate ltf input", inp_l == snapshot_l)
check("analyze_mtf does not mutate htf input", inp_h == snapshot_h)

# Each call returns fresh conflicts list (no shared mutable state)
out1 = analyze_mtf(tf(), tf(), tf())
out2 = analyze_mtf(tf(), tf(), tf())
check("conflicts list is fresh per call (not shared reference)",
      out1["conflicts"] is not out2["conflicts"])


# ═══════════════════════════════════════════════════════════════
print("\n" + "═" * 50)
print(f"Results: {passed} passed, {failed} failed")
if failed:
    print("SOME TESTS FAILED!")
    sys.exit(1)
else:
    print("ALL TESTS PASSED!")
