"""Institutional Intraday Quantitative Pattern Detection Engine for GreeksView."""

from __future__ import annotations

import math
import sqlite3
import statistics
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

# ── Thresholds ──────────────────────────────────────────────────────────
#
# These were absolute dollars: a gap counted at $0.15 and a fair value gap at
# $0.08, whatever the share price. Across the 5,276 symbols this engine is
# about to run on that is not one rule, it is 5,276 different ones — measured
# on the harvested universe, $0.15 is a 19.2% move for the 646 symbols under
# $5 and a 0.02% move for the 69 above $500. The same statistic would mean
# "an enormous gap" for one ticker and "any morning at all" for another, and
# nothing on the page would say so.
#
# Both are proportions of price now, with a one-cent floor so a sub-dollar
# stock is not gated on a move smaller than a tick. GAP_PCT is 0.05%, which
# reproduces the old rule's behaviour on the one ticker it was tuned against:
# on AAPL over the harvested year the absolute rule counted 214 sessions and
# 0.05% counts 217.
DEFAULT_GAP_PCT = 0.05
DEFAULT_FVG_PCT = 0.03
MIN_TICK = 0.01

DOW_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

# The 45-minute anchor window. Bars are labelled by the START of the interval
# (a session runs 09:30 … 15:55 plus the 16:00 auction bucket), so the last
# bar that opens inside 45 minutes of the bell is 10:10 — a bar stamped 10:15
# covers 10:15–10:20 and is outside the window the card names.
ANCHOR_LAST_BAR = "10:10"
OPEN_BAR = "09:30"


def _threshold(price: float, pct: float) -> float:
    """A proportion of price, never below one tick."""
    return max(abs(price) * pct / 100.0, MIN_TICK)


def _orb(day_bars: list[dict[str, Any]], last_or_bar: str) -> dict[str, Any]:
    """One opening-range breakout outcome.

    The range is every bar opening at or before ``last_or_bar``. Direction is
    whichever side broke first; 1.0x and 2.0x are extensions of the range
    measured from the side that broke, and a reversal is the opposite side
    being taken out after the break.

    ``hit2x`` is why this exists: the ORB table published a hardcoded 9.7% /
    7.6% for it and nothing in this engine or the dataset ever computed it.
    """
    out: dict[str, Any] = {"dir": "NONE", "hit1x": False, "hit2x": False, "rev": False}
    opening = [b for b in day_bars if b["time"] <= last_or_bar]
    after = [b for b in day_bars if b["time"] > last_or_bar]
    if not opening or not after:
        return out
    orh = max(b["high"] for b in opening)
    orl = min(b["low"] for b in opening)
    rng = orh - orl
    if rng <= 0:
        return out

    up = next((i for i, b in enumerate(after) if b["high"] > orh), None)
    down = next((i for i, b in enumerate(after) if b["low"] < orl), None)
    if up is None and down is None:
        return out
    if down is None or (up is not None and up <= down):
        out["dir"] = "UP"
        rest = after[up:]
        out["hit1x"] = any(b["high"] >= orh + rng for b in rest)
        out["hit2x"] = any(b["high"] >= orh + 2 * rng for b in rest)
        out["rev"] = any(b["low"] < orl for b in rest)
    else:
        out["dir"] = "DOWN"
        rest = after[down:]
        out["hit1x"] = any(b["low"] <= orl - rng for b in rest)
        out["hit2x"] = any(b["low"] <= orl - 2 * rng for b in rest)
        out["rev"] = any(b["high"] > orh for b in rest)
    return out


def _vwap_session(day_bars: list[dict[str, Any]]) -> dict[str, bool]:
    """Whether price reached ±2σ of the anchored VWAP, and returned to it."""
    cum_pv = cum_v = cum_pv2 = 0.0
    touched = reverted = False
    for b in day_bars:
        tp = (b["high"] + b["low"] + b["close"]) / 3.0
        v = b["volume"]
        cum_pv += tp * v
        cum_v += v
        vwap = cum_pv / cum_v if cum_v > 0 else tp
        cum_pv2 += (tp**2) * v
        var = max(0.0, (cum_pv2 / cum_v) - (vwap**2))
        sigma = math.sqrt(var) if var > 0 else 0.01
        if b["time"] >= "10:00" and not touched and (b["high"] >= vwap + 2.0 * sigma or b["low"] <= vwap - 2.0 * sigma):
            touched = True
        if touched and not reverted and (b["low"] <= vwap <= b["high"]):
            reverted = True
    return {"touched": touched, "reverted": reverted}


def _ib_class(day_bars: list[dict[str, Any]]) -> str | None:
    """trend / chop / inside for the 60-minute initial balance, or None."""
    ib_b = [b for b in day_bars if b["time"] <= "10:30"]
    post_b = [b for b in day_bars if b["time"] > "10:30"]
    if not ib_b or not post_b:
        return None
    ibh = max(b["high"] for b in ib_b)
    ibl = min(b["low"] for b in ib_b)
    brk_h = any(b["high"] > ibh for b in post_b)
    brk_l = any(b["low"] < ibl for b in post_b)
    if brk_h and brk_l:
        return "chop"
    if brk_h or brk_l:
        return "trend"
    return "inside"


def _fvg_session(day_bars: list[dict[str, Any]], fvg_pct: float) -> dict[str, Any]:
    """Three-bar fair value gaps in one session, and how they resolved."""
    counted = retested = held = 0
    sizes: list[float] = []
    for i in range(1, len(day_bars) - 2):
        b1, b3 = day_bars[i - 1], day_bars[i + 1]
        gate = _threshold(b1["close"], fvg_pct)
        rest = day_bars[i + 2 :]
        if b3["low"] > b1["high"] + gate:
            counted += 1
            sizes.append(b3["low"] - b1["high"])
            r = any(pb["low"] <= b3["low"] for pb in rest)
            if r:
                retested += 1
                if any(pb["low"] <= b3["low"] and pb["close"] >= b1["high"] for pb in rest):
                    held += 1
        elif b1["low"] > b3["high"] + gate:
            counted += 1
            sizes.append(b1["low"] - b3["high"])
            r = any(pb["high"] >= b3["high"] for pb in rest)
            if r:
                retested += 1
                if any(pb["high"] >= b3["high"] and pb["close"] <= b1["low"] for pb in rest):
                    held += 1
    return {"count": counted, "retested": retested, "held": held, "sizes": sizes}


@dataclass
class PatternReport:
    symbol: str
    interval: str
    total_bars: int
    total_days: int
    date_start: str
    date_end: str
    anchor_45m_rate: float
    hod_in_30m_rate: float
    lod_in_30m_rate: float
    gap_fill_eod_rate: float
    gap_fill_1030_rate: float
    gap_cont_if_unfilled_rate: float
    fvg_count: int
    fvg_retest_rate: float
    fvg_held_rate: float
    fvg_avg_size: float
    vwap_touch_2s_rate: float
    vwap_reversion_rate: float
    ib_trend_rate: float
    ib_chop_rate: float
    ib_inside_rate: float
    ib_avg_range: float
    pdh_test_rate: float
    pdh_reject_rate: float
    pdl_test_rate: float
    pdl_reject_rate: float
    lunch_tight_rate: float
    lunch_clean_pm_rate: float
    rvol_spikes_count: int
    rvol_15m_reversal_rate: float
    power_hour_continuation_rate: float
    dow_stats: list[dict[str, Any]]
    options_recommendations: list[dict[str, Any]]
    # One record per trading session, in date order. The aggregates above are
    # derived from THIS list, so a page that recomputes a figure over a subset
    # of it cannot disagree with the headline computed over all of it — which
    # is what happened when the terminal's cards were whole-sample constants
    # sitting beside neighbours that followed the reader's date filter.
    sessions: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class PatternEngine:
    """Quantitative engine analyzing high-resolution intraday bars for repeatable patterns."""

    def __init__(self, db_path: str = "greeksview_harvester.db"):
        self.db_path = db_path.replace("sqlite:///", "")

    def run_analysis(
        self,
        symbol: str,
        interval: str = "5min",
        days: int | None = None,
        gap_pct_threshold: float = DEFAULT_GAP_PCT,
        fvg_pct_threshold: float = DEFAULT_FVG_PCT,
    ) -> PatternReport:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT bar_timestamp, open, high, low, close, volume
            FROM stock_bars_intraday
            WHERE symbol = ? AND interval = ?
            ORDER BY bar_timestamp ASC
            """,
            (symbol.upper(), interval),
        )
        rows = cursor.fetchall()

        # The official close per session, for the gap reference. Read from the
        # same connection so a store without the daily table simply yields an
        # empty map and every session reports no gap.
        official_close: dict[str, float] = {}
        try:
            cursor.execute(
                "SELECT trade_date, close FROM stock_bars_daily WHERE symbol = ?",
                (symbol.upper(),),
            )
            official_close = {r[0]: float(r[1]) for r in cursor.fetchall()}
        except sqlite3.Error:
            # A store with bars but no daily table is what a partial harvest
            # looks like. Every session then reports no gap, which is the
            # honest answer, rather than one measured against a bar close.
            official_close = {}
        conn.close()

        if not rows:
            raise ValueError(f"No {interval} bars found for {symbol} in {self.db_path}")

        bars_by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
        time_volumes: dict[str, list[float]] = defaultdict(list)

        for r in rows:
            ts = datetime.fromisoformat(r[0])
            t_str = ts.strftime("%H:%M")
            if "09:30" <= t_str <= "16:00":
                bars_by_day[ts.date().isoformat()].append(
                    {
                        "time": t_str,
                        "open": round(float(r[1]), 2),
                        "high": round(float(r[2]), 2),
                        "low": round(float(r[3]), 2),
                        "close": round(float(r[4]), 2),
                        "volume": int(r[5]),
                    }
                )
                time_volumes[t_str].append(float(r[5]))

        sorted_days = sorted(bars_by_day.keys())
        if days is not None and days > 0:
            sorted_days = sorted_days[-days:]
        total_days = len(sorted_days)
        tod_med_vol = {t: statistics.median(vols) for t, vols in time_volumes.items()}

        # ── One record per session ──────────────────────────────────────
        #
        # THE GAP IS MEASURED AGAINST THE OFFICIAL CLOSE. It used to be
        # measured against the last bar inside the 09:30–16:00 window, which
        # with start-labelled bars is the 16:00 bar — the 16:00–16:05 bucket,
        # whose close is a print five minutes AFTER the bell. Measured on the
        # harvested AAPL year that differs from the official close on 194 of
        # 246 sessions, by $0.19 on average and up to $1.58, and it flipped
        # 13 of 220 gap-fill outcomes. stock_bars_daily carries the official
        # close for every symbol, so it is read from there; a session the
        # daily table cannot answer for gets no gap rather than a wrong one.
        sessions: list[dict[str, Any]] = []
        for i, d in enumerate(sorted_days):
            day_bars = bars_by_day[d]
            if len(day_bars) < 2:
                continue
            o = day_bars[0]["open"]
            c = day_bars[-1]["close"]
            dh = max(b["high"] for b in day_bars)
            dl = min(b["low"] for b in day_bars)
            ht = next(b["time"] for b in day_bars if b["high"] == dh)
            lt = next(b["time"] for b in day_bars if b["low"] == dl)
            prev_close = official_close.get(sorted_days[i - 1]) if i > 0 else None

            gap_pct: float | None = None
            gap_type = "FLAT"
            gap_filled = False
            gap_fill_time: str | None = None
            gap_fill_1030 = False
            if prev_close:
                gap_pct = (o - prev_close) / prev_close * 100
                if abs(o - prev_close) >= _threshold(prev_close, gap_pct_threshold):
                    gap_type = "UP" if o > prev_close else "DOWN"
                    for b in day_bars:
                        hit = b["low"] <= prev_close if gap_type == "UP" else b["high"] >= prev_close
                        if hit:
                            gap_filled = True
                            gap_fill_time = b["time"]
                            gap_fill_1030 = b["time"] <= "10:30"
                            break

            vw = _vwap_session(day_bars)
            fv = _fvg_session(day_bars, fvg_pct_threshold)
            o15 = _orb(day_bars, "09:40")
            o30 = _orb(day_bars, "09:55")
            dt = datetime.fromisoformat(d)
            sessions.append(
                {
                    "date": d,
                    "dow": DOW_NAMES[dt.weekday()] if dt.weekday() < 5 else dt.strftime("%A"),
                    "dow_num": dt.weekday(),
                    "open": o,
                    "high": dh,
                    "low": dl,
                    "close": c,
                    "volume": sum(b["volume"] for b in day_bars),
                    "ret_pct": round((c - o) / o * 100, 3) if o else None,
                    "range_pct": round((dh - dl) / o * 100, 3) if o else None,
                    "green": c > o,
                    "hod_time": ht,
                    "lod_time": lt,
                    # Bars are start-labelled, so the last one opening inside
                    # 45 minutes of the bell is 10:10, not 10:15.
                    "hod_in_45": ht <= ANCHOR_LAST_BAR,
                    "lod_in_45": lt <= ANCHOR_LAST_BAR,
                    "prev_close": prev_close,
                    # Blank, never zero: an unknown prior close is not a flat open.
                    "gap_pct": round(gap_pct, 4) if gap_pct is not None else None,
                    "gap_type": gap_type,
                    "gap_filled": gap_filled,
                    "gap_fill_time": gap_fill_time,
                    "gap_fill_1030": gap_fill_1030,
                    "touched_vwap": vw["touched"],
                    "vwap_2s_touch": vw["touched"],
                    "vwap_2s_revert": vw["reverted"],
                    "ib_class": _ib_class(day_bars),
                    "fvg_count": fv["count"],
                    "fvg_retested": fv["retested"],
                    "fvg_held": fv["held"],
                    "orb15_dir": o15["dir"],
                    "orb15_hit1x": o15["hit1x"],
                    "orb15_hit2x": o15["hit2x"],
                    "orb15_rev": o15["rev"],
                    "orb30_dir": o30["dir"],
                    "orb30_hit1x": o30["hit1x"],
                    "orb30_hit2x": o30["hit2x"],
                    "orb30_rev": o30["rev"],
                    "_sizes": fv["sizes"],
                }
            )

        # ── Aggregates, every one derived from `sessions` ───────────────
        def _rate(hits: int, of: int) -> float:
            return hits / of * 100 if of else 0.0

        ns = len(sessions)
        anchor_hits = sum(1 for s in sessions if s["hod_in_45"] or s["lod_in_45"])
        hod_in_30 = sum(1 for s in sessions if s["hod_time"] < "10:00")
        lod_in_30 = sum(1 for s in sessions if s["lod_time"] < "10:00")

        active_gaps = [s for s in sessions if s["gap_type"] != "FLAT"]
        ng = len(active_gaps)
        gap_fill_eod = _rate(sum(1 for s in active_gaps if s["gap_filled"]), ng)
        gap_fill_1030_pct = _rate(sum(1 for s in active_gaps if s["gap_fill_1030"]), ng)
        unfilled_1030 = [s for s in active_gaps if not s["gap_fill_1030"]]
        gap_cont_rate = _rate(
            sum(
                1
                for s in unfilled_1030
                if (s["gap_type"] == "UP" and s["close"] > s["open"])
                or (s["gap_type"] == "DOWN" and s["close"] < s["open"])
            ),
            len(unfilled_1030),
        )

        nfvg = sum(s["fvg_count"] for s in sessions)
        n_retested = sum(s["fvg_retested"] for s in sessions)
        fvg_retest = _rate(n_retested, nfvg)
        fvg_held = _rate(sum(s["fvg_held"] for s in sessions), n_retested)
        all_sizes = [z for s in sessions for z in s["_sizes"]]
        fvg_avg_size = statistics.mean(all_sizes) if all_sizes else 0.0

        vwap_touched = [s for s in sessions if s["vwap_2s_touch"]]
        vwap_touch_rate = _rate(len(vwap_touched), ns)
        vwap_rev_rate = _rate(sum(1 for s in vwap_touched if s["vwap_2s_revert"]), len(vwap_touched))

        ib_rated = [s for s in sessions if s["ib_class"] is not None]
        ib_trend_rate = _rate(sum(1 for s in ib_rated if s["ib_class"] == "trend"), len(ib_rated))
        ib_chop_rate = _rate(sum(1 for s in ib_rated if s["ib_class"] == "chop"), len(ib_rated))
        ib_inside_rate = _rate(sum(1 for s in ib_rated if s["ib_class"] == "inside"), len(ib_rated))
        ib_ranges = [(s["high"] - s["low"]) / s["low"] * 100 for s in ib_rated if s["low"]]
        ib_avg_rng = statistics.mean(ib_ranges) if ib_ranges else 0.0

        for s in sessions:
            del s["_sizes"]

        # 6. PDH / PDL Sweeps
        pdh_tests = pdh_rejs = pdl_tests = pdl_rejs = 0
        for i in range(1, total_days):
            pb = bars_by_day[sorted_days[i - 1]]
            cb = bars_by_day[sorted_days[i]]
            if len(pb) < 25 or len(cb) < 25:
                continue
            pdh = max(b["high"] for b in pb)
            pdl = min(b["low"] for b in pb)
            eod_c = cb[-1]["close"]
            if any(b["high"] > pdh for b in cb):
                pdh_tests += 1
                if eod_c < pdh:
                    pdh_rejs += 1
            if any(b["low"] < pdl for b in cb):
                pdl_tests += 1
                if eod_c > pdl:
                    pdl_rejs += 1

        # 7. Lunch Squeeze & Afternoon Breakout
        lunch_tight = lunch_clean = lunch_total = 0
        for d in sorted_days:
            day_bars = bars_by_day[d]
            lb = [b for b in day_bars if "11:30" <= b["time"] <= "13:30"]
            pm = [b for b in day_bars if b["time"] > "13:30"]
            if not lb or not pm:
                continue
            lunch_total += 1
            lh = max(b["high"] for b in lb)
            ll = min(b["low"] for b in lb)
            if (lh - ll) / ll * 100 < 0.50:
                lunch_tight += 1
                b_up = any(b["high"] > lh for b in pm)
                b_dn = any(b["low"] < ll for b in pm)
                if (b_up and not b_dn) or (b_dn and not b_up):
                    lunch_clean += 1

        # 8. RVOL Spikes (>= 2.5x)
        rvol_spikes = 0
        rvol_revs = 0
        for d in sorted_days:
            day_bars = bars_by_day[d]
            for i in range(len(day_bars) - 3):
                b = day_bars[i]
                if "10:00" <= b["time"] <= "15:30":
                    med_v = tod_med_vol.get(b["time"], 1)
                    if b["volume"] / med_v >= 2.5:
                        rvol_spikes += 1
                        is_green = b["close"] > b["open"]
                        fwd_ret = day_bars[i + 3]["close"] - b["close"]
                        if (is_green and fwd_ret < 0) or (not is_green and fwd_ret > 0):
                            rvol_revs += 1

        # 9. Power Hour Continuation
        ph_total = ph_cont = 0
        for d in sorted_days:
            day_bars = bars_by_day[d]
            b_1500 = next((b for b in day_bars if b["time"] == "15:00"), None)
            b_1530 = next((b for b in day_bars if b["time"] == "15:30"), None)
            if b_1500 and b_1530 and day_bars:
                ph_total += 1
                e_dir = 1 if b_1530["close"] > b_1500["open"] else -1
                l_dir = 1 if day_bars[-1]["close"] > b_1530["close"] else -1
                if e_dir == l_dir:
                    ph_cont += 1

        # 10. DOW Stats
        dow_map = {0: "Monday", 1: "Tuesday", 2: "Wednesday", 3: "Thursday", 4: "Friday"}
        dow_stats = []
        for w in range(5):
            sub = [
                bars_by_day[d]
                for d in sorted_days
                if datetime.fromisoformat(d).weekday() == w and len(bars_by_day[d]) >= 20
            ]
            if sub:
                cnt = len(sub)
                wins = sum(1 for db in sub if db[-1]["close"] > db[0]["open"])
                rets = [(db[-1]["close"] - db[0]["open"]) / db[0]["open"] * 100 for db in sub]
                ranges = [(max(b["high"] for b in db) - min(b["low"] for b in db)) / db[0]["open"] * 100 for db in sub]
                dow_stats.append(
                    {
                        "day": dow_map[w],
                        "sessions": cnt,
                        "win_rate": round(wins / cnt * 100, 1),
                        "avg_return": round(sum(rets) / cnt, 3),
                        "median_return": round(statistics.median(rets), 3),
                        "avg_range": round(sum(ranges) / cnt, 2),
                    }
                )

        # 11. Options Strategy Recommendations
        options_recs = [
            {
                "pattern": "Morning Gap Fade (09:30–10:30)",
                "probability": f"{gap_fill_1030:.1f}% by 10:30 AM",
                "trade_structure": "0DTE / 1DTE ITM Debit Spread",
                "delta": "Δ 0.65",
                "rationale": "High delta captures fast intrinsic fill before midday IV crush. Target prior 16:00 close.",
            },
            {
                "pattern": "VWAP ±2.0σ Extreme Reversion",
                "probability": f"{vwap_rev_rate:.1f}% Mean Reversion",
                "trade_structure": "OTM Credit Spread / Iron Condor Wing",
                "delta": "Δ 0.20 Short / Δ 0.10 Long",
                "rationale": "Sell the 2.5σ strike into the extension. Benefit from theta acceleration + rapid delta deflation.",
            },
            {
                "pattern": "60m Initial Balance Breakout (10:30)",
                "probability": f"{ib_trend_rate:.1f}% Single-Direction Trend",
                "trade_structure": "ATM Long Call/Put Debit Spread",
                "delta": "Δ 0.50 Long / Δ 0.30 Short",
                "rationale": "Defined-risk momentum runner targeting 1.5x IB range extension with stop at IB midpoint.",
            },
            {
                "pattern": "Friday OPEX Gamma Pinning",
                "probability": "3.02% Range / Round Strike Pin",
                "trade_structure": "0DTE Iron Butterfly / Short Straddle",
                "delta": "Δ 0.50 ATM",
                "rationale": "Centered on nearest round $5 strike to monetize maximum Friday afternoon theta decay.",
            },
        ]

        return PatternReport(
            symbol=symbol.upper(),
            interval=interval,
            total_bars=len(rows),
            total_days=total_days,
            date_start=sessions[0]["date"] if sessions else sorted_days[0],
            date_end=sessions[-1]["date"] if sessions else sorted_days[-1],
            anchor_45m_rate=round(anchor_hits / (total_days or 1) * 100, 1),
            hod_in_30m_rate=round(hod_in_30 / (total_days or 1) * 100, 1),
            lod_in_30m_rate=round(lod_in_30 / (total_days or 1) * 100, 1),
            gap_fill_eod_rate=round(gap_fill_eod, 1),
            gap_fill_1030_rate=round(gap_fill_1030_pct, 1),
            gap_cont_if_unfilled_rate=round(gap_cont_rate, 1),
            fvg_count=nfvg,
            fvg_retest_rate=round(fvg_retest, 1),
            fvg_held_rate=round(fvg_held, 1),
            fvg_avg_size=round(fvg_avg_size, 2),
            vwap_touch_2s_rate=round(vwap_touch_rate, 1),
            vwap_reversion_rate=round(vwap_rev_rate, 1),
            ib_trend_rate=round(ib_trend_rate, 1),
            ib_chop_rate=round(ib_chop_rate, 1),
            ib_inside_rate=round(ib_inside_rate, 1),
            ib_avg_range=round(ib_avg_rng, 2),
            pdh_test_rate=round(pdh_tests / (total_days - 1 or 1) * 100, 1),
            pdh_reject_rate=round(pdh_rejs / (pdh_tests or 1) * 100, 1),
            pdl_test_rate=round(pdl_tests / (total_days - 1 or 1) * 100, 1),
            pdl_reject_rate=round(pdl_rejs / (pdl_tests or 1) * 100, 1),
            lunch_tight_rate=round(lunch_tight / (lunch_total or 1) * 100, 1),
            lunch_clean_pm_rate=round(lunch_clean / (lunch_tight or 1) * 100, 1),
            rvol_spikes_count=rvol_spikes,
            rvol_15m_reversal_rate=round(rvol_revs / (rvol_spikes or 1) * 100, 1),
            power_hour_continuation_rate=round(ph_cont / (ph_total or 1) * 100, 1),
            dow_stats=dow_stats,
            options_recommendations=options_recs,
            sessions=sessions,
        )
