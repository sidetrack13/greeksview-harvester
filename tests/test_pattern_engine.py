"""The intraday pattern engine, over hand-built sessions.

This engine produces every figure the EdgeMatrix pages publish and had no
tests at all — 255 statements, 0% covered — while being one command away from
generating a dataset per ticker for 5,276 symbols.

Bars here are synthetic and hand-computed. Nothing reads the harvested store:
a test that needs a 39 GB file is a test nobody runs.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pytest

from harvester.analytics.pattern_engine import (
    DEFAULT_GAP_PCT,
    MIN_TICK,
    PatternEngine,
    _ib_class,
    _orb,
    _threshold,
)

RTH_OPEN = 9 * 60 + 30


def _t(minutes_from_open: int) -> str:
    m = RTH_OPEN + minutes_from_open
    return f"{m // 60:02d}:{m % 60:02d}"


def _bar(time: str, o: float, h: float, low: float, c: float, v: int = 1000) -> dict[str, Any]:
    return {"time": time, "open": o, "high": h, "low": low, "close": c, "volume": v}


def _flat_session(n: int = 40, price: float = 100.0) -> list[dict[str, Any]]:
    """A session that does nothing: every bar the same, so no pattern fires."""
    return [_bar(_t(i * 5), price, price, price, price) for i in range(n)]


def _write(db: Path, symbol: str, day: str, bars: list[dict[str, Any]], daily_close: float | None) -> None:
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS stock_bars_intraday (symbol TEXT, bar_timestamp TEXT, interval TEXT,"
        " open REAL, high REAL, low REAL, close REAL, volume INTEGER)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS stock_bars_daily (symbol TEXT, trade_date TEXT, open REAL,"
        " high REAL, low REAL, close REAL, adjusted_close REAL, volume INTEGER)"
    )
    for b in bars:
        conn.execute(
            "INSERT INTO stock_bars_intraday VALUES (?,?,?,?,?,?,?,?)",
            (symbol, f"{day} {b['time']}:00", "5min", b["open"], b["high"], b["low"], b["close"], b["volume"]),
        )
    if daily_close is not None:
        conn.execute(
            "INSERT INTO stock_bars_daily VALUES (?,?,?,?,?,?,?,?)",
            (
                symbol,
                day,
                bars[0]["open"],
                max(b["high"] for b in bars),
                min(b["low"] for b in bars),
                daily_close,
                daily_close,
                1,
            ),
        )
    conn.commit()
    conn.close()


class TestThreshold:
    def test_it_scales_with_price(self) -> None:
        """The defect this replaced: one dollar amount for every share price."""
        assert _threshold(200.0, 0.05) == pytest.approx(0.10)
        assert _threshold(20.0, 0.05) == pytest.approx(0.01)

    def test_it_never_falls_below_a_tick(self) -> None:
        """A sub-dollar stock must not be gated on a move smaller than a tick."""
        assert _threshold(0.50, 0.05) == MIN_TICK
        assert _threshold(0.0, 0.05) == MIN_TICK


class TestOpeningRange:
    """The ORB fields, which this engine did not compute at all before."""

    def _session(self, highs: list[float], lows: list[float]) -> list[dict[str, Any]]:
        return [_bar(_t(i * 5), 100.0, highs[i], lows[i], 100.0) for i in range(len(highs))]

    def test_no_break_reports_no_direction(self) -> None:
        out = _orb(_flat_session(12), "09:40")
        assert out == {"dir": "NONE", "hit1x": False, "hit2x": False, "rev": False}

    def test_an_upside_break_that_reaches_neither_extension(self) -> None:
        # opening range 99-101 over the first three bars, so range = 2
        highs = [101, 101, 101, 101.5, 101.5, 101.5]
        lows = [99, 99, 99, 100.5, 100.5, 100.5]
        out = _orb(self._session(highs, lows), "09:40")
        assert out["dir"] == "UP"
        assert out["hit1x"] is False and out["hit2x"] is False

    def test_one_x_is_the_range_added_to_the_broken_side(self) -> None:
        # range 99-101 (=2); 1.0x needs 103, 2.0x needs 105
        highs = [101, 101, 101, 103.0, 103.0, 103.0]
        lows = [99, 99, 99, 100.5, 100.5, 100.5]
        out = _orb(self._session(highs, lows), "09:40")
        assert out["dir"] == "UP"
        assert out["hit1x"] is True
        assert out["hit2x"] is False

    def test_two_x_needs_twice_the_range(self) -> None:
        highs = [101, 101, 101, 105.0, 105.0, 105.0]
        lows = [99, 99, 99, 100.5, 100.5, 100.5]
        out = _orb(self._session(highs, lows), "09:40")
        assert out["hit1x"] is True and out["hit2x"] is True

    def test_a_downside_break_is_measured_downwards(self) -> None:
        highs = [101, 101, 101, 100.5, 100.5, 100.5]
        lows = [99, 99, 99, 97.0, 97.0, 95.0]
        out = _orb(self._session(highs, lows), "09:40")
        assert out["dir"] == "DOWN"
        assert out["hit1x"] is True  # 99 - 2 = 97
        assert out["hit2x"] is True  # 99 - 4 = 95

    def test_taking_out_the_other_side_after_a_break_is_a_reversal(self) -> None:
        highs = [101, 101, 101, 102.0, 102.0, 102.0]
        lows = [99, 99, 99, 100.0, 98.0, 98.0]
        out = _orb(self._session(highs, lows), "09:40")
        assert out["dir"] == "UP" and out["rev"] is True

    def test_whichever_side_breaks_first_sets_the_direction(self) -> None:
        highs = [101, 101, 101, 100.5, 102.0, 102.0]
        lows = [99, 99, 99, 98.0, 98.0, 98.0]
        out = _orb(self._session(highs, lows), "09:40")
        assert out["dir"] == "DOWN"

    def test_a_session_with_no_bars_after_the_range_reports_nothing(self) -> None:
        assert _orb(_flat_session(12)[:3], "09:40")["dir"] == "NONE"


class TestInitialBalance:
    def test_both_sides_broken_is_chop(self) -> None:
        bars = _flat_session(30)
        bars[-1] = _bar(bars[-1]["time"], 100, 101, 99, 100)
        assert _ib_class(bars) == "chop"

    def test_one_side_only_is_trend(self) -> None:
        bars = _flat_session(30)
        bars[-1] = _bar(bars[-1]["time"], 100, 101, 100, 100)
        assert _ib_class(bars) == "trend"

    def test_neither_side_is_inside(self) -> None:
        assert _ib_class(_flat_session(30)) == "inside"

    def test_a_session_that_never_leaves_the_balance_window(self) -> None:
        assert _ib_class([_bar("09:35", 100, 100, 100, 100)]) is None


class TestGapReference:
    """The gap is measured against the OFFICIAL close, not the last bar."""

    def _two_days(
        self, tmp_path: Path, *, daily_close: float, last_bar_close: float, day2_open: float
    ) -> PatternEngine:
        db = tmp_path / "t.db"
        d1 = _flat_session(40)
        d1[-1] = _bar(d1[-1]["time"], last_bar_close, last_bar_close, last_bar_close, last_bar_close)
        _write(db, "TEST", "2026-01-05", d1, daily_close)
        d2 = [_bar(_t(i * 5), day2_open, day2_open, day2_open, day2_open) for i in range(40)]
        _write(db, "TEST", "2026-01-06", d2, day2_open)
        return PatternEngine(db_path=str(db))

    def test_the_official_close_sets_the_gap_not_the_last_bar(self, tmp_path: Path) -> None:
        """The bug: the 16:00 bar spans 16:00-16:05, so its close is a print
        after the bell. Here the two differ by a dollar and only one of them
        makes the open a gap."""
        eng = self._two_days(tmp_path, daily_close=100.0, last_bar_close=101.0, day2_open=100.0)
        r = eng.run_analysis("TEST", "5min")
        day2 = r.sessions[1]
        assert day2["prev_close"] == 100.0, "the gap reference is not the official close"
        assert day2["gap_type"] == "FLAT", "an open equal to the official close is not a gap"

    def test_the_same_open_against_a_different_close_is_a_gap(self, tmp_path: Path) -> None:
        eng = self._two_days(tmp_path, daily_close=100.0, last_bar_close=100.0, day2_open=101.0)
        day2 = eng.run_analysis("TEST", "5min").sessions[1]
        assert day2["gap_type"] == "UP"
        assert day2["gap_pct"] == pytest.approx(1.0)

    def test_the_first_session_has_no_prior_close_and_reports_blank(self, tmp_path: Path) -> None:
        """Blank, never zero: an unknown prior close is not a flat open."""
        eng = self._two_days(tmp_path, daily_close=100.0, last_bar_close=100.0, day2_open=100.0)
        first = eng.run_analysis("TEST", "5min").sessions[0]
        assert first["prev_close"] is None
        assert first["gap_pct"] is None
        assert first["gap_type"] == "FLAT"


class TestReportShape:
    def test_it_refuses_a_symbol_it_has_no_bars_for(self, tmp_path: Path) -> None:
        db = tmp_path / "e.db"
        _write(db, "TEST", "2026-01-05", _flat_session(40), 100.0)
        with pytest.raises(ValueError, match="No 5min bars"):
            PatternEngine(db_path=str(db)).run_analysis("NOSUCH", "5min")

    def test_every_session_carries_the_fields_the_pages_read(self, tmp_path: Path) -> None:
        db = tmp_path / "f.db"
        _write(db, "TEST", "2026-01-05", _flat_session(40), 100.0)
        _write(db, "TEST", "2026-01-06", _flat_session(40, 101.0), 101.0)
        r = PatternEngine(db_path=str(db)).run_analysis("TEST", "5min")
        assert len(r.sessions) == 2
        for s in r.sessions:
            for f in (
                "date",
                "dow",
                "dow_num",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "ret_pct",
                "range_pct",
                "green",
                "hod_time",
                "lod_time",
                "hod_in_45",
                "lod_in_45",
                "prev_close",
                "gap_pct",
                "gap_type",
                "gap_filled",
                "gap_fill_time",
                "gap_fill_1030",
                "touched_vwap",
                "vwap_2s_touch",
                "vwap_2s_revert",
                "ib_class",
                "fvg_count",
                "fvg_retested",
                "fvg_held",
                "orb15_dir",
                "orb15_hit1x",
                "orb15_hit2x",
                "orb15_rev",
                "orb30_dir",
                "orb30_hit1x",
                "orb30_hit2x",
                "orb30_rev",
            ):
                assert f in s, f"sessions are missing {f}, which the terminal reads"
            assert "_sizes" not in s, "an internal key leaked into the published record"

    def test_the_headline_rates_are_the_sessions_recomputed(self, tmp_path: Path) -> None:
        """The property that keeps a card from disagreeing with its neighbour:
        every aggregate must be the same statistic over the same list."""
        db = tmp_path / "g.db"
        for i, day in enumerate(["2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08"]):
            bars = _flat_session(40, 100.0 + i)
            bars[5] = _bar(bars[5]["time"], 100.0 + i, 100.0 + i + 2, 100.0 + i - 2, 100.0 + i)
            _write(db, "TEST", day, bars, 100.0 + i)
        r = PatternEngine(db_path=str(db)).run_analysis("TEST", "5min")
        S = r.sessions
        assert len(S) == 4
        hits = sum(1 for s in S if s["hod_in_45"] or s["lod_in_45"])
        assert r.anchor_45m_rate == pytest.approx(round(hits / len(S) * 100, 1))
        rated = [s for s in S if s["ib_class"] is not None]
        if rated:
            trend = sum(1 for s in rated if s["ib_class"] == "trend")
            assert r.ib_trend_rate == pytest.approx(round(trend / len(rated) * 100, 1))

    def test_the_lookback_trims_the_oldest_sessions(self, tmp_path: Path) -> None:
        db = tmp_path / "h.db"
        for i, day in enumerate(["2026-01-05", "2026-01-06", "2026-01-07"]):
            _write(db, "TEST", day, _flat_session(40, 100.0 + i), 100.0 + i)
        r = PatternEngine(db_path=str(db)).run_analysis("TEST", "5min", days=2)
        assert [s["date"] for s in r.sessions] == ["2026-01-06", "2026-01-07"]

    def test_the_gap_threshold_is_a_caller_decision(self, tmp_path: Path) -> None:
        db = tmp_path / "i.db"
        _write(db, "TEST", "2026-01-05", _flat_session(40, 100.0), 100.0)
        _write(db, "TEST", "2026-01-06", _flat_session(40, 100.2), 100.2)
        eng = PatternEngine(db_path=str(db))
        loose = eng.run_analysis("TEST", "5min", gap_pct_threshold=DEFAULT_GAP_PCT)
        strict = eng.run_analysis("TEST", "5min", gap_pct_threshold=1.0)
        assert loose.sessions[1]["gap_type"] == "UP", "a 0.2% move clears the default"
        assert strict.sessions[1]["gap_type"] == "FLAT", "and does not clear a 1% rule"


def _full_session(price: float = 100.0, *, with_fvg: bool = False) -> list[dict[str, Any]]:
    """A whole 09:30-15:55 session, so the sections that need an afternoon
    (lunch, power hour, prior-day levels) actually run."""
    bars: list[dict[str, Any]] = []
    for i in range(78):
        p = price + (i % 7) * 0.05
        bars.append(_bar(_t(i * 5), p, p + 0.10, p - 0.10, p + 0.02, 1000 + (i % 5) * 400))
    if with_fvg:
        # a three-bar imbalance: bar 20's high is left behind by bar 22's low,
        # then bar 40 trades back down into it and closes above bar 20's high.
        b = bars[20]
        bars[20] = _bar(b["time"], price, price + 0.10, price - 0.10, price)
        bars[21] = _bar(bars[21]["time"], price + 0.5, price + 1.2, price + 0.4, price + 1.0)
        bars[22] = _bar(bars[22]["time"], price + 1.0, price + 1.3, price + 0.9, price + 1.2)
        bars[40] = _bar(bars[40]["time"], price + 1.0, price + 1.1, price + 0.5, price + 1.0)
    return bars


class TestSectionsNeedingAWholeSession:
    def test_a_three_bar_imbalance_is_counted_retested_and_held(self, tmp_path: Path) -> None:
        db = tmp_path / "fvg.db"
        _write(db, "TEST", "2026-01-05", _full_session(100.0, with_fvg=True), 100.0)
        _write(db, "TEST", "2026-01-06", _full_session(100.0, with_fvg=True), 100.0)
        r = PatternEngine(db_path=str(db)).run_analysis("TEST", "5min")
        assert r.fvg_count > 0, "no fair value gap was detected in a session built to contain one"
        assert sum(s["fvg_count"] for s in r.sessions) == r.fvg_count, (
            "the per-session counts and the headline count disagree"
        )
        assert any(s["fvg_retested"] > 0 for s in r.sessions), "the retest never fired"

    def test_a_downward_imbalance_is_counted_too(self, tmp_path: Path) -> None:
        db = tmp_path / "fvgd.db"
        bars = _full_session(100.0)
        bars[20] = _bar(bars[20]["time"], 100.0, 100.1, 99.9, 100.0)
        bars[21] = _bar(bars[21]["time"], 99.0, 99.3, 98.5, 98.8)
        bars[22] = _bar(bars[22]["time"], 98.5, 98.7, 98.3, 98.5)
        bars[40] = _bar(bars[40]["time"], 98.5, 99.2, 98.4, 98.6)
        _write(db, "TEST", "2026-01-05", bars, 100.0)
        r = PatternEngine(db_path=str(db)).run_analysis("TEST", "5min")
        assert r.fvg_count > 0, "a downward imbalance was not counted"

    def test_a_whole_session_drives_every_remaining_section(self, tmp_path: Path) -> None:
        """A smoke test over the sections the pages do not read but the CLI
        prints: they must produce a number rather than raise."""
        db = tmp_path / "full.db"
        for i, day in enumerate(["2026-01-05", "2026-01-06", "2026-01-07"]):
            _write(db, "TEST", day, _full_session(100.0 + i), 100.0 + i)
        r = PatternEngine(db_path=str(db)).run_analysis("TEST", "5min")
        for field in (
            "pdh_test_rate",
            "pdh_reject_rate",
            "pdl_test_rate",
            "pdl_reject_rate",
            "lunch_tight_rate",
            "lunch_clean_pm_rate",
            "rvol_15m_reversal_rate",
            "power_hour_continuation_rate",
        ):
            v = getattr(r, field)
            assert isinstance(v, float) and 0.0 <= v <= 100.0, f"{field} is {v!r}"
        assert r.dow_stats, "no day-of-week rows were produced"
        assert r.options_recommendations, "no strategy rows were produced"
        assert r.to_dict()["sessions"], "to_dict drops the per-session records"


class TestAnchorWindow:
    """09:30 + 45 minutes = 10:15, and bars are labelled by the START of the
    interval — so a bar stamped 10:15 covers 10:15-10:20 and falls OUTSIDE
    the window the card names. The last bar inside is 10:10."""

    def _high_at(self, tmp_path: Path, name: str, high_bar_index: int) -> dict[str, Any]:
        db = tmp_path / name
        bars = _flat_session(40, 100.0)
        b = bars[high_bar_index]
        bars[high_bar_index] = _bar(b["time"], 100.0, 101.0, 100.0, 100.0)
        _write(db, "TEST", "2026-01-05", bars, 100.0)
        return PatternEngine(db_path=str(db)).run_analysis("TEST", "5min").sessions[0]

    def test_a_high_at_10_10_is_inside_the_window(self, tmp_path: Path) -> None:
        s = self._high_at(tmp_path, "a.db", 8)  # 09:30 + 8*5 = 10:10
        assert s["hod_time"] == "10:10"
        assert s["hod_in_45"] is True

    def test_a_high_at_10_15_is_outside_it(self, tmp_path: Path) -> None:
        s = self._high_at(tmp_path, "b.db", 9)  # 09:30 + 9*5 = 10:15
        assert s["hod_time"] == "10:15"
        assert s["hod_in_45"] is False, (
            "a bar stamped 10:15 opens 45 minutes after the bell and runs to "
            "10:20, so counting it makes the window 50 minutes"
        )


class TestGapFillTiming:
    """`gap_fill_1030` is the whole morning-fade claim on the pillar page, and
    the boundary is inclusive: a fill in the bar stamped 10:30 counts."""

    def _fill_at(self, tmp_path: Path, name: str, fill_index: int | None) -> dict[str, Any]:
        db = tmp_path / name
        _write(db, "TEST", "2026-01-05", _flat_session(40, 100.0), 100.0)
        # day two opens a dollar above the official close, then trades back
        # down to touch it in exactly one bar
        bars = [_bar(_t(i * 5), 101.0, 101.0, 101.0, 101.0) for i in range(40)]
        if fill_index is not None:
            b = bars[fill_index]
            bars[fill_index] = _bar(b["time"], 101.0, 101.0, 100.0, 101.0)
        _write(db, "TEST", "2026-01-06", bars, 101.0)
        return PatternEngine(db_path=str(db)).run_analysis("TEST", "5min").sessions[1]

    def test_a_fill_before_1030_is_recorded_with_its_time(self, tmp_path: Path) -> None:
        s = self._fill_at(tmp_path, "a.db", 5)  # 09:55
        assert s["gap_type"] == "UP" and s["gap_filled"] is True
        assert s["gap_fill_time"] == "09:55"
        assert s["gap_fill_1030"] is True

    def test_the_1030_bar_itself_counts(self, tmp_path: Path) -> None:
        s = self._fill_at(tmp_path, "b.db", 12)  # 10:30
        assert s["gap_fill_time"] == "10:30"
        assert s["gap_fill_1030"] is True

    def test_a_fill_after_1030_is_filled_but_not_by_1030(self, tmp_path: Path) -> None:
        s = self._fill_at(tmp_path, "c.db", 15)  # 10:45
        assert s["gap_filled"] is True
        assert s["gap_fill_time"] == "10:45"
        assert s["gap_fill_1030"] is False, "a fill at 10:45 cannot count toward the 10:30 rate"

    def test_a_gap_that_never_fills_records_no_time(self, tmp_path: Path) -> None:
        """Blank, not a time nobody observed."""
        s = self._fill_at(tmp_path, "d.db", None)
        assert s["gap_filled"] is False
        assert s["gap_fill_time"] is None
        assert s["gap_fill_1030"] is False


class TestPartialStore:
    def test_bars_without_a_daily_table_report_no_gap(self, tmp_path: Path) -> None:
        """What a partial harvest looks like. No official close means no gap
        reference, and no gap reference means no gap — not one measured
        against a bar close, which is the defect this replaced."""
        db = tmp_path / "partial.db"
        _write(db, "TEST", "2026-01-05", _flat_session(40, 100.0), 100.0)
        _write(db, "TEST", "2026-01-06", _flat_session(40, 105.0), 105.0)
        conn = sqlite3.connect(db)
        conn.execute("DROP TABLE stock_bars_daily")
        conn.commit()
        conn.close()
        r = PatternEngine(db_path=str(db)).run_analysis("TEST", "5min")
        assert len(r.sessions) == 2
        assert all(s["prev_close"] is None for s in r.sessions)
        assert all(s["gap_type"] == "FLAT" for s in r.sessions), (
            "a 5% open with no known prior close was reported as a gap"
        )
        assert r.gap_fill_eod_rate == 0.0

    def test_the_reported_range_names_sessions_that_exist(self, tmp_path: Path) -> None:
        db = tmp_path / "range.db"
        _write(db, "TEST", "2026-01-05", _flat_session(40, 100.0), 100.0)
        _write(db, "TEST", "2026-01-06", _flat_session(40, 101.0), 101.0)
        r = PatternEngine(db_path=str(db)).run_analysis("TEST", "5min")
        assert r.date_start == r.sessions[0]["date"]
        assert r.date_end == r.sessions[-1]["date"]
