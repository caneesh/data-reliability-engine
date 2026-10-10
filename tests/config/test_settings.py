"""Settings resolve defaults.yaml, then the feed, then the dataset (spec section 3)."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from hcsc.datalake.dre.config.loader import load, resolve_settings
from hcsc.datalake.dre.config.models import SettingsOverride
from tests.config.conftest import edit

FEED = "feeds/example_realtime.yaml"
CURATED = "datasets/example_curated_enrollment.yaml"
GOLD = "datasets/gold_member_coverage.yaml"


def test_lower_level_wins() -> None:
    top = SettingsOverride(sla_hours=8, email_sample_keys=False, min_rows_per_load=1,
                           volume_tolerance_pct=50, compute_budget_minutes=8, full_sweep_day="SUNDAY",
                           settle_minutes=15, initial_lookback_hours=24, max_window_hours=72)
    mid = SettingsOverride(volume_tolerance_pct=30, sla_hours=4)
    low = SettingsOverride(volume_tolerance_pct=20)
    resolved = resolve_settings(top, mid, low)
    assert (resolved.volume_tolerance_pct, resolved.sla_hours, resolved.min_rows_per_load) == (20, 4, 1)


def test_null_means_inherit() -> None:
    top = SettingsOverride(sla_hours=8, email_sample_keys=False, min_rows_per_load=1,
                           volume_tolerance_pct=50, compute_budget_minutes=8, full_sweep_day="SUNDAY",
                           settle_minutes=15, initial_lookback_hours=24, max_window_hours=72)
    assert resolve_settings(top, SettingsOverride(sla_hours=None)).sla_hours == 8


def test_incomplete_settings_raise() -> None:
    with pytest.raises(ValidationError):
        resolve_settings(SettingsOverride(sla_hours=8))


def test_resolution_through_feed_and_dataset(conf: Path) -> None:
    edit(conf, "defaults.yaml", "volume_tolerance_pct: 50", "volume_tolerance_pct: 60")
    edit(conf, FEED, "email_sample_keys: false", "email_sample_keys: false\nvolume_tolerance_pct: 30")
    edit(conf, GOLD, "volume_tolerance_pct: 50", "volume_tolerance_pct: 20")
    config, errors = load(conf)
    assert errors == []
    assert config.dataset_settings("example_curated_enrollment").volume_tolerance_pct == 30  # from the feed
    assert config.dataset_settings("gold_member_coverage").volume_tolerance_pct == 20  # dataset wins
    assert config.feed_settings("example_realtime").volume_tolerance_pct == 30


def test_spec_defaults_apply_when_defaults_file_omits_them(conf: Path) -> None:
    for line in ("email_sample_keys: false\n", "volume_tolerance_pct: 50\n", "full_sweep_day: SUNDAY\n"):
        edit(conf, "defaults.yaml", line, "")
    edit(conf, FEED, "email_sample_keys: false\n", "")
    edit(conf, GOLD, "volume_tolerance_pct: 50\n", "")
    config, errors = load(conf)
    assert errors == []
    s = config.dataset_settings("gold_member_coverage")
    assert (s.email_sample_keys, s.volume_tolerance_pct, s.full_sweep_day) == (False, 50, "SUNDAY")


def test_table_wide_dataset_takes_settings_from_defaults(conf: Path) -> None:
    edit(conf, FEED, "email_sample_keys: false", "email_sample_keys: false\nvolume_tolerance_pct: 30")
    config, errors = load(conf)
    assert errors == []
    assert config.feed_of("gold_member_coverage_all") is None
    assert config.dataset_settings("gold_member_coverage_all").volume_tolerance_pct == 50
    assert config.dataset_settings("example_curated_enrollment").volume_tolerance_pct == 30
