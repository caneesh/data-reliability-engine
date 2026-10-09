"""Resolved settings for tests that build check contexts by hand."""

from hcsc.datalake.dre.config.models import Settings

SETTINGS = Settings(sla_hours=8, email_sample_keys=False, min_rows_per_load=1, volume_tolerance_pct=50,
                    compute_budget_minutes=8, full_sweep_day="SUNDAY", settle_minutes=15,
                    initial_lookback_hours=24)
