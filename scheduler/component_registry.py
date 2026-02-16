"""
Scheduler Component Registry
===========================
Defines the canonical list of components that should appear in the dashboard.

This is intentionally lightweight (no imports of master.py/master2.py) so it can
be used by both the website API and per-component runner without side effects.
"""

from __future__ import annotations

from scheduler.control import ComponentDef, ensure_components_registered


DEFAULT_COMPONENT_DEFS = [
    # master.py jobs
    ComponentDef("fetch_jupiter_prices", "job", "master", "Fetch Jupiter prices (every 1s)", expected_interval_ms=1000),
    ComponentDef("sync_trades_from_webhook", "job", "master", "Sync trades from webhook (every 1s)", expected_interval_ms=1000),
    ComponentDef("process_price_cycles", "job", "master", "Process price cycles (every 2s)", expected_interval_ms=2000),
    # master.py services/streams
    ComponentDef("webhook_server", "service", "master", "FastAPI Webhook Server (port 8001)", expected_interval_ms=5000),
    ComponentDef("php_server", "service", "master", "PHP Built-in Server (port 8000)", expected_interval_ms=5000),
    ComponentDef("binance_stream", "stream", "master", "Binance Order Book Stream (SOLUSDT)", expected_interval_ms=5000),
    # master2.py jobs
    ComponentDef("follow_the_goat", "job", "master2", "Follow The Goat - Wallet Tracker (every 1s)", expected_interval_ms=1000),
    ComponentDef("trailing_stop_seller", "job", "master2", "Trailing Stop Seller (every 1s)", expected_interval_ms=1000),
    ComponentDef("train_validator", "job", "master2", "Train Validator (every 5s)", expected_interval_ms=5000),
    ComponentDef("update_potential_gains", "job", "master2", "Update Potential Gains (every 15s)", expected_interval_ms=15000),
    ComponentDef("create_new_patterns", "job", "master2", "Create New Patterns (every 10 min)", expected_interval_ms=600000),
    ComponentDef("create_profiles", "job", "master2", "Create Wallet Profiles (every 30s)", expected_interval_ms=30000),
    ComponentDef("archive_old_data", "job", "master2", "Archive Old Data (hourly)", expected_interval_ms=3600000),
    ComponentDef("restart_quicknode_streams", "job", "master2", "Monitor QuickNode Stream Latency (every 15s)", expected_interval_ms=15000),
    ComponentDef("recalculate_pump_filters", "job", "master2", "Recalculate pump continuation filters (every 5 min)", expected_interval_ms=300000),
    ComponentDef("refresh_pump_model", "job", "master2", "Refresh Pump Signal V2 Model (every 15 min)", expected_interval_ms=900000),
    ComponentDef("export_job_status", "job", "master2", "Export Job Status (every 5s)", expected_interval_ms=5000),
    # master2.py service
    ComponentDef("local_api_5052", "service", "master2", "FastAPI Local API (port 5052)", expected_interval_ms=5000),
    # standalone jobs
    ComponentDef("sde_overnight_sweep", "job", "standalone", "Signal Discovery Engine - Overnight Sweep (every 12h)", expected_interval_ms=43200000),
]


def ensure_default_components_registered() -> None:
    ensure_components_registered(DEFAULT_COMPONENT_DEFS)

