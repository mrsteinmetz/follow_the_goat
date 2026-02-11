#!/usr/bin/env python3
"""
Overnight pump signal simulation (V2)
======================================
Runs the V2 pump model refresh with a configurable lookback window.

V2 uses a GradientBoostingClassifier with walk-forward validation instead of
V1's exhaustive threshold combo search. This script triggers a full model
refresh using a larger lookback window (default 48h) and logs the results.

The model is saved to the pickle cache so the live system picks it up
on next restart or refresh cycle.

Usage:
    cd /root/follow_the_goat
    python3 scripts/overnight_pump_simulation.py
    python3 scripts/overnight_pump_simulation.py --hours 72
"""
import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "000trading"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("overnight_pump_sim")


def main():
    parser = argparse.ArgumentParser(description="Overnight pump signal simulation (V2)")
    parser.add_argument("--hours", type=int, default=48, help="Lookback hours of data (default 48)")
    args = parser.parse_args()

    logger.info("Starting overnight pump V2 simulation")
    logger.info("  Lookback hours: %d", args.hours)

    import pump_signal_logic

    # Override lookback for this run
    original_lookback = pump_signal_logic.LOOKBACK_HOURS
    pump_signal_logic.LOOKBACK_HOURS = args.hours

    try:
        pump_signal_logic.refresh_pump_rules()
    finally:
        pump_signal_logic.LOOKBACK_HOURS = original_lookback

    status = pump_signal_logic.get_pump_status()
    if status.get('has_model'):
        meta = status.get('metadata', {})
        logger.info("Model trained successfully:")
        logger.info("  Features: %d", meta.get('n_features', 0))
        logger.info("  Samples: %d (%d positive)", meta.get('n_samples', 0), meta.get('n_positive', 0))
        logger.info("  Avg precision: %.1f%%", meta.get('avg_precision', 0))
        logger.info("  Min precision: %.1f%%", meta.get('min_precision', 0))
        logger.info("  Avg E[profit]: %.4f%%", meta.get('avg_expected_profit', 0))
        logger.info("  Confidence threshold: %.0f%%", meta.get('confidence_threshold', 0) * 100)
        logger.info("  Cache saved to: %s", pump_signal_logic.MODEL_CACHE_PATH)
        sys.exit(0)
    else:
        logger.error("Model training did not produce a valid model")
        sys.exit(1)


if __name__ == "__main__":
    main()
