#!/usr/bin/env python3
"""
Pump Signal V2 — 24-Hour Backtest Validator
============================================
Loads the last 24h of trail data, trains the V2 model on the first ~55% of
data, then simulates signal-by-signal on the remaining ~45% (the walk-forward
test windows). For each fired signal, reports:

  - Timestamp, entry price, model confidence
  - Actual price path over the next 10 minutes (minute-by-minute returns)
  - Whether price actually climbed (clean_pump label)
  - Max gain, max drawdown, time to peak

Summary stats at the end:
  - Total signals fired, true positives, false positives
  - Precision, avg gain on TPs, avg loss on FPs
  - Overall expected profit per trade

Usage:
    cd /root/follow_the_goat
    python3 scripts/validate_pump_v2_24h.py
    python3 scripts/validate_pump_v2_24h.py --hours 48
    python3 scripts/validate_pump_v2_24h.py --hours 24 --confidence 0.65
"""
import argparse
import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "000trading"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("validate_pump_v2")


def run_validation(hours: int = 24, confidence: float = 0.70):
    """Run the full 24h backtest validation."""
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.metrics import precision_score

    # Import V2 internals
    import pump_signal_logic as psl

    logger.info("=" * 80)
    logger.info("PUMP SIGNAL V2 — 24-HOUR BACKTEST VALIDATION")
    logger.info("=" * 80)
    logger.info(f"  Lookback: {hours}h | Confidence threshold: {confidence:.0%}")
    logger.info("")

    # ─── Step 1: Load and label data ──────────────────────────────────────
    logger.info("Step 1: Loading and labeling trail data...")
    t0 = time.time()
    df = psl._load_and_label_data(lookback_hours=hours)
    if df is None or len(df) == 0:
        logger.error("No data available for the specified lookback period.")
        return False

    load_time = time.time() - t0
    logger.info(f"  Loaded {len(df):,} rows in {load_time:.1f}s")
    logger.info("")

    # ─── Step 2: Prepare features ────────────────────────────────────────
    logger.info("Step 2: Preparing features...")
    analysis_df = df[df['label'].isin(['clean_pump', 'no_pump'])].copy()
    analysis_df['target'] = (analysis_df['label'] == 'clean_pump').astype(int)

    n_total = len(analysis_df)
    n_pos = int(analysis_df['target'].sum())
    n_neg = n_total - n_pos
    logger.info(f"  Total samples: {n_total:,} ({n_pos} clean_pump, {n_neg} no_pump)")

    if n_pos < 10:
        logger.error(f"  Only {n_pos} clean_pump samples — insufficient for validation.")
        return False

    base_cols = psl._get_base_feature_columns(analysis_df)
    analysis_df, feature_cols = psl._engineer_features(analysis_df, base_cols)

    # Drop high-NaN columns
    feature_cols = [c for c in feature_cols if c in analysis_df.columns and analysis_df[c].isna().mean() < 0.5]
    logger.info(f"  Usable features: {len(feature_cols)}")
    logger.info("")

    # ─── Step 3: Time-ordered split (train 55%, test 45%) ────────────────
    logger.info("Step 3: Splitting data chronologically...")
    analysis_df = analysis_df.sort_values('followed_at').reset_index(drop=True)
    n = len(analysis_df)
    train_end = int(n * 0.55)

    df_train = analysis_df.iloc[:train_end]
    df_test = analysis_df.iloc[train_end:]

    logger.info(f"  Train: {len(df_train):,} rows (first 55%)")
    logger.info(f"  Test:  {len(df_test):,} rows (last 45%)")
    logger.info(f"  Train positives: {int(df_train['target'].sum())}")
    logger.info(f"  Test positives:  {int(df_test['target'].sum())}")
    logger.info("")

    if df_train['target'].sum() < 10:
        logger.error("Not enough positive samples in training set.")
        return False

    # ─── Step 4: Train model ─────────────────────────────────────────────
    logger.info("Step 4: Training GBM model...")
    t0 = time.time()
    model = GradientBoostingClassifier(
        n_estimators=150, max_depth=3, learning_rate=0.05,
        subsample=0.7, max_features=0.7,
        min_samples_leaf=10, min_samples_split=20,
        random_state=42,
    )
    X_train = df_train[feature_cols].fillna(0)
    y_train = df_train['target']
    model.fit(X_train, y_train)
    train_time = time.time() - t0
    logger.info(f"  Model trained in {train_time:.1f}s")

    # Feature importance
    importances = pd.Series(model.feature_importances_, index=feature_cols).nlargest(10)
    logger.info("  Top 10 features by importance:")
    for feat, imp in importances.items():
        logger.info(f"    {feat}: {imp:.4f}")
    logger.info("")

    # ─── Step 5: Simulate signals on test set ────────────────────────────
    logger.info("Step 5: Simulating signals on test data...")
    X_test = df_test[feature_cols].fillna(0)
    probas = model.predict_proba(X_test)[:, 1]
    df_test = df_test.copy()
    df_test['confidence'] = probas

    # Apply multi-timeframe trend gate
    gate_results = []
    for idx, row in df_test.iterrows():
        row_dict = row.to_dict()
        gate_ok, gate_desc = psl._check_multi_timeframe_trend(row_dict)

        # Also check 5m crash gate
        pm_5m = row_dict.get('pm_price_change_5m')
        if pm_5m is not None and float(pm_5m) < psl.CRASH_GATE_5M:
            gate_ok = False
            gate_desc += " [5m crash]"

        gate_results.append({'gate_ok': gate_ok, 'gate_desc': gate_desc})

    gate_df = pd.DataFrame(gate_results, index=df_test.index)
    df_test['gate_ok'] = gate_df['gate_ok']
    df_test['gate_desc'] = gate_df['gate_desc']

    # Signal fires when: gate passes AND confidence >= threshold
    df_test['signal'] = df_test['gate_ok'] & (df_test['confidence'] >= confidence)

    signals = df_test[df_test['signal']].copy()
    n_signals = len(signals)
    logger.info(f"  Signals fired: {n_signals}")
    logger.info(f"  Gate blocked: {int((~df_test['gate_ok']).sum())} / {len(df_test)}")
    logger.info(f"  Low confidence: {int((df_test['gate_ok'] & (df_test['confidence'] < confidence)).sum())}")
    logger.info("")

    if n_signals == 0:
        logger.warning("No signals fired in the test period.")
        logger.info("This means either:")
        logger.info("  - The model is very conservative (confidence threshold too high)")
        logger.info("  - The multi-timeframe gate is blocking most entries")
        logger.info("  - Market conditions didn't produce qualifying setups")
        return True

    # ─── Step 6: Signal-by-signal analysis ───────────────────────────────
    logger.info("=" * 80)
    logger.info("SIGNAL-BY-SIGNAL RESULTS")
    logger.info("=" * 80)
    logger.info("")

    # Collect forward return columns if available
    fwd_cols = [c for c in df_test.columns if c.startswith('fwd_') and c.endswith('m')
                and c not in ('max_fwd', 'min_fwd', 'max_fwd_early', 'min_fwd_imm')]
    # Sort by minute number
    fwd_cols_sorted = sorted(fwd_cols, key=lambda x: int(x.replace('fwd_', '').replace('m', '')))

    true_positives = 0
    false_positives = 0
    tp_gains = []
    fp_losses = []
    all_outcomes = []

    for i, (idx, row) in enumerate(signals.iterrows(), 1):
        is_clean = row['target'] == 1
        conf = row['confidence']
        max_fwd = row.get('max_fwd', None)
        min_fwd = row.get('min_fwd', None)
        ttp = row.get('time_to_peak', None)
        followed_at = row.get('followed_at', 'N/A')
        pm_close = row.get('pm_close_price', None)

        # Build price path string
        path_parts = []
        for fc in fwd_cols_sorted:
            v = row.get(fc)
            if v is not None and not np.isnan(v):
                path_parts.append(f"{v:+.3f}%")
            else:
                path_parts.append("---")
        price_path = " | ".join(path_parts)

        # Track outcomes
        outcome = "TRUE POSITIVE" if is_clean else "FALSE POSITIVE"
        if is_clean:
            true_positives += 1
            if max_fwd is not None:
                tp_gains.append(float(max_fwd))
        else:
            false_positives += 1
            if min_fwd is not None:
                fp_losses.append(float(min_fwd))

        all_outcomes.append({
            'time': str(followed_at),
            'entry_price': float(pm_close) if pm_close is not None else None,
            'confidence': float(conf),
            'is_clean_pump': bool(is_clean),
            'max_gain': float(max_fwd) if max_fwd is not None else None,
            'max_drawdown': float(min_fwd) if min_fwd is not None else None,
            'time_to_peak': float(ttp) if ttp is not None else None,
        })

        # Log signal detail
        marker = "+++" if is_clean else "---"
        logger.info(f"  Signal #{i:3d} [{marker} {outcome}]")
        logger.info(f"    Time: {followed_at}")
        if pm_close is not None:
            logger.info(f"    Entry price: ${pm_close:.4f}")
        logger.info(f"    Confidence: {conf:.1%}")
        logger.info(f"    Gate: {row.get('gate_desc', 'N/A')}")
        if max_fwd is not None:
            logger.info(f"    Max gain: {max_fwd:+.3f}%")
        if min_fwd is not None:
            logger.info(f"    Max drawdown: {min_fwd:+.3f}%")
        if ttp is not None:
            logger.info(f"    Time to peak: {ttp:.0f}m")
        if price_path:
            logger.info(f"    Price path (1-10m): {price_path}")
        logger.info("")

    # ─── Step 7: Summary statistics ──────────────────────────────────────
    logger.info("=" * 80)
    logger.info("SUMMARY")
    logger.info("=" * 80)
    logger.info("")

    precision = true_positives / n_signals * 100 if n_signals > 0 else 0
    avg_tp_gain = float(np.mean(tp_gains)) if tp_gains else 0
    avg_fp_loss = float(np.mean(fp_losses)) if fp_losses else 0
    expected_profit = (precision / 100) * avg_tp_gain - psl.TRADE_COST_PCT

    logger.info(f"  Total signals fired:   {n_signals}")
    logger.info(f"  True positives (TP):   {true_positives}")
    logger.info(f"  False positives (FP):  {false_positives}")
    logger.info(f"  Precision:             {precision:.1f}%")
    logger.info(f"  Avg TP gain:           {avg_tp_gain:+.4f}%")
    logger.info(f"  Avg FP worst dip:      {avg_fp_loss:+.4f}%")
    logger.info(f"  Trade cost:            {psl.TRADE_COST_PCT:.2f}%")
    logger.info(f"  Expected profit/trade: {expected_profit:+.4f}%")
    logger.info("")

    # Confidence breakdown
    if n_signals > 5:
        logger.info("  Confidence breakdown:")
        for lo, hi, label in [(0.70, 0.80, "70-80%"), (0.80, 0.90, "80-90%"), (0.90, 1.01, "90%+")]:
            band = signals[(signals['confidence'] >= lo) & (signals['confidence'] < hi)]
            if len(band) > 0:
                band_tp = int(band['target'].sum())
                band_prec = band_tp / len(band) * 100
                logger.info(f"    {label}: {len(band)} signals, {band_tp} TP, precision={band_prec:.1f}%")
        logger.info("")

    # Gate analysis
    total_test = len(df_test)
    gated_out = int((~df_test['gate_ok']).sum())
    gated_out_tp = int((~df_test['gate_ok'] & (df_test['target'] == 1)).sum())
    gated_out_fp = int((~df_test['gate_ok'] & (df_test['target'] == 0)).sum())
    logger.info("  Multi-timeframe gate analysis:")
    logger.info(f"    Total test rows:        {total_test}")
    logger.info(f"    Blocked by gate:        {gated_out} ({gated_out/total_test*100:.1f}%)")
    logger.info(f"    Blocked clean_pumps:    {gated_out_tp}")
    logger.info(f"    Blocked no_pumps:       {gated_out_fp}")
    if gated_out > 0:
        gate_selectivity = gated_out_fp / gated_out * 100
        logger.info(f"    Gate selectivity:       {gate_selectivity:.1f}% of blocked were bad trades")
    logger.info("")

    # Verdict
    logger.info("  VERDICT:")
    if precision >= 50 and expected_profit > 0:
        logger.info(f"    GOOD — Precision {precision:.1f}% with positive expected profit {expected_profit:+.4f}%")
        logger.info("    The V2 signals align with actual price climbs.")
    elif precision >= 40 and expected_profit > 0:
        logger.info(f"    ACCEPTABLE — Precision {precision:.1f}% with marginal profit {expected_profit:+.4f}%")
        logger.info("    Signals are somewhat aligned but could be tighter.")
    elif n_signals == 0:
        logger.info("    NO SIGNALS — Model was too conservative for this period.")
    else:
        logger.info(f"    NEEDS WORK — Precision {precision:.1f}%, E[profit]={expected_profit:+.4f}%")
        logger.info("    Signals do not reliably match price climbs.")

    logger.info("")
    logger.info("=" * 80)
    return True


def main():
    parser = argparse.ArgumentParser(description="Pump Signal V2 — 24-Hour Backtest Validator")
    parser.add_argument("--hours", type=int, default=24, help="Lookback hours (default 24)")
    parser.add_argument("--confidence", type=float, default=0.70,
                        help="Confidence threshold for signal (default 0.70)")
    args = parser.parse_args()

    success = run_validation(hours=args.hours, confidence=args.confidence)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
