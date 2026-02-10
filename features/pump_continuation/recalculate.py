"""
Pump Continuation Filter Recalculation
=======================================
Lightweight version of the pump continuation analytics that runs every 5 minutes.
Discovers the best filter combination from recent buyins and writes the active
rules to the `pump_continuation_rules` PostgreSQL table.

The pattern_validator reads these rules at validation time to gate trade entries.
"""

import sys
import time
import logging
import warnings
from pathlib import Path
from itertools import combinations
from typing import Dict, List, Any, Optional, Tuple

import numpy as np
import pandas as pd
import duckdb

PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

warnings.filterwarnings("ignore", category=FutureWarning)

logger = logging.getLogger("pump_continuation.recalculate")

# ── Constants ─────────────────────────────────────────────────────────

TRADE_COST_PCT = 0.1
DEFAULT_MIN_GAIN_PCT = 0.2
DEFAULT_HOURS = 24
TRAIN_FRAC = 0.70
MIN_PRECISION = 40.0
MIN_SIGNALS = 15

SKIP_COLUMNS = frozenset([
    'buyin_id', 'trade_id', 'play_id', 'wallet_address', 'followed_at',
    'our_status', 'minute', 'sub_minute', 'interval_idx',
    'potential_gains', 'pat_detected_list', 'pat_swing_trend',
    'is_good', 'label', 'created_at', 'pre_entry_trend',
    'max_fwd_return', 'min_fwd_return',
])

ABSOLUTE_PRICE_COLUMNS = frozenset([
    'pm_open_price', 'pm_close_price', 'pm_high_price', 'pm_low_price', 'pm_avg_price',
    'btc_open_price', 'btc_close_price', 'btc_high_price', 'btc_low_price',
    'eth_open_price', 'eth_close_price', 'eth_high_price', 'eth_low_price',
    'sp_min_price', 'sp_max_price', 'sp_avg_price', 'sp_start_price', 'sp_end_price',
    'sp_price_count',
    'ts_open_price', 'ts_close_price', 'ts_high_price', 'ts_low_price',
    'pre_entry_price_1m_before', 'pre_entry_price_2m_before',
    'pre_entry_price_3m_before', 'pre_entry_price_5m_before',
    'pre_entry_price_10m_before',
    'ob_mid_price', 'ob_total_liquidity', 'ob_bid_total', 'ob_ask_total',
    'tx_vwap', 'tx_total_volume_usd', 'tx_buy_volume_usd', 'tx_sell_volume_usd',
    'tx_delta_divergence', 'tx_cumulative_delta',
    'wh_total_sol_moved', 'wh_inflow_sol', 'wh_outflow_sol',
    'pat_asc_tri_resistance_level', 'pat_asc_tri_support_level',
    'pat_inv_hs_neckline', 'pat_cup_handle_rim',
])


# ── PostgreSQL connection ─────────────────────────────────────────────

def _get_pg_connection_string() -> str:
    from core.config import settings
    pg = settings.postgres
    return f"host={pg.host} port={pg.port} dbname={pg.database} user={pg.user} password={pg.password}"


# ── Helpers ───────────────────────────────────────────────────────────

def _get_section(col: str) -> str:
    for prefix in ['pre_entry_', 'pm_', 'ob_', 'tx_', 'wh_', 'xa_', 'mm_',
                    'pat_', 'mp_', 'sp_', 'ts_', 'btc_', 'eth_']:
        if col.startswith(prefix):
            return prefix.rstrip('_')
    return 'other'


def _get_filterable_columns(df: pd.DataFrame) -> List[str]:
    cols = []
    for col in df.columns:
        if col in SKIP_COLUMNS or col in ABSOLUTE_PRICE_COLUMNS:
            continue
        if col in ('label', 'followed_at', 'buyin_id', 'our_entry_price',
                    'our_status', 'price_cycle', 'potential_gains', 'minute',
                    'sub_minute', 'interval_idx', 'created_at'):
            continue
        if df[col].dtype not in ('float64', 'int64', 'float32', 'int32'):
            continue
        if df[col].isna().mean() >= 0.90:
            continue
        cols.append(col)
    return sorted(cols)


# ── Core Analysis Functions ───────────────────────────────────────────

def _load_data(con: duckdb.DuckDBPyConnection, hours: int) -> int:
    """Load buyins + trail into DuckDB. Returns merged row count."""
    pg_conn = _get_pg_connection_string()
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")
    con.execute(f"ATTACH '{pg_conn}' AS pg (TYPE POSTGRES, READ_ONLY)")

    con.execute(f"""
        CREATE TABLE buyins AS
        SELECT id AS buyin_id, followed_at, our_entry_price, potential_gains, our_status, price_cycle
        FROM pg.follow_the_goat_buyins
        WHERE potential_gains IS NOT NULL AND our_entry_price > 0
          AND followed_at >= (NOW()::TIMESTAMP - INTERVAL '{hours} hours')
        ORDER BY followed_at
    """)

    con.execute("""
        CREATE TABLE trail AS
        SELECT * FROM pg.buyin_trail_minutes
        WHERE buyin_id IN (SELECT buyin_id FROM buyins)
          AND minute = 0 AND COALESCE(sub_minute, 0) = 0
    """)

    con.execute("""
        CREATE TABLE merged AS
        SELECT b.buyin_id, b.followed_at, b.our_entry_price, b.potential_gains, b.our_status, b.price_cycle, t.*
        FROM buyins b INNER JOIN trail t ON t.buyin_id = b.buyin_id
    """)

    n_merged = con.execute("SELECT COUNT(*) FROM merged").fetchone()[0]
    con.execute("DETACH pg")
    return n_merged


def _label(con: duckdb.DuckDBPyConnection, min_gain_pct: float) -> pd.DataFrame:
    """Label buyins: pump_continuation / pump_reversal / no_pump."""
    return con.execute(f"""
        SELECT *,
            CASE
                WHEN pre_entry_change_1m > 0 AND potential_gains >= {min_gain_pct} THEN 'pump_continuation'
                WHEN pre_entry_change_1m > 0 AND potential_gains <  {min_gain_pct} THEN 'pump_reversal'
                ELSE 'no_pump'
            END AS label
        FROM merged ORDER BY followed_at
    """).fetchdf()


def _rank_filters(df: pd.DataFrame, columns: List[str]) -> List[Dict[str, Any]]:
    """Rank individual filters by expected profit."""
    is_cont = (df['label'] == 'pump_continuation').values
    is_rev = (df['label'] == 'pump_reversal').values
    results = []

    for col in columns:
        cont_vals = df.loc[is_cont, col].dropna()
        rev_vals = df.loc[is_rev, col].dropna()
        if len(cont_vals) < 20 or len(rev_vals) < 20:
            continue

        n_c, n_r = len(cont_vals), len(rev_vals)
        pooled_std = np.sqrt(
            ((n_c - 1) * cont_vals.std()**2 + (n_r - 1) * rev_vals.std()**2) / (n_c + n_r - 2)
        )
        if pooled_std == 0:
            continue
        cohens_d = abs(cont_vals.mean() - rev_vals.mean()) / pooled_std
        if cohens_d < 0.02:
            continue

        all_vals = pd.concat([cont_vals, rev_vals])
        thresholds = np.quantile(all_vals.dropna(), np.linspace(0.05, 0.95, 40))

        best_j, best_cut, best_dir = -1.0, None, None
        for cut in thresholds:
            j_above = float((cont_vals >= cut).mean() + (rev_vals < cut).mean() - 1)
            j_below = float((cont_vals <= cut).mean() + (rev_vals > cut).mean() - 1)
            if j_above > best_j:
                best_j, best_cut, best_dir = j_above, cut, 'above'
            if j_below > best_j:
                best_j, best_cut, best_dir = j_below, cut, 'below'

        if best_cut is None or best_j <= 0:
            continue

        from_val = float(best_cut) if best_dir == 'above' else float(cont_vals.quantile(0.02))
        to_val = float(cont_vals.quantile(0.98)) if best_dir == 'above' else float(best_cut)
        if from_val >= to_val:
            continue

        vals = df[col].values
        mask_pass = (vals >= from_val) & (vals <= to_val) & ~np.isnan(vals)
        cont_pass = int((is_cont & mask_pass).sum())
        rev_pass = int((is_rev & mask_pass).sum())
        total_pass = cont_pass + rev_pass
        if total_pass < 10:
            continue

        precision = cont_pass / total_pass * 100
        pass_cont_mask = is_cont & mask_pass
        avg_gain = float(df.loc[pass_cont_mask, 'potential_gains'].mean()) if pass_cont_mask.sum() > 0 else 0
        expected_profit = (precision / 100) * avg_gain - TRADE_COST_PCT

        results.append({
            'column': col, 'section': _get_section(col),
            'from': round(from_val, 8), 'to': round(to_val, 8),
            'cohens_d': round(cohens_d, 4), 'youdens_j': round(best_j, 4),
            'precision': round(precision, 2), 'n_signals': total_pass,
            'n_cont_pass': cont_pass, 'avg_gain': round(avg_gain, 4),
            'expected_profit': round(expected_profit, 4),
        })

    results.sort(key=lambda x: x['expected_profit'], reverse=True)
    return results


def _find_combos(
    df_train: pd.DataFrame,
    df_test: pd.DataFrame,
    ranked: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Find best filter combinations validated on test set."""
    if len(ranked) < 2:
        return []

    is_cont_tr = (df_train['label'] == 'pump_continuation').values
    is_rev_tr = (df_train['label'] == 'pump_reversal').values
    gains_tr = df_train['potential_gains'].values

    is_cont_ts = (df_test['label'] == 'pump_continuation').values
    is_rev_ts = (df_test['label'] == 'pump_reversal').values
    gains_ts = df_test['potential_gains'].values

    base_prec = is_cont_tr.sum() / max(is_cont_tr.sum() + is_rev_tr.sum(), 1) * 100
    base_gain = float(np.nanmean(gains_tr[is_cont_tr])) if is_cont_tr.sum() > 0 else 0
    base_profit = (base_prec / 100) * base_gain - TRADE_COST_PCT

    def _masks(df, feats):
        m = {}
        for f in feats:
            c = f['column']
            if c not in df.columns:
                continue
            v = df[c].values
            m[c] = (v >= f['from']) & (v <= f['to']) & ~np.isnan(v)
        return m

    masks_tr = _masks(df_train, ranked)
    masks_ts = _masks(df_test, ranked)

    def _score(cols, masks, is_c, is_r, g):
        combined = masks[cols[0]].copy()
        for c in cols[1:]:
            combined &= masks[c]
        cp = int((is_c & combined).sum())
        tp = cp + int((is_r & combined).sum())
        if tp < MIN_SIGNALS:
            return None
        prec = cp / tp * 100
        ag = float(np.nanmean(g[is_c & combined])) if (is_c & combined).sum() > 0 else 0
        ep = (prec / 100) * ag - TRADE_COST_PCT
        return {'precision': round(prec, 2), 'n_signals': tp, 'n_cont_pass': cp,
                'avg_gain': round(ag, 4), 'expected_profit': round(ep, 4)}

    results = []

    def _test(cols):
        if not all(c in masks_tr and c in masks_ts for c in cols):
            return
        tr = _score(cols, masks_tr, is_cont_tr, is_rev_tr, gains_tr)
        if not tr or tr['expected_profit'] <= base_profit or tr['n_cont_pass'] < 5:
            return
        ts = _score(cols, masks_ts, is_cont_ts, is_rev_ts, gains_ts)
        if not ts or ts['expected_profit'] <= 0 or ts['precision'] < MIN_PRECISION:
            return
        results.append({
            'columns': cols,
            'train_precision': tr['precision'], 'train_expected_profit': tr['expected_profit'],
            'test_precision': ts['precision'], 'test_expected_profit': ts['expected_profit'],
            'test_n_signals': ts['n_signals'], 'test_avg_gain': ts['avg_gain'],
            'overfit_delta': round(tr['expected_profit'] - ts['expected_profit'], 4),
        })

    n = len(ranked)
    top30 = [r['column'] for r in ranked[:min(30, n)]]
    top20 = [r['column'] for r in ranked[:min(20, n)]]
    top15 = [r['column'] for r in ranked[:min(15, n)]]

    for combo in combinations(top30, 2):
        _test(combo)
    for combo in combinations(top20, 3):
        _test(combo)
    if len(top15) >= 4:
        for combo in combinations(top15, 4):
            _test(combo)

    # Greedy forward selection
    best_cols: List[str] = []
    current_profit = base_profit
    for _ in range(min(8, len(top30))):
        best_add, best_p = None, current_profit
        for col in top30:
            if col in best_cols:
                continue
            cand = tuple(sorted(best_cols + [col]))
            if not all(c in masks_tr for c in cand):
                continue
            m = _score(cand, masks_tr, is_cont_tr, is_rev_tr, gains_tr)
            if m and m['n_cont_pass'] >= 5 and m['expected_profit'] > best_p:
                best_p, best_add = m['expected_profit'], col
        if best_add is None:
            break
        best_cols.append(best_add)
        current_profit = best_p
        _test(tuple(sorted(best_cols)))

    results.sort(key=lambda x: x['test_expected_profit'], reverse=True)
    return results


def _check_stability(
    df: pd.DataFrame,
    ranked: List[Dict[str, Any]],
    combo: Dict[str, Any],
) -> bool:
    """Check if a combo is temporally stable across 4-hour windows."""
    climbing = df[df['label'].isin(['pump_continuation', 'pump_reversal'])].copy()
    if len(climbing) < 100 or 'followed_at' not in climbing.columns:
        return False

    ts_min, ts_max = climbing['followed_at'].min(), climbing['followed_at'].max()
    total_h = (ts_max - ts_min).total_seconds() / 3600
    n_win = max(2, int(total_h / 4))
    win_h = total_h / n_win

    feat_lookup = {r['column']: r for r in ranked}
    valid_precs = []

    for i in range(n_win):
        w_start = ts_min + pd.Timedelta(hours=i * win_h)
        w_end = ts_min + pd.Timedelta(hours=(i + 1) * win_h)
        w_df = climbing[(climbing['followed_at'] >= w_start) & (climbing['followed_at'] < w_end)]
        if len(w_df) < 20:
            continue

        is_cont = (w_df['label'] == 'pump_continuation').values
        combined = np.ones(len(w_df), dtype=bool)
        ok = True
        for col in combo['columns']:
            feat = feat_lookup.get(col)
            if not feat or col not in w_df.columns:
                ok = False
                break
            v = w_df[col].values
            combined &= (v >= feat['from']) & (v <= feat['to']) & ~np.isnan(v)

        if not ok:
            continue
        tp = int(combined.sum())
        if tp > 0:
            valid_precs.append(int((is_cont & combined).sum()) / tp * 100)

    if len(valid_precs) < 2:
        return False
    return min(valid_precs) >= 25.0 and np.std(valid_precs) < 20.0


# ── Write Rules to PostgreSQL ─────────────────────────────────────────

def _write_rules(
    ranked: List[Dict[str, Any]],
    combo: Dict[str, Any],
    is_stable: bool,
):
    """Write the selected combo's filter rules to pump_continuation_rules table."""
    from core.database import get_postgres

    feat_lookup = {r['column']: r for r in ranked}
    rules = []
    for col in combo['columns']:
        feat = feat_lookup.get(col)
        if not feat:
            continue
        rules.append({
            'column_name': col,
            'section': feat['section'],
            'from_value': feat['from'],
            'to_value': feat['to'],
            'precision_pct': combo['test_precision'],
            'expected_profit': combo['test_expected_profit'],
            'test_n_signals': combo['test_n_signals'],
            'is_stable': is_stable,
        })

    if not rules:
        logger.warning("No rules to write -- skipping DB update")
        return

    with get_postgres() as conn:
        with conn.cursor() as cursor:
            # Clear existing rules and insert new ones in one transaction
            cursor.execute("DELETE FROM pump_continuation_rules")
            for r in rules:
                cursor.execute("""
                    INSERT INTO pump_continuation_rules
                        (column_name, section, from_value, to_value, precision_pct,
                         expected_profit, test_n_signals, is_stable, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """, [
                    r['column_name'], r['section'], r['from_value'], r['to_value'],
                    r['precision_pct'], r['expected_profit'], r['test_n_signals'],
                    r['is_stable'],
                ])
        conn.commit()

    logger.info(f"Wrote {len(rules)} pump continuation rules to DB "
                f"(prec={combo['test_precision']:.1f}%, stable={is_stable}): "
                f"{[r['column_name'] for r in rules]}")


# ── Main Entry Point ──────────────────────────────────────────────────

def recalculate(
    hours: int = DEFAULT_HOURS,
    min_gain_pct: float = DEFAULT_MIN_GAIN_PCT,
) -> Dict[str, Any]:
    """
    Run lightweight pump continuation analysis and write best rules to DB.
    Returns summary dict.
    """
    t0 = time.time()
    logger.info(f"Recalculating pump continuation filters ({hours}h, min_gain={min_gain_pct}%)...")

    # Load data
    con = duckdb.connect(":memory:")
    try:
        n_merged = _load_data(con, hours)
        if n_merged < 100:
            logger.warning(f"Only {n_merged} merged rows -- skipping recalculation")
            return {'status': 'skipped', 'reason': 'insufficient_data', 'n_merged': n_merged}

        df = _label(con, min_gain_pct)
    finally:
        con.close()

    climbing = df[df['label'].isin(['pump_continuation', 'pump_reversal'])].copy()
    n_climbing = len(climbing)
    n_cont = int((climbing['label'] == 'pump_continuation').sum())

    if n_climbing < 50:
        logger.warning(f"Only {n_climbing} rising buyins -- skipping")
        return {'status': 'skipped', 'reason': 'insufficient_rising', 'n_climbing': n_climbing}

    base_prec = n_cont / n_climbing * 100
    logger.info(f"  {n_climbing} rising buyins, baseline precision {base_prec:.1f}%")

    # Rank individual filters
    columns = _get_filterable_columns(climbing)
    ranked = _rank_filters(climbing, columns)
    logger.info(f"  {len(ranked)} filters ranked, "
                f"{sum(1 for r in ranked if r['expected_profit'] > 0)} profitable")

    if len(ranked) < 2:
        logger.warning("Not enough ranked filters -- skipping")
        return {'status': 'skipped', 'reason': 'insufficient_filters'}

    # Train/test split
    climbing = climbing.sort_values('followed_at').copy()
    n_train = int(len(climbing) * TRAIN_FRAC)
    df_train = climbing.iloc[:n_train].copy()
    df_test = climbing.iloc[n_train:].copy()

    if len(df_test) < 20:
        logger.warning("Test set too small -- skipping")
        return {'status': 'skipped', 'reason': 'test_too_small'}

    # Find combinations
    combos = _find_combos(df_train, df_test, ranked)
    logger.info(f"  {len(combos)} profitable combinations found")

    if not combos:
        logger.warning("No profitable combos found -- keeping existing rules")
        return {'status': 'no_combos', 'n_ranked': len(ranked)}

    # Pick best: prefer stable, otherwise best overall
    best_combo = None
    best_stable = False

    for combo in combos[:10]:
        stable = _check_stability(df, ranked, combo)
        if stable:
            best_combo = combo
            best_stable = True
            break

    if best_combo is None:
        best_combo = combos[0]
        best_stable = False

    # Write to DB
    _write_rules(ranked, best_combo, best_stable)

    elapsed = time.time() - t0
    result = {
        'status': 'ok',
        'elapsed_s': round(elapsed, 1),
        'n_climbing': n_climbing,
        'base_precision': round(base_prec, 1),
        'n_ranked': len(ranked),
        'n_combos': len(combos),
        'best_test_precision': best_combo['test_precision'],
        'best_test_profit': best_combo['test_expected_profit'],
        'best_test_signals': best_combo['test_n_signals'],
        'is_stable': best_stable,
        'columns': list(best_combo['columns']),
    }
    logger.info(f"  Done in {elapsed:.1f}s: prec={best_combo['test_precision']:.1f}%, "
                f"profit={best_combo['test_expected_profit']:+.4f}%, stable={best_stable}")
    return result
