"""
System Health Email Report
==========================
Queries PostgreSQL for all system metrics and renders a complete HTML report.
Includes an embedded matplotlib chart showing recent trade entries and exits.

Usage:
    from features.email_report.report import generate_html
    html = generate_html()
"""

from __future__ import annotations

import sys
import base64
import io
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres

logger = logging.getLogger("email_report")

# ---------------------------------------------------------------------------
# Data fetchers
# ---------------------------------------------------------------------------

def _fetch_transaction_stats() -> dict:
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    COUNT(*)                                                      AS total,
                    COUNT(*) FILTER (WHERE direction = 'buy')                     AS buys,
                    COUNT(*) FILTER (WHERE direction = 'sell')                    AS sells,
                    COALESCE(SUM(stablecoin_amount) FILTER (WHERE direction = 'buy'),  0) AS buy_volume_usd,
                    COALESCE(SUM(stablecoin_amount) FILTER (WHERE direction = 'sell'), 0) AS sell_volume_usd,
                    COALESCE(SUM(sol_amount) FILTER (WHERE direction = 'buy'),  0)  AS buy_volume_sol,
                    COALESCE(SUM(sol_amount) FILTER (WHERE direction = 'sell'), 0)  AS sell_volume_sol,
                    COUNT(DISTINCT wallet_address)                                AS unique_wallets,
                    MAX(trade_timestamp)                                          AS latest_trade
                FROM sol_stablecoin_trades
                WHERE trade_timestamp >= NOW() - INTERVAL '24 hours'
            """)
            return cursor.fetchone() or {}


def _fetch_whale_stats() -> dict:
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    COUNT(*)                                                              AS total,
                    COUNT(*) FILTER (WHERE direction = 'buy')                             AS buys,
                    COUNT(*) FILTER (WHERE direction = 'sell')                            AS sells,
                    COALESCE(SUM(stablecoin_amount) FILTER (WHERE direction = 'buy'),  0) AS buy_volume_usd,
                    COALESCE(SUM(stablecoin_amount) FILTER (WHERE direction = 'sell'), 0) AS sell_volume_usd,
                    COALESCE(SUM(sol_amount) FILTER (WHERE direction = 'buy'),  0)        AS buy_volume_sol,
                    COALESCE(SUM(sol_amount) FILTER (WHERE direction = 'sell'), 0)        AS sell_volume_sol,
                    COUNT(DISTINCT wallet_address)                                        AS unique_wallets
                FROM sol_stablecoin_trades
                WHERE trade_timestamp >= NOW() - INTERVAL '24 hours'
                  AND stablecoin_amount >= 10000
            """)
            stats = cursor.fetchone() or {}

            # Top 5 whale wallets by volume
            cursor.execute("""
                SELECT
                    wallet_address,
                    COUNT(*) AS trade_count,
                    SUM(stablecoin_amount) AS total_volume_usd,
                    SUM(stablecoin_amount) FILTER (WHERE direction = 'buy')  AS buy_vol,
                    SUM(stablecoin_amount) FILTER (WHERE direction = 'sell') AS sell_vol
                FROM sol_stablecoin_trades
                WHERE trade_timestamp >= NOW() - INTERVAL '24 hours'
                  AND stablecoin_amount >= 10000
                GROUP BY wallet_address
                ORDER BY total_volume_usd DESC
                LIMIT 5
            """)
            stats['top_wallets'] = cursor.fetchall()
            return stats


def _fetch_order_book() -> dict:
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            # Latest snapshot
            cursor.execute("""
                SELECT
                    mid_price, spread_bps,
                    bid_liquidity AS total_bid_volume,
                    ask_liquidity AS total_ask_volume,
                    CASE WHEN ask_liquidity > 0 THEN bid_liquidity / ask_liquidity ELSE NULL END AS bid_ask_ratio,
                    volume_imbalance,
                    depth_imbalance_ratio AS depth_imbalance,
                    microprice, vwap,
                    timestamp
                FROM order_book_features
                ORDER BY timestamp DESC
                LIMIT 1
            """)
            latest = cursor.fetchone() or {}

            # 1h averages
            cursor.execute("""
                SELECT
                    AVG(mid_price)               AS avg_mid_price,
                    AVG(spread_bps)              AS avg_spread_bps,
                    AVG(volume_imbalance)        AS avg_volume_imbalance,
                    AVG(depth_imbalance_ratio)   AS avg_depth_imbalance,
                    AVG(CASE WHEN ask_liquidity > 0 THEN bid_liquidity / ask_liquidity ELSE NULL END) AS avg_bid_ask_ratio
                FROM order_book_features
                WHERE timestamp >= NOW() - INTERVAL '1 hour'
            """)
            avg = cursor.fetchone() or {}
            latest['hourly_avg'] = avg
            return latest


def _fetch_trade_performance() -> dict:
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    COUNT(*)                                                          AS total_sold,
                    COUNT(*) FILTER (WHERE our_profit_loss > 0)                       AS wins,
                    COUNT(*) FILTER (WHERE our_profit_loss <= 0)                      AS losses,
                    COALESCE(SUM(our_profit_loss), 0)                                 AS total_pnl,
                    COALESCE(AVG(our_profit_loss) FILTER (WHERE our_profit_loss > 0), 0) AS avg_win,
                    COALESCE(AVG(our_profit_loss) FILTER (WHERE our_profit_loss <= 0), 0) AS avg_loss,
                    COALESCE(MAX(our_profit_loss), 0)                                 AS best_trade,
                    COALESCE(MIN(our_profit_loss), 0)                                 AS worst_trade,
                    COALESCE(AVG(potential_gains) FILTER (WHERE potential_gains IS NOT NULL), 0) AS avg_potential_gains
                FROM follow_the_goat_buyins
                WHERE our_status = 'sold'
                  AND our_exit_timestamp >= NOW() - INTERVAL '24 hours'
            """)
            stats = cursor.fetchone() or {}

            # Recent 20 trades for the chart
            cursor.execute("""
                SELECT
                    id,
                    our_entry_price,
                    our_exit_price,
                    our_profit_loss,
                    followed_at,
                    our_exit_timestamp,
                    potential_gains
                FROM follow_the_goat_buyins
                WHERE our_status = 'sold'
                  AND our_exit_timestamp >= NOW() - INTERVAL '24 hours'
                  AND our_entry_price IS NOT NULL
                  AND our_exit_price IS NOT NULL
                ORDER BY our_exit_timestamp DESC
                LIMIT 40
            """)
            stats['recent_trades'] = cursor.fetchall()

            # All-time counts
            cursor.execute("""
                SELECT
                    COUNT(*) AS total_all_time,
                    COUNT(*) FILTER (WHERE our_status = 'pending') AS currently_open,
                    COUNT(*) FILTER (WHERE our_status = 'validating') AS validating,
                    COUNT(*) FILTER (WHERE our_status = 'no_go') AS no_go
                FROM follow_the_goat_buyins
            """)
            stats['summary'] = cursor.fetchone() or {}
            return stats


def _fetch_errors() -> list:
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    component_id,
                    occurred_at,
                    message,
                    traceback
                FROM scheduler_error_events
                WHERE occurred_at >= NOW() - INTERVAL '24 hours'
                ORDER BY occurred_at DESC
                LIMIT 50
            """)
            return cursor.fetchall()


def _fetch_component_health() -> list:
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    c.component_id,
                    c.description,
                    COALESCE(s.enabled, c.default_enabled, true) AS is_enabled,
                    h.last_heartbeat_at AS reported_at,
                    h.status,
                    EXTRACT(EPOCH FROM (NOW() - h.last_heartbeat_at)) AS seconds_since_heartbeat
                FROM scheduler_components c
                LEFT JOIN scheduler_component_settings s ON s.component_id = c.component_id
                LEFT JOIN (
                    SELECT DISTINCT ON (component_id)
                        component_id, last_heartbeat_at, status
                    FROM scheduler_component_heartbeats
                    ORDER BY component_id, last_heartbeat_at DESC
                ) h ON h.component_id = c.component_id
                ORDER BY c.component_id
            """)
            return cursor.fetchall()


# ---------------------------------------------------------------------------
# Chart generation
# ---------------------------------------------------------------------------

def _build_trade_chart(trades: list) -> str:
    """Render entry/exit trade chart as base64 PNG using matplotlib."""
    import os, tempfile
    if not os.environ.get('MPLCONFIGDIR'):
        os.environ['MPLCONFIGDIR'] = tempfile.gettempdir()
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches

    if not trades:
        fig, ax = plt.subplots(figsize=(10, 3))
        ax.text(0.5, 0.5, 'No closed trades in last 24h', ha='center', va='center',
                fontsize=13, color='#888', transform=ax.transAxes)
        ax.set_facecolor('#1a1a2e')
        fig.patch.set_facecolor('#16213e')
        ax.tick_params(colors='#aaa')
        for spine in ax.spines.values():
            spine.set_edgecolor('#333')
    else:
        # Sort oldest first for chart
        sorted_trades = sorted(trades, key=lambda t: t.get('followed_at') or datetime.min.replace(tzinfo=timezone.utc))

        fig, ax = plt.subplots(figsize=(12, 5))
        fig.patch.set_facecolor('#16213e')
        ax.set_facecolor('#1a1a2e')

        for i, trade in enumerate(sorted_trades):
            entry_price = trade.get('our_entry_price')
            exit_price = trade.get('our_exit_price')
            pnl = trade.get('our_profit_loss', 0) or 0

            if entry_price is None or exit_price is None:
                continue

            color = '#00d4aa' if pnl > 0 else '#ff4757'

            # Entry dot (blue)
            ax.scatter(i, entry_price, color='#3498db', zorder=5, s=60, marker='o')
            # Exit dot (green/red)
            ax.scatter(i, exit_price, color=color, zorder=5, s=60, marker='D')
            # Connecting line
            ax.plot([i, i], [entry_price, exit_price], color=color, alpha=0.5, linewidth=1.5, zorder=4)

        ax.set_xlabel('Trade Index (oldest → newest)', color='#aaa', fontsize=10)
        ax.set_ylabel('SOL Price (USD)', color='#aaa', fontsize=10)
        ax.set_title('Trade Entries & Exits — Last 24h', color='#e0e0e0', fontsize=13, pad=12)
        ax.tick_params(colors='#aaa', labelsize=8)
        for spine in ax.spines.values():
            spine.set_edgecolor('#333')
        ax.grid(axis='y', color='#2a2a4a', linewidth=0.5, linestyle='--')

        entry_patch = mpatches.Patch(color='#3498db', label='Entry price')
        win_patch = mpatches.Patch(color='#00d4aa', label='Exit (win)')
        loss_patch = mpatches.Patch(color='#ff4757', label='Exit (loss)')
        ax.legend(handles=[entry_patch, win_patch, loss_patch],
                  facecolor='#0d1b2a', edgecolor='#333', labelcolor='#ccc', fontsize=9)

    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=120, bbox_inches='tight')
    plt.close('all')
    buf.seek(0)
    return base64.b64encode(buf.read()).decode('utf-8')


# ---------------------------------------------------------------------------
# Auto-insights
# ---------------------------------------------------------------------------

def _build_insights(tx: dict, whale: dict, ob: dict, perf: dict, errors: list) -> list[str]:
    insights = []

    # Win rate
    total_sold = perf.get('total_sold') or 0
    wins = perf.get('wins') or 0
    if total_sold > 0:
        wr = wins / total_sold * 100
        if wr >= 60:
            insights.append(f"Win rate is strong at {wr:.0f}% ({wins}/{total_sold} trades).")
        elif wr >= 40:
            insights.append(f"Win rate is moderate at {wr:.0f}% — consider tightening pattern filters.")
        else:
            insights.append(f"Win rate is low at {wr:.0f}% ({wins}/{total_sold}) — review play configs and filter thresholds.")
    else:
        insights.append("No closed trades in the last 24h — check if follow_the_goat and trailing_stop_seller are running.")

    # Whale bias
    whale_buys = float(whale.get('buy_volume_usd') or 0)
    whale_sells = float(whale.get('sell_volume_usd') or 0)
    if whale_buys + whale_sells > 0:
        bias = (whale_buys - whale_sells) / (whale_buys + whale_sells) * 100
        if bias > 20:
            insights.append(f"Whales are net BUYING — {bias:.0f}% buy bias (${whale_buys:,.0f} vs ${whale_sells:,.0f} sell). Bullish signal.")
        elif bias < -20:
            insights.append(f"Whales are net SELLING — {abs(bias):.0f}% sell bias (${whale_sells:,.0f} vs ${whale_buys:,.0f} buy). Bearish signal.")
        else:
            insights.append(f"Whale activity is balanced — buy/sell within 20% of each other.")

    # Order book
    vi = ob.get('hourly_avg', {}).get('avg_volume_imbalance') or ob.get('volume_imbalance')
    if vi is not None:
        vi = float(vi)
        if vi > 0.15:
            insights.append(f"Order book shows BID pressure (volume imbalance={vi:.3f}) — buyers dominating liquidity.")
        elif vi < -0.15:
            insights.append(f"Order book shows ASK pressure (volume imbalance={vi:.3f}) — sellers dominating liquidity.")
        else:
            insights.append(f"Order book is balanced (volume imbalance={vi:.3f}).")

    # Errors
    if errors:
        from collections import Counter
        component_counts = Counter(e.get('component_id') for e in errors)
        top = component_counts.most_common(3)
        parts = ', '.join(f"{cid} ({n})" for cid, n in top)
        insights.append(f"{len(errors)} system errors in 24h. Most affected: {parts}.")
    else:
        insights.append("No system errors in the last 24h. All components running cleanly.")

    # Transaction flow
    tx_total = int(tx.get('total') or 0)
    if tx_total < 1000:
        insights.append(f"Transaction count is low ({tx_total:,} in 24h) — check webhook_server and QuickNode stream health.")
    elif tx_total > 50000:
        insights.append(f"High transaction volume ({tx_total:,} in 24h) — system is seeing strong market activity.")

    return insights


# ---------------------------------------------------------------------------
# HTML helpers
# ---------------------------------------------------------------------------

def _fmt_num(val, decimals=2, prefix='', suffix='') -> str:
    if val is None:
        return 'N/A'
    try:
        v = float(val)
        return f"{prefix}{v:,.{decimals}f}{suffix}"
    except (TypeError, ValueError):
        return str(val)


def _pct_color(val) -> str:
    """Return a CSS color class based on positive/negative/neutral value."""
    try:
        v = float(val)
        if v > 0:
            return 'positive'
        elif v < 0:
            return 'negative'
    except (TypeError, ValueError):
        pass
    return 'neutral'


def _fmt_dt(dt) -> str:
    if dt is None:
        return 'N/A'
    if isinstance(dt, str):
        return dt
    try:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.strftime('%Y-%m-%d %H:%M UTC')
    except Exception:
        return str(dt)


# ---------------------------------------------------------------------------
# HTML builder
# ---------------------------------------------------------------------------

def generate_html() -> str:
    generated_at = datetime.now(timezone.utc)

    # Fetch all data
    tx = _fetch_transaction_stats()
    whale = _fetch_whale_stats()
    ob = _fetch_order_book()
    perf = _fetch_trade_performance()
    errors = _fetch_errors()
    components = _fetch_component_health()
    insights = _build_insights(tx, whale, ob, perf, errors)
    chart_b64 = _build_trade_chart(perf.get('recent_trades') or [])

    # Group errors by component
    from collections import defaultdict
    errors_by_comp: dict[str, list] = defaultdict(list)
    for e in errors:
        errors_by_comp[e.get('component_id', 'unknown')].append(e)

    # Trade stats
    total_sold = int(perf.get('total_sold') or 0)
    wins = int(perf.get('wins') or 0)
    losses = int(perf.get('losses') or 0)
    win_rate = (wins / total_sold * 100) if total_sold > 0 else 0
    total_pnl = float(perf.get('total_pnl') or 0)
    summary = perf.get('summary') or {}

    # Component health colours
    def _comp_status_html(row: dict) -> str:
        secs = row.get('seconds_since_heartbeat')
        enabled = row.get('is_enabled', True)
        if not enabled:
            return '<span class="badge badge-disabled">DISABLED</span>'
        if secs is None:
            return '<span class="badge badge-warning">NO HEARTBEAT</span>'
        secs = float(secs)
        if secs < 30:
            return '<span class="badge badge-ok">HEALTHY</span>'
        elif secs < 120:
            return '<span class="badge badge-warning">SLOW</span>'
        else:
            return '<span class="badge badge-error">STALE</span>'

    def _whale_row(w: dict) -> str:
        addr = w.get('wallet_address', '')
        short = addr[:6] + '...' + addr[-4:] if len(addr) > 10 else addr
        return (
            f"<tr><td><code>{short}</code></td>"
            f"<td>{int(w.get('trade_count') or 0)}</td>"
            f"<td>${float(w.get('total_volume_usd') or 0):,.0f}</td>"
            f"<td class='positive'>${float(w.get('buy_vol') or 0):,.0f}</td>"
            f"<td class='negative'>${float(w.get('sell_vol') or 0):,.0f}</td></tr>"
        )

    whale_rows = ''.join(_whale_row(w) for w in (whale.get('top_wallets') or []))

    def _error_section(comp: str, errs: list) -> str:
        rows = ''
        for e in errs[:5]:
            msg = (e.get('message') or '')[:120]
            ts = _fmt_dt(e.get('occurred_at'))
            rows += f"<tr><td class='ts'>{ts}</td><td>{msg}</td></tr>"
        return (
            f"<div class='error-group'>"
            f"<div class='error-comp-title'>{comp} <span class='badge badge-error'>{len(errs)}</span></div>"
            f"<table class='inner-table'>{rows}</table></div>"
        )

    error_html = ''.join(_error_section(comp, errs) for comp, errs in sorted(errors_by_comp.items()))
    if not error_html:
        error_html = "<p class='ok-msg'>No errors in the last 24 hours.</p>"

    comp_rows = ''
    for c in components:
        comp_rows += (
            f"<tr><td>{c.get('component_id','')}</td>"
            f"<td class='small-text'>{c.get('description','')}</td>"
            f"<td>{_comp_status_html(c)}</td>"
            f"<td class='ts'>{_fmt_dt(c.get('reported_at'))}</td></tr>"
        )

    insight_items = ''.join(f"<li>{i}</li>" for i in insights)

    ob_avg = ob.get('hourly_avg') or {}

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Follow The Goat — System Report</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    background: #0d1117;
    color: #c9d1d9;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    font-size: 14px;
    line-height: 1.6;
  }}
  .wrapper {{ max-width: 960px; margin: 0 auto; padding: 24px 16px; }}

  /* Header */
  .header {{
    background: linear-gradient(135deg, #161b22 0%, #0d1117 100%);
    border: 1px solid #30363d;
    border-radius: 10px;
    padding: 28px 32px;
    margin-bottom: 24px;
    display: flex;
    justify-content: space-between;
    align-items: center;
  }}
  .header h1 {{ font-size: 22px; color: #e6edf3; font-weight: 700; }}
  .header .subtitle {{ color: #8b949e; font-size: 12px; margin-top: 4px; }}
  .header .gen-time {{ color: #8b949e; font-size: 12px; text-align: right; }}

  /* Section cards */
  .section {{
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 10px;
    padding: 20px 24px;
    margin-bottom: 20px;
  }}
  .section-title {{
    font-size: 15px;
    font-weight: 600;
    color: #e6edf3;
    margin-bottom: 16px;
    padding-bottom: 10px;
    border-bottom: 1px solid #21262d;
    display: flex;
    align-items: center;
    gap: 8px;
  }}
  .section-title .icon {{ font-size: 18px; }}

  /* Stat grid */
  .stat-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(140px, 1fr));
    gap: 12px;
    margin-bottom: 4px;
  }}
  .stat-card {{
    background: #0d1117;
    border: 1px solid #21262d;
    border-radius: 8px;
    padding: 14px 16px;
  }}
  .stat-label {{ font-size: 11px; color: #8b949e; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px; }}
  .stat-value {{ font-size: 22px; font-weight: 700; color: #e6edf3; }}
  .stat-sub {{ font-size: 11px; color: #6e7681; margin-top: 2px; }}

  /* Tables */
  table {{ width: 100%; border-collapse: collapse; }}
  th {{ text-align: left; font-size: 11px; color: #8b949e; text-transform: uppercase;
        letter-spacing: 0.5px; padding: 6px 8px; border-bottom: 1px solid #21262d; }}
  td {{ padding: 8px 8px; border-bottom: 1px solid #161b22; vertical-align: top; }}
  tr:last-child td {{ border-bottom: none; }}
  tr:hover td {{ background: #1c2128; }}
  .inner-table th, .inner-table td {{ padding: 4px 6px; font-size: 12px; }}

  /* Colours */
  .positive {{ color: #3fb950; }}
  .negative {{ color: #f85149; }}
  .neutral  {{ color: #8b949e; }}

  /* Badges */
  .badge {{
    display: inline-block;
    padding: 2px 8px;
    border-radius: 12px;
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
  }}
  .badge-ok       {{ background: #1a3b2a; color: #3fb950; }}
  .badge-warning  {{ background: #3b2f1a; color: #e3b341; }}
  .badge-error    {{ background: #3b1a1a; color: #f85149; }}
  .badge-disabled {{ background: #21262d; color: #8b949e; }}

  /* Insights */
  .insights ul {{ padding-left: 18px; }}
  .insights li {{ margin-bottom: 8px; color: #c9d1d9; }}
  .insights li::marker {{ color: #58a6ff; }}

  /* Chart */
  .chart-img {{ width: 100%; border-radius: 8px; border: 1px solid #21262d; }}

  /* Errors */
  .error-group {{ margin-bottom: 16px; }}
  .error-comp-title {{ font-size: 13px; font-weight: 600; color: #e6edf3; margin-bottom: 6px; }}
  .ok-msg {{ color: #3fb950; padding: 12px 0; }}
  .ts {{ color: #6e7681; font-size: 11px; white-space: nowrap; }}
  .small-text {{ font-size: 12px; color: #8b949e; }}
  code {{ font-family: 'SFMono-Regular', Consolas, monospace; font-size: 12px; color: #79c0ff; }}

  /* Two-column layout for tables */
  .two-col {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
  @media (max-width: 640px) {{ .two-col {{ grid-template-columns: 1fr; }} }}
  .sub-section-title {{ font-size: 12px; color: #8b949e; text-transform: uppercase;
                        letter-spacing: 0.5px; margin-bottom: 8px; margin-top: 12px; }}
</style>
</head>
<body>
<div class="wrapper">

  <!-- HEADER -->
  <div class="header">
    <div>
      <h1>Follow The Goat — System Report</h1>
      <div class="subtitle">24-hour system health & performance overview</div>
    </div>
    <div class="gen-time">Generated<br>{_fmt_dt(generated_at)}</div>
  </div>

  <!-- INSIGHTS -->
  <div class="section insights">
    <div class="section-title"><span class="icon">💡</span> Automated Insights</div>
    <ul>{insight_items}</ul>
  </div>

  <!-- TRANSACTIONS -->
  <div class="section">
    <div class="section-title"><span class="icon">📊</span> Transactions (Last 24h)</div>
    <div class="stat-grid">
      <div class="stat-card">
        <div class="stat-label">Total Trades</div>
        <div class="stat-value">{int(tx.get('total') or 0):,}</div>
        <div class="stat-sub">{int(tx.get('unique_wallets') or 0):,} unique wallets</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Buy Trades</div>
        <div class="stat-value positive">{int(tx.get('buys') or 0):,}</div>
        <div class="stat-sub">{_fmt_num(tx.get('buy_volume_usd'), 0, '$')} USD</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Sell Trades</div>
        <div class="stat-value negative">{int(tx.get('sells') or 0):,}</div>
        <div class="stat-sub">{_fmt_num(tx.get('sell_volume_usd'), 0, '$')} USD</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Buy Volume SOL</div>
        <div class="stat-value positive">{_fmt_num(tx.get('buy_volume_sol'), 0)}</div>
        <div class="stat-sub">SOL bought</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Sell Volume SOL</div>
        <div class="stat-value negative">{_fmt_num(tx.get('sell_volume_sol'), 0)}</div>
        <div class="stat-sub">SOL sold</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Net Flow USD</div>
        <div class="stat-value {_pct_color(float(tx.get('buy_volume_usd') or 0) - float(tx.get('sell_volume_usd') or 0))}">{_fmt_num(float(tx.get('buy_volume_usd') or 0) - float(tx.get('sell_volume_usd') or 0), 0, '$')}</div>
        <div class="stat-sub">buy - sell</div>
      </div>
    </div>
    <div class="sub-section-title">Latest trade</div>
    <p class="small-text">{_fmt_dt(tx.get('latest_trade'))}</p>
  </div>

  <!-- WHALES -->
  <div class="section">
    <div class="section-title"><span class="icon">🐳</span> Whale Activity (Last 24h, &gt;$10k)</div>
    <div class="stat-grid">
      <div class="stat-card">
        <div class="stat-label">Whale Trades</div>
        <div class="stat-value">{int(whale.get('total') or 0):,}</div>
        <div class="stat-sub">{int(whale.get('unique_wallets') or 0):,} unique whales</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Whale Buys</div>
        <div class="stat-value positive">{int(whale.get('buys') or 0):,}</div>
        <div class="stat-sub">{_fmt_num(whale.get('buy_volume_usd'), 0, '$')} USD</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Whale Sells</div>
        <div class="stat-value negative">{int(whale.get('sells') or 0):,}</div>
        <div class="stat-sub">{_fmt_num(whale.get('sell_volume_usd'), 0, '$')} USD</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Buy Vol SOL</div>
        <div class="stat-value positive">{_fmt_num(whale.get('buy_volume_sol'), 1)}</div>
        <div class="stat-sub">SOL</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Sell Vol SOL</div>
        <div class="stat-value negative">{_fmt_num(whale.get('sell_volume_sol'), 1)}</div>
        <div class="stat-sub">SOL</div>
      </div>
    </div>
    {"<div class='sub-section-title'>Top 5 Whale Wallets by Volume</div><table><thead><tr><th>Wallet</th><th>Trades</th><th>Total USD</th><th>Buy USD</th><th>Sell USD</th></tr></thead><tbody>" + whale_rows + "</tbody></table>" if whale_rows else ""}
  </div>

  <!-- ORDER BOOK -->
  <div class="section">
    <div class="section-title"><span class="icon">📖</span> Order Book (SOLUSDT)</div>
    <div class="stat-grid">
      <div class="stat-card">
        <div class="stat-label">Mid Price</div>
        <div class="stat-value">{_fmt_num(ob.get('mid_price'), 2, '$')}</div>
        <div class="stat-sub">Latest snapshot</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Spread (bps)</div>
        <div class="stat-value">{_fmt_num(ob.get('spread_bps'), 2)}</div>
        <div class="stat-sub">1h avg: {_fmt_num(ob_avg.get('avg_spread_bps'), 2)}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Bid/Ask Ratio</div>
        <div class="stat-value {_pct_color(float(ob.get('bid_ask_ratio') or 1) - 1)}">{_fmt_num(ob.get('bid_ask_ratio'), 3)}</div>
        <div class="stat-sub">1h avg: {_fmt_num(ob_avg.get('avg_bid_ask_ratio'), 3)}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Volume Imbalance</div>
        <div class="stat-value {_pct_color(ob.get('volume_imbalance'))}">{_fmt_num(ob.get('volume_imbalance'), 4)}</div>
        <div class="stat-sub">1h avg: {_fmt_num(ob_avg.get('avg_volume_imbalance'), 4)}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Depth Imbalance</div>
        <div class="stat-value {_pct_color(ob.get('depth_imbalance'))}">{_fmt_num(ob.get('depth_imbalance'), 4)}</div>
        <div class="stat-sub">1h avg: {_fmt_num(ob_avg.get('avg_depth_imbalance'), 4)}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Bid Volume</div>
        <div class="stat-value">{_fmt_num(ob.get('total_bid_volume'), 1)}</div>
        <div class="stat-sub">vs ask: {_fmt_num(ob.get('total_ask_volume'), 1)}</div>
      </div>
    </div>
    <p class="ts" style="margin-top:8px">Snapshot at: {_fmt_dt(ob.get('timestamp'))}</p>
  </div>

  <!-- TRADE PERFORMANCE -->
  <div class="section">
    <div class="section-title"><span class="icon">💰</span> Trade Performance (Last 24h)</div>
    <div class="stat-grid">
      <div class="stat-card">
        <div class="stat-label">Closed Trades</div>
        <div class="stat-value">{total_sold}</div>
        <div class="stat-sub">all-time open: {int(summary.get('currently_open') or 0)}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Win Rate</div>
        <div class="stat-value {_pct_color(win_rate - 50)}">{win_rate:.0f}%</div>
        <div class="stat-sub">{wins}W / {losses}L</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Total P&L</div>
        <div class="stat-value {_pct_color(total_pnl)}">{_fmt_num(total_pnl, 4)}</div>
        <div class="stat-sub">SOL</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Avg Win</div>
        <div class="stat-value positive">{_fmt_num(perf.get('avg_win'), 4)}</div>
        <div class="stat-sub">SOL per win</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Avg Loss</div>
        <div class="stat-value negative">{_fmt_num(perf.get('avg_loss'), 4)}</div>
        <div class="stat-sub">SOL per loss</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Best Trade</div>
        <div class="stat-value positive">{_fmt_num(perf.get('best_trade'), 4)}</div>
        <div class="stat-sub">SOL</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Worst Trade</div>
        <div class="stat-value negative">{_fmt_num(perf.get('worst_trade'), 4)}</div>
        <div class="stat-sub">SOL</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Avg Potential</div>
        <div class="stat-value">{_fmt_num(perf.get('avg_potential_gains'), 2, suffix='%')}</div>
        <div class="stat-sub">cycle gain</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Validating</div>
        <div class="stat-value">{int(summary.get('validating') or 0)}</div>
        <div class="stat-sub">no_go: {int(summary.get('no_go') or 0)}</div>
      </div>
    </div>

    <!-- Trade Chart -->
    <div class="sub-section-title" style="margin-top:20px">Entry / Exit Chart</div>
    <img src="data:image/png;base64,{chart_b64}" class="chart-img" alt="Trade entry/exit chart">
  </div>

  <!-- SYSTEM ERRORS -->
  <div class="section">
    <div class="section-title"><span class="icon">⚠️</span> System Errors (Last 24h) — {len(errors)} total</div>
    {error_html}
  </div>

  <!-- COMPONENT HEALTH -->
  <div class="section">
    <div class="section-title"><span class="icon">🖥️</span> Component Health</div>
    <table>
      <thead>
        <tr>
          <th>Component</th>
          <th>Description</th>
          <th>Status</th>
          <th>Last Heartbeat</th>
        </tr>
      </thead>
      <tbody>{comp_rows}</tbody>
    </table>
  </div>

</div>
</body>
</html>"""

    return html
