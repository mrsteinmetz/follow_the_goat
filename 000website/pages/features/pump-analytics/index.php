<?php
/**
 * Pump Analytics Dashboard
 *
 * Real-time overview of V4 fingerprint-based pump signal detection:
 * signal outcomes, win rates, circuit breaker, continuation checks,
 * and loaded fingerprint rules.
 */

require_once __DIR__ . '/../../../includes/config.php';
require_once __DIR__ . '/../../../includes/DatabaseClient.php';
$db = new DatabaseClient(DATABASE_API_URL);
$api_available = $db->isAvailable();

$baseUrl = '';

// Parameters
$hours = isset($_GET['hours']) ? (int)$_GET['hours'] : 24;
$hours = max(1, min(168, $hours));
$refresh = isset($_GET['refresh']) ? (int)$_GET['refresh'] : 30;
$refresh = max(10, min(120, $refresh));

$data = [];
$error_message = '';

if ($api_available) {
    $response = $db->get('/pump/analytics', ['hours' => $hours, 'limit' => 100]);
    if ($response) {
        $data = $response;
    } else {
        $error_message = 'Failed to fetch pump analytics data from API.';
    }
} else {
    $error_message = 'API server is not available.';
}

$sig = $data['signal_summary'] ?? [];
$cont = $data['continuation_summary'] ?? [];
$fp = $data['fingerprint_rules'] ?? [];
$outcomes = $data['recent_outcomes'] ?? [];
$continuations = $data['recent_continuation'] ?? [];
$cb = $sig['circuit_breaker'] ?? [];
$rc = $data['raw_cache'] ?? [];
$ct = $data['cycle_tracker'] ?? [];
$live = $rc['live_features'] ?? [];

// Opportunity funnel data
$funnel_data = [];
if ($api_available) {
    $funnel_days = max(intval($hours / 24), 1);
    $funnel_resp = $db->get('/pump/opportunity_funnel', ['days' => min($funnel_days, 30)]);
    if ($funnel_resp) {
        $funnel_data = $funnel_resp;
    }
}
$daily_funnel = $funnel_data['daily_funnel'] ?? [];
$all_time_trades = $funnel_data['all_time_trades'] ?? [];
$funnel_summary = $funnel_data['summary'] ?? [];

// ── Styles ──────────────────────────────────────────────────────────────────
ob_start();
?>
<style>
.pump-stats-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 1rem;
    margin-bottom: 1.5rem;
}
.pump-stat-card {
    background: rgba(var(--body-bg-rgb2), 1);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 0.75rem;
    padding: 1.25rem;
}
.pump-stat-card .stat-label {
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: rgba(255,255,255,0.45);
    margin-bottom: 0.25rem;
}
.pump-stat-card .stat-value {
    font-size: 1.75rem;
    font-weight: 700;
    color: #fff;
    line-height: 1.2;
}
.pump-stat-card .stat-sub {
    font-size: 0.8rem;
    color: rgba(255,255,255,0.5);
    margin-top: 0.25rem;
}
.stat-value.text-success { color: rgb(16,185,129) !important; }
.stat-value.text-danger  { color: rgb(239,68,68) !important; }
.stat-value.text-warning { color: rgb(245,158,11) !important; }
.stat-value.text-info    { color: rgb(56,189,248) !important; }

.cb-badge {
    display: inline-block;
    padding: 0.2rem 0.6rem;
    border-radius: 0.375rem;
    font-size: 0.75rem;
    font-weight: 600;
}
.cb-badge.active  { background: rgba(239,68,68,0.15); color: #ef4444; }
.cb-badge.ok      { background: rgba(16,185,129,0.15); color: #10b981; }

.section-title {
    font-size: 1rem;
    font-weight: 600;
    color: rgba(255,255,255,0.85);
    margin-bottom: 1rem;
    padding-bottom: 0.5rem;
    border-bottom: 1px solid rgba(255,255,255,0.06);
}

.fp-feature-row {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    padding: 0.4rem 0;
    border-bottom: 1px solid rgba(255,255,255,0.04);
}
.fp-feature-row:last-child { border-bottom: none; }
.fp-feature-name {
    width: 260px;
    font-size: 0.8rem;
    color: rgba(255,255,255,0.7);
    font-family: monospace;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}
.fp-bar-wrap {
    flex: 1;
    height: 8px;
    background: rgba(255,255,255,0.06);
    border-radius: 4px;
    overflow: hidden;
}
.fp-bar {
    height: 100%;
    border-radius: 4px;
    background: rgb(var(--primary-rgb));
    transition: width 0.3s ease;
}
.fp-sep-val {
    width: 60px;
    text-align: right;
    font-size: 0.8rem;
    color: rgba(255,255,255,0.6);
    font-family: monospace;
}
.fp-eligible {
    width: 20px;
    text-align: center;
}

.pump-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.82rem;
}
.pump-table thead th {
    background: rgba(255,255,255,0.03);
    padding: 0.6rem 0.75rem;
    text-align: left;
    font-weight: 600;
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: rgba(255,255,255,0.5);
    border-bottom: 1px solid rgba(255,255,255,0.08);
    white-space: nowrap;
}
.pump-table tbody td {
    padding: 0.5rem 0.75rem;
    border-bottom: 1px solid rgba(255,255,255,0.04);
    color: rgba(255,255,255,0.75);
    vertical-align: middle;
}
.pump-table tbody tr:hover { background: rgba(255,255,255,0.02); }

.hit-badge {
    display: inline-block;
    padding: 0.15rem 0.5rem;
    border-radius: 0.3rem;
    font-size: 0.72rem;
    font-weight: 600;
}
.hit-badge.win  { background: rgba(16,185,129,0.15); color: #10b981; }
.hit-badge.loss { background: rgba(239,68,68,0.15); color: #ef4444; }

.pass-badge {
    display: inline-block;
    padding: 0.15rem 0.5rem;
    border-radius: 0.3rem;
    font-size: 0.72rem;
    font-weight: 600;
}
.pass-badge.yes { background: rgba(16,185,129,0.15); color: #10b981; }
.pass-badge.no  { background: rgba(239,68,68,0.15); color: #ef4444; }

.filter-bar {
    display: flex;
    align-items: center;
    gap: 1rem;
    margin-bottom: 1.5rem;
    flex-wrap: wrap;
}
.filter-bar select {
    background: rgba(var(--body-bg-rgb2), 1);
    border: 1px solid rgba(255,255,255,0.1);
    color: #fff;
    padding: 0.4rem 0.75rem;
    border-radius: 0.5rem;
    font-size: 0.85rem;
}
.filter-bar .filter-label {
    font-size: 0.8rem;
    color: rgba(255,255,255,0.5);
}
.refresh-bar {
    height: 2px;
    background: rgba(var(--primary-rgb), 0.3);
    border-radius: 1px;
    margin-bottom: 1rem;
    overflow: hidden;
}
.refresh-bar-inner {
    height: 100%;
    background: rgb(var(--primary-rgb));
    border-radius: 1px;
    width: 100%;
    animation: refreshCountdown <?php echo $refresh; ?>s linear infinite;
}
@keyframes refreshCountdown {
    from { width: 100%; }
    to   { width: 0%; }
}
.panel-card {
    background: rgba(var(--body-bg-rgb2), 1);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 0.75rem;
    padding: 1.25rem;
    margin-bottom: 1.5rem;
}
.two-col-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 1.5rem;
}
@media (max-width: 1200px) {
    .two-col-grid { grid-template-columns: 1fr; }
}
.mono { font-family: monospace; font-size: 0.8rem; }
.text-dim { color: rgba(255,255,255,0.4); }

/* Risk Gradient styles */
.risk-cards-grid {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 1.25rem;
    margin-bottom: 1.5rem;
}
@media (max-width: 900px) {
    .risk-cards-grid { grid-template-columns: 1fr; }
}
.risk-card {
    border-radius: 0.75rem;
    padding: 1.25rem;
    position: relative;
    overflow: hidden;
    transition: transform 0.15s ease;
}
.risk-card:hover { transform: translateY(-2px); }
.risk-card.conservative {
    background: rgba(16,185,129,0.08);
    border: 2px solid rgba(16,185,129,0.35);
}
.risk-card.moderate {
    background: rgba(245,158,11,0.06);
    border: 1px solid rgba(245,158,11,0.18);
    opacity: 0.75;
}
.risk-card.aggressive {
    background: rgba(239,68,68,0.06);
    border: 1px solid rgba(239,68,68,0.18);
    opacity: 0.75;
}
.risk-card .risk-level {
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    font-weight: 700;
    margin-bottom: 0.5rem;
}
.risk-card.conservative .risk-level { color: #10b981; }
.risk-card.moderate .risk-level { color: #f59e0b; }
.risk-card.aggressive .risk-level { color: #ef4444; }
.risk-card .risk-title {
    font-size: 1.1rem;
    font-weight: 700;
    color: #fff;
    margin-bottom: 0.35rem;
}
.risk-card .risk-est {
    font-size: 1.5rem;
    font-weight: 800;
    margin: 0.5rem 0;
}
.risk-card.conservative .risk-est { color: #10b981; }
.risk-card.moderate .risk-est { color: #f59e0b; }
.risk-card.aggressive .risk-est { color: #ef4444; }
.risk-card .risk-desc {
    font-size: 0.82rem;
    color: rgba(255,255,255,0.55);
    line-height: 1.5;
}
.risk-card .risk-desc strong { color: rgba(255,255,255,0.8); }
.risk-badge-active {
    display: inline-block;
    padding: 0.15rem 0.55rem;
    border-radius: 0.3rem;
    font-size: 0.68rem;
    font-weight: 700;
    background: rgba(16,185,129,0.2);
    color: #10b981;
}
.risk-badge-future {
    display: inline-block;
    padding: 0.15rem 0.55rem;
    border-radius: 0.3rem;
    font-size: 0.68rem;
    font-weight: 700;
    background: rgba(255,255,255,0.06);
    color: rgba(255,255,255,0.35);
}
.risk-card .gate-list {
    margin-top: 0.75rem;
    padding: 0;
    list-style: none;
}
.risk-card .gate-list li {
    font-size: 0.78rem;
    color: rgba(255,255,255,0.5);
    padding: 0.2rem 0;
    padding-left: 1.2rem;
    position: relative;
}
.risk-card .gate-list li::before {
    position: absolute;
    left: 0;
    font-size: 0.72rem;
}
.gate-on::before  { content: '\2713'; color: #10b981; }
.gate-off::before { content: '\2717'; color: rgba(255,255,255,0.2); text-decoration: line-through; }

#funnelChart, #scatterChart {
    min-height: 300px;
}
.fp-meta {
    display: flex;
    gap: 2rem;
    flex-wrap: wrap;
    margin-bottom: 1rem;
    font-size: 0.82rem;
    color: rgba(255,255,255,0.6);
}
.fp-meta strong { color: rgba(255,255,255,0.85); }

/* Raw cache status */
.raw-cache-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
    gap: 0.75rem;
    margin-bottom: 1.25rem;
}
.raw-cache-card {
    background: rgba(var(--body-bg-rgb2), 1);
    border: 1px solid rgba(255,255,255,0.07);
    border-radius: 0.65rem;
    padding: 0.9rem 1rem;
}
.raw-cache-card .rc-label {
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: rgba(255,255,255,0.4);
    margin-bottom: 0.2rem;
}
.raw-cache-card .rc-val {
    font-size: 1.15rem;
    font-weight: 700;
    color: #e2e8f0;
}
.rc-age-ok  { color: #10b981; }
.rc-age-warn { color: #f59e0b; }
.rc-age-bad  { color: #ef4444; }
.live-feat-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(175px, 1fr));
    gap: 0.5rem;
}
.live-feat-item {
    background: rgba(255,255,255,0.03);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 0.45rem;
    padding: 0.45rem 0.65rem;
    font-size: 0.78rem;
    display: flex;
    justify-content: space-between;
}
.live-feat-item .lf-name { color: rgba(255,255,255,0.5); }
.live-feat-item .lf-val  { font-weight: 600; color: #e2e8f0; font-family: monospace; }
</style>
<?php $styles = ob_get_clean(); ?>

<?php
// ── Content ─────────────────────────────────────────────────────────────────
ob_start();
?>

<!-- Page Header -->
<div class="d-flex align-items-center justify-content-between flex-wrap gap-2 mb-4">
    <div>
        <h5 class="fw-semibold mb-0">Pump Analytics</h5>
        <p class="text-muted mb-0" style="font-size: 0.85rem;">V4 fingerprint-based signal detection overview</p>
    </div>
    <div class="filter-bar mb-0">
        <span class="filter-label">Window:</span>
        <select id="hoursSelect" onchange="applyFilters()">
            <option value="1" <?php echo $hours == 1 ? 'selected' : ''; ?>>1 hour</option>
            <option value="6" <?php echo $hours == 6 ? 'selected' : ''; ?>>6 hours</option>
            <option value="12" <?php echo $hours == 12 ? 'selected' : ''; ?>>12 hours</option>
            <option value="24" <?php echo $hours == 24 ? 'selected' : ''; ?>>24 hours</option>
            <option value="48" <?php echo $hours == 48 ? 'selected' : ''; ?>>48 hours</option>
            <option value="168" <?php echo $hours == 168 ? 'selected' : ''; ?>>7 days</option>
        </select>
        <span class="filter-label">Refresh:</span>
        <select id="refreshSelect" onchange="applyFilters()">
            <option value="15" <?php echo $refresh == 15 ? 'selected' : ''; ?>>15s</option>
            <option value="30" <?php echo $refresh == 30 ? 'selected' : ''; ?>>30s</option>
            <option value="60" <?php echo $refresh == 60 ? 'selected' : ''; ?>>60s</option>
        </select>
    </div>
</div>

<div class="refresh-bar"><div class="refresh-bar-inner"></div></div>

<?php if ($error_message): ?>
<div class="alert alert-danger"><?php echo htmlspecialchars($error_message); ?></div>
<?php endif; ?>

<!-- Stats Grid -->
<div class="pump-stats-grid">
    <!-- Win Rate -->
    <div class="pump-stat-card">
        <div class="stat-label">Win Rate (<?php echo $hours; ?>h)</div>
        <?php
        $wr = $sig['win_rate'] ?? null;
        $wr_class = $wr === null ? '' : ($wr >= 50 ? 'text-success' : ($wr >= 35 ? 'text-warning' : 'text-danger'));
        ?>
        <div class="stat-value <?php echo $wr_class; ?>"><?php echo $wr !== null ? $wr . '%' : '—'; ?></div>
        <div class="stat-sub"><?php echo ($sig['n_hits'] ?? 0); ?> / <?php echo ($sig['n_total'] ?? 0); ?> signals hit target</div>
    </div>

    <!-- Signals Fired -->
    <div class="pump-stat-card">
        <div class="stat-label">Signals Fired</div>
        <div class="stat-value text-info"><?php echo ($sig['n_total'] ?? 0); ?></div>
        <div class="stat-sub">
            Avg conf: <?php echo isset($sig['avg_confidence']) ? round($sig['avg_confidence'] * 100, 1) . '%' : '—'; ?>
            &middot; Avg readiness: <?php echo isset($sig['avg_readiness']) ? number_format($sig['avg_readiness'], 3) : '—'; ?>
        </div>
    </div>

    <!-- Continuation Pass Rate -->
    <div class="pump-stat-card">
        <div class="stat-label">Continuation Pass Rate</div>
        <?php
        $pr = $cont['pass_rate_pct'] ?? null;
        $pr_class = $pr === null ? '' : ($pr >= 50 ? 'text-success' : ($pr >= 25 ? 'text-warning' : 'text-danger'));
        ?>
        <div class="stat-value <?php echo $pr_class; ?>"><?php echo $pr !== null ? $pr . '%' : '—'; ?></div>
        <div class="stat-sub"><?php echo ($cont['n_passed'] ?? 0); ?> / <?php echo ($cont['n_total'] ?? 0); ?> checks passed</div>
    </div>

    <!-- Circuit Breaker -->
    <div class="pump-stat-card">
        <div class="stat-label">Circuit Breaker</div>
        <?php $tripped = !empty($cb['tripped']); ?>
        <div class="stat-value">
            <span class="cb-badge <?php echo $tripped ? 'active' : 'ok'; ?>">
                <?php echo $tripped ? 'TRIPPED' : 'OK'; ?>
            </span>
        </div>
        <div class="stat-sub">
            Last <?php echo ($cb['n_recent'] ?? 0); ?>: win rate <?php echo ($cb['win_rate_recent'] ?? '—'); echo ($cb['win_rate_recent'] !== null ? '%' : ''); ?>
        </div>
    </div>

    <!-- Avg Gain -->
    <div class="pump-stat-card">
        <div class="stat-label">Avg Gain %</div>
        <?php
        $ag = $sig['avg_gain'] ?? null;
        $ag_class = $ag === null ? '' : ($ag > 0 ? 'text-success' : 'text-danger');
        ?>
        <div class="stat-value <?php echo $ag_class; ?>"><?php echo $ag !== null ? number_format($ag, 4) . '%' : '—'; ?></div>
        <div class="stat-sub">Across all resolved signals</div>
    </div>
</div>

<!-- Raw Data Cache Status -->
<div class="panel-card">
    <div class="section-title" style="margin-bottom:0.85rem;">
        Raw Data Cache
        <?php
        $ob_age = $rc['ob']['age_seconds'] ?? null;
        if ($ob_age !== null) {
            $age_class = $ob_age < 5 ? 'rc-age-ok' : ($ob_age < 30 ? 'rc-age-warn' : 'rc-age-bad');
            echo '<span style="font-size:0.72rem;font-weight:400;margin-left:0.75rem;" class="' . $age_class . '">OB ' . round($ob_age, 1) . 's ago</span>';
        }
        ?>
    </div>

    <div class="raw-cache-grid">
        <!-- OB Snapshots -->
        <div class="raw-cache-card">
            <div class="rc-label">OB Snapshots</div>
            <?php $ob_rows = $rc['ob']['rows'] ?? 0; ?>
            <div class="rc-val"><?php echo number_format($ob_rows); ?></div>
            <div style="font-size:0.72rem;color:rgba(255,255,255,0.4);"><?php echo ($rc['ob']['size_kb'] ?? 0); ?> KB</div>
        </div>
        <!-- Trades -->
        <div class="raw-cache-card">
            <div class="rc-label">Raw Trades</div>
            <?php $tr_rows = $rc['trades']['rows'] ?? 0; ?>
            <div class="rc-val"><?php echo number_format($tr_rows); ?></div>
            <div style="font-size:0.72rem;color:rgba(255,255,255,0.4);"><?php echo ($rc['trades']['size_kb'] ?? 0); ?> KB</div>
        </div>
        <!-- Whales -->
        <div class="raw-cache-card">
            <div class="rc-label">Whale Events</div>
            <?php $wh_rows = $rc['whales']['rows'] ?? 0; ?>
            <div class="rc-val"><?php echo number_format($wh_rows); ?></div>
            <div style="font-size:0.72rem;color:rgba(255,255,255,0.4);"><?php echo ($rc['whales']['size_kb'] ?? 0); ?> KB</div>
        </div>
        <!-- Pump counts from cycle_tracker -->
        <div class="raw-cache-card">
            <div class="rc-label">Pumps (≥0.3%) <?php echo $hours; ?>h</div>
            <div class="rc-val text-success"><?php echo ($ct['pumps_0_3pct'] ?? '—'); ?></div>
            <div style="font-size:0.72rem;color:rgba(255,255,255,0.4);">≥0.4%: <?php echo ($ct['pumps_0_4pct'] ?? '—'); ?> &middot; ≥0.2%: <?php echo ($ct['pumps_0_2pct'] ?? '—'); ?></div>
        </div>
    </div>

    <?php if (!empty($live)): ?>
    <div style="margin-top:0.75rem;">
        <div style="font-size:0.75rem;text-transform:uppercase;letter-spacing:.05em;color:rgba(255,255,255,.4);margin-bottom:0.5rem;">Live Market State (last 5 min)</div>
        <div class="live-feat-grid">
            <?php
            $feat_labels = [
                'ob_avg_vol_imb'     => 'OB Vol Imbalance',
                'ob_avg_depth_ratio' => 'OB Depth Ratio',
                'ob_avg_spread_bps'  => 'OB Spread bps',
                'ob_net_liq_change'  => 'OB Net Liq',
                'ob_bid_ask_ratio'   => 'OB Bid/Ask Ratio',
                'ob_imb_1m'          => 'OB Imb 1m',
                'ob_imb_trend'       => 'OB Imb Trend',
                'ob_depth_trend'     => 'OB Depth Trend',
                'tr_buy_ratio'       => 'Buy Vol %',
                'tr_large_ratio'     => 'Large Trade %',
                'tr_n'               => 'Trade Count (5m)',
                'wh_net_flow'        => 'Whale Net Flow',
                'wh_n'               => 'Whale Events',
            ];
            foreach ($feat_labels as $key => $label) {
                $val = $live[$key] ?? null;
                if ($val === null) continue;
                $fmt = is_float($val) ? number_format($val, 4) : $val;
                echo '<div class="live-feat-item"><span class="lf-name">' . htmlspecialchars($label) . '</span><span class="lf-val">' . htmlspecialchars($fmt) . '</span></div>';
            }
            ?>
        </div>
    </div>
    <?php endif; ?>
</div>

<!-- Fingerprint Rules Panel -->
<div class="panel-card">
    <div class="section-title">Fingerprint Rules (V4)</div>
    <?php if (!empty($fp['error'])): ?>
        <p class="text-dim"><?php echo htmlspecialchars($fp['error']); ?></p>
    <?php else: ?>
        <div class="fp-meta">
            <span>Generated: <strong><?php
                $gen = $fp['generated_at'] ?? null;
                if ($gen) {
                    $dt = new DateTime($gen);
                    $dt->setTimezone(new DateTimeZone('UTC'));
                    echo $dt->format('Y-m-d H:i') . ' UTC';
                } else {
                    echo '—';
                }
            ?></strong></span>
            <span>Lookback: <strong><?php echo isset($fp['lookback_hours']) ? round($fp['lookback_hours'], 1) . 'h' : '—'; ?></strong></span>
            <span>Entries: <strong><?php echo ($fp['n_entries'] ?? '—'); ?></strong></span>
            <span>Pumps: <strong><?php echo ($fp['n_independent_pumps'] ?? $fp['n_pumps'] ?? '—'); ?></strong></span>
            <span>Pump Rate: <strong><?php echo isset($fp['pump_rate_pct']) ? $fp['pump_rate_pct'] . '%' : '—'; ?></strong></span>
            <span>Patterns: <strong><?php echo ($fp['n_approved_patterns'] ?? 0); ?></strong></span>
            <span>Combos: <strong><?php echo ($fp['n_combinations'] ?? 0); ?></strong></span>
        </div>

        <?php
        $features = $fp['top_features'] ?? [];
        if ($features):
            $max_sep = max(array_column($features, 'separation')) ?: 1;
        ?>
        <div class="section-title" style="font-size: 0.85rem; margin-top: 1rem;">Top Features by Separation</div>
        <?php foreach ($features as $i => $f): ?>
            <div class="fp-feature-row">
                <span class="text-dim" style="width: 20px; text-align: right; font-size: 0.75rem;"><?php echo $i + 1; ?></span>
                <span class="fp-feature-name" title="<?php echo htmlspecialchars($f['feature']); ?>"><?php echo htmlspecialchars($f['feature']); ?></span>
                <div class="fp-bar-wrap">
                    <div class="fp-bar" style="width: <?php echo round($f['separation'] / $max_sep * 100, 1); ?>%;
                        <?php echo $f['rule_eligible'] ? '' : 'opacity: 0.35;'; ?>"></div>
                </div>
                <span class="fp-sep-val"><?php echo number_format($f['separation'], 3); ?></span>
                <span class="fp-eligible" title="<?php echo $f['rule_eligible'] ? 'Rule eligible' : 'Excluded (dip proxy)'; ?>">
                    <?php echo $f['rule_eligible'] ? '<span style="color:#10b981;">&#10003;</span>' : '<span style="color:rgba(255,255,255,0.2);">&#10005;</span>'; ?>
                </span>
            </div>
        <?php endforeach; ?>
        <?php endif; ?>

        <?php
        $patterns = $fp['approved_patterns'] ?? [];
        if ($patterns):
        ?>
        <div class="section-title" style="font-size: 0.85rem; margin-top: 1.5rem;">Approved Patterns</div>
        <table class="pump-table">
            <thead><tr>
                <th>Cluster</th>
                <th>Precision</th>
                <th>Pumps</th>
                <th>Features</th>
            </tr></thead>
            <tbody>
            <?php foreach ($patterns as $p): ?>
                <tr>
                    <td class="mono"><?php echo htmlspecialchars($p['cluster_id'] ?? '—'); ?></td>
                    <td><?php echo isset($p['precision']) ? round($p['precision'] * 100, 1) . '%' : '—'; ?></td>
                    <td><?php echo ($p['n_pumps'] ?? '—'); ?></td>
                    <td class="mono" style="font-size: 0.72rem; max-width: 400px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;" title="<?php echo htmlspecialchars(implode(', ', $p['features'] ?? [])); ?>">
                        <?php echo htmlspecialchars(implode(', ', $p['features'] ?? [])); ?>
                    </td>
                </tr>
            <?php endforeach; ?>
            </tbody>
        </table>
        <?php endif; ?>

        <?php
        $combos = $fp['combinations'] ?? [];
        if ($combos):
        ?>
        <div class="section-title" style="font-size: 0.85rem; margin-top: 1.5rem;">Top Combinations</div>
        <table class="pump-table">
            <thead><tr>
                <th>Features</th>
                <th>Precision</th>
                <th>Support</th>
            </tr></thead>
            <tbody>
            <?php foreach ($combos as $c): ?>
                <tr>
                    <td class="mono" style="font-size: 0.72rem;"><?php echo htmlspecialchars(implode(' + ', $c['features'] ?? [])); ?></td>
                    <td><?php echo isset($c['precision']) ? round($c['precision'] * 100, 1) . '%' : '—'; ?></td>
                    <td><?php echo ($c['support'] ?? '—'); ?></td>
                </tr>
            <?php endforeach; ?>
            </tbody>
        </table>
        <?php endif; ?>
    <?php endif; ?>
</div>

<!-- Two-column: Outcomes + Continuation -->
<div class="two-col-grid">
    <!-- Signal Outcomes -->
    <div class="panel-card">
        <div class="section-title">Recent Signal Outcomes <span class="text-dim" style="font-size: 0.75rem; font-weight: 400;">(<?php echo count($outcomes); ?> rows)</span></div>
        <?php if (empty($outcomes)): ?>
            <p class="text-dim">No signal outcomes in the selected window.</p>
        <?php else: ?>
        <div style="overflow-x: auto;">
            <table class="pump-table">
                <thead><tr>
                    <th>Time</th>
                    <th>Rule / Pattern</th>
                    <th>Conf</th>
                    <th>Readiness</th>
                    <th>Result</th>
                    <th>Gain %</th>
                </tr></thead>
                <tbody>
                <?php foreach ($outcomes as $o): ?>
                    <tr>
                        <td class="mono" style="white-space: nowrap;">
                            <?php
                            if ($o['created_at']) {
                                $dt = new DateTime($o['created_at']);
                                echo $dt->format('m-d H:i:s');
                            } else {
                                echo '—';
                            }
                            ?>
                        </td>
                        <td class="mono" style="font-size: 0.72rem; max-width: 160px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;" title="<?php echo htmlspecialchars(($o['rule_id'] ?? '') . ' / ' . ($o['pattern_id'] ?? '')); ?>">
                            <?php echo htmlspecialchars($o['rule_id'] ?? '—'); ?><br>
                            <span class="text-dim"><?php echo htmlspecialchars($o['pattern_id'] ?? ''); ?></span>
                        </td>
                        <td><?php echo isset($o['confidence']) ? round($o['confidence'] * 100, 1) . '%' : '—'; ?></td>
                        <td><?php echo isset($o['readiness_score']) ? number_format($o['readiness_score'], 3) : '—'; ?></td>
                        <td>
                            <?php if ($o['hit_target'] === true): ?>
                                <span class="hit-badge win">WIN</span>
                            <?php elseif ($o['hit_target'] === false): ?>
                                <span class="hit-badge loss">LOSS</span>
                            <?php else: ?>
                                <span class="text-dim">—</span>
                            <?php endif; ?>
                        </td>
                        <td class="mono <?php echo ($o['gain_pct'] ?? 0) >= 0 ? 'text-success' : 'text-danger'; ?>">
                            <?php echo $o['gain_pct'] !== null ? number_format($o['gain_pct'], 4) . '%' : '—'; ?>
                        </td>
                    </tr>
                <?php endforeach; ?>
                </tbody>
            </table>
        </div>
        <?php endif; ?>
    </div>

    <!-- Continuation History -->
    <div class="panel-card">
        <div class="section-title">Continuation History <span class="text-dim" style="font-size: 0.75rem; font-weight: 400;">(<?php echo count($continuations); ?> rows)</span></div>
        <?php if (empty($continuations)): ?>
            <p class="text-dim">No continuation checks in the selected window.</p>
        <?php else: ?>
        <div style="overflow-x: auto;">
            <table class="pump-table">
                <thead><tr>
                    <th>Time</th>
                    <th>Buyin</th>
                    <th>Result</th>
                    <th>Rules</th>
                    <th>1m Chg</th>
                    <th>Reason</th>
                </tr></thead>
                <tbody>
                <?php foreach ($continuations as $c): ?>
                    <tr>
                        <td class="mono" style="white-space: nowrap;">
                            <?php
                            if ($c['created_at']) {
                                $dt = new DateTime($c['created_at']);
                                echo $dt->format('m-d H:i:s');
                            } else {
                                echo '—';
                            }
                            ?>
                        </td>
                        <td class="mono">#<?php echo ($c['buyin_id'] ?? '—'); ?></td>
                        <td>
                            <?php if ($c['passed'] === true): ?>
                                <span class="pass-badge yes">PASS</span>
                            <?php elseif ($c['passed'] === false): ?>
                                <span class="pass-badge no">FAIL</span>
                            <?php else: ?>
                                <span class="text-dim">—</span>
                            <?php endif; ?>
                        </td>
                        <td><?php echo ($c['rules_checked'] ?? '—'); ?></td>
                        <td class="mono <?php echo ($c['pre_entry_change_1m'] ?? 0) >= 0 ? 'text-success' : 'text-danger'; ?>">
                            <?php echo $c['pre_entry_change_1m'] !== null ? number_format($c['pre_entry_change_1m'], 4) . '%' : '—'; ?>
                        </td>
                        <td style="max-width: 200px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; font-size: 0.75rem;" title="<?php echo htmlspecialchars($c['reason'] ?? ''); ?>">
                            <?php echo htmlspecialchars($c['reason'] ?? '—'); ?>
                        </td>
                    </tr>
                <?php endforeach; ?>
                </tbody>
            </table>
        </div>
        <?php endif; ?>
    </div>
</div>

<!-- ═══════════════════════════════════════════════════════════════════════════
     RISK GRADIENT & OPPORTUNITY FUNNEL
     ═══════════════════════════════════════════════════════════════════════════ -->

<div style="margin-top: 2rem; padding-top: 1.5rem; border-top: 1px solid rgba(255,255,255,0.06);">
    <div class="d-flex align-items-center justify-content-between flex-wrap gap-2 mb-3">
        <div>
            <h6 class="fw-semibold mb-0">Risk Gradient & Opportunity Funnel</h6>
            <p class="text-muted mb-0" style="font-size: 0.82rem;">How many pump opportunities exist daily and what risk levels could capture them</p>
        </div>
    </div>

    <!-- Panel A: Opportunity Funnel Chart -->
    <div class="panel-card">
        <div class="section-title">Daily Opportunity Funnel
            <span class="text-dim" style="font-size: 0.75rem; font-weight: 400;">
                — <?php echo ($funnel_summary['total_cycles'] ?? 0); ?> cycles,
                <?php echo ($funnel_summary['total_trades'] ?? 0); ?> trades
                (<?php echo ($funnel_summary['avg_trades_per_day'] ?? 0); ?>/day avg)
            </span>
        </div>
        <?php if (empty($daily_funnel)): ?>
            <p class="text-dim">No funnel data available for the selected window.</p>
        <?php else: ?>
            <div id="funnelChart"></div>
        <?php endif; ?>
    </div>

    <!-- Panel B: Risk Level Cards -->
    <div class="section-title" style="margin-top: 0.5rem;">Risk Levels</div>
    <div class="risk-cards-grid">
        <!-- Conservative (Current) -->
        <div class="risk-card conservative">
            <div class="risk-level">Conservative <span class="risk-badge-active">ACTIVE</span></div>
            <div class="risk-title">All gates enforced</div>
            <div class="risk-est">
                <?php echo ($funnel_summary['avg_trades_per_day'] ?? '1-5'); ?><span style="font-size: 0.8rem; font-weight: 400;"> trades/day</span>
            </div>
            <div class="risk-desc">
                Win rate: <strong><?php echo ($funnel_summary['win_rate_pct'] ?? '—'); echo ($funnel_summary['win_rate_pct'] !== null ? '%' : ''); ?></strong> measured across <?php echo ($funnel_summary['total_all_time_trades'] ?? 0); ?> trades
            </div>
            <ul class="gate-list">
                <li class="gate-on"><strong>Fingerprint match</strong> — pattern must match approved V4 rule</li>
                <li class="gate-on"><strong>Crash gate</strong> — blocks if 30s micro-trend &lt; -0.05%</li>
                <li class="gate-on"><strong>Chase gate</strong> — blocks if 1m price already up &gt;0.15%</li>
                <li class="gate-on"><strong>Circuit breaker</strong> — pauses all signals if live win rate &lt;35%</li>
                <li class="gate-on"><strong>Continuation check</strong> — requires price to be rising at entry</li>
            </ul>
        </div>

        <!-- Moderate (Future) -->
        <div class="risk-card moderate">
            <div class="risk-level">Moderate <span class="risk-badge-future">COMING SOON</span></div>
            <div class="risk-title">Relaxed gates</div>
            <div class="risk-est">5–20 <span style="font-size: 0.8rem; font-weight: 400;">est. trades/day</span></div>
            <div class="risk-desc">
                Estimated win rate: <strong>~35–45%</strong><br>
                Captures more opportunities by loosening the chase detection and lowering readiness threshold to 0.45.
            </div>
            <ul class="gate-list">
                <li class="gate-on"><strong>Fingerprint match</strong> — still required</li>
                <li class="gate-on"><strong>Crash gate</strong> — still blocks crashes</li>
                <li class="gate-off"><strong>Chase gate</strong> — disabled, allows buying into momentum</li>
                <li class="gate-on"><strong>Circuit breaker</strong> — still active (safety net)</li>
                <li class="gate-off"><strong>Continuation check</strong> — relaxed, fires on flat price too</li>
            </ul>
        </div>

        <!-- Aggressive (Future) -->
        <div class="risk-card aggressive">
            <div class="risk-level">Aggressive <span class="risk-badge-future">COMING SOON</span></div>
            <div class="risk-title">Minimal gates</div>
            <div class="risk-est">20–100 <span style="font-size: 0.8rem; font-weight: 400;">est. trades/day</span></div>
            <div class="risk-desc">
                Estimated win rate: <strong>~25–35%</strong><br>
                Fires on any fingerprint match. Higher volume, lower precision. Best for gathering more data quickly.
            </div>
            <ul class="gate-list">
                <li class="gate-on"><strong>Fingerprint match</strong> — still required</li>
                <li class="gate-off"><strong>Crash gate</strong> — disabled</li>
                <li class="gate-off"><strong>Chase gate</strong> — disabled</li>
                <li class="gate-off"><strong>Circuit breaker</strong> — disabled</li>
                <li class="gate-off"><strong>Continuation check</strong> — disabled</li>
            </ul>
        </div>
    </div>

    <!-- Panel C: Historical Trades Scatter -->
    <div class="panel-card">
        <div class="section-title">Historical Play #3 Trades
            <span class="text-dim" style="font-size: 0.75rem; font-weight: 400;">
                — <?php echo count($all_time_trades); ?> total,
                win rate <?php echo ($funnel_summary['win_rate_pct'] ?? '—'); echo ($funnel_summary['win_rate_pct'] !== null ? '%' : ''); ?>
            </span>
        </div>
        <?php if (empty($all_time_trades)): ?>
            <p class="text-dim">No play #3 trades found yet.</p>
        <?php else: ?>
            <div id="scatterChart"></div>
        <?php endif; ?>
    </div>
</div>

<?php $content = ob_get_clean(); ?>

<?php
// ── Scripts ─────────────────────────────────────────────────────────────────
ob_start();
?>
<script src="<?php echo $baseUrl; ?>/assets/libs/apexcharts/apexcharts.min.js"></script>
<script>
function applyFilters() {
    const hours = document.getElementById('hoursSelect').value;
    const refresh = document.getElementById('refreshSelect').value;
    window.location.href = '?hours=' + hours + '&refresh=' + refresh;
}

// Auto-refresh
setTimeout(function() {
    window.location.reload();
}, <?php echo $refresh * 1000; ?>);

// ── Opportunity Funnel Chart ────────────────────────────────────────────────
<?php if (!empty($daily_funnel)): ?>
(function() {
    var funnel = <?php echo json_encode($daily_funnel); ?>;
    var dates = funnel.map(function(d) { return d.date; });

    var opts = {
        chart: {
            type: 'bar',
            height: 320,
            stacked: true,
            background: 'transparent',
            toolbar: { show: false },
            fontFamily: 'inherit',
        },
        series: [
            { name: 'Passed (favorable)', data: funnel.map(function(d) { return d.cont_passed; }) },
            { name: 'Flat / not rising',  data: funnel.map(function(d) { return d.cont_flat; }) },
            { name: 'Trail errors',       data: funnel.map(function(d) { return d.cont_errors; }) },
        ],
        colors: ['#14b8a6', '#f59e0b', '#f97316'],
        plotOptions: {
            bar: { columnWidth: '60%', borderRadius: 3, borderRadiusApplication: 'end' },
        },
        xaxis: {
            categories: dates,
            labels: { style: { colors: 'rgba(255,255,255,0.4)', fontSize: '11px' } },
            axisBorder: { show: false },
            axisTicks: { show: false },
        },
        yaxis: {
            labels: {
                style: { colors: 'rgba(255,255,255,0.4)', fontSize: '11px' },
                formatter: function(v) { return v >= 1000 ? (v/1000).toFixed(1) + 'k' : v; }
            },
        },
        grid: {
            borderColor: 'rgba(255,255,255,0.04)',
            strokeDashArray: 3,
        },
        legend: {
            position: 'top',
            horizontalAlign: 'left',
            labels: { colors: 'rgba(255,255,255,0.6)' },
            fontSize: '12px',
            markers: { size: 4, offsetX: -4 },
            itemMargin: { horizontal: 12 },
        },
        tooltip: {
            theme: 'dark',
            y: { formatter: function(v) { return v.toLocaleString() + ' checks'; } },
        },
        dataLabels: { enabled: false },
        annotations: {
            points: funnel.filter(function(d){ return d.trades_made > 0; }).map(function(d) {
                return {
                    x: d.date,
                    y: d.cont_passed + d.cont_flat + d.cont_errors + 200,
                    marker: { size: 6, fillColor: '#10b981', strokeColor: '#10b981', shape: 'circle' },
                    label: {
                        text: d.trades_made + ' trade' + (d.trades_made !== 1 ? 's' : ''),
                        borderColor: '#10b981',
                        style: { color: '#fff', background: 'rgba(16,185,129,0.25)', fontSize: '10px', padding: { left: 4, right: 4, top: 1, bottom: 1 } },
                        offsetY: -10
                    }
                };
            })
        }
    };
    new ApexCharts(document.querySelector('#funnelChart'), opts).render();
})();
<?php endif; ?>

// ── Historical Trades Scatter ───────────────────────────────────────────────
<?php if (!empty($all_time_trades)): ?>
(function() {
    var trades = <?php echo json_encode($all_time_trades); ?>;
    var wins = [], losses = [];
    trades.forEach(function(t) {
        if (t.gain_pct === null) return;
        var point = { x: new Date(t.created_at).getTime(), y: t.gain_pct };
        if (t.our_profit_loss !== null && t.our_profit_loss > 0) {
            wins.push(point);
        } else {
            losses.push(point);
        }
    });

    var opts = {
        chart: {
            type: 'scatter',
            height: 300,
            background: 'transparent',
            toolbar: { show: false },
            fontFamily: 'inherit',
            zoom: { enabled: true, type: 'x' },
        },
        series: [
            { name: 'Win',  data: wins },
            { name: 'Loss', data: losses },
        ],
        colors: ['#10b981', '#ef4444'],
        markers: { size: 6, hover: { sizeOffset: 2 } },
        xaxis: {
            type: 'datetime',
            labels: { style: { colors: 'rgba(255,255,255,0.4)', fontSize: '11px' } },
            axisBorder: { show: false },
            axisTicks: { show: false },
        },
        yaxis: {
            title: { text: 'Gain %', style: { color: 'rgba(255,255,255,0.4)', fontSize: '11px' } },
            labels: {
                style: { colors: 'rgba(255,255,255,0.4)', fontSize: '11px' },
                formatter: function(v) { return v.toFixed(2) + '%'; }
            },
        },
        grid: {
            borderColor: 'rgba(255,255,255,0.04)',
            strokeDashArray: 3,
        },
        legend: {
            position: 'top',
            horizontalAlign: 'left',
            labels: { colors: 'rgba(255,255,255,0.6)' },
            fontSize: '12px',
            markers: { size: 5, offsetX: -4 },
        },
        tooltip: {
            theme: 'dark',
            x: { format: 'MMM dd, HH:mm' },
            y: { formatter: function(v) { return v.toFixed(4) + '%'; } },
        },
        annotations: {
            yaxis: [{
                y: 0,
                borderColor: 'rgba(255,255,255,0.12)',
                strokeDashArray: 4,
                label: {
                    text: 'Break even',
                    position: 'front',
                    style: { color: 'rgba(255,255,255,0.35)', background: 'transparent', fontSize: '10px' }
                }
            }]
        }
    };
    new ApexCharts(document.querySelector('#scatterChart'), opts).render();
})();
<?php endif; ?>
</script>
<?php $scripts = ob_get_clean(); ?>

<?php include __DIR__ . '/../../layouts/base.php'; ?>
