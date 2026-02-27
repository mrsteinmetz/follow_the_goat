<?php
/**
 * Pump Analytics Dashboard
 *
 * Real-time overview of the GA-based directional precision pump detection system:
 * pipeline explainer, feature engineering reference, active plays (3-6),
 * GA-discovered simulation rules, signal outcomes, win rates, and opportunity funnel.
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

/* How It Works / explainer */
.explainer-toggle {
    background: none;
    border: none;
    color: rgba(255,255,255,0.45);
    font-size: 0.78rem;
    cursor: pointer;
    padding: 0;
    margin-left: auto;
    display: flex;
    align-items: center;
    gap: 0.35rem;
}
.explainer-toggle:hover { color: rgba(255,255,255,0.8); }
.pipeline-flow {
    display: flex;
    align-items: flex-start;
    gap: 0;
    flex-wrap: wrap;
    margin: 1rem 0;
}
.pipeline-step {
    display: flex;
    flex-direction: column;
    align-items: center;
    min-width: 130px;
    flex: 1;
}
.pipeline-box {
    background: rgba(var(--primary-rgb), 0.1);
    border: 1px solid rgba(var(--primary-rgb), 0.25);
    border-radius: 0.6rem;
    padding: 0.65rem 0.75rem;
    text-align: center;
    width: 100%;
}
.pipeline-box .pb-title {
    font-size: 0.75rem;
    font-weight: 700;
    color: rgba(255,255,255,0.9);
    margin-bottom: 0.2rem;
}
.pipeline-box .pb-detail {
    font-size: 0.68rem;
    color: rgba(255,255,255,0.5);
    line-height: 1.4;
}
.pipeline-arrow {
    align-self: center;
    font-size: 1rem;
    color: rgba(var(--primary-rgb), 0.5);
    padding: 0 0.3rem;
    flex-shrink: 0;
}
@media (max-width: 900px) {
    .pipeline-flow { flex-direction: column; }
    .pipeline-arrow { transform: rotate(90deg); align-self: flex-start; margin-left: 50%; }
    .pipeline-step { min-width: unset; }
}
.method-pills {
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem;
    margin-top: 0.75rem;
}
.method-pill {
    background: rgba(255,255,255,0.05);
    border: 1px solid rgba(255,255,255,0.1);
    border-radius: 2rem;
    padding: 0.25rem 0.75rem;
    font-size: 0.75rem;
    color: rgba(255,255,255,0.65);
}
.method-pill strong { color: rgba(255,255,255,0.9); }

/* Feature Engineering panel */
.feat-group {
    margin-bottom: 0.75rem;
}
.feat-group-header {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    cursor: pointer;
    padding: 0.5rem 0;
    border-bottom: 1px solid rgba(255,255,255,0.05);
    user-select: none;
}
.feat-group-header:hover { background: rgba(255,255,255,0.02); }
.feat-group-label {
    font-size: 0.82rem;
    font-weight: 600;
    color: rgba(255,255,255,0.75);
    flex: 1;
}
.feat-group-count {
    font-size: 0.72rem;
    color: rgba(255,255,255,0.35);
    margin-right: 0.5rem;
}
.feat-group-chevron {
    font-size: 0.65rem;
    color: rgba(255,255,255,0.3);
    transition: transform 0.2s;
}
.feat-group-chevron.open { transform: rotate(180deg); }
.feat-table {
    width: 100%;
    border-collapse: collapse;
    margin-top: 0.25rem;
}
.feat-table td {
    padding: 0.35rem 0.5rem;
    font-size: 0.78rem;
    border-bottom: 1px solid rgba(255,255,255,0.04);
    vertical-align: top;
}
.feat-table td:first-child {
    font-family: monospace;
    color: rgba(255,255,255,0.7);
    white-space: nowrap;
    width: 220px;
}
.feat-table td:last-child {
    color: rgba(255,255,255,0.5);
    line-height: 1.45;
}
.feat-source-badge {
    display: inline-block;
    font-size: 0.65rem;
    padding: 0.1rem 0.45rem;
    border-radius: 0.25rem;
    font-weight: 600;
    margin-right: 0.35rem;
    vertical-align: middle;
}
.feat-source-ob  { background: rgba(56,189,248,0.12); color: #38bdf8; }
.feat-source-tr  { background: rgba(167,139,250,0.12); color: #a78bfa; }
.feat-source-wh  { background: rgba(251,191,36,0.12); color: #fbbf24; }
.feat-source-pm  { background: rgba(16,185,129,0.12); color: #10b981; }

/* Active plays */
.plays-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 1rem;
    margin-bottom: 1.5rem;
}
@media (max-width: 1100px) {
    .plays-grid { grid-template-columns: repeat(2, 1fr); }
}
@media (max-width: 600px) {
    .plays-grid { grid-template-columns: 1fr; }
}
.play-card {
    border-radius: 0.75rem;
    padding: 1.1rem;
    border: 1px solid rgba(255,255,255,0.08);
    background: rgba(var(--body-bg-rgb2), 1);
    position: relative;
}
.play-card-id {
    font-size: 0.65rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    margin-bottom: 0.3rem;
    opacity: 0.6;
}
.play-card-name {
    font-size: 1rem;
    font-weight: 700;
    color: #fff;
    margin-bottom: 0.6rem;
}
.play-card-filters {
    list-style: none;
    padding: 0;
    margin: 0 0 0.6rem 0;
    font-size: 0.76rem;
}
.play-card-filters li {
    padding: 0.15rem 0;
    color: rgba(255,255,255,0.55);
    display: flex;
    align-items: baseline;
    gap: 0.4rem;
}
.play-card-filters li::before {
    content: '›';
    color: rgba(255,255,255,0.25);
    font-size: 0.9rem;
}
.play-card-filters strong { color: rgba(255,255,255,0.85); }
.play-card-desc {
    font-size: 0.75rem;
    color: rgba(255,255,255,0.4);
    line-height: 1.45;
}
.play-card.p3 { border-color: rgba(56,189,248,0.2); }
.play-card.p4 { border-color: rgba(245,158,11,0.2); }
.play-card.p5 { border-color: rgba(16,185,129,0.2); }
.play-card.p6 { border-color: rgba(167,139,250,0.2); }
.play-card.p3 .play-card-id { color: #38bdf8; }
.play-card.p4 .play-card-id { color: #f59e0b; }
.play-card.p5 .play-card-id { color: #10b981; }
.play-card.p6 .play-card-id { color: #a78bfa; }

/* What This Produces strip */
.produces-strip {
    display: flex;
    flex-wrap: wrap;
    gap: 0.75rem 1.5rem;
    padding: 0.85rem 1.1rem;
    background: rgba(var(--primary-rgb), 0.06);
    border: 1px solid rgba(var(--primary-rgb), 0.15);
    border-radius: 0.65rem;
    margin-bottom: 1.5rem;
    font-size: 0.78rem;
}
.produces-item {
    color: rgba(255,255,255,0.55);
    display: flex;
    align-items: center;
    gap: 0.4rem;
}
.produces-item strong { color: rgba(255,255,255,0.85); }
.produces-divider {
    width: 1px;
    background: rgba(255,255,255,0.08);
    align-self: stretch;
}
@media (max-width: 700px) { .produces-divider { display: none; } }

/* GA methodology note */
.ga-note {
    background: rgba(255,255,255,0.03);
    border-left: 3px solid rgba(var(--primary-rgb), 0.4);
    border-radius: 0 0.5rem 0.5rem 0;
    padding: 0.75rem 1rem;
    margin-bottom: 1rem;
    font-size: 0.8rem;
    color: rgba(255,255,255,0.55);
    line-height: 1.6;
}
.ga-note strong { color: rgba(255,255,255,0.8); }

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
        <p class="text-muted mb-0" style="font-size: 0.85rem;">GA-evolved directional precision engine &mdash; multi-play live signal detection</p>
    </div>
    <div class="d-flex align-items-center gap-2 flex-wrap">
        <button id="exportBtn" onclick="exportFeatureData()" style="
            background: rgba(var(--primary-rgb), 0.15);
            border: 1px solid rgba(var(--primary-rgb), 0.35);
            color: rgb(var(--primary-rgb));
            padding: 0.4rem 1rem;
            border-radius: 0.5rem;
            font-size: 0.82rem;
            font-weight: 600;
            cursor: pointer;
            display: flex;
            align-items: center;
            gap: 0.4rem;
            transition: background 0.15s;
        " onmouseover="this.style.background='rgba(var(--primary-rgb),0.25)'" onmouseout="this.style.background='rgba(var(--primary-rgb),0.15)'">
            <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
            Export Features CSV
        </button>
        <span id="exportStatus" style="font-size:0.75rem;color:rgba(255,255,255,.4);"></span>
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

<!-- How This Feature Works -->
<div class="panel-card" style="margin-bottom: 1.5rem;">
    <div class="section-title" style="display:flex;align-items:center;">
        How This Feature Works
        <button class="explainer-toggle" onclick="toggleExplainer()" id="explainerToggleBtn">
            <span id="explainerToggleLabel">hide</span> &#9650;
        </button>
    </div>
    <div id="explainerBody">
        <!-- Pipeline flow -->
        <div class="pipeline-flow">
            <div class="pipeline-step">
                <div class="pipeline-box">
                    <div class="pb-title">Raw Data</div>
                    <div class="pb-detail">Binance order book snapshots (1s), SOL/USD trades, on-chain whale events</div>
                </div>
            </div>
            <div class="pipeline-arrow">&#8594;</div>
            <div class="pipeline-step">
                <div class="pipeline-box">
                    <div class="pb-title">Feature Engineering</div>
                    <div class="pb-detail">26 features bucketed to 30s, 5-min rolling windows: order book, trades, whale, momentum</div>
                </div>
            </div>
            <div class="pipeline-arrow">&#8594;</div>
            <div class="pipeline-step">
                <div class="pipeline-box">
                    <div class="pb-title">Genetic Algorithm</div>
                    <div class="pb-detail">pop=600, gen=250, 96h lookback. Evolves 2&ndash;4 condition entry rules. Runs every ~20 min.</div>
                </div>
            </div>
            <div class="pipeline-arrow">&#8594;</div>
            <div class="pipeline-step">
                <div class="pipeline-box">
                    <div class="pb-title">OOS Validation</div>
                    <div class="pb-detail">4-fold walk-forward. Rule must hit &ge;55% in-sample AND &ge;52% on &ge;3 of 4 OOS folds to pass.</div>
                </div>
            </div>
            <div class="pipeline-arrow">&#8594;</div>
            <div class="pipeline-step">
                <div class="pipeline-box">
                    <div class="pb-title">simulation_results</div>
                    <div class="pb-detail">Approved rules stored in PostgreSQL. Each play reads rules matching its own win-rate threshold.</div>
                </div>
            </div>
            <div class="pipeline-arrow">&#8594;</div>
            <div class="pipeline-step">
                <div class="pipeline-box">
                    <div class="pb-title">Live Signal Check</div>
                    <div class="pb-detail">train_validator runs every 5s. Applies live features to each rule. Fires buyin for Play 3/4/5/6.</div>
                </div>
            </div>
        </div>

        <!-- Goal & anti-overfitting -->
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:1rem;margin-top:1rem;">
            <div>
                <div style="font-size:0.75rem;font-weight:700;text-transform:uppercase;letter-spacing:.05em;color:rgba(255,255,255,.4);margin-bottom:.5rem;">Target &amp; Goal</div>
                <div class="method-pills">
                    <span class="method-pill"><strong>Objective:</strong> SOL price up &ge;0.1% within 7 minutes of signal</span>
                    <span class="method-pill"><strong>Pump base rate:</strong> ~40&ndash;50% of all 30s windows</span>
                    <span class="method-pill"><strong>Min precision:</strong> rules must beat base rate significantly</span>
                    <span class="method-pill"><strong>Pre-entry guard:</strong> skip if price already rose &gt;0.15% in last 2 min</span>
                </div>
            </div>
            <div>
                <div style="font-size:0.75rem;font-weight:700;text-transform:uppercase;letter-spacing:.05em;color:rgba(255,255,255,.4);margin-bottom:.5rem;">Anti-Overfitting Measures</div>
                <div class="method-pills">
                    <span class="method-pill"><strong>No exit optimisation in GA</strong> &mdash; v1 got 93% in-sample by gaming exits; collapsed to 43% live</span>
                    <span class="method-pill"><strong>96h lookback</strong> (was 25h in v1) &mdash; harder to overfit</span>
                    <span class="method-pill"><strong>Hard OOS reject:</strong> any fold below 52% precision = rule discarded</span>
                    <span class="method-pill"><strong>Diversity injection:</strong> if GA stagnates 40 gens, bottom 50% replaced with fresh randoms</span>
                </div>
            </div>
        </div>
        @media (max-width: 900px) { #explainerBody > div:last-child { grid-template-columns: 1fr; } }
    </div>
</div>

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

<!-- Feature Engineering Panel -->
<div class="panel-card">
    <div class="section-title" style="display:flex;align-items:center;">
        Feature Engineering
        <span style="font-size:0.72rem;font-weight:400;color:rgba(255,255,255,.4);margin-left:0.75rem;">26 features &bull; 30s buckets &bull; 5-min rolling windows</span>
        <button class="explainer-toggle" onclick="toggleFeatures()" id="featToggleBtn" style="margin-left:auto;">
            <span id="featToggleLabel">show</span> &#9660;
        </button>
    </div>
    <div id="featBody" style="display:none;">
        <p style="font-size:0.8rem;color:rgba(255,255,255,.5);margin-bottom:1rem;line-height:1.6;">
            Raw tick data from three sources (order book, trades, whale events) is resampled into 30-second buckets.
            Each feature is then computed as a 5-minute rolling aggregate of those buckets.
            A separate 1-minute rolling window is used to compute <em>acceleration</em> features (short-term vs baseline).
            The GA operates on this feature matrix to discover entry rules of the form
            <code style="background:rgba(255,255,255,.06);padding:.1rem .35rem;border-radius:.25rem;font-size:.78rem;">feature &gt; threshold AND feature2 &lt; threshold2 [AND ...]</code>.
        </p>

        <!-- Order Book: 5-min rolling -->
        <div class="feat-group">
            <div class="feat-group-header" onclick="toggleFeatGroup('ob5', this)">
                <span class="feat-source-badge feat-source-ob">OB</span>
                <span class="feat-group-label">Order Book &mdash; 5-min Rolling Averages</span>
                <span class="feat-group-count">5 features</span>
                <span class="feat-group-chevron" id="chev-ob5">&#9660;</span>
            </div>
            <div id="fg-ob5" style="display:none;">
            <table class="feat-table">
                <tr><td>ob_avg_vol_imb</td><td>(bid_vol &minus; ask_vol) / total vol &mdash; positive = buy-side pressure dominates</td></tr>
                <tr><td>ob_avg_depth_ratio</td><td>bid depth / ask depth over 5-min window &mdash; &gt;1 means more buy-side liquidity</td></tr>
                <tr><td>ob_avg_spread_bps</td><td>bid-ask spread in basis points (lower = tighter market, more liquid)</td></tr>
                <tr><td>ob_net_liq_change</td><td>sum of net_liq_1s over window &mdash; net liquidity flow, positive = liquidity building on buy side</td></tr>
                <tr><td>ob_bid_ask_ratio</td><td>bid_liq / ask_liq &mdash; overall liquidity skew across all price levels</td></tr>
            </table>
            </div>
        </div>

        <!-- Order Book: acceleration -->
        <div class="feat-group">
            <div class="feat-group-header" onclick="toggleFeatGroup('obac', this)">
                <span class="feat-source-badge feat-source-ob">OB</span>
                <span class="feat-group-label">Order Book &mdash; 1-min Acceleration vs 5-min Baseline</span>
                <span class="feat-group-count">3 features</span>
                <span class="feat-group-chevron" id="chev-obac">&#9660;</span>
            </div>
            <div id="fg-obac" style="display:none;">
            <table class="feat-table">
                <tr><td>ob_imb_trend</td><td>ob_imb_1m &minus; ob_imb_5m &mdash; is volume imbalance accelerating short-term vs the recent baseline?</td></tr>
                <tr><td>ob_depth_trend</td><td>ob_depth_1m &minus; ob_depth_5m &mdash; is buy-side depth building faster than average?</td></tr>
                <tr><td>ob_liq_accel</td><td>ob_bid_ask_1m &minus; ob_bid_ask_5m &mdash; is liquidity skew rising in the last minute?</td></tr>
            </table>
            </div>
        </div>

        <!-- Order Book: microstructure -->
        <div class="feat-group">
            <div class="feat-group-header" onclick="toggleFeatGroup('obms', this)">
                <span class="feat-source-badge feat-source-ob">OB</span>
                <span class="feat-group-label">Order Book &mdash; Microstructure Signals</span>
                <span class="feat-group-count">3 features</span>
                <span class="feat-group-chevron" id="chev-obms">&#9660;</span>
            </div>
            <div id="fg-obms" style="display:none;">
            <table class="feat-table">
                <tr><td>ob_slope_ratio</td><td>bid_slope / |ask_slope| &mdash; how steep is the buy side vs sell side of the book?</td></tr>
                <tr><td>ob_depth_5bps_ratio</td><td>bid_dep_5bps / ask_dep_5bps &mdash; close-to-mid depth imbalance within 5bps of mid price</td></tr>
                <tr><td>ob_microprice_dev</td><td>microprice &minus; mid_price &mdash; directional pressure signal; positive = micro buying pressure</td></tr>
            </table>
            </div>
        </div>

        <!-- Trades -->
        <div class="feat-group">
            <div class="feat-group-header" onclick="toggleFeatGroup('tr', this)">
                <span class="feat-source-badge feat-source-tr">TR</span>
                <span class="feat-group-label">Trades</span>
                <span class="feat-group-count">5 features</span>
                <span class="feat-group-chevron" id="chev-tr">&#9660;</span>
            </div>
            <div id="fg-tr" style="display:none;">
            <table class="feat-table">
                <tr><td>tr_buy_ratio</td><td>buy_vol / total_vol &mdash; fraction of volume on the buy side; &gt;0.55 = buying dominance</td></tr>
                <tr><td>tr_large_ratio</td><td>vol from trades &gt;50 SOL / total &mdash; institutional/whale trade activity proxy</td></tr>
                <tr><td>tr_buy_accel</td><td>1-min buy ratio / 5-min buy ratio &mdash; is momentum building? &gt;1 = acceleration</td></tr>
                <tr><td>tr_avg_size</td><td>average trade size in SOL &mdash; larger = more conviction per execution</td></tr>
                <tr><td>tr_n</td><td>total trade count in 5-min window &mdash; market activity level</td></tr>
            </table>
            </div>
        </div>

        <!-- Whale -->
        <div class="feat-group">
            <div class="feat-group-header" onclick="toggleFeatGroup('wh', this)">
                <span class="feat-source-badge feat-source-wh">WH</span>
                <span class="feat-group-label">Whale Activity</span>
                <span class="feat-group-count">6 features</span>
                <span class="feat-group-chevron" id="chev-wh">&#9660;</span>
            </div>
            <div id="fg-wh" style="display:none;">
            <table class="feat-table">
                <tr><td>wh_inflow_ratio</td><td>whale_in_sol / total_whale_sol &mdash; net accumulation fraction; &gt;0.6 = whales accumulating</td></tr>
                <tr><td>wh_net_flow</td><td>whale_in_sol &minus; whale_out_sol &mdash; signed net SOL flow from tracked whales</td></tr>
                <tr><td>wh_large_count</td><td>events with significance &gt;0.5 (MAJOR / SIGNIFICANT moves) in last 5 min</td></tr>
                <tr><td>wh_n</td><td>total whale event count in window &mdash; how active are tracked wallets?</td></tr>
                <tr><td>wh_avg_pct_moved</td><td>avg % of each whale&rsquo;s wallet moved &mdash; conviction signal; high = meaningful position change</td></tr>
                <tr><td>wh_urgency_ratio</td><td>fraction of events where &gt;50% of wallet was moved &mdash; urgent / panic moves</td></tr>
            </table>
            </div>
        </div>

        <!-- Price Momentum -->
        <div class="feat-group" style="margin-bottom:0;">
            <div class="feat-group-header" onclick="toggleFeatGroup('pm', this)">
                <span class="feat-source-badge feat-source-pm">PM</span>
                <span class="feat-group-label">Price Momentum</span>
                <span class="feat-group-count">4 features</span>
                <span class="feat-group-chevron" id="chev-pm">&#9660;</span>
            </div>
            <div id="fg-pm" style="display:none;">
            <table class="feat-table">
                <tr><td>pm_price_change_30s</td><td>price % change in last 30 seconds</td></tr>
                <tr><td>pm_price_change_1m</td><td>price % change in last 1 minute</td></tr>
                <tr><td>pm_price_change_5m</td><td>price % change in last 5 minutes</td></tr>
                <tr><td>pm_velocity_30s</td><td>momentum acceleration: 1m_change &minus; 5m_change &mdash; positive = price speeding up short-term</td></tr>
            </table>
            </div>
        </div>
    </div>
</div>

<!-- Simulation Rules (GA-Discovered) -->
<div class="panel-card">
    <div class="section-title">Simulation Rules <span style="font-weight:400;color:rgba(255,255,255,.4);font-size:0.85rem;">(GA-Discovered)</span></div>

    <div class="ga-note">
        Rules are discovered by the <strong>mega_simulator</strong> genetic algorithm (population 600, 250 generations, 96h of data).
        Each rule is <strong>2&ndash;4 conditions</strong> on the 26 features, e.g.
        <code style="background:rgba(255,255,255,.08);padding:.1rem .3rem;border-radius:.2rem;font-size:.78rem;">ob_avg_vol_imb &gt; 0.12 AND tr_buy_ratio &gt; 0.61</code>.
        A rule is only saved to <code style="background:rgba(255,255,255,.08);padding:.1rem .3rem;border-radius:.2rem;font-size:.78rem;">simulation_results</code> if it passes
        <strong>&ge;55% directional precision in-sample</strong> AND <strong>&ge;52% precision on at least 3 of 4 out-of-sample folds</strong>.
        Each play (3&ndash;6) then filters this table further by its own <em>win_rate</em> threshold before applying rules live.
    </div>

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

<!-- What This Produces -->
<div class="produces-strip">
    <span class="produces-item"><strong>Signal check cadence:</strong> every 5s (train_validator)</span>
    <span class="produces-divider"></span>
    <span class="produces-item"><strong>Rules refreshed:</strong> every ~20 min (mega_simulator GA run)</span>
    <span class="produces-divider"></span>
    <span class="produces-item"><strong>Plays running simultaneously:</strong> Play 3, 4, 5, 6</span>
    <span class="produces-divider"></span>
    <span class="produces-item"><strong>Rule format:</strong> 2&ndash;4 conditions &bull; <code style="background:rgba(255,255,255,.08);padding:.1rem .3rem;border-radius:.2rem;">feature OP threshold AND ...</code></span>
    <span class="produces-divider"></span>
    <span class="produces-item"><strong>Output:</strong> <code style="background:rgba(255,255,255,.08);padding:.1rem .3rem;border-radius:.2rem;">follow_the_goat_buyins</code> row per fired signal</span>
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
            <h6 class="fw-semibold mb-0">Active Plays &amp; Opportunity Funnel</h6>
            <p class="text-muted mb-0" style="font-size: 0.82rem;">The 4 simultaneously-running strategies and daily opportunity volume through the continuation filter</p>
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

    <!-- Panel B: Active Plays -->
    <div class="section-title" style="margin-top: 0.5rem;">Active Plays (Running Simultaneously)</div>
    <p style="font-size:0.8rem;color:rgba(255,255,255,.45);margin-bottom:1rem;line-height:1.6;">
        All four plays run in parallel via <code style="background:rgba(255,255,255,.07);padding:.1rem .3rem;border-radius:.2rem;font-size:.77rem;">train_validator</code> (every 5s).
        Each play reads GA rules from <code style="background:rgba(255,255,255,.07);padding:.1rem .3rem;border-radius:.2rem;font-size:.77rem;">simulation_results</code>
        filtered to its own <strong>win_rate threshold</strong>. Higher threshold = fewer but higher-precision rules = fewer signals.
        Each play has an independent cooldown preventing repeated signals on the same token.
    </p>
    <div class="plays-grid">
        <!-- Play 3 -->
        <div class="play-card p3">
            <div class="play-card-id">Play #3</div>
            <div class="play-card-name">Balanced</div>
            <ul class="play-card-filters">
                <li>Win-rate filter: <strong>&ge; 65%</strong></li>
                <li>Cooldown: <strong>120s</strong></li>
                <li>OOS requirement: <strong>standard</strong></li>
            </ul>
            <div class="play-card-desc">Baseline strategy. Balances signal frequency against precision. The primary reference play for evaluating system performance.</div>
        </div>
        <!-- Play 4 -->
        <div class="play-card p4">
            <div class="play-card-id">Play #4</div>
            <div class="play-card-name">Aggressive</div>
            <ul class="play-card-filters">
                <li>Win-rate filter: <strong>&ge; 55%</strong></li>
                <li>Cooldown: <strong>60s</strong></li>
                <li>OOS requirement: <strong>relaxed</strong></li>
            </ul>
            <div class="play-card-desc">Lower precision bar means more rules qualify and signals fire more often. Useful for volume-weighted data collection and A/B comparison.</div>
        </div>
        <!-- Play 5 -->
        <div class="play-card p5">
            <div class="play-card-id">Play #5</div>
            <div class="play-card-name">Conservative</div>
            <ul class="play-card-filters">
                <li>Win-rate filter: <strong>&ge; 75%</strong></li>
                <li>Cooldown: <strong>180s</strong></li>
                <li>OOS requirement: <strong>strict</strong></li>
            </ul>
            <div class="play-card-desc">Only the highest-precision rules qualify. Signals are infrequent but represent the strongest evidence of an upcoming pump.</div>
        </div>
        <!-- Play 6 -->
        <div class="play-card p6">
            <div class="play-card-id">Play #6</div>
            <div class="play-card-name">High-EV</div>
            <ul class="play-card-filters">
                <li>Win-rate filter: <strong>&ge; 65%</strong></li>
                <li>daily_ev filter: <strong>&ge; 0.002</strong></li>
                <li>Cooldown: <strong>120s</strong></li>
            </ul>
            <div class="play-card-desc">Same precision bar as Balanced but also requires strong expected-value signal volume (daily_ev). Targets rules that fire reliably <em>and</em> frequently.</div>
        </div>
    </div>

    <!-- Panel C: Historical Trades Scatter -->
    <div class="panel-card">
        <div class="section-title">Historical Trade Outcomes (Play #3 Reference)
            <span class="text-dim" style="font-size: 0.75rem; font-weight: 400;">
                — <?php echo count($all_time_trades); ?> total,
                win rate <?php echo ($funnel_summary['win_rate_pct'] ?? '—'); echo ($funnel_summary['win_rate_pct'] !== null ? '%' : ''); ?>
            </span>
        </div>
        <?php if (empty($all_time_trades)): ?>
            <p class="text-dim">No historical trade data found yet.</p>
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

// Explainer panel toggle
function toggleExplainer() {
    var body = document.getElementById('explainerBody');
    var label = document.getElementById('explainerToggleLabel');
    var btn = document.getElementById('explainerToggleBtn');
    if (body.style.display === 'none') {
        body.style.display = '';
        label.textContent = 'hide';
        btn.innerHTML = '<span id="explainerToggleLabel">hide</span> &#9650;';
    } else {
        body.style.display = 'none';
        label.textContent = 'show';
        btn.innerHTML = '<span id="explainerToggleLabel">show</span> &#9660;';
    }
}

// Feature panel toggle
function toggleFeatures() {
    var body = document.getElementById('featBody');
    var label = document.getElementById('featToggleLabel');
    var btn = document.getElementById('featToggleBtn');
    if (body.style.display === 'none') {
        body.style.display = '';
        btn.innerHTML = '<span id="featToggleLabel">hide</span> &#9650;';
    } else {
        body.style.display = 'none';
        btn.innerHTML = '<span id="featToggleLabel">show</span> &#9660;';
    }
}

// Feature group accordion
function toggleFeatGroup(id, headerEl) {
    var body = document.getElementById('fg-' + id);
    var chev = document.getElementById('chev-' + id);
    if (body.style.display === 'none') {
        body.style.display = '';
        chev.classList.add('open');
    } else {
        body.style.display = 'none';
        chev.classList.remove('open');
    }
}

// ── Feature CSV Export ────────────────────────────────────────────────────────
function exportFeatureData() {
    var btn    = document.getElementById('exportBtn');
    var status = document.getElementById('exportStatus');
    btn.disabled = true;
    status.textContent = 'Fetching data…';

    var apiBase = '<?php echo rtrim(DATABASE_API_URL, '/'); ?>';
    fetch(apiBase + '/pump/feature_export?rows=100')
        .then(function(r) {
            if (!r.ok) throw new Error('HTTP ' + r.status);
            return r.json();
        })
        .then(function(data) {
            if (!data.rows || data.rows.length === 0) {
                status.textContent = 'No data available yet.';
                btn.disabled = false;
                return;
            }

            // ── Sheet 1: Feature Matrix ──────────────────────────────────────
            var rows    = data.rows;
            var meta    = data.feature_meta || [];
            var rules   = data.simulation_rules || [];

            // Build CSV content
            var csv = [];

            // ── Section header ───────────────────────────────────────────────
            csv.push('# Pump Signal Feature Matrix Export');
            csv.push('# Generated: ' + new Date().toISOString());
            csv.push('# Rows: ' + rows.length + ' | Bucket: 30s | Rolling window: 5 min (10 buckets)');
            csv.push('# Source: Binance order book + SOL/USD trades + on-chain whale events');
            csv.push('# Note: ' + (data.note || ''));
            csv.push('');

            // ── Feature data header ──────────────────────────────────────────
            if (rows.length > 0) {
                var cols = Object.keys(rows[0]);
                csv.push(cols.join(','));
                rows.forEach(function(row) {
                    var line = cols.map(function(c) {
                        var v = row[c];
                        if (v === null || v === undefined) return '';
                        var s = String(v);
                        if (s.indexOf(',') !== -1 || s.indexOf('"') !== -1 || s.indexOf('\n') !== -1) {
                            s = '"' + s.replace(/"/g, '""') + '"';
                        }
                        return s;
                    });
                    csv.push(line.join(','));
                });
            }

            csv.push('');
            csv.push('');

            // ── Section 2: Feature Descriptions ─────────────────────────────
            csv.push('# FEATURE DESCRIPTIONS');
            csv.push('feature_name,group,description');
            meta.forEach(function(m) {
                csv.push([
                    m.name,
                    '"' + (m.group || '').replace(/"/g, '""') + '"',
                    '"' + (m.description || '').replace(/"/g, '""') + '"'
                ].join(','));
            });

            csv.push('');
            csv.push('');

            // ── Section 3: GA-Approved Simulation Rules ──────────────────────
            if (rules.length > 0) {
                csv.push('# GA-APPROVED SIMULATION RULES (simulation_results, win_rate >= 55%)');
                csv.push('run_id,rank,win_rate_pct,signals_per_day,oos_precision_pct,oos_consistency_pct,data_hours,conditions');
                rules.forEach(function(r) {
                    var condStr = '';
                    if (Array.isArray(r.conditions)) {
                        condStr = r.conditions.map(function(c) {
                            return c.feature + ' ' + c.direction + ' ' + c.threshold;
                        }).join(' AND ');
                    }
                    csv.push([
                        r.run_id || '',
                        r.rank || '',
                        r.win_rate_pct || '',
                        r.signals_per_day || '',
                        r.oos_precision_pct || '',
                        r.oos_consistency_pct || '',
                        r.data_hours || '',
                        '"' + condStr.replace(/"/g, '""') + '"'
                    ].join(','));
                });
            }

            // ── Download ─────────────────────────────────────────────────────
            var csvContent = csv.join('\n');
            var blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
            var url  = URL.createObjectURL(blob);
            var ts   = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
            var link = document.createElement('a');
            link.href = url;
            link.download = 'pump_features_' + ts + '.csv';
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            URL.revokeObjectURL(url);

            status.textContent = '✓ Downloaded ' + rows.length + ' rows, ' + rules.length + ' rules';
            btn.disabled = false;
            setTimeout(function() { status.textContent = ''; }, 4000);
        })
        .catch(function(err) {
            status.textContent = 'Export failed: ' + err.message;
            btn.disabled = false;
        });
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
