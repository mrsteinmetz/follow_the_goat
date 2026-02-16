<?php
/**
 * Filter Analysis Dashboard - View auto-generated filter suggestions with historical tracking
 * Migrated from: 000old_code/solana_node/v2/filter-analizes/index.php
 * 
 * Uses DuckDB API for data operations
 */

// --- Database API Client ---
require_once __DIR__ . '/../../../includes/DatabaseClient.php';
require_once __DIR__ . '/../../../includes/config.php';
$db = new DatabaseClient(DATABASE_API_URL);
$api_available = $db->isAvailable();

// --- Base URL for template ---
$baseUrl = '';

// Section display names
$section_names = [
    'price_movements' => 'Price Movements',
    'order_book' => 'Order Book',
    'order_book_signals' => 'Order Book',  // Legacy alias
    'transactions' => 'Transactions',
    'whale_activity' => 'Whale Activity',
    'patterns' => 'Patterns',
    'second_prices' => 'Second Prices',
    'btc_correlation' => 'BTC Correlation',
    'eth_correlation' => 'ETH Correlation',
    // NEW: Velocity and micro-move sections
    'cross_asset' => 'Cross-Asset',
    'thirty_second' => '30-Second',
    'micro_move' => 'Micro-Move',
    'velocity' => 'Velocity',
    'micro_patterns' => 'Micro Patterns',
    'unknown' => 'Other',
];

$value_type_colors = [
    'ratio' => '#8b5cf6', 'percentage' => '#3b82f6', 'actual' => '#10b981',
    'count' => '#f59e0b', 'boolean' => '#ec4899', 'change' => '#06b6d4',
    'score' => '#6366f1', 'time' => '#84cc16', 'price' => '#f97316', 'trend' => '#14b8a6',
];

// Initialize data
$suggestions = [];
$summary = [];
$minute_distribution = [];
$combinations = [];
$scheduler_runs = [];
$filter_consistency = [];
$trend_chart_data = [];
$error_message = '';
$auto_filter_settings = [];
$scheduler_stats = ['runs_today' => 0, 'last_run' => null, 'avg_filters' => 0];
$rolling_avgs = [];
$play_updates = [];

// Helper functions
function get_effectiveness_class($bad_removed, $good_kept) {
    $score = ($bad_removed * $good_kept) / 100;
    if ($score >= 40) return 'excellent';
    if ($score >= 25) return 'good';
    if ($score >= 15) return 'fair';
    return 'poor';
}

function format_number($val, $decimals = 2) {
    if ($val === null) return '-';
    return number_format((float)$val, $decimals);
}

function get_trend_indicator($current, $avg) {
    if ($avg === null || $avg == 0) return ['icon' => 'ri-subtract-line', 'class' => 'text-muted', 'label' => 'New'];
    $diff = $current - $avg;
    $pct = ($diff / $avg) * 100;
    if ($pct > 5) return ['icon' => 'ri-arrow-up-line', 'class' => 'text-success', 'label' => 'Improving'];
    if ($pct < -5) return ['icon' => 'ri-arrow-down-line', 'class' => 'text-danger', 'label' => 'Declining'];
    return ['icon' => 'ri-subtract-line', 'class' => 'text-primary', 'label' => 'Stable'];
}

function get_consistency_stars($pct) {
    if ($pct >= 90) return 5;
    if ($pct >= 70) return 4;
    if ($pct >= 50) return 3;
    if ($pct >= 30) return 2;
    if ($pct >= 10) return 1;
    return 0;
}

// Fetch data from API
if (!$api_available) {
    $error_message = "Website API is not available. Please start the API: python scheduler/website_api.py";
} else {
    $response = $db->getFilterAnalysisDashboard();
    if ($response && isset($response['success']) && $response['success']) {
        $suggestions = $response['suggestions'] ?? [];
        $summary = $response['summary'] ?? [];
        $minute_distribution = $response['minute_distribution'] ?? [];
        $combinations = $response['combinations'] ?? [];
        $scheduler_runs = $response['scheduler_runs'] ?? [];
        $filter_consistency = $response['filter_consistency'] ?? [];
        $trend_chart_data = $response['trend_chart_data'] ?? [];
        $auto_filter_settings = $response['settings'] ?? [];
        $scheduler_stats = $response['scheduler_stats'] ?? $scheduler_stats;
        $rolling_avgs = $response['rolling_avgs'] ?? [];
        $play_updates = $response['play_updates'] ?? [];
    } else {
        $error_message = $response['error'] ?? 'Failed to fetch filter analysis data';
    }
}

// Extract current settings values
$currentThreshold = '0.3';
$currentHours = '24';
$currentMinFilters = '1';
$currentMinGoodKept = '50';
$currentMinBadRemoved = '10';
$currentIsRatio = 'false';
foreach ($auto_filter_settings as $s) {
    if ($s['setting_key'] === 'good_trade_threshold') $currentThreshold = $s['setting_value'];
    if ($s['setting_key'] === 'analysis_hours') $currentHours = $s['setting_value'];
    if ($s['setting_key'] === 'min_filters_in_combo') $currentMinFilters = $s['setting_value'];
    if ($s['setting_key'] === 'min_good_trades_kept_pct') $currentMinGoodKept = $s['setting_value'];
    if ($s['setting_key'] === 'min_bad_trades_removed_pct') $currentMinBadRemoved = $s['setting_value'];
    if ($s['setting_key'] === 'is_ratio') $currentIsRatio = $s['setting_value'];
}
$isRatioOn = in_array(strtolower((string)$currentIsRatio), ['1', 'true', 'yes', 'on'], true);
$isRatioLabel = $isRatioOn ? 'On' : 'Off';

// Prepare chart data for JavaScript
$chart_labels = array_column($trend_chart_data, 'time_bucket');
$chart_bad_removed = array_column($trend_chart_data, 'avg_bad_removed');
$chart_good_kept = array_column($trend_chart_data, 'avg_good_kept');
$chart_effectiveness = array_column($trend_chart_data, 'avg_effectiveness');

// --- Page Styles ---
ob_start();
?>
<style>
    .summary-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
        gap: 0.75rem;
        margin-bottom: 1.5rem;
    }
    
    .summary-card {
        background: var(--custom-white);
        border: 1px solid var(--default-border);
        border-radius: 0.5rem;
        padding: 0.75rem;
        text-align: center;
    }
    
    .summary-card .label {
        font-size: 0.65rem;
        text-transform: uppercase;
        color: var(--text-muted);
        margin-bottom: 0.25rem;
    }
    
    .summary-card .value {
        font-size: 1.25rem;
        font-weight: 700;
        color: var(--default-text-color);
    }
    
    .summary-card.highlight .value { color: rgb(var(--primary-rgb)); }
    .summary-card.success .value { color: rgb(var(--success-rgb)); }
    .summary-card.info .value { color: rgb(var(--info-rgb)); }
    
    .filters-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
        gap: 1rem;
    }
    
    .filter-card {
        background: var(--custom-white);
        border: 1px solid var(--default-border);
        border-radius: 0.5rem;
        padding: 1rem;
        border-left: 4px solid rgb(var(--primary-rgb));
    }
    
    .filter-card.excellent { border-left-color: rgb(var(--success-rgb)); }
    .filter-card.good { border-left-color: rgb(var(--primary-rgb)); }
    .filter-card.fair { border-left-color: rgb(var(--warning-rgb)); }
    
    .filter-name {
        font-weight: 600;
        font-size: 0.85rem;
        color: var(--default-text-color);
        margin-bottom: 0.5rem;
    }
    
    .filter-range {
        display: flex;
        gap: 1rem;
        background: var(--light);
        padding: 0.5rem;
        border-radius: 0.25rem;
        margin-bottom: 0.75rem;
    }
    
    .range-item { flex: 1; }
    .range-label { font-size: 0.6rem; text-transform: uppercase; color: var(--text-muted); }
    .range-value { font-family: monospace; font-size: 0.75rem; color: var(--default-text-color); }
    
    .filter-stats {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 0.5rem;
    }
    
    .stat-item {
        text-align: center;
        padding: 0.35rem;
        background: var(--light);
        border-radius: 0.25rem;
    }
    
    .stat-label { font-size: 0.6rem; color: var(--text-muted); }
    .stat-value { font-size: 0.9rem; font-weight: 600; }
    .stat-value.good { color: rgb(var(--success-rgb)); }
    .stat-value.warning { color: rgb(var(--warning-rgb)); }
    .stat-value.danger { color: rgb(var(--danger-rgb)); }
    
    .combinations-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
        gap: 1rem;
    }
    
    .combo-card {
        background: var(--custom-white);
        border: 2px solid rgb(var(--primary-rgb));
        border-radius: 0.5rem;
        padding: 1rem;
    }
    
    .minute-pills {
        display: flex;
        flex-wrap: wrap;
        gap: 0.5rem;
    }
    
    .minute-pill {
        background: rgba(var(--primary-rgb), 0.1);
        border: 1px solid var(--default-border);
        border-radius: 0.5rem;
        padding: 0.5rem 0.75rem;
        text-align: center;
        min-width: 70px;
    }
    
    .minute-pill.best {
        background: rgba(var(--success-rgb), 0.15);
        border-color: rgb(var(--success-rgb));
    }
    
    .effectiveness-bar {
        width: 100%;
        height: 6px;
        background: var(--light);
        border-radius: 3px;
        overflow: hidden;
    }
    
    .effectiveness-fill {
        height: 100%;
        border-radius: 3px;
    }
    
    .effectiveness-fill.excellent { background: rgb(var(--success-rgb)); }
    .effectiveness-fill.good { background: rgb(var(--primary-rgb)); }
    .effectiveness-fill.fair { background: rgb(var(--warning-rgb)); }
    .effectiveness-fill.poor { background: var(--text-muted); }
    
    .consistency-stars {
        color: rgb(var(--warning-rgb));
        font-size: 0.9rem;
    }
    
    .trend-indicator {
        font-size: 1rem;
        margin-left: 0.25rem;
    }
    
    .scheduler-status {
        display: flex;
        align-items: center;
        gap: 0.5rem;
        padding: 0.5rem 1rem;
        background: rgba(var(--success-rgb), 0.1);
        border-radius: 0.5rem;
        font-size: 0.8rem;
    }
    
    .scheduler-status.warning {
        background: rgba(var(--warning-rgb), 0.1);
    }
    
    .run-status-badge {
        padding: 0.2rem 0.5rem;
        border-radius: 0.25rem;
        font-size: 0.7rem;
        font-weight: 600;
    }
    
    .run-status-badge.completed { background: rgba(var(--success-rgb), 0.2); color: rgb(var(--success-rgb)); }
    .run-status-badge.running { background: rgba(var(--info-rgb), 0.2); color: rgb(var(--info-rgb)); }
    .run-status-badge.failed { background: rgba(var(--danger-rgb), 0.2); color: rgb(var(--danger-rgb)); }
    
    /* API Status Badge */
    .api-status-badge {
        position: fixed;
        top: 70px;
        right: 20px;
        z-index: 9999;
        padding: 4px 12px;
        border-radius: 4px;
        font-size: 11px;
        font-weight: 600;
    }
</style>
<?php
$styles = ob_get_clean();

// --- Page Content ---
ob_start();
?>

<!-- API Status Badge -->
<div class="api-status-badge" style="background: <?php echo $api_available ? 'rgb(var(--success-rgb))' : 'rgb(var(--danger-rgb))'; ?>; color: white;">
    🦆 <?php echo $api_available ? 'API Connected' : 'API Disconnected'; ?>
</div>

<!-- Page Header -->
<div class="d-flex align-items-center justify-content-between page-header-breadcrumb flex-wrap gap-2 mb-3">
    <div>
        <nav>
            <ol class="breadcrumb mb-1">
                <li class="breadcrumb-item"><a href="<?php echo $baseUrl; ?>/">Dashboard</a></li>
                <li class="breadcrumb-item"><a href="<?php echo $baseUrl; ?>/pages/features/patterns/">Features</a></li>
                <li class="breadcrumb-item active" aria-current="page">Filter Analysis</li>
            </ol>
        </nav>
        <h1 class="page-title fw-medium fs-18 mb-0">Filter Analysis Dashboard</h1>
        <?php if (!empty($summary['last_updated'])): ?>
        <span class="text-muted fs-11">Data updated: <?php echo date('M j, Y g:i A', strtotime($summary['last_updated'])); ?></span>
        <?php endif; ?>
    </div>
    <div class="d-flex gap-2 align-items-center">
        <?php if ($scheduler_stats['last_run']): ?>
        <div class="scheduler-status <?php echo (time() - strtotime($scheduler_stats['last_run'])) > 1800 ? 'warning' : ''; ?>">
            <i class="ri-timer-line"></i>
            <span>Last run: <?php echo date('H:i', strtotime($scheduler_stats['last_run'])); ?></span>
            <span class="badge bg-primary-transparent"><?php echo $scheduler_stats['runs_today']; ?> today</span>
        </div>
        <?php endif; ?>
    </div>
</div>

<!-- Auto Filter Settings Panel -->
<div class="card custom-card mb-3">
    <div class="card-header d-flex justify-content-between align-items-center" style="cursor: pointer;" data-bs-toggle="collapse" data-bs-target="#settingsPanel">
        <h6 class="mb-0"><i class="ri-settings-3-line me-1"></i>Auto Filter Settings</h6>
        <div class="d-flex align-items-center gap-2">
            <span class="badge bg-info-transparent" id="settingsStatus">
                Good: <?php echo $currentThreshold; ?>% | Hours: <?php echo $currentHours; ?> | Min Filters: <?php echo $currentMinFilters; ?> | Min Good: <?php echo $currentMinGoodKept; ?>% | Min Bad: <?php echo $currentMinBadRemoved; ?>% | Ratio Only: <?php echo $isRatioLabel; ?>
            </span>
            <i class="ri-arrow-down-s-line fs-18" id="settingsArrow"></i>
        </div>
    </div>
    <div class="collapse" id="settingsPanel">
        <div class="card-body">
            <div class="alert alert-info-transparent mb-3">
                <i class="ri-information-line me-1"></i>
                <strong>Optimize for fewer bad trades:</strong> 
                Try setting Good Trade Threshold to <strong>0.6%</strong>, Analysis Hours to <strong>12</strong>, 
                and Min Filters to <strong>4</strong> for more aggressive filtering.
            </div>
            
            <form id="settingsForm">
                <div class="row g-3">
                    <div class="col-md-4">
                        <label class="form-label fw-semibold">Good Trade Threshold (%)</label>
                        <input type="number" class="form-control" name="good_trade_threshold" 
                               value="<?php echo htmlspecialchars($currentThreshold); ?>" 
                               step="0.1" min="0.1" max="5.0">
                        <small class="text-muted">Current: <?php echo $currentThreshold; ?>% (Default: 0.3%)</small>
                    </div>
                    <div class="col-md-4">
                        <label class="form-label fw-semibold">Analysis Hours</label>
                        <input type="number" class="form-control" name="analysis_hours" 
                               value="<?php echo htmlspecialchars($currentHours); ?>" 
                               min="1" max="168">
                        <small class="text-muted">Current: <?php echo $currentHours; ?>h (Default: 24h)</small>
                    </div>
                    <div class="col-md-4">
                        <label class="form-label fw-semibold">Minimum Filters in Combo</label>
                        <input type="number" class="form-control" name="min_filters_in_combo" 
                               value="<?php echo htmlspecialchars($currentMinFilters); ?>" 
                               min="1" max="10">
                        <small class="text-muted">Current: <?php echo $currentMinFilters; ?> (Default: 1)</small>
                    </div>
                </div>
                
                <div class="row g-3 mt-2">
                    <div class="col-md-4">
                        <label class="form-label fw-semibold">Min Good Trades Kept (%)</label>
                        <input type="number" class="form-control" name="min_good_trades_kept_pct" 
                               value="<?php echo htmlspecialchars($currentMinGoodKept); ?>" 
                               step="1" min="0" max="100">
                        <small class="text-muted">Filters must keep at least this % of good trades (Default: 50%)</small>
                    </div>
                    <div class="col-md-4">
                        <label class="form-label fw-semibold">Min Bad Trades Removed (%)</label>
                        <input type="number" class="form-control" name="min_bad_trades_removed_pct" 
                               value="<?php echo htmlspecialchars($currentMinBadRemoved); ?>" 
                               step="1" min="0" max="100">
                        <small class="text-muted">Filters must remove at least this % of bad trades (Default: 10%)</small>
                    </div>
                    <div class="col-md-4">
                        <label class="form-label fw-semibold">Ratio Only</label>
                        <div class="form-check form-switch mt-2">
                            <input class="form-check-input" type="checkbox" role="switch" id="isRatioToggle" name="is_ratio" value="true"
                                   <?php echo $isRatioOn ? 'checked' : ''; ?>>
                            <label class="form-check-label" for="isRatioToggle">Only use ratio-based filters</label>
                        </div>
                        <small class="text-muted">On: only ratio fields. Off: all filters.</small>
                    </div>
                </div>
                
                <div class="d-flex gap-2 mt-4">
                    <button type="submit" class="btn btn-primary" id="saveSettingsBtn">
                        <i class="ri-save-line me-1"></i>Save Settings
                    </button>
                    <button type="button" class="btn btn-outline-secondary" id="resetDefaultsBtn">
                        <i class="ri-refresh-line me-1"></i>Reset to Defaults
                    </button>
                </div>
            </form>
        </div>
    </div>
</div>

<!-- AI Play Updates Section -->
<?php if (!empty($play_updates)): ?>
<div class="card custom-card mb-3">
    <div class="card-header">
        <h6 class="mb-0"><i class="ri-robot-line me-1"></i>Auto-Updated Plays</h6>
        <p class="text-muted mb-0 mt-1 small">Plays automatically updated with filter patterns by the AI system</p>
    </div>
    <div class="card-body">
        <div class="table-responsive">
            <table class="table table-striped table-hover">
                <thead>
                    <tr>
                        <th>Play ID</th>
                        <th>Play Name</th>
                        <th>Project</th>
                        <th>Patterns</th>
                        <th>Filters</th>
                        <th>Filter Version</th>
                        <th>Updated</th>
                        <th>Status</th>
                    </tr>
                </thead>
                <tbody>
                    <?php foreach ($play_updates as $update): ?>
                    <tr>
                        <td><span class="badge bg-primary-transparent">#<?php echo $update['play_id']; ?></span></td>
                        <td><strong><?php echo htmlspecialchars($update['play_name']); ?></strong></td>
                        <td>
                            <span class="badge bg-info-transparent">
                                <?php echo htmlspecialchars($update['project_name'] ?? 'AutoFilters'); ?> 
                                (ID: <?php echo $update['project_id']; ?>)
                            </span>
                        </td>
                        <td><span class="badge bg-success-transparent"><?php echo $update['pattern_count'] ?? 0; ?> patterns</span></td>
                        <td><span class="badge bg-warning-transparent"><?php echo $update['filters_applied'] ?? 0; ?> filters</span></td>
                        <td>
                            <?php 
                            $filter_version = null;
                            if (isset($update['filter_version'])) {
                                $filter_version = $update['filter_version'];
                            } elseif (isset($update['project_updated_at'])) {
                                $filter_version = $update['project_updated_at'];
                            }
                            if ($filter_version):
                                $version_date = is_string($filter_version) ? strtotime($filter_version) : $filter_version;
                                if ($version_date):
                            ?>
                                <small class="text-muted">
                                    <i class="ri-time-line me-1"></i>
                                    <?php echo date('M d, H:i', $version_date); ?>
                                </small>
                            <?php 
                                else:
                                    echo '<small class="text-muted">-</small>';
                                endif;
                            else:
                                echo '<small class="text-muted">-</small>';
                            endif;
                            ?>
                        </td>
                        <td><small class="text-muted"><?php echo $update['updated_at']; ?></small></td>
                        <td>
                            <?php 
                            $statusClass = $update['status'] === 'success' ? 'success' : 'danger';
                            $statusIcon = $update['status'] === 'success' ? 'check-line' : 'close-line';
                            ?>
                            <span class="badge bg-<?php echo $statusClass; ?>-transparent">
                                <i class="ri-<?php echo $statusIcon; ?> me-1"></i><?php echo ucfirst($update['status']); ?>
                            </span>
                        </td>
                    </tr>
                    <?php endforeach; ?>
                </tbody>
            </table>
        </div>
        <div class="alert alert-info-transparent mt-3 mb-0">
            <i class="ri-information-line me-1"></i>
            <strong>How it works:</strong> Plays with <code>pattern_update_by_ai=1</code> are automatically updated 
            to use the latest auto-generated filter patterns from the AutoFilters project. This happens every time 
            the filter analysis runs (every 5-15 minutes).
        </div>
    </div>
</div>
<?php endif; ?>

<?php if ($error_message): ?>
<div class="alert alert-danger alert-dismissible fade show" role="alert">
    <?php echo htmlspecialchars($error_message); ?>
    <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
</div>
<?php elseif (empty($suggestions)): ?>
<div class="card custom-card">
    <div class="card-body text-center py-5">
        <i class="ri-filter-off-line fs-48 text-muted mb-3"></i>
        <h4 class="text-muted">No Filter Suggestions Found</h4>
        <p class="text-muted mb-3">The filter analysis requires trades with resolved outcomes (potential_gains) and filter values stored in trade_filter_values table.</p>
        <p class="text-muted mb-2">The create_new_patterns job runs automatically via the scheduler. You can also run it manually:</p>
        <code class="d-block bg-light p-3 rounded text-success">python 000data_feeds/7_create_new_patterns/create_new_paterns.py</code>
        <p class="text-muted mt-3 small">Note: Trades need to have the <code>trade_filter_values</code> populated by trail_generator during follow_the_goat or train_validator execution.</p>
    </div>
</div>
<?php else: ?>

<!-- Summary Cards -->
<div class="summary-grid">
    <div class="summary-card highlight">
        <div class="label">Total Filters</div>
        <div class="value"><?php echo $summary['total_filters'] ?? 0; ?></div>
    </div>
    <div class="summary-card success">
        <div class="label">Avg Bad Removed</div>
        <div class="value"><?php echo $summary['avg_bad_removed'] ?? 0; ?>%</div>
    </div>
    <div class="summary-card">
        <div class="label">Avg Good Kept</div>
        <div class="value"><?php echo $summary['avg_good_kept'] ?? 0; ?>%</div>
    </div>
    <div class="summary-card success">
        <div class="label">Best Removal</div>
        <div class="value"><?php echo $summary['best_bad_removed'] ?? 0; ?>%</div>
    </div>
    <div class="summary-card">
        <div class="label">Good Trades</div>
        <div class="value"><?php echo number_format($summary['total_good_trades'] ?? 0); ?></div>
    </div>
    <div class="summary-card">
        <div class="label">Bad Trades</div>
        <div class="value"><?php echo number_format($summary['total_bad_trades'] ?? 0); ?></div>
    </div>
    <div class="summary-card info">
        <div class="label">Runs Today</div>
        <div class="value"><?php echo $scheduler_stats['runs_today'] ?? 0; ?></div>
    </div>
    <div class="summary-card">
        <div class="label">Analysis Window</div>
        <div class="value"><?php echo $summary['analysis_hours'] ?? 24; ?>h</div>
    </div>
</div>

<!-- Performance Trend Chart -->
<?php if (!empty($trend_chart_data)): ?>
<div class="card custom-card mb-3">
    <div class="card-header d-flex justify-content-between align-items-center">
        <h6 class="mb-0"><i class="ri-line-chart-line me-1"></i>Performance Trend (Last 24 Hours)</h6>
        <span class="text-muted fs-11"><?php echo count($trend_chart_data); ?> data points</span>
    </div>
    <div class="card-body">
        <div id="trendChart" style="height: 250px;"></div>
    </div>
</div>
<?php endif; ?>

<!-- Filter Consistency (Historical Performance) -->
<?php if (!empty($filter_consistency)): ?>
<div class="card custom-card mb-3">
    <div class="card-header d-flex justify-content-between align-items-center">
        <h6 class="mb-0"><i class="ri-medal-line me-1"></i>Filter Consistency (24h History) - Gateway Ready</h6>
        <button class="btn btn-sm btn-primary-light" onclick="copyFiltersToClipboard()" title="Copy all filters as JSON">
            <i class="ri-file-copy-line me-1"></i>Copy for Gateway
        </button>
    </div>
    <div class="card-body">
        <p class="text-muted fs-12 mb-3">
            <strong>Use these filters in your gateway!</strong> Filters that consistently appear in the best combination. 
            Higher consistency = more reliable.
        </p>
        <div class="table-responsive">
            <table class="table table-sm table-bordered mb-0" id="consistencyTable">
                <thead>
                    <tr>
                        <th>Filter Column</th>
                        <th class="text-center">Interval</th>
                        <th class="text-end">From Value</th>
                        <th class="text-end">To Value</th>
                        <th class="text-center">Consistency</th>
                        <th class="text-end">Avg Bad Rem</th>
                        <th class="text-end">Avg Good Kept</th>
                        <th class="text-center">Runs</th>
                    </tr>
                </thead>
                <tbody>
                    <?php foreach (array_slice($filter_consistency, 0, 20) as $fc): 
                        $stars = get_consistency_stars($fc['consistency_pct']);
                        $current_val = null;
                        foreach ($suggestions as $s) {
                            if ($s['column_name'] === $fc['filter_column']) {
                                $current_val = $s['bad_trades_removed_pct'];
                                break;
                            }
                        }
                        $trend = get_trend_indicator($current_val ?? $fc['avg_bad_removed'], $fc['avg_bad_removed']);
                    ?>
                    <tr data-filter="<?php echo htmlspecialchars($fc['filter_column']); ?>" 
                        data-minute="<?php echo $fc['latest_minute'] ?? 0; ?>"
                        data-from="<?php echo $fc['latest_from']; ?>"
                        data-to="<?php echo $fc['latest_to']; ?>"
                        data-consistency="<?php echo $fc['consistency_pct']; ?>">
                        <td>
                            <strong class="fs-12"><?php echo htmlspecialchars($fc['filter_column']); ?></strong>
                        </td>
                        <td class="text-center">
                            <?php 
                            $min_val = $fc['latest_minute'] ?? 0;
                            $interval_label = floor($min_val) . ':00';
                            ?>
                            <span class="badge bg-purple-transparent text-purple"><?php echo $interval_label; ?></span>
                        </td>
                        <td class="text-end font-monospace fs-11 text-primary fw-semibold"><?php echo $fc['latest_from'] !== null ? format_number($fc['latest_from'], 6) : '-'; ?></td>
                        <td class="text-end font-monospace fs-11 text-primary fw-semibold"><?php echo $fc['latest_to'] !== null ? format_number($fc['latest_to'], 6) : '-'; ?></td>
                        <td class="text-center">
                            <span class="consistency-stars">
                                <?php for ($i = 0; $i < 5; $i++): ?>
                                <i class="ri-star-<?php echo $i < $stars ? 'fill' : 'line'; ?>"></i>
                                <?php endfor; ?>
                            </span>
                            <span class="fs-10 text-muted d-block"><?php echo $fc['consistency_pct']; ?>%</span>
                        </td>
                        <td class="text-end text-success fw-semibold"><?php echo $fc['avg_bad_removed']; ?>%</td>
                        <td class="text-end"><?php echo $fc['avg_good_kept']; ?>%</td>
                        <td class="text-center">
                            <span class="badge bg-light text-dark"><?php echo $fc['total_runs']; ?></span>
                        </td>
                    </tr>
                    <?php endforeach; ?>
                </tbody>
            </table>
        </div>
    </div>
</div>
<?php endif; ?>

<!-- Interval Distribution (30-second intervals) -->
<?php if (!empty($minute_distribution) && count($minute_distribution) > 1): ?>
<div class="card custom-card mb-3">
    <div class="card-header">
        <h6 class="mb-0"><i class="ri-time-line me-1"></i>Best Intervals (Signal Timing)</h6>
    </div>
    <div class="card-body">
        <p class="text-muted fs-12 mb-3">Each filter was tested across all 30 intervals (30-second granularity) before entry. The interval shown is when that filter works best.</p>
        <div class="minute-pills">
            <?php foreach ($minute_distribution as $idx => $m): 
                // Convert minute to 30-second interval format
                $min_val = $m['minute_analyzed'] ?? 0;
                $interval_label = floor($min_val) . ':00';
            ?>
            <div class="minute-pill <?php echo $idx === 0 ? 'best' : ''; ?>">
                <div class="fs-16 fw-bold <?php echo $idx === 0 ? 'text-success' : 'text-primary'; ?>"><?php echo $interval_label; ?></div>
                <div class="fs-10 text-muted"><?php echo $m['filter_count']; ?> filters</div>
                <div class="fs-11 fw-semibold text-success"><?php echo $m['avg_bad_removed']; ?>%</div>
                <?php if ($idx === 0): ?><div class="fs-10 text-success">Best</div><?php endif; ?>
            </div>
            <?php endforeach; ?>
        </div>
    </div>
</div>
<?php endif; ?>

<!-- Filter Combinations -->
<?php if (!empty($combinations)): ?>
<div class="card custom-card mb-3">
    <div class="card-header">
        <h6 class="mb-0"><i class="ri-link me-1"></i>Best Filter Combinations</h6>
    </div>
    <div class="card-body">
        <div class="combinations-grid">
            <?php foreach (array_slice($combinations, 0, 4) as $combo): 
                $filter_columns = json_decode($combo['filter_columns'] ?? '[]', true) ?: [];
            ?>
            <div class="combo-card">
                <div class="d-flex justify-content-between mb-2">
                    <div>
                        <?php 
                        $combo_min = $combo['minute_analyzed'] ?? 0;
                        $combo_interval = floor($combo_min) . ':00';
                        ?>
                        <span class="fw-semibold"><?php echo $combo['filter_count']; ?>-Filter Combo</span>
                        <span class="badge bg-purple-transparent text-purple ms-1"><?php echo $combo_interval; ?></span>
                    </div>
                    <div class="text-success fw-bold fs-18"><?php echo number_format($combo['bad_trades_removed_pct'], 1); ?>%</div>
                </div>
                <div class="mb-2">
                    <?php foreach ($filter_columns as $col): ?>
                    <div class="fs-11"><span class="text-primary">→</span> <?php echo htmlspecialchars($col); ?></div>
                    <?php endforeach; ?>
                </div>
                <div class="d-flex gap-2">
                    <div class="flex-fill text-center p-2 bg-success-transparent rounded">
                        <div class="fs-10 text-muted">Good Kept</div>
                        <div class="fw-bold text-success"><?php echo number_format($combo['good_trades_kept_pct'], 1); ?>%</div>
                    </div>
                    <div class="flex-fill text-center p-2 bg-danger-transparent rounded">
                        <div class="fs-10 text-muted">Bad Left</div>
                        <div class="fw-bold text-danger"><?php echo number_format($combo['bad_trades_after'] ?? 0); ?></div>
                    </div>
                </div>
            </div>
            <?php endforeach; ?>
        </div>
    </div>
</div>
<?php endif; ?>

<!-- Top Filters with Trends -->
<div class="card custom-card mb-3">
    <div class="card-header">
        <h6 class="mb-0"><i class="ri-trophy-line me-1"></i>Top Performing Filters (Current)</h6>
    </div>
    <div class="card-body">
        <div class="filters-grid">
            <?php foreach (array_slice($suggestions, 0, 6) as $filter): 
                $eff_class = get_effectiveness_class($filter['bad_trades_removed_pct'], $filter['good_trades_kept_pct']);
                $rolling_avg = $rolling_avgs[$filter['column_name']] ?? null;
                $trend = get_trend_indicator($filter['bad_trades_removed_pct'], $rolling_avg);
            ?>
            <div class="filter-card <?php echo $eff_class; ?>">
                <div class="filter-name">
                    <?php 
                    $filter_min = $filter['minute_analyzed'] ?? 0;
                    $filter_interval = floor($filter_min) . ':00';
                    ?>
                    <?php echo htmlspecialchars($filter['column_name']); ?>
                    <span class="badge bg-purple-transparent text-purple ms-1"><?php echo $filter_interval; ?></span>
                    <i class="<?php echo $trend['icon']; ?> <?php echo $trend['class']; ?> trend-indicator" title="<?php echo $trend['label']; ?>"></i>
                </div>
                <div class="filter-range">
                    <div class="range-item">
                        <div class="range-label">From</div>
                        <div class="range-value"><?php echo format_number($filter['from_value'], 6); ?></div>
                    </div>
                    <div class="range-item">
                        <div class="range-label">To</div>
                        <div class="range-value"><?php echo format_number($filter['to_value'], 6); ?></div>
                    </div>
                </div>
                <div class="filter-stats">
                    <div class="stat-item">
                        <div class="stat-label">Good Kept</div>
                        <div class="stat-value <?php echo $filter['good_trades_kept_pct'] >= 70 ? 'good' : ($filter['good_trades_kept_pct'] >= 50 ? 'warning' : 'danger'); ?>">
                            <?php echo format_number($filter['good_trades_kept_pct'], 1); ?>%
                        </div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">Bad Removed</div>
                        <div class="stat-value good"><?php echo format_number($filter['bad_trades_removed_pct'], 1); ?>%</div>
                    </div>
                </div>
                <?php if ($rolling_avg !== null): ?>
                <div class="mt-2 fs-10 text-muted text-center">
                    6h avg: <?php echo format_number($rolling_avg, 1); ?>%
                </div>
                <?php endif; ?>
            </div>
            <?php endforeach; ?>
        </div>
    </div>
</div>

<!-- Recent Scheduler Runs -->
<?php if (!empty($scheduler_runs)): ?>
<div class="card custom-card mb-3">
    <div class="card-header">
        <h6 class="mb-0"><i class="ri-history-line me-1"></i>Recent Scheduler Runs</h6>
    </div>
    <div class="card-body">
        <div class="table-responsive">
            <table class="table table-sm table-bordered mb-0">
                <thead>
                    <tr>
                        <th>Time</th>
                        <th class="text-center">Status</th>
                        <th class="text-end">Filters</th>
                        <th class="text-end">Best Bad%</th>
                        <th class="text-end">Best Good%</th>
                        <th class="text-end">Duration</th>
                    </tr>
                </thead>
                <tbody>
                    <?php foreach (array_slice($scheduler_runs, 0, 10) as $run): 
                        $duration = ($run['completed_at'] && $run['run_timestamp']) ? 
                            round(strtotime($run['completed_at']) - strtotime($run['run_timestamp'])) . 's' : '-';
                    ?>
                    <tr>
                        <td class="fs-11"><?php echo date('M j H:i', strtotime($run['run_timestamp'])); ?></td>
                        <td class="text-center">
                            <span class="run-status-badge <?php echo $run['status'] ?? 'completed'; ?>"><?php echo strtoupper($run['status'] ?? 'N/A'); ?></span>
                        </td>
                        <td class="text-end"><?php echo $run['total_filters_analyzed'] ?? '-'; ?></td>
                        <td class="text-end text-success"><?php echo isset($run['best_bad_removed_pct']) ? number_format($run['best_bad_removed_pct'], 1) . '%' : '-'; ?></td>
                        <td class="text-end"><?php echo isset($run['best_good_kept_pct']) ? number_format($run['best_good_kept_pct'], 1) . '%' : '-'; ?></td>
                        <td class="text-end text-muted fs-11"><?php echo $duration; ?></td>
                    </tr>
                    <?php endforeach; ?>
                </tbody>
            </table>
        </div>
    </div>
</div>
<?php endif; ?>

<!-- All Filters Table -->
<div class="card custom-card">
    <div class="card-header d-flex justify-content-between align-items-center flex-wrap gap-2">
        <h6 class="mb-0"><i class="ri-list-check me-1"></i>All Filter Suggestions</h6>
        <div class="d-flex gap-2 flex-wrap">
            <select id="sectionFilter" class="form-select form-select-sm" style="width: auto;" onchange="filterTable()">
                <option value="">All Sections</option>
                <?php foreach ($section_names as $key => $name): ?>
                <option value="<?php echo $key; ?>"><?php echo $name; ?></option>
                <?php endforeach; ?>
            </select>
            <select id="minuteFilter" class="form-select form-select-sm" style="width: auto;" onchange="filterTable()">
                <option value="">All Intervals</option>
                <?php for ($m = 0; $m < 15; $m++): ?>
                    <option value="<?php echo $m; ?>"><?php echo $m; ?>:00</option>
                <?php endfor; ?>
            </select>
            <select id="minBadRemoved" class="form-select form-select-sm" style="width: auto;" onchange="filterTable()">
                <option value="0">All Bad %</option>
                <option value="30">≥ 30%</option>
                <option value="40">≥ 40%</option>
                <option value="50">≥ 50%</option>
                <option value="60">≥ 60%</option>
            </select>
            <select id="minGoodKept" class="form-select form-select-sm" style="width: auto;" onchange="filterTable()">
                <option value="0">All Good %</option>
                <option value="60">≥ 60%</option>
                <option value="70">≥ 70%</option>
                <option value="80">≥ 80%</option>
            </select>
        </div>
    </div>
    <div class="card-body">
        <div class="table-responsive">
            <table class="table table-bordered table-sm" id="filtersTable">
                <thead>
                    <tr>
                        <th>Column</th>
                        <th>Section</th>
                        <th>Interval</th>
                        <th class="text-end">From</th>
                        <th class="text-end">To</th>
                        <th class="text-end">Good Kept</th>
                        <th class="text-end">Bad Removed</th>
                        <th>Effectiveness</th>
                    </tr>
                </thead>
                <tbody>
                    <?php foreach ($suggestions as $filter): 
                        $eff_class = get_effectiveness_class($filter['bad_trades_removed_pct'], $filter['good_trades_kept_pct']);
                        $score = ($filter['bad_trades_removed_pct'] * $filter['good_trades_kept_pct']) / 100;
                    ?>
                    <tr data-section="<?php echo $filter['section']; ?>" 
                        data-bad-removed="<?php echo $filter['bad_trades_removed_pct']; ?>"
                        data-good-kept="<?php echo $filter['good_trades_kept_pct']; ?>"
                        data-minute="<?php echo $filter['minute_analyzed'] ?? 0; ?>">
                        <td><strong class="fs-12"><?php echo htmlspecialchars($filter['column_name']); ?></strong></td>
                        <td class="fs-11"><?php echo $section_names[$filter['section']] ?? $filter['section']; ?></td>
                        <td>
                            <?php 
                            $tbl_min = $filter['minute_analyzed'] ?? 0;
                            $tbl_interval = floor($tbl_min) . ':00';
                            ?>
                            <span class="badge bg-purple-transparent text-purple"><?php echo $tbl_interval; ?></span>
                        </td>
                        <td class="text-end font-monospace fs-11"><?php echo format_number($filter['from_value'], 6); ?></td>
                        <td class="text-end font-monospace fs-11"><?php echo format_number($filter['to_value'], 6); ?></td>
                        <td class="text-end <?php echo $filter['good_trades_kept_pct'] >= 70 ? 'text-success' : ($filter['good_trades_kept_pct'] >= 50 ? 'text-warning' : 'text-danger'); ?>">
                            <?php echo format_number($filter['good_trades_kept_pct'], 1); ?>%
                        </td>
                        <td class="text-end text-success"><?php echo format_number($filter['bad_trades_removed_pct'], 1); ?>%</td>
                        <td style="min-width: 90px;">
                            <div class="fs-10 text-muted mb-1">Score: <?php echo format_number($score, 1); ?></div>
                            <div class="effectiveness-bar">
                                <div class="effectiveness-fill <?php echo $eff_class; ?>" style="width: <?php echo min($score, 100); ?>%;"></div>
                            </div>
                        </td>
                    </tr>
                    <?php endforeach; ?>
                </tbody>
            </table>
        </div>
    </div>
</div>
<?php endif; ?>

<script>
    function filterTable() {
        const section = document.getElementById('sectionFilter').value;
        const minute = document.getElementById('minuteFilter').value;
        const minBadRemoved = parseFloat(document.getElementById('minBadRemoved').value) || 0;
        const minGoodKept = parseFloat(document.getElementById('minGoodKept').value) || 0;
        
        document.querySelectorAll('#filtersTable tbody tr').forEach(row => {
            const rowSection = row.dataset.section;
            const rowMinute = row.dataset.minute;
            const badRemoved = parseFloat(row.dataset.badRemoved);
            const goodKept = parseFloat(row.dataset.goodKept);
            
            let show = true;
            if (section && rowSection !== section) show = false;
            if (minute && rowMinute !== minute) show = false;
            if (badRemoved < minBadRemoved) show = false;
            if (goodKept < minGoodKept) show = false;
            
            row.style.display = show ? '' : 'none';
        });
    }
    
    function copyFiltersToClipboard() {
        const rows = document.querySelectorAll('#consistencyTable tbody tr');
        const filters = [];
        
        rows.forEach(row => {
            const filter = row.dataset.filter;
            const minute = parseInt(row.dataset.minute) || 0;
            const from = parseFloat(row.dataset.from);
            const to = parseFloat(row.dataset.to);
            const consistency = parseFloat(row.dataset.consistency);
            
            if (filter && !isNaN(from) && !isNaN(to)) {
                filters.push({
                    column: filter,
                    minute: minute,
                    from_value: from,
                    to_value: to,
                    consistency_pct: consistency
                });
            }
        });
        
        const json = JSON.stringify(filters, null, 2);
        
        navigator.clipboard.writeText(json).then(() => {
            const btn = event.target.closest('button');
            const originalHtml = btn.innerHTML;
            btn.innerHTML = '<i class="ri-check-line me-1"></i>Copied!';
            btn.classList.remove('btn-primary-light');
            btn.classList.add('btn-success-light');
            
            setTimeout(() => {
                btn.innerHTML = originalHtml;
                btn.classList.remove('btn-success-light');
                btn.classList.add('btn-primary-light');
            }, 2000);
        });
    }
    
    // Settings Panel Functionality
    document.addEventListener('DOMContentLoaded', function() {
        const settingsPanel = document.getElementById('settingsPanel');
        const settingsArrow = document.getElementById('settingsArrow');
        if (settingsPanel && settingsArrow) {
            settingsPanel.addEventListener('show.bs.collapse', () => {
                settingsArrow.classList.add('ri-arrow-up-s-line');
                settingsArrow.classList.remove('ri-arrow-down-s-line');
            });
            settingsPanel.addEventListener('hide.bs.collapse', () => {
                settingsArrow.classList.remove('ri-arrow-up-s-line');
                settingsArrow.classList.add('ri-arrow-down-s-line');
            });
        }
    });
    
    // Save Settings
    document.getElementById('settingsForm')?.addEventListener('submit', async function(e) {
        e.preventDefault();
        const btn = document.getElementById('saveSettingsBtn');
        const originalHtml = btn.innerHTML;
        btn.innerHTML = '<i class="ri-loader-4-line ri-spin me-1"></i>Saving...';
        btn.disabled = true;
        
        const formData = new FormData(this);
        const settings = {};
        formData.forEach((value, key) => settings[key] = value);
        const isRatioToggle = document.getElementById('isRatioToggle');
        settings['is_ratio'] = isRatioToggle && isRatioToggle.checked ? 'true' : 'false';
        
        try {
            const response = await fetch('<?php echo defined("DATABASE_API_PUBLIC_URL") ? DATABASE_API_PUBLIC_URL : DATABASE_API_URL; ?>/filter-analysis/settings', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ settings })
            });
            
            const result = await response.json();
            
            if (result.success) {
                btn.innerHTML = '<i class="ri-check-line me-1"></i>Saved!';
                btn.classList.remove('btn-primary');
                btn.classList.add('btn-success');
                
                // Update form inputs with saved values
                if (result.current_settings) {
                    const s = result.current_settings;
                    
                    // Update each form input
                    const thresholdInput = document.querySelector('input[name="good_trade_threshold"]');
                    if (thresholdInput && s.good_trade_threshold) thresholdInput.value = s.good_trade_threshold;
                    
                    const hoursInput = document.querySelector('input[name="analysis_hours"]');
                    if (hoursInput && s.analysis_hours) hoursInput.value = s.analysis_hours;
                    
                    const minFiltersInput = document.querySelector('input[name="min_filters_in_combo"]');
                    if (minFiltersInput && s.min_filters_in_combo) minFiltersInput.value = s.min_filters_in_combo;
                    
                    const minGoodInput = document.querySelector('input[name="min_good_trades_kept_pct"]');
                    if (minGoodInput && s.min_good_trades_kept_pct) minGoodInput.value = s.min_good_trades_kept_pct;
                    
                    const minBadInput = document.querySelector('input[name="min_bad_trades_removed_pct"]');
                    if (minBadInput && s.min_bad_trades_removed_pct) minBadInput.value = s.min_bad_trades_removed_pct;
                    
                    const ratioToggle = document.getElementById('isRatioToggle');
                    if (ratioToggle) {
                        const isRatio = s.is_ratio === true || s.is_ratio === 'true' || s.is_ratio === '1';
                        ratioToggle.checked = isRatio;
                    }
                }
                
                // Update status badge
                const statusBadge = document.getElementById('settingsStatus');
                if (statusBadge && result.current_settings) {
                    const s = result.current_settings;
                    const ratioLabel = (s.is_ratio === true || s.is_ratio === 'true' || s.is_ratio === '1') ? 'On' : 'Off';
                    statusBadge.textContent = `Good: ${s.good_trade_threshold || '0.3'}% | Hours: ${s.analysis_hours || '24'} | Min Filters: ${s.min_filters_in_combo || '1'} | Min Good: ${s.min_good_trades_kept_pct || '50'}% | Min Bad: ${s.min_bad_trades_removed_pct || '10'}% | Ratio Only: ${ratioLabel}`;
                }
                
                // Reload page after 2 seconds to ensure all data is fresh
                setTimeout(() => {
                    window.location.reload();
                }, 2000);
            } else {
                throw new Error(result.error || 'Failed to save');
            }
        } catch (error) {
            btn.innerHTML = '<i class="ri-error-warning-line me-1"></i>Error';
            btn.classList.remove('btn-primary');
            btn.classList.add('btn-danger');
            alert('Error saving settings: ' + error.message);
            
            setTimeout(() => {
                btn.innerHTML = originalHtml;
                btn.classList.remove('btn-danger');
                btn.classList.add('btn-primary');
                btn.disabled = false;
            }, 2000);
        }
    });
    
    // Reset to Defaults
    document.getElementById('resetDefaultsBtn')?.addEventListener('click', function() {
        if (!confirm('Reset all settings to defaults?')) return;
        
        const defaults = {
            'good_trade_threshold': '0.3',
            'analysis_hours': '24',
            'min_filters_in_combo': '1',
            'min_good_trades_kept_pct': '50',
            'min_bad_trades_removed_pct': '10',
            'is_ratio': 'false'
        };
        
        for (const [key, value] of Object.entries(defaults)) {
            const input = document.querySelector(`input[name="${key}"]`);
            if (input && input.type !== 'checkbox') input.value = value;
        }
        
        const ratioToggle = document.getElementById('isRatioToggle');
        if (ratioToggle) ratioToggle.checked = false;
        
        document.getElementById('saveSettingsBtn').click();
    });
</script>

<?php
$content = ob_get_clean();

// --- Scripts ---
ob_start();
?>
<script src="/assets/libs/apexcharts/apexcharts.min.js"></script>
<?php if (!empty($trend_chart_data)): ?>
<script>
    document.addEventListener('DOMContentLoaded', function() {
        var options = {
            series: [{
                name: 'Bad Removed %',
                data: <?php echo json_encode(array_map('floatval', $chart_bad_removed)); ?>
            }, {
                name: 'Good Kept %',
                data: <?php echo json_encode(array_map('floatval', $chart_good_kept)); ?>
            }],
            chart: {
                height: 250,
                type: 'area',
                toolbar: { show: false },
                zoom: { enabled: false }
            },
            dataLabels: { enabled: false },
            stroke: { curve: 'smooth', width: 2 },
            colors: ['#10b981', '#3b82f6'],
            fill: {
                type: 'gradient',
                gradient: {
                    shadeIntensity: 1,
                    opacityFrom: 0.4,
                    opacityTo: 0.1,
                    stops: [0, 90, 100]
                }
            },
            xaxis: {
                categories: <?php echo json_encode($chart_labels); ?>,
                labels: {
                    show: true,
                    rotate: -45,
                    rotateAlways: false,
                    formatter: function(val) {
                        if (!val) return '';
                        return val.split(' ')[1] || val;
                    },
                    style: { fontSize: '10px' }
                },
                tickAmount: 8
            },
            yaxis: {
                labels: {
                    formatter: function(val) { return val.toFixed(1) + '%'; }
                }
            },
            tooltip: {
                shared: true,
                x: { format: 'MMM dd HH:mm' },
                y: { formatter: function(val) { return val.toFixed(1) + '%'; } }
            },
            legend: {
                position: 'top',
                horizontalAlign: 'right'
            },
            grid: {
                borderColor: 'var(--default-border)',
                strokeDashArray: 3
            }
        };
        
        var chart = new ApexCharts(document.querySelector("#trendChart"), options);
        chart.render();
    });
</script>
<?php endif; ?>
<?php
$scripts = ob_get_clean();

// Include the base layout
include __DIR__ . '/../../layouts/base.php';
?>

