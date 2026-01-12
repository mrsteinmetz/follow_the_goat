<?php
/**
 * Unique Play Details - Follow The Goat Trading Play
 * Migrated to use DatabaseClient API
 */

// Set timezone to UTC (server time) - never use browser time
date_default_timezone_set('UTC');

// --- Timing for performance debugging ---
$timing = [];
$timing['script_start'] = microtime(true);

// --- Load Database Client ---
require_once __DIR__ . '/../../includes/DatabaseClient.php';
$timing['client_loaded'] = microtime(true);

// --- Initialize API Client ---
$client = new DatabaseClient();
$api_available = $client->isAvailable();
// API Base URL - uses PHP proxy to reach Flask API on server
$API_BASE = dirname($_SERVER["SCRIPT_NAME"]) . '/../../api/proxy.php';

// Get play ID from query parameter
$play_id = (int)($_GET['id'] ?? 0);

if ($play_id <= 0) {
    header('Location: ../index.php?error=' . urlencode('Invalid play ID'));
    exit;
}

$is_restricted_play = ($play_id === 46);

// --- Base URL for v2 template ---
$rootFolder = basename($_SERVER['DOCUMENT_ROOT']);
$baseUrl = (isset($_SERVER['HTTPS']) && $_SERVER['HTTPS'] === 'on' ? 'https://' : 'http://') . $_SERVER['HTTP_HOST'] . dirname(dirname(dirname($_SERVER['SCRIPT_NAME'])));

$play = null;
$trades = [];
$no_go_trades = [];
$total_count = 0;
$error_message = '';
$success_message = '';
$chart_data = [
    'prices' => [],
    'trade_markers' => []
];

// Check for success/error messages
if (isset($_GET['success'])) {
    $success_message = 'Play updated successfully!';
}
if (isset($_GET['error'])) {
    $error_message = htmlspecialchars($_GET['error']);
}

// Handle AJAX Load More Request (DuckDB only)
if (isset($_GET['ajax_load_more']) && $api_available) {
    $offset = (int)($_GET['offset'] ?? 0);
    
    // For AJAX load more, we call the API directly
    $hours = '168'; // Use 7-day window for trades (DuckDB only)
    $limit = 100;
    
    $url = $API_BASE . '/buyins?' . http_build_query([
        'play_id' => $play_id,
        'hours' => $hours,
        'limit' => $limit
    ]);
    
    // Note: API doesn't support offset yet - will need API enhancement for pagination
    echo '<!-- Load more requires API offset support - enhancement needed -->';
    exit;
}

// --- Fetch play from API ---
if (!$api_available) {
    $error_message = 'API server is not available. Please ensure master.py is running.';
} else {
    $timing['before_play_query'] = microtime(true);
    $result = $client->getPlay($play_id);
    $timing['after_play_query'] = microtime(true);
    
    if ($result && isset($result['play'])) {
        $play = $result['play'];
    } else {
        header('Location: ../index.php?error=' . urlencode('Play not found'));
        exit;
    }
    
    // Fetch all pattern config projects for the dropdown
    $pattern_projects = [];
    $timing['before_pattern_projects_query'] = microtime(true);
    $projects_result = $client->getPatternProjects();
    $timing['after_pattern_projects_query'] = microtime(true);
    if ($projects_result && isset($projects_result['projects'])) {
        $pattern_projects = $projects_result['projects'];
    }
    
    // Fetch trades from API
    // For play 46 (training validator), use shorter window (24h) due to high volume
    // Other plays use 168h (7 days)
    $hours_window = ($play_id === 46) ? '24' : '168';
    $timing['before_trades_query'] = microtime(true);
    $buyins_result = $client->getBuyins($play_id, null, $hours_window, 100);
    $timing['after_trades_query'] = microtime(true);
    
    if ($buyins_result && isset($buyins_result['buyins'])) {
        // Split into live trades (non-no_go) and no_go trades
        $all_buyins = $buyins_result['buyins'];
        $trades = array_filter($all_buyins, function($t) {
            return ($t['our_status'] ?? '') !== 'no_go';
        });
        $trades = array_values($trades); // Re-index
        
        // Separate no_go trades for display
        $no_go_trades = array_filter($all_buyins, function($t) {
            return ($t['our_status'] ?? '') === 'no_go';
        });
        $no_go_trades = array_values($no_go_trades); // Re-index
        
        $live_total_count = $buyins_result['total'] ?? count($trades);
    }
    $timing['trades_count'] = count($trades);
    
    // Get performance stats
    // For play 46 (training validator), use shorter window (24h) due to high volume
    // Other plays use 168h (7 days)
    $perf_hours = ($play_id === 46) ? '24' : '168';
    $timing['before_stats_query'] = microtime(true);
    $perf_result = $client->getPlayPerformance($play_id, $perf_hours);
    $timing['after_stats_query'] = microtime(true);
}

// Parse sell_logic JSON
$tolerance_rules = ['increases' => [], 'decreases' => []];
if ($play && !empty($play['sell_logic'])) {
    $sell_logic = is_string($play['sell_logic']) ? json_decode($play['sell_logic'], true) : $play['sell_logic'];
    if ($sell_logic && isset($sell_logic['tolerance_rules'])) {
        $tolerance_rules = $sell_logic['tolerance_rules'];
    }
}

// Extract stats from performance result
$total_trades = 0;
$active_trades = 0;
$completed_trades = 0;
$no_go_count = 0;
$total_profit_loss = 0;
$total_potential_gains = 0;

if (isset($perf_result) && $perf_result && $perf_result['success']) {
    $total_profit_loss = (float)($perf_result['total_profit_loss'] ?? 0);
    $active_trades = (int)($perf_result['active_trades'] ?? 0);
    $completed_trades = (int)($perf_result['winning_trades'] ?? 0) + (int)($perf_result['losing_trades'] ?? 0);
    $no_go_count = (int)($perf_result['total_no_gos'] ?? 0);
    $total_trades = $completed_trades + $no_go_count;
}

$json_chart_data = json_encode($chart_data);
$timing['page_ready'] = microtime(true);

// Calculate timing differences
$timing_report = [];
if (isset($timing['after_play_query'], $timing['before_play_query'])) {
    $timing_report['Play Query'] = ($timing['after_play_query'] - $timing['before_play_query']) * 1000;
}
if (isset($timing['after_pattern_projects_query'], $timing['before_pattern_projects_query'])) {
    $timing_report['Pattern Projects Query'] = ($timing['after_pattern_projects_query'] - $timing['before_pattern_projects_query']) * 1000;
}
if (isset($timing['after_trades_query'], $timing['before_trades_query'])) {
    $timing_report['Trades Query'] = ($timing['after_trades_query'] - $timing['before_trades_query']) * 1000;
}
if (isset($timing['trades_count'])) {
    $timing_report['Trades Count'] = $timing['trades_count'];
}
if (isset($timing['after_stats_query'], $timing['before_stats_query'])) {
    $timing_report['Stats Query'] = ($timing['after_stats_query'] - $timing['before_stats_query']) * 1000;
}
$timing_report['Total Page Time'] = ($timing['page_ready'] - $timing['script_start']) * 1000;

$json_timing_report = json_encode($timing_report);

$status_badge_map = [
    'pending' => ['label' => 'active', 'class' => 'bg-info-transparent'],
    'sold' => ['label' => 'completed', 'class' => 'bg-success-transparent'],
    'cancelled' => ['label' => 'cancelled', 'class' => 'bg-danger-transparent'],
];

// --- Page Styles ---
ob_start();
?>
<style>
    .stats-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
        gap: 1rem;
    }
    
    .stat-card {
        background: var(--custom-white);
        padding: 1.25rem;
        border-radius: 0.5rem;
        border: 1px solid var(--default-border);
    }
    
    .stat-label {
        font-size: 0.75rem;
        color: var(--text-muted);
        margin-bottom: 0.5rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    .stat-value {
        font-size: 1.5rem;
        font-weight: 700;
        color: var(--default-text-color);
    }
    
    .stat-value.positive { color: rgb(var(--success-rgb)); }
    .stat-value.negative { color: rgb(var(--danger-rgb)); }
    
    .tolerance-grid {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 1.5rem;
    }
    
    @media (max-width: 768px) {
        .tolerance-grid {
            grid-template-columns: 1fr;
        }
    }
    
    .tolerance-card {
        background: var(--custom-white);
        border: 1px solid var(--default-border);
        border-radius: 0.5rem;
        overflow: hidden;
    }
    
    .tolerance-header {
        padding: 1rem 1.25rem;
        border-bottom: 2px solid rgb(var(--primary-rgb));
        background: rgba(var(--primary-rgb), 0.05);
    }
    
    .tolerance-header h3 {
        font-size: 1rem;
        font-weight: 600;
        margin: 0;
        color: var(--default-text-color);
    }
    
    .tolerance-header .subtitle {
        font-size: 0.8rem;
        color: var(--text-muted);
        margin-top: 0.25rem;
    }
    
    #tradeChart {
        min-height: 500px;
    }
    
    .form-container {
        background: var(--custom-white);
        border: 1px solid var(--default-border);
        border-radius: 0.5rem;
        padding: 1.5rem;
        margin-bottom: 1.5rem;
    }
    
    .form-container.hidden {
        display: none;
    }
    
    .form-container h3 {
        margin-bottom: 1.25rem;
        color: var(--default-text-color);
    }
    
    .form-actions {
        display: flex;
        gap: 0.75rem;
        margin-top: 1.5rem;
    }
    
    .tolerance-rules-container {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 1.5rem;
        margin-top: 1rem;
    }
    
    .tolerance-section h4 {
        font-size: 0.9rem;
        margin-bottom: 0.75rem;
        color: var(--default-text-color);
    }
    
    .tolerance-rule, .tolerance-rule-single {
        display: flex;
        gap: 0.75rem;
        align-items: flex-end;
        margin-bottom: 0.75rem;
        flex-wrap: wrap;
    }
    
    .tolerance-rule-label {
        font-size: 0.75rem;
        color: var(--text-muted);
        display: block;
        margin-bottom: 0.25rem;
    }
    
    .form-control-inline {
        width: 100px;
        padding: 0.375rem 0.5rem;
        font-size: 0.85rem;
        border: 1px solid var(--default-border);
        border-radius: 0.25rem;
        background: var(--custom-white);
        color: var(--default-text-color);
    }
    
    .pip-help-text {
        background: rgba(var(--info-rgb), 0.1);
        border-left: 4px solid rgb(var(--info-rgb));
        padding: 0.75rem;
        border-radius: 0.25rem;
        font-size: 0.85rem;
        color: var(--default-text-color);
    }
    
    .pip-conversion {
        font-size: 0.7rem;
        color: var(--text-muted);
        display: block;
        margin-top: 0.25rem;
    }
    
    @media (max-width: 768px) {
        .tolerance-rules-container {
            grid-template-columns: 1fr;
        }
    }
    
    .hour-header-row td {
        background: rgba(var(--primary-rgb), 0.15) !important;
        color: rgb(var(--primary-rgb));
        font-weight: 600;
        font-size: 0.9rem;
    }
    
    .api-status {
        display: inline-flex;
        align-items: center;
        gap: 0.5rem;
        padding: 0.25rem 0.75rem;
        border-radius: 0.25rem;
        font-size: 0.75rem;
        font-weight: 500;
    }
    
    .api-status.online {
        background: rgba(var(--success-rgb), 0.1);
        color: rgb(var(--success-rgb));
    }
    
    .api-status.offline {
        background: rgba(var(--danger-rgb), 0.1);
        color: rgb(var(--danger-rgb));
    }
    
    .api-status-dot {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        background: currentColor;
    }
</style>
<?php
$styles = ob_get_clean();

// --- Page Content ---
ob_start();
?>

<!-- Page Header -->
<div class="d-flex align-items-center justify-content-between page-header-breadcrumb flex-wrap gap-2 mb-3">
    <div>
        <nav>
            <ol class="breadcrumb mb-1">
                <li class="breadcrumb-item"><a href="<?php echo $baseUrl; ?>/">Dashboard</a></li>
                <li class="breadcrumb-item"><a href="<?php echo $baseUrl; ?>/goats/">Goats</a></li>
                <li class="breadcrumb-item active" aria-current="page"><?php echo htmlspecialchars($play['name'] ?? 'Unknown'); ?></li>
            </ol>
        </nav>
        <h1 class="page-title fw-medium fs-18 mb-0"><?php echo htmlspecialchars($play['name'] ?? 'Unknown'); ?></h1>
    </div>
    <div class="d-flex gap-2 align-items-center">
        <span class="api-status <?php echo $api_available ? 'online' : 'offline'; ?>">
            <span class="api-status-dot"></span>
            API <?php echo $api_available ? 'Online' : 'Offline'; ?>
        </span>
        <div class="d-flex align-items-center gap-2">
            <label for="playSorting" class="text-muted fs-12">Priority:</label>
            <select id="playSorting" class="form-select form-select-sm" style="width: auto;" onchange="updatePlaySorting(<?php echo $play_id; ?>, this.value)" <?php echo $is_restricted_play ? 'disabled title="Editing disabled for this play"' : ''; ?>>
                <?php for ($i = 1; $i <= 10; $i++): ?>
                    <option value="<?php echo $i; ?>" <?php echo (($play['sorting'] ?? 10) == $i) ? 'selected' : ''; ?>>
                        <?php echo $i; ?>
                    </option>
                <?php endfor; ?>
            </select>
        </div>
        <button id="editPlayBtn" class="btn btn-primary" onclick="toggleEditForm()" <?php echo $is_restricted_play ? 'disabled title="Editing disabled for this play"' : ''; ?>>
            <i class="ri-edit-line me-1"></i>Edit Play
        </button>
    </div>
</div>

<!-- Messages -->
<?php if ($success_message): ?>
<div class="alert alert-success alert-dismissible fade show" role="alert">
    <?php echo $success_message; ?>
    <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
</div>
<?php endif; ?>

<?php if ($error_message): ?>
<div class="alert alert-danger alert-dismissible fade show" role="alert">
    <?php echo $error_message; ?>
    <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
</div>
<?php endif; ?>

<?php if ($is_restricted_play): ?>
<div class="alert alert-warning" role="alert">
    <i class="ri-error-warning-line me-1"></i> Editing and deleting are disabled for this play.
</div>
<?php endif; ?>

<!-- Play Info & Badges -->
<div class="card custom-card mb-3">
    <div class="card-body">
        <p class="text-muted mb-3"><?php echo htmlspecialchars($play['description'] ?? ''); ?></p>
        
        <!-- Badges Section -->
        <div class="d-flex flex-wrap gap-2 justify-content-center">
            <?php 
                $trigger_mode = 'any';
                if (!empty($play['tricker_on_perp'])) {
                    $trigger_data = is_string($play['tricker_on_perp']) 
                        ? json_decode($play['tricker_on_perp'], true) 
                        : $play['tricker_on_perp'];
                    $trigger_mode = $trigger_data['mode'] ?? 'any';
                }
                
                $timing_enabled = false;
                $timing_display = '';
                if (!empty($play['timing_conditions'])) {
                    $timing_data = is_string($play['timing_conditions']) 
                        ? json_decode($play['timing_conditions'], true) 
                        : $play['timing_conditions'];
                    if (!empty($timing_data['enabled'])) {
                        $timing_enabled = true;
                        $direction = ($timing_data['price_direction'] ?? 'increase') === 'increase' ? '↑' : '↓';
                        $time_window = $timing_data['time_window_seconds'] ?? 0;
                        $threshold_decimal = $timing_data['price_change_threshold'] ?? 0;
                        $threshold_percent = $threshold_decimal * 100;
                        
                        if ($time_window < 60) {
                            $time_str = $time_window . 's';
                        } elseif ($time_window < 3600) {
                            $time_str = round($time_window / 60, 1) . 'm';
                        } else {
                            $time_str = round($time_window / 3600, 2) . 'h';
                        }
                        $timing_display = $direction . ' ' . number_format($threshold_percent, 2) . '% / ' . $time_str;
                    }
                }
            ?>
            
            <!-- Short/Long Badge -->
            <?php if (!empty($play['short_play'])): ?>
                <span class="badge bg-danger fs-12 fw-semibold">SHORT</span>
            <?php else: ?>
                <span class="badge bg-success fs-12 fw-semibold">LONG</span>
            <?php endif; ?>
            
            <!-- Trigger Mode Badge -->
            <?php if ($trigger_mode === 'short_only'): ?>
                <span class="badge bg-purple-transparent text-purple">SHORT WALLETS ONLY</span>
            <?php elseif ($trigger_mode === 'long_only'): ?>
                <span class="badge bg-success-transparent text-success">LONG WALLETS ONLY</span>
            <?php else: ?>
                <span class="badge bg-secondary-transparent text-muted">ANY WALLET</span>
            <?php endif; ?>
            
            <!-- Timing Badge -->
            <?php if ($timing_enabled): ?>
                <span class="badge bg-warning-transparent text-warning">⏱️ TIMING <?php echo $timing_display; ?></span>
            <?php else: ?>
                <span class="badge bg-secondary-transparent text-muted">⏱️ NO TIMING</span>
            <?php endif; ?>
        </div>
    </div>
</div>

<!-- Edit Play Form -->
<div class="form-container hidden" id="editPlayForm">
    <h3>Edit Play</h3>
    <form id="editForm" onsubmit="return handleUpdatePlay(event)">
        <input type="hidden" id="edit_play_id" name="play_id" value="<?php echo $play_id; ?>">
        
        <div class="mb-3">
            <label class="form-label" for="edit_name">Name *</label>
            <input type="text" class="form-control" id="edit_name" name="name" maxlength="60" required>
        </div>

        <div class="mb-3">
            <label class="form-label" for="edit_description">Description *</label>
            <textarea class="form-control" id="edit_description" name="description" maxlength="500" rows="3" required></textarea>
        </div>

        <div class="mb-3">
            <label class="form-label" for="edit_find_wallets_sql">Find Wallets SQL Query *</label>
            <textarea class="form-control" id="edit_find_wallets_sql" name="find_wallets_sql" rows="8" required placeholder="Enter SQL query to find wallets..."></textarea>
            <div class="form-text">This query will be validated before saving. <strong>Mandatory field:</strong> query must return <code>wallet_address</code></div>
        </div>

        <div class="mb-3">
            <label class="form-label">Sell Logic Tolerance Rules *</label>
            <div class="pip-help-text mb-3">
                <strong class="text-info">Understanding PIPs:</strong><br>
                PIPs make it easy to enter small decimal values. <strong>1 PIP = 0.0001 = 0.01%</strong><br>
                <strong>Examples:</strong> 10 PIPs = 0.001 (0.1%) | 100 PIPs = 0.01 (1%) | 1000 PIPs = 0.1 (10%) | 4000 PIPs = 0.4 (40%)<br>
                <strong>Formula:</strong> Decimal Value = PIPs / 10,000 | Percentage = PIPs / 100
            </div>
            
            <div class="tolerance-rules-container">
                <div class="tolerance-section">
                    <h4>Decreases</h4>
                    <div id="edit-decreases-container"></div>
                </div>

                <div class="tolerance-section">
                    <h4>Increases</h4>
                    <div id="edit-increases-container"></div>
                    <button type="button" class="btn btn-sm btn-success mt-2" onclick="addEditIncreaseRule()">+ Add Increase Rule</button>
                </div>
            </div>
        </div>

        <div class="mb-3">
            <label class="form-label" for="edit_max_buys_per_cycle">Max Buys Per Cycle *</label>
            <input type="number" class="form-control" id="edit_max_buys_per_cycle" name="max_buys_per_cycle" value="5" min="1" required style="max-width: 150px;">
        </div>

        <div class="mb-3">
            <div class="form-check">
                <input type="checkbox" class="form-check-input" id="edit_short_play" name="short_play" value="1">
                <label class="form-check-label" for="edit_short_play">Play is using SHORT play</label>
            </div>
            <div class="form-text">Enable this if the play profits from price decreases (short positions)</div>
        </div>

        <div class="mb-3">
            <label class="form-label" for="edit_trigger_on_perp">Trigger On Perpetual Type *</label>
            <select class="form-select" id="edit_trigger_on_perp" name="trigger_on_perp" required>
                <option value="any">Trigger on any wallet</option>
                <option value="short_only">Trigger only on wallets running SHORT plays</option>
                <option value="long_only">Trigger only on wallets running LONG plays</option>
            </select>
            <div class="form-text">Control which wallet types trigger this play based on their perpetual position</div>
        </div>

        <!-- Timing Conditions Section -->
        <div class="card custom-card mb-3">
            <div class="card-body">
                <div class="form-check mb-3">
                    <input type="checkbox" class="form-check-input" id="enable_timing" name="timing_enabled" value="1">
                    <label class="form-check-label fw-semibold" for="enable_timing">Enable Timing Conditions</label>
                </div>
                <div id="timing_settings" style="display: none;">
                    <div class="row g-3">
                        <div class="col-md-4">
                            <label class="form-label">Price Direction:</label>
                            <select class="form-select" id="timing_price_direction" name="timing_price_direction">
                                <option value="decrease">Decrease ↓</option>
                                <option value="increase">Increase ↑</option>
                            </select>
                        </div>
                        <div class="col-md-4">
                            <label class="form-label">Time Window (seconds):</label>
                            <input type="number" class="form-control" id="timing_time_window" name="timing_time_window" min="1" step="1" placeholder="e.g., 60">
                        </div>
                        <div class="col-md-4">
                            <label class="form-label">Price Change Threshold (decimal):</label>
                            <input type="number" class="form-control" id="timing_price_threshold" name="timing_price_threshold" min="0" step="0.001" placeholder="e.g., 0.005">
                            <div class="form-text">0.005 = 0.5% change</div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Bundle Trades Section -->
        <div class="card custom-card mb-3">
            <div class="card-body">
                <div class="form-check mb-3">
                    <input type="checkbox" class="form-check-input" id="enable_bundle_trades" name="bundle_enabled" value="1">
                    <label class="form-check-label fw-semibold" for="enable_bundle_trades">Enable Bundle Trades</label>
                </div>
                <div id="bundle_trades_settings" style="display: none;">
                    <div class="row g-3">
                        <div class="col-md-6">
                            <label class="form-label">Number of Trades:</label>
                            <input type="number" class="form-control" id="bundle_num_trades" name="bundle_num_trades" min="1" step="1" placeholder="e.g., 3">
                        </div>
                        <div class="col-md-6">
                            <label class="form-label">Within Seconds:</label>
                            <input type="number" class="form-control" id="bundle_seconds" name="bundle_seconds" min="1" step="1" placeholder="e.g., 15">
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Cache Found Wallets Section -->
        <div class="card custom-card mb-3">
            <div class="card-body">
                <div class="form-check mb-3">
                    <input type="checkbox" class="form-check-input" id="enable_cashe_wallets" name="cashe_enabled" value="1">
                    <label class="form-check-label fw-semibold" for="enable_cashe_wallets">Cache Found Wallets</label>
                </div>
                <div id="cashe_wallets_settings" style="display: none;">
                    <div>
                        <label class="form-label">Cache Duration (seconds):</label>
                        <input type="number" class="form-control" id="cashe_seconds" name="cashe_seconds" min="1" step="1" placeholder="e.g., 300" style="max-width: 200px;">
                        <div class="form-text" id="cashe_time_display">How long to cache wallet results</div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Pattern Validator Settings Section -->
        <div class="card custom-card mb-3">
            <div class="card-body">
                <h6 class="fw-semibold mb-3">Pattern Validator Settings</h6>
                
                <div class="mb-3">
                    <label class="form-label" for="edit_project_ids">Pattern Config Projects</label>
                    <select class="form-select" id="edit_project_ids" name="project_ids[]" multiple size="5" style="min-height: 120px;">
                        <?php foreach ($pattern_projects as $project): ?>
                        <option value="<?php echo $project['id']; ?>">
                            <?php echo htmlspecialchars($project['name']); ?>
                            <?php if (isset($project['filter_count'])): ?>
                                (<?php echo $project['filter_count']; ?> filters)
                            <?php endif; ?>
                        </option>
                        <?php endforeach; ?>
                    </select>
                    <div class="form-text">Hold Ctrl (Cmd on Mac) to select multiple projects for trade validation. Leave empty for no project filter.</div>
                </div>
                
                <div class="form-check mb-3">
                    <input type="checkbox" class="form-check-input" id="edit_pattern_validator_enable" name="pattern_validator_enable" value="1">
                    <label class="form-check-label fw-semibold" for="edit_pattern_validator_enable">Enable Pattern Validator</label>
                    <div class="form-text">When enabled, trades will be validated against pattern rules before execution</div>
                </div>
                <div class="form-check mb-3">
                    <input type="checkbox" class="form-check-input" id="edit_pattern_update_by_ai" name="pattern_update_by_ai" value="1">
                    <label class="form-check-label fw-semibold" for="edit_pattern_update_by_ai">AI Auto-Update Pattern Config</label>
                    <div class="form-text">Let AI automatically select the best performing filter projects</div>
                </div>
                <div id="ai_update_notice" class="alert alert-info mb-0" style="display: none;">
                    <i class="ri-robot-line me-2"></i>
                    <strong>AI Management Enabled:</strong> Projects will be automatically selected based on the best performing filters. The project selector above has been disabled.
                </div>
            </div>
        </div>

        <div class="form-actions">
            <button type="submit" class="btn btn-primary" id="updatePlayBtn" <?php echo $is_restricted_play ? 'disabled' : ''; ?>>Update Play</button>
            <button type="button" class="btn btn-secondary" onclick="toggleEditForm()">Cancel</button>
            <button type="button" class="btn btn-danger" onclick="deletePlay()" id="deletePlayBtn" <?php echo $is_restricted_play ? 'disabled' : ''; ?>>Delete Play</button>
        </div>
    </form>
</div>

<!-- Statistics Grid -->
<div class="stats-grid mb-3">
    <div class="stat-card">
        <div class="stat-label">Total Trades</div>
        <div class="stat-value"><?php echo $total_trades; ?></div>
    </div>
    <div class="stat-card">
        <div class="stat-label">Active Trades</div>
        <div class="stat-value"><?php echo $active_trades; ?></div>
    </div>
    <div class="stat-card">
        <div class="stat-label">Completed Trades</div>
        <div class="stat-value"><?php echo $completed_trades; ?></div>
    </div>
    <div class="stat-card">
        <div class="stat-label">No Go Trades</div>
        <div class="stat-value" style="color: rgb(var(--warning-rgb));"><?php echo $no_go_count; ?></div>
    </div>
    <div class="stat-card">
        <div class="stat-label">Sum Profit/Loss</div>
        <div class="stat-value <?php 
            if (!empty($play['short_play'])) {
                echo $total_profit_loss > 0 ? 'negative' : ($total_profit_loss < 0 ? 'positive' : '');
            } else {
                echo $total_profit_loss > 0 ? 'positive' : ($total_profit_loss < 0 ? 'negative' : '');
            }
        ?>">
            <?php echo $total_profit_loss > 0 ? '+' : ''; ?><?php echo number_format($total_profit_loss, 2); ?>%
        </div>
    </div>
</div>

<!-- Tolerance Rules Section -->
<div class="tolerance-grid mb-3">
    <!-- Increases Table -->
    <div class="tolerance-card">
        <div class="tolerance-header">
            <h3>Tolerance Above Entry Price</h3>
            <div class="subtitle">Increase</div>
        </div>
        <div class="table-responsive">
            <table class="table table-bordered mb-0">
                <thead>
                    <tr>
                        <th class="text-center">Range</th>
                        <th class="text-center">Tolerance</th>
                    </tr>
                </thead>
                <tbody>
                    <?php if (!empty($tolerance_rules['increases'])): ?>
                        <?php foreach ($tolerance_rules['increases'] as $rule): ?>
                            <tr>
                                <td class="text-center">
                                    <span class="text-success fw-semibold">
                                        <?php echo number_format($rule['range'][0] * 100, 2); ?>% to <?php echo number_format($rule['range'][1] * 100, 2); ?>%
                                    </span>
                                    <br>
                                    <small class="text-muted">
                                        (<?php echo number_format($rule['range'][0], 4); ?> to <?php echo number_format($rule['range'][1], 4); ?>)
                                    </small>
                                </td>
                                <td class="text-center">
                                    <?php echo number_format($rule['tolerance'] * 100, 3); ?>%
                                    <br>
                                    <small class="text-muted">(<?php echo number_format($rule['tolerance'], 4); ?>)</small>
                                </td>
                            </tr>
                        <?php endforeach; ?>
                    <?php else: ?>
                        <tr>
                            <td colspan="2" class="text-center text-muted">No rules defined</td>
                        </tr>
                    <?php endif; ?>
                </tbody>
            </table>
        </div>
    </div>

    <!-- Decreases Table -->
    <div class="tolerance-card">
        <div class="tolerance-header">
            <h3>Tolerance Below Entry Price</h3>
            <div class="subtitle">Decrease</div>
        </div>
        <div class="p-4 text-center">
            <?php if (!empty($tolerance_rules['decreases'])): ?>
                <?php $rule = $tolerance_rules['decreases'][0]; ?>
                <div class="fs-2 fw-bold text-danger mb-2">
                    <?php echo number_format($rule['tolerance'] * 100, 3); ?>%
                </div>
                <div class="text-muted">
                    (<?php echo number_format($rule['tolerance'], 4); ?>)
                </div>
            <?php else: ?>
                <div class="text-muted">No tolerance defined</div>
            <?php endif; ?>
        </div>
    </div>
</div>

<!-- Live Trades -->
<div class="card custom-card mb-3">
    <div class="card-header">
        <div class="card-title">Live Trades (<?php echo ($play_id === 46) ? '24-Hour' : '7-Day'; ?> DuckDB Data)</div>
        <div class="ms-auto d-flex gap-2 align-items-center">
            <span class="badge bg-info-transparent">Showing <?php echo count($trades); ?></span>
        </div>
    </div>
    <div class="card-body">
        <?php if (empty($trades)): ?>
        <div class="text-center py-5">
            <i class="ri-bar-chart-line fs-48 text-muted mb-3 d-block"></i>
            <h5 class="text-muted">No Live Trades</h5>
            <p class="text-muted mb-0">This play doesn't have any live trades right now.</p>
        </div>
        <?php else: ?>
        <div class="table-responsive">
            <table class="table table-bordered text-nowrap">
                <thead>
                    <tr>
                        <th>Wallet Address</th>
                        <th class="text-center">Entered</th>
                        <th class="text-center">Exited</th>
                        <th class="text-center">Entry Price</th>
                        <th class="text-center">Exit Price</th>
                        <th class="text-center">Profit/Loss</th>
                        <th class="text-center">Status</th>
                        <th class="text-center">Details</th>
                    </tr>
                </thead>
                <tbody id="live-trades-body">
                    <?php 
                    foreach ($trades as $trade): 
                        $current_price = $trade['current_price'] ?? null;
                        $display_exit_price = $trade['our_exit_price'] ?? $current_price;
                        
                        $status_key = strtolower($trade['our_status'] ?? '');
                        if (isset($status_badge_map[$status_key])) {
                            $status_badge = $status_badge_map[$status_key];
                        } else {
                            $status_badge = [
                                'label' => $status_key !== '' ? $status_key : 'unknown',
                                'class' => 'bg-secondary-transparent'
                            ];
                        }
                    ?>
                    <tr onclick="viewTradeDetail(<?php echo $trade['id']; ?>, <?php echo $play_id; ?>)" style="cursor: pointer;">
                        <td>
                            <a href="https://solscan.io/token/<?php echo urlencode($trade['wallet_address'] ?? ''); ?>" target="_blank" rel="noopener" class="text-primary" title="<?php echo htmlspecialchars($trade['wallet_address'] ?? ''); ?>" onclick="event.stopPropagation();">
                                <code><?php echo substr(htmlspecialchars($trade['wallet_address'] ?? ''), 0, 12); ?>...</code>
                            </a>
                        </td>
                        <td class="text-center">
                            <?php 
                            if (!empty($trade['followed_at'])) {
                                date_default_timezone_set('UTC');
                                echo gmdate('M d, H:i', strtotime($trade['followed_at'])) . ' UTC';
                            } else {
                                echo '--';
                            }
                            ?>
                        </td>
                        <td class="text-center">
                            <?php 
                            if (!empty($trade['our_exit_timestamp'])) {
                                date_default_timezone_set('UTC');
                                echo gmdate('M d, H:i', strtotime($trade['our_exit_timestamp'])) . ' UTC';
                            } else {
                                echo '--';
                            }
                            ?>
                        </td>
                        <td class="text-center">
                            <?php if (!empty($trade['our_entry_price'])): ?>
                                $<?php echo number_format($trade['our_entry_price'], 6); ?>
                            <?php else: ?>
                                <span class="text-muted">--</span>
                            <?php endif; ?>
                        </td>
                        <td class="text-center">
                            <?php if ($display_exit_price): ?>
                                $<?php echo number_format($display_exit_price, 6); ?>
                                <?php if (empty($trade['our_exit_price']) && $current_price): ?>
                                    <small class="text-warning">(current)</small>
                                <?php endif; ?>
                            <?php else: ?>
                                <span class="text-muted">--</span>
                            <?php endif; ?>
                        </td>
                        <td class="text-center">
                            <?php if ($trade['our_profit_loss'] !== null): ?>
                                <span class="fw-semibold <?php 
                                    if (!empty($play['short_play'])) {
                                        echo $trade['our_profit_loss'] > 0 ? 'text-danger' : ($trade['our_profit_loss'] < 0 ? 'text-success' : 'text-muted');
                                    } else {
                                        echo $trade['our_profit_loss'] > 0 ? 'text-success' : ($trade['our_profit_loss'] < 0 ? 'text-danger' : 'text-muted');
                                    }
                                ?>">
                                    <?php echo $trade['our_profit_loss'] > 0 ? '+' : ''; ?><?php echo number_format($trade['our_profit_loss'], 2); ?>%
                                </span>
                            <?php elseif (($trade['our_status'] ?? '') === 'pending' && !empty($trade['current_price']) && !empty($trade['our_entry_price'])): ?>
                                <?php 
                                    $pending_gain_loss = (($trade['current_price'] - $trade['our_entry_price']) / $trade['our_entry_price']) * 100;
                                    $pending_class = 'text-warning';
                                ?>
                                <span class="fw-semibold <?php echo $pending_class; ?>">
                                    <?php echo $pending_gain_loss > 0 ? '+' : ''; ?><?php echo number_format($pending_gain_loss, 2); ?>%
                                </span>
                            <?php else: ?>
                                <span class="text-muted">--</span>
                            <?php endif; ?>
                        </td>
                        <td class="text-center">
                            <span class="badge <?php echo htmlspecialchars($status_badge['class']); ?>">
                                <?php echo htmlspecialchars($status_badge['label']); ?>
                            </span>
                        </td>
                        <td class="text-center">
                            <button class="btn btn-sm btn-icon btn-primary-light" onclick="viewTradeDetail(<?php echo $trade['id']; ?>, <?php echo $play_id; ?>); event.stopPropagation();" title="View Details">
                                <i class="ri-eye-line"></i>
                            </button>
                        </td>
                    </tr>
                    <?php endforeach; ?>
                </tbody>
            </table>
        </div>
        <?php endif; ?>
    </div>
</div>

<!-- No-Go Trades Section -->
<?php if (!empty($no_go_trades)): ?>
<div class="card custom-card mb-3">
    <div class="card-header" style="background: rgba(var(--warning-rgb), 0.1); border-bottom-color: rgba(var(--warning-rgb), 0.3);">
        <div class="card-title" style="color: rgb(var(--warning-rgb));">
            <i class="ri-close-circle-line me-1"></i>No-Go Trades (Blocked by Validator)
        </div>
        <div class="ms-auto d-flex gap-2 align-items-center">
            <span class="badge bg-warning-transparent text-warning"><?php echo count($no_go_trades); ?> trades</span>
            <button class="btn btn-sm btn-outline-secondary" type="button" onclick="toggleNoGoTrades()">
                <i class="ri-arrow-down-s-line" id="noGoToggleIcon"></i>
            </button>
        </div>
    </div>
    <div class="card-body" id="noGoTradesBody">
        <div class="table-responsive">
            <table class="table table-bordered text-nowrap table-sm">
                <thead>
                    <tr>
                        <th>Wallet Address</th>
                        <th class="text-center">Timestamp</th>
                        <th class="text-center">Entry Price</th>
                        <th class="text-center">Trade Price</th>
                        <th class="text-center">Reason</th>
                        <th class="text-center">Details</th>
                    </tr>
                </thead>
                <tbody>
                    <?php foreach ($no_go_trades as $trade): 
                        // Try to extract reason from pattern_validator_log
                        $reason = 'Validation Failed';
                        if (!empty($trade['pattern_validator_log'])) {
                            $log = is_string($trade['pattern_validator_log']) 
                                ? json_decode($trade['pattern_validator_log'], true) 
                                : $trade['pattern_validator_log'];
                            if (isset($log['decision'])) {
                                $reason = $log['decision'];
                                if (isset($log['reason'])) {
                                    $reason .= ': ' . $log['reason'];
                                } elseif (isset($log['error'])) {
                                    $reason .= ': ' . $log['error'];
                                }
                            }
                        }
                    ?>
                    <tr onclick="viewTradeDetail(<?php echo $trade['id']; ?>, <?php echo $play_id; ?>)" style="cursor: pointer; opacity: 0.8;">
                        <td>
                            <a href="https://solscan.io/token/<?php echo urlencode($trade['wallet_address'] ?? ''); ?>" target="_blank" rel="noopener" class="text-muted" title="<?php echo htmlspecialchars($trade['wallet_address'] ?? ''); ?>" onclick="event.stopPropagation();">
                                <code><?php echo substr(htmlspecialchars($trade['wallet_address'] ?? ''), 0, 12); ?>...</code>
                            </a>
                        </td>
                        <td class="text-center text-muted">
                            <?php 
                            if (!empty($trade['followed_at'])) {
                                date_default_timezone_set('UTC');
                                echo gmdate('M d, H:i', strtotime($trade['followed_at'])) . ' UTC';
                            } else {
                                echo '--';
                            }
                            ?>
                        </td>
                        <td class="text-center">
                            <?php if (!empty($trade['our_entry_price'])): ?>
                                $<?php echo number_format($trade['our_entry_price'], 4); ?>
                            <?php else: ?>
                                <span class="text-muted">--</span>
                            <?php endif; ?>
                        </td>
                        <td class="text-center">
                            <?php if (!empty($trade['price'])): ?>
                                $<?php echo number_format($trade['price'], 4); ?>
                            <?php else: ?>
                                <span class="text-muted">--</span>
                            <?php endif; ?>
                        </td>
                        <td class="text-center">
                            <span class="badge bg-warning-transparent text-warning" style="font-size: 0.7rem; max-width: 200px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;" title="<?php echo htmlspecialchars($reason); ?>">
                                <?php echo htmlspecialchars(strlen($reason) > 30 ? substr($reason, 0, 30) . '...' : $reason); ?>
                            </span>
                        </td>
                        <td class="text-center">
                            <button class="btn btn-sm btn-icon btn-warning-light" onclick="viewTradeDetail(<?php echo $trade['id']; ?>, <?php echo $play_id; ?>); event.stopPropagation();" title="View Details">
                                <i class="ri-eye-line"></i>
                            </button>
                        </td>
                    </tr>
                    <?php endforeach; ?>
                </tbody>
            </table>
        </div>
    </div>
</div>
<?php endif; ?>

<?php
$content = ob_get_clean();

// --- Page Scripts ---
ob_start();
?>

<script>
    // API Configuration
    const API_BASE = '<?php echo $API_BASE; ?>';
    window.isRestrictedPlay = <?php echo $is_restricted_play ? 'true' : 'false'; ?>;
    window.playId = <?php echo $play_id; ?>;

    // Performance Timing Report
    (function() {
        const timingData = <?php echo $json_timing_report; ?>;
        console.log('%c⏱️ PAGE PERFORMANCE TIMING REPORT', 'color: rgb(var(--primary-rgb)); font-size: 14px; font-weight: bold;');
        console.table(timingData);
    })();
    
    // Update play sorting via API
    async function updatePlaySorting(playId, sorting) {
        if (window.isRestrictedPlay) {
            alert('Priority changes are disabled for this play.');
            return;
        }
        try {
            const response = await fetch(API_BASE + '?endpoint=/plays/' + playId, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ sorting: parseInt(sorting) })
            });
            
            const data = await response.json();
            
            if (data.success) {
                const select = document.getElementById('playSorting');
                const originalBg = select.style.backgroundColor;
                select.style.backgroundColor = 'rgba(var(--success-rgb), 0.3)';
                setTimeout(() => { select.style.backgroundColor = originalBg; }, 500);
            } else {
                alert('Error updating priority: ' + data.error);
            }
        } catch (error) {
            console.error('Error updating sorting:', error);
            alert('Error updating priority. Please try again.');
        }
    }
    
    // Play editing functions
    function toggleEditForm() {
        if (window.isRestrictedPlay) {
            alert('Editing is disabled for this play.');
            return;
        }
        const editForm = document.getElementById('editPlayForm');
        const isHidden = editForm.classList.contains('hidden');
        
        if (isHidden) {
            loadPlayForEdit();
            editForm.classList.remove('hidden');
            editForm.scrollIntoView({ behavior: 'smooth', block: 'start' });
        } else {
            editForm.classList.add('hidden');
        }
    }

    async function loadPlayForEdit() {
        if (window.isRestrictedPlay) return;
        const playId = <?php echo $play_id; ?>;
        
        try {
            const response = await fetch(API_BASE + '?endpoint=/plays/' + playId + '/for_edit');
            
            if (!response.ok) {
                const errorText = await response.text();
                console.error('API Response Error:', response.status, errorText);
                alert('Error loading play data: Server returned ' + response.status);
                return;
            }
            
            const data = await response.json();
            
            if (data.success) {
                document.getElementById('edit_play_id').value = data.id;
                document.getElementById('edit_name').value = data.name;
                document.getElementById('edit_description').value = data.description;
                document.getElementById('edit_find_wallets_sql').value = data.find_wallets_sql?.query || '';
                document.getElementById('edit_max_buys_per_cycle').value = data.max_buys_per_cycle;
                document.getElementById('edit_short_play').checked = data.short_play == 1;
                
                const triggerMode = data.trigger_on_perp?.mode || 'any';
                document.getElementById('edit_trigger_on_perp').value = triggerMode;
                
                // Handle timing conditions
                if (data.timing_conditions && data.timing_conditions.enabled) {
                    document.getElementById('enable_timing').checked = true;
                    document.getElementById('timing_settings').style.display = 'block';
                    document.getElementById('timing_price_direction').value = data.timing_conditions.price_direction || 'decrease';
                    document.getElementById('timing_time_window').value = data.timing_conditions.time_window_seconds || '';
                    document.getElementById('timing_price_threshold').value = data.timing_conditions.price_change_threshold || '';
                } else {
                    document.getElementById('enable_timing').checked = false;
                    document.getElementById('timing_settings').style.display = 'none';
                }
                
                // Handle bundle trades
                if (data.bundle_trades && data.bundle_trades.enabled) {
                    document.getElementById('enable_bundle_trades').checked = true;
                    document.getElementById('bundle_trades_settings').style.display = 'block';
                    document.getElementById('bundle_num_trades').value = data.bundle_trades.num_trades || '';
                    document.getElementById('bundle_seconds').value = data.bundle_trades.seconds || '';
                } else {
                    document.getElementById('enable_bundle_trades').checked = false;
                    document.getElementById('bundle_trades_settings').style.display = 'none';
                }
                
                // Handle cache wallets
                if (data.cashe_wallets && data.cashe_wallets.enabled) {
                    document.getElementById('enable_cashe_wallets').checked = true;
                    document.getElementById('cashe_wallets_settings').style.display = 'block';
                    document.getElementById('cashe_seconds').value = data.cashe_wallets.seconds || '';
                    document.getElementById('cashe_seconds').dispatchEvent(new Event('input'));
                } else {
                    document.getElementById('enable_cashe_wallets').checked = false;
                    document.getElementById('cashe_wallets_settings').style.display = 'none';
                }
                
                // Handle multi-select for project_ids
                const projectSelect = document.getElementById('edit_project_ids');
                if (projectSelect) {
                    let projectIds = data.project_ids || [];
                    
                    // Handle case where project_ids might be a string (e.g., "[1,2]" or "1,2")
                    if (typeof projectIds === 'string') {
                        try {
                            projectIds = JSON.parse(projectIds);
                        } catch (e) {
                            // Fallback: try comma-separated
                            projectIds = projectIds.split(',').map(id => parseInt(id.trim())).filter(id => !isNaN(id));
                        }
                    }
                    
                    // Ensure all IDs are integers for comparison
                    const projectIdNumbers = Array.isArray(projectIds) 
                        ? projectIds.map(id => parseInt(id)).filter(id => !isNaN(id))
                        : [];
                    
                    // Clear all selections first, then set the correct ones
                    Array.from(projectSelect.options).forEach(opt => {
                        const optValue = parseInt(opt.value);
                        const shouldSelect = projectIdNumbers.includes(optValue);
                        opt.selected = shouldSelect;
                    });
                }
                
                // Handle pattern validator settings
                const patternValidatorEnabled = data.pattern_validator_enable == 1;
                const patternUpdateByAi = data.pattern_update_by_ai == 1;
                
                document.getElementById('edit_pattern_validator_enable').checked = patternValidatorEnabled;
                document.getElementById('edit_pattern_update_by_ai').checked = patternUpdateByAi;
                
                // Apply AI update state to UI elements
                const validatorEnableCheckbox = document.getElementById('edit_pattern_validator_enable');
                const aiNotice = document.getElementById('ai_update_notice');
                
                if (patternUpdateByAi) {
                    // Auto-enable pattern validator when AI update is enabled
                    validatorEnableCheckbox.checked = true;
                    validatorEnableCheckbox.disabled = true;
                    
                    // Disable and grey out project selector
                    if (projectSelect) {
                        projectSelect.disabled = true;
                        projectSelect.style.opacity = '0.5';
                        projectSelect.style.cursor = 'not-allowed';
                    }
                    aiNotice.style.display = 'block';
                } else {
                    validatorEnableCheckbox.disabled = false;
                    
                    // Re-enable project selector
                    if (projectSelect) {
                        projectSelect.disabled = false;
                        projectSelect.style.opacity = '1';
                        projectSelect.style.cursor = '';
                    }
                    aiNotice.style.display = 'none';
                }
                
                document.getElementById('edit-decreases-container').innerHTML = '';
                document.getElementById('edit-increases-container').innerHTML = '';
                
                if (data.sell_logic?.tolerance_rules?.decreases && data.sell_logic.tolerance_rules.decreases.length > 0) {
                    const rule = data.sell_logic.tolerance_rules.decreases[0];
                    addEditDecreaseRule(rule.range?.[0], rule.range?.[1], rule.tolerance);
                }
                
                if (data.sell_logic?.tolerance_rules?.increases) {
                    data.sell_logic.tolerance_rules.increases.forEach(rule => {
                        addEditIncreaseRule(rule.range?.[0], rule.range?.[1], rule.tolerance);
                    });
                }
            } else {
                alert('Error loading play data: ' + (data.error || 'Unknown error'));
            }
        } catch (error) {
            console.error('Error loading play for edit:', error);
            alert('Error loading play data: ' + error.message);
        }
    }

    function addEditDecreaseRule(rangeFrom = null, rangeTo = null, tolerance = null) {
        const container = document.getElementById('edit-decreases-container');
        const tolerancePips = tolerance !== null ? Math.round(tolerance * 10000) : '';
        const ruleHtml = `
            <div class="tolerance-rule-single">
                <div>
                    <label class="tolerance-rule-label">Tolerance (PIPs)</label>
                    <input type="number" step="1" class="form-control-inline pip-input" name="decrease_tolerance[]" value="${tolerancePips}" required oninput="updatePipConversion(this)">
                    <small class="pip-conversion">${tolerance !== null ? '= ' + tolerance.toFixed(4) + ' (' + (tolerance * 100).toFixed(2) + '%)' : ''}</small>
                    <input type="hidden" name="decrease_range_from[]" value="-999999">
                    <input type="hidden" name="decrease_range_to[]" value="0">
                </div>
            </div>
        `;
        container.innerHTML = ruleHtml;
    }

    function addEditIncreaseRule(rangeFrom = null, rangeTo = null, tolerance = null) {
        const container = document.getElementById('edit-increases-container');
        const rangeFromPips = rangeFrom !== null ? Math.round(rangeFrom * 10000) : '';
        const rangeToPips = rangeTo !== null ? Math.round(rangeTo * 10000) : '';
        const tolerancePips = tolerance !== null ? Math.round(tolerance * 10000) : '';
        const ruleHtml = `
            <div class="tolerance-rule">
                <div>
                    <label class="tolerance-rule-label">Range From (PIPs)</label>
                    <input type="number" step="1" class="form-control-inline pip-input" name="increase_range_from[]" value="${rangeFromPips}" required oninput="updatePipConversion(this)">
                    <small class="pip-conversion">${rangeFrom !== null ? '= ' + rangeFrom + ' (' + (rangeFrom * 100).toFixed(2) + '%)' : ''}</small>
                </div>
                <div>
                    <label class="tolerance-rule-label">Range To (PIPs)</label>
                    <input type="number" step="1" class="form-control-inline pip-input" name="increase_range_to[]" value="${rangeToPips}" required oninput="updatePipConversion(this)">
                    <small class="pip-conversion">${rangeTo !== null ? '= ' + rangeTo + ' (' + (rangeTo * 100).toFixed(2) + '%)' : ''}</small>
                </div>
                <div>
                    <label class="tolerance-rule-label">Tolerance (PIPs)</label>
                    <input type="number" step="1" class="form-control-inline pip-input" name="increase_tolerance[]" value="${tolerancePips}" required oninput="updatePipConversion(this)">
                    <small class="pip-conversion">${tolerance !== null ? '= ' + tolerance + ' (' + (tolerance * 100).toFixed(2) + '%)' : ''}</small>
                </div>
                <button type="button" class="btn btn-sm btn-danger" onclick="removeRule(this)">Remove</button>
            </div>
        `;
        container.insertAdjacentHTML('beforeend', ruleHtml);
    }

    function removeRule(button) {
        button.closest('.tolerance-rule').remove();
    }

    function updatePipConversion(input) {
        const pips = parseFloat(input.value) || 0;
        const decimal = pips / 10000;
        const percentage = pips / 100;
        const conversionText = input.parentElement.querySelector('.pip-conversion');
        if (conversionText) {
            conversionText.textContent = '= ' + decimal.toFixed(4) + ' (' + percentage.toFixed(2) + '%)';
        }
    }

    function buildSellLogic(form) {
        const decreases = [];
        const increases = [];
        
        const decreaseTolerance = form.querySelectorAll('[name="decrease_tolerance[]"]');
        const decreaseFrom = form.querySelectorAll('[name="decrease_range_from[]"]');
        const decreaseTo = form.querySelectorAll('[name="decrease_range_to[]"]');
        
        for (let i = 0; i < decreaseTolerance.length; i++) {
            const pips = parseFloat(decreaseTolerance[i].value) || 0;
            decreases.push({
                range: [parseFloat(decreaseFrom[i]?.value || -999999) / 10000, parseFloat(decreaseTo[i]?.value || 0) / 10000],
                tolerance: pips / 10000
            });
        }
        
        const increaseTolerance = form.querySelectorAll('[name="increase_tolerance[]"]');
        const increaseFrom = form.querySelectorAll('[name="increase_range_from[]"]');
        const increaseTo = form.querySelectorAll('[name="increase_range_to[]"]');
        
        for (let i = 0; i < increaseTolerance.length; i++) {
            increases.push({
                range: [parseFloat(increaseFrom[i].value) / 10000, parseFloat(increaseTo[i].value) / 10000],
                tolerance: parseFloat(increaseTolerance[i].value) / 10000
            });
        }
        
        return {
            tolerance_rules: {
                decreases: decreases,
                increases: increases
            }
        };
    }

    async function handleUpdatePlay(event) {
        event.preventDefault();
        
        if (window.isRestrictedPlay) {
            alert('Editing is disabled for this play.');
            return false;
        }
        
        const form = event.target;
        const submitBtn = document.getElementById('updatePlayBtn');
        submitBtn.disabled = true;
        submitBtn.innerHTML = '<i class="ri-loader-4-line me-1"></i>Updating...';
        
        try {
            // Get project_ids from multi-select
            const projectSelect = document.getElementById('edit_project_ids');
            const selectedProjects = Array.from(projectSelect.selectedOptions).map(opt => parseInt(opt.value));
            
            // Build timing conditions
            const timingEnabled = document.getElementById('enable_timing').checked;
            const timingConditions = {
                enabled: timingEnabled,
                price_direction: timingEnabled ? document.getElementById('timing_price_direction').value : 'decrease',
                time_window_seconds: timingEnabled ? parseInt(document.getElementById('timing_time_window').value) || 60 : 60,
                price_change_threshold: timingEnabled ? parseFloat(document.getElementById('timing_price_threshold').value) || 0.005 : 0.005
            };
            
            // Build bundle trades
            const bundleEnabled = document.getElementById('enable_bundle_trades').checked;
            const bundleTrades = {
                enabled: bundleEnabled,
                num_trades: bundleEnabled ? parseInt(document.getElementById('bundle_num_trades').value) || null : null,
                seconds: bundleEnabled ? parseInt(document.getElementById('bundle_seconds').value) || null : null
            };
            
            // Build cache wallets
            const casheEnabled = document.getElementById('enable_cashe_wallets').checked;
            const casheWallets = {
                enabled: casheEnabled,
                seconds: casheEnabled ? parseInt(document.getElementById('cashe_seconds').value) || null : null
            };
            
            const data = {
                name: form.querySelector('[name="name"]').value,
                description: form.querySelector('[name="description"]').value,
                find_wallets_sql: form.querySelector('[name="find_wallets_sql"]').value,
                sell_logic: buildSellLogic(form),
                max_buys_per_cycle: parseInt(form.querySelector('[name="max_buys_per_cycle"]').value),
                short_play: form.querySelector('[name="short_play"]').checked ? 1 : 0,
                trigger_on_perp: { mode: form.querySelector('[name="trigger_on_perp"]').value },
                timing_conditions: timingConditions,
                bundle_trades: bundleTrades,
                cashe_wallets: casheWallets,
                pattern_validator_enable: document.getElementById('edit_pattern_validator_enable').checked ? 1 : 0,
                pattern_update_by_ai: document.getElementById('edit_pattern_update_by_ai').checked ? 1 : 0,
                project_ids: selectedProjects
            };
            
            const response = await fetch(API_BASE + '?endpoint=/plays/' + window.playId, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(data)
            });
            
            const result = await response.json();
            
            if (result.success) {
                window.location.href = '?id=' + window.playId + '&success=1';
            } else {
                alert('Error updating play: ' + (result.error || 'Unknown error'));
            }
        } catch (error) {
            console.error('Error updating play:', error);
            alert('Error updating play. Please try again.');
        } finally {
            submitBtn.disabled = false;
            submitBtn.innerHTML = 'Update Play';
        }
        
        return false;
    }

    async function deletePlay() {
        if (window.isRestrictedPlay) {
            alert('Deleting is disabled for this play.');
            return;
        }
        const playName = document.getElementById('edit_name').value;
        
        if (!confirm(`Are you sure you want to delete the play "${playName}"?\n\nThis action cannot be undone.`)) {
            return;
        }
        
        try {
            const response = await fetch(API_BASE + '?endpoint=/plays/' + window.playId, { method: 'DELETE' });
            const data = await response.json();
            
            if (data.success) {
                alert('Play deleted successfully!');
                window.location.href = '../';
            } else {
                alert('Error deleting play: ' + data.error);
            }
        } catch (error) {
            console.error('Error deleting play:', error);
            alert('Error deleting play. Please try again.');
        }
    }

    // Initialize form handlers
    document.addEventListener('DOMContentLoaded', function() {
        document.querySelectorAll('.pip-input').forEach(input => {
            updatePipConversion(input);
            input.addEventListener('input', function() {
                updatePipConversion(this);
            });
        });
        
        // Timing conditions toggle
        document.getElementById('enable_timing').addEventListener('change', function(e) {
            document.getElementById('timing_settings').style.display = e.target.checked ? 'block' : 'none';
        });
        
        // Bundle trades toggle
        document.getElementById('enable_bundle_trades').addEventListener('change', function(e) {
            document.getElementById('bundle_trades_settings').style.display = e.target.checked ? 'block' : 'none';
        });
        
        // Cache wallets toggle
        document.getElementById('enable_cashe_wallets').addEventListener('change', function(e) {
            document.getElementById('cashe_wallets_settings').style.display = e.target.checked ? 'block' : 'none';
        });
        
        // Cache duration display updater
        document.getElementById('cashe_seconds').addEventListener('input', function(e) {
            const seconds = parseInt(e.target.value) || 0;
            const display = document.getElementById('cashe_time_display');
            
            if (seconds < 60) {
                display.textContent = `= ${seconds} second${seconds !== 1 ? 's' : ''}`;
            } else if (seconds < 3600) {
                const minutes = (seconds / 60).toFixed(1);
                display.textContent = `= ${minutes} minute${minutes !== '1.0' ? 's' : ''}`;
            } else {
                const hours = (seconds / 3600).toFixed(2);
                display.textContent = `= ${hours} hour${hours !== '1.00' ? 's' : ''}`;
            }
        });
        
        // Pattern Validator AI Update checkbox handler
        document.getElementById('edit_pattern_update_by_ai').addEventListener('change', function(e) {
            const aiEnabled = e.target.checked;
            const validatorEnableCheckbox = document.getElementById('edit_pattern_validator_enable');
            const projectSelect = document.getElementById('edit_project_ids');
            const aiNotice = document.getElementById('ai_update_notice');
            
            if (aiEnabled) {
                // Auto-enable pattern validator when AI update is enabled
                validatorEnableCheckbox.checked = true;
                validatorEnableCheckbox.disabled = true;
                
                // Disable and grey out project selector
                projectSelect.disabled = true;
                projectSelect.style.opacity = '0.5';
                projectSelect.style.cursor = 'not-allowed';
                
                // Show AI notice
                aiNotice.style.display = 'block';
            } else {
                // Re-enable pattern validator checkbox
                validatorEnableCheckbox.disabled = false;
                
                // Re-enable project selector
                projectSelect.disabled = false;
                projectSelect.style.opacity = '1';
                projectSelect.style.cursor = '';
                
                // Hide AI notice
                aiNotice.style.display = 'none';
            }
        });
    });
    
    function viewTradeDetail(tradeId, playId, tradeType = 'live') {
        const params = new URLSearchParams({
            id: tradeId,
            play_id: playId,
            return_url: window.location.pathname + window.location.search
        });
        window.location.href = `trade/?${params.toString()}`;
    }
    
    function toggleNoGoTrades() {
        const body = document.getElementById('noGoTradesBody');
        const icon = document.getElementById('noGoToggleIcon');
        
        if (body.style.display === 'none') {
            body.style.display = 'block';
            icon.classList.remove('ri-arrow-right-s-line');
            icon.classList.add('ri-arrow-down-s-line');
        } else {
            body.style.display = 'none';
            icon.classList.remove('ri-arrow-down-s-line');
            icon.classList.add('ri-arrow-right-s-line');
        }
    }
</script>
<?php
$scripts = ob_get_clean();

// Include the base layout
include __DIR__ . '/../../pages/layouts/base.php';
?>
