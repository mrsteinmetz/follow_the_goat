<?php
/**
 * Trade Details Page - Individual Trade View
 * Ported from chart/plays/unique/trade/index.php to v2 template system
 */

// --- Load Configuration from .env ---
require_once __DIR__ . '/../../../../chart/config.php';

// Get trade ID, play ID, and return URL from query parameters
$trade_id = (int)($_GET['id'] ?? 0);
$play_id = (int)($_GET['play_id'] ?? 0);
$return_url = $_GET['return_url'] ?? '../?id=' . $play_id;
$requested_source = strtolower($_GET['source'] ?? '');
$source = null;

if ($trade_id <= 0) {
    header('Location: ../../?error=' . urlencode('Invalid trade ID'));
    exit;
}

// --- Base URL for v2 template ---
$rootFolder = basename($_SERVER['DOCUMENT_ROOT']);
$baseUrl = (isset($_SERVER['HTTPS']) && $_SERVER['HTTPS'] === 'on' ? 'https://' : 'http://') . $_SERVER['HTTP_HOST'] . dirname(dirname(dirname(dirname($_SERVER['SCRIPT_NAME']))));

// --- Data Fetching ---
$dsn = "mysql:host=$db_host;dbname=$db_name;charset=$db_charset";
$options = [
    PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
    PDO::ATTR_EMULATE_PREPARES   => false,
];

$trade = null;
$play = null;
$error_message = '';
$trade_table_name = 'solcatcher.follow_the_goat_buyins_archive';

try {
    $pdo = new PDO($dsn, $db_user, $db_pass, $options);

    // Fetch trade details from the requested source, falling back if needed
    $tableMap = [
        'archive' => 'solcatcher.follow_the_goat_buyins_archive',
        'live' => 'solcatcher.follow_the_goat_buyins',
    ];
    $queryTemplate = "
        SELECT 
            id,
            play_id,
            wallet_address,
            tolerance,
            block_timestamp,
            price as org_price_entry,
            followed_at,
            our_entry_price,
            our_exit_price,
            our_exit_timestamp,
            our_profit_loss,
            our_status,
            price_movements,
            entry_log
        FROM %s
        WHERE id = :id
    ";
    $sourceOrder = ['archive', 'live'];

    foreach ($sourceOrder as $sourceKey) {
        $stmt = $pdo->prepare(sprintf($queryTemplate, $tableMap[$sourceKey]));
        $stmt->execute(['id' => $trade_id]);
        $trade = $stmt->fetch();

        if ($trade) {
            $source = $sourceKey;
            $trade_table_name = $tableMap[$sourceKey];
            break;
        }
    }
    
    if (!$trade) {
        header('Location: ../../?error=' . urlencode('Trade not found'));
        exit;
    }
    
    // Update play_id if not provided
    if ($play_id <= 0) {
        $play_id = $trade['play_id'];
    }
    
    // Fetch play details
    $stmt = $pdo->prepare("SELECT id, name, description, short_play FROM solcatcher.follow_the_goat_plays WHERE id = :id");
    $stmt->execute(['id' => $play_id]);
    $play = $stmt->fetch();
    
    // Fetch price checks from the appropriate table based on trade source
    $price_checks = [];
    $price_checks_source = 'live';
    
    $priceChecksTableMap = [
        'live' => 'solcatcher.follow_the_goat_buyins_price_checks',
        'archive' => 'solcatcher.follow_the_goat_buyins_price_checks_archive',
    ];
    
    $priceChecksOrder = ($source === 'archive') ? ['archive', 'live'] : ['live', 'archive'];
    
    $priceChecksQuery = "
        SELECT 
            id,
            buyin_id,
            checked_at,
            current_price,
            entry_price,
            highest_price,
            reference_price,
            gain_from_entry,
            drop_from_high,
            drop_from_entry,
            drop_from_reference,
            tolerance,
            basis,
            bucket,
            applied_rule,
            should_sell,
            is_backfill,
            created_at
        FROM %s
        WHERE buyin_id = :buyin_id
        ORDER BY checked_at ASC
    ";
    
    foreach ($priceChecksOrder as $pcSource) {
        try {
            $stmt = $pdo->prepare(sprintf($priceChecksQuery, $priceChecksTableMap[$pcSource]));
            $stmt->execute(['buyin_id' => $trade_id]);
            $price_checks = $stmt->fetchAll();
            
            if (!empty($price_checks)) {
                $price_checks_source = $pcSource;
                break;
            }
        } catch (\PDOException $e) {
            continue;
        }
    }

    // Fetch filter validation results from trade_filter_results table
    $filter_results = [];
    $filter_results_summary = [];
    try {
        $stmt = $pdo->prepare("
            SELECT 
                tfr.id,
                tfr.project_id,
                tfr.filter_id,
                tfr.filter_name,
                tfr.field_column,
                tfr.section,
                tfr.minute,
                tfr.from_value,
                tfr.to_value,
                tfr.actual_value,
                tfr.passed,
                tfr.error,
                tfr.evaluated_at,
                pcp.name as project_name
            FROM trade_filter_results tfr
            LEFT JOIN pattern_config_projects pcp ON tfr.project_id = pcp.id
            WHERE tfr.buyin_id = :buyin_id
            ORDER BY tfr.project_id, tfr.filter_id
        ");
        $stmt->execute(['buyin_id' => $trade_id]);
        $filter_results = $stmt->fetchAll();
        
        // Build summary per project
        if (!empty($filter_results)) {
            $project_groups = [];
            foreach ($filter_results as $fr) {
                $pid = $fr['project_id'];
                if (!isset($project_groups[$pid])) {
                    $project_groups[$pid] = [
                        'project_id' => $pid,
                        'project_name' => $fr['project_name'] ?? "Project #$pid",
                        'total' => 0,
                        'passed' => 0,
                        'failed' => 0,
                        'filters' => []
                    ];
                }
                $project_groups[$pid]['total']++;
                if ($fr['passed']) {
                    $project_groups[$pid]['passed']++;
                } else {
                    $project_groups[$pid]['failed']++;
                }
                $project_groups[$pid]['filters'][] = $fr;
            }
            
            // Determine which projects passed (all filters passed)
            foreach ($project_groups as $pid => &$pg) {
                $pg['all_passed'] = ($pg['failed'] === 0 && $pg['passed'] > 0);
            }
            unset($pg);
            
            $filter_results_summary = array_values($project_groups);
        }
    } catch (\PDOException $e) {
        // Table might not exist yet - that's okay
    }

} catch (\PDOException $e) {
    $error_message = "Database error: " . $e->getMessage();
}

$entry_log_pretty = null;
$entry_log_raw = null;

if ($trade && array_key_exists('entry_log', $trade)) {
    $entry_log_raw = $trade['entry_log'];

    if ($entry_log_raw !== null && $entry_log_raw !== '') {
        $candidates = [
            $entry_log_raw,
            trim($entry_log_raw, "\"'"),
            stripcslashes(trim($entry_log_raw, "\"'"))
        ];

        foreach ($candidates as $candidate) {
            if ($candidate === '' || $candidate === null) continue;

            $decoded = json_decode($candidate, true);

            if (json_last_error() === JSON_ERROR_NONE) {
                if (is_string($decoded)) {
                    $decodedAgain = json_decode($decoded, true);
                    if (json_last_error() === JSON_ERROR_NONE) {
                        $decoded = $decodedAgain;
                    }
                }

                if ($decoded !== null) {
                    $entry_log_pretty = json_encode($decoded, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
                }
                break;
            }
        }
    }
}

// Compute duration
$duration_text = '--';
$effective_exit_ms = null;
$effective_exit_price = null;
$effective_exit_timestamp = null;

if ($trade && $trade['followed_at']) {
    $entry_time = strtotime($trade['followed_at']);
    $status = strtolower($trade['our_status']);
    
    if ($status !== 'completed' && $status !== 'sold') {
        $stmt = $pdo->query("SELECT NOW() as server_time");
        $current_time_row = $stmt->fetch();
        $effective_exit_timestamp = $current_time_row['server_time'];
        $effective_exit_ms = strtotime($effective_exit_timestamp);
        
        $price_movements = json_decode($trade['price_movements'] ?? '[]', true);
        if (is_array($price_movements) && count($price_movements) > 0) {
            $last = end($price_movements);
            $effective_exit_price = $last['current_price'] ?? null;
        }
    } elseif ($trade['our_exit_timestamp']) {
        $effective_exit_timestamp = $trade['our_exit_timestamp'];
        $effective_exit_ms = strtotime($trade['our_exit_timestamp']);
        $effective_exit_price = $trade['our_exit_price'];
    } else {
        $effective_exit_timestamp = $trade['followed_at'];
        $effective_exit_ms = $entry_time;
    }
    
    if ($effective_exit_ms && $effective_exit_ms >= $entry_time) {
        $minutes = ($effective_exit_ms - $entry_time) / 60;
        $duration_text = number_format($minutes, 2) . ' min';
    }
}

// Pass computed values to JavaScript
// Use server timezone (same as how data is stored in database)
$followed_at_ms = $trade['followed_at'] ? strtotime($trade['followed_at']) * 1000 : null;
$block_timestamp_ms = $trade['block_timestamp'] ? strtotime($trade['block_timestamp']) * 1000 : null;

$js_data = [
    'trade' => $trade,
    'followed_at_ms' => $followed_at_ms,
    'block_timestamp_ms' => $block_timestamp_ms,
    'effective_exit_ms' => $effective_exit_ms ? $effective_exit_ms * 1000 : null,
    'effective_exit_price' => $effective_exit_price,
    'price_checks' => $price_checks,
    'price_checks_source' => $price_checks_source
];

// --- Page Styles ---
ob_start();
?>
<style>
    .trade-info-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        gap: 1rem;
    }
    
    .trade-info-item {
        background: var(--custom-white);
        padding: 1.25rem;
        border-radius: 0.5rem;
        border: 1px solid var(--default-border);
    }
    
    .trade-info-label {
        font-size: 0.75rem;
        color: var(--text-muted);
        margin-bottom: 0.5rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    .trade-info-value {
        font-size: 1rem;
        font-weight: 600;
        color: var(--default-text-color);
    }
    
    .trade-info-value.editable {
        position: relative;
    }
    
    .trade-info-value input,
    .trade-info-value select {
        width: 100%;
        background: rgba(var(--light-rgb), 0.5);
        border: 1px solid var(--default-border);
        border-radius: 0.25rem;
        color: var(--default-text-color);
        padding: 0.375rem 0.5rem;
        font-size: 0.9rem;
        font-weight: 600;
        display: none;
    }
    
    .trade-info-value.edit-mode input,
    .trade-info-value.edit-mode select {
        display: block;
    }
    
    .trade-info-value.edit-mode .display-value {
        display: none;
    }
    
    .trade-info-value input:focus,
    .trade-info-value select:focus {
        outline: none;
        border-color: rgb(var(--primary-rgb));
    }
    
    #tradeDetailChart {
        min-height: 500px;
    }
    
    .timeline-table-wrapper {
        overflow-x: auto;
    }
    
    .sell-true {
        color: rgb(var(--danger-rgb));
        font-weight: 700;
    }
    
    .sell-false {
        color: rgb(var(--success-rgb));
        font-weight: 600;
    }
    
    .trade-log-wrapper {
        background: rgba(var(--dark-rgb), 0.3);
        border: 1px solid var(--default-border);
        border-radius: 0.5rem;
        padding: 1rem;
        max-height: 400px;
        overflow: auto;
    }
    
    .trade-log-wrapper pre {
        margin: 0;
        color: var(--default-text-color);
        font-size: 0.85rem;
        font-family: 'Courier New', monospace;
        white-space: pre-wrap;
        word-break: break-word;
    }
    
    .analysis-row .positive-change {
        color: rgb(var(--success-rgb));
        font-weight: 600;
    }
    
    .analysis-row .negative-change {
        color: rgb(var(--danger-rgb));
        font-weight: 600;
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
                <li class="breadcrumb-item"><a href="<?php echo $baseUrl; ?>/goats/unique/?id=<?php echo $play_id; ?>"><?php echo htmlspecialchars($play['name'] ?? 'Play'); ?></a></li>
                <li class="breadcrumb-item active" aria-current="page">Trade #<?php echo $trade_id; ?></li>
            </ol>
        </nav>
        <h1 class="page-title fw-medium fs-18 mb-0">
            Trade #<?php echo $trade_id; ?>
            <?php if ($play && !empty($play['short_play'])): ?>
                <span class="badge bg-danger fs-10 ms-2">SHORT</span>
            <?php endif; ?>
        </h1>
    </div>
    <div class="d-flex gap-2">
        <button id="editBtn" class="btn btn-primary" onclick="toggleEditMode()">
            <i class="ri-edit-line me-1"></i>Edit Trade
        </button>
        <button id="saveBtn" class="btn btn-success" onclick="saveTrade()" style="display: none;">
            <i class="ri-save-line me-1"></i>Save Changes
        </button>
        <button id="cancelBtn" class="btn btn-secondary" onclick="cancelEdit()" style="display: none;">Cancel</button>
    </div>
</div>

<!-- Error Message -->
<?php if ($error_message): ?>
<div class="alert alert-danger" role="alert">
    <?php echo $error_message; ?>
</div>
<?php endif; ?>

<?php if ($trade): ?>

<!-- Trade Details Grid -->
<div class="trade-info-grid mb-3">
    <div class="trade-info-item">
        <div class="trade-info-label">Wallet Address</div>
        <div class="trade-info-value">
            <a href="https://solscan.io/token/<?php echo urlencode($trade['wallet_address']); ?>" target="_blank" rel="noopener" class="text-primary">
                <code><?php echo substr($trade['wallet_address'], 0, 16); ?>...</code>
            </a>
        </div>
    </div>
    <div class="trade-info-item">
        <div class="trade-info-label">Entry Time</div>
        <div class="trade-info-value">
            <?php echo $trade['followed_at'] ? date('M d, Y H:i:s', strtotime($trade['followed_at'])) : '--'; ?>
        </div>
    </div>
    <div class="trade-info-item">
        <div class="trade-info-label">Exit Time</div>
        <div class="trade-info-value editable" data-field="our_exit_timestamp">
            <span class="display-value">
                <?php 
                if ($effective_exit_timestamp) {
                    $formatted_time = date('M d, Y H:i:s', strtotime($effective_exit_timestamp));
                    $is_estimated = ($trade['our_status'] !== 'completed' && $trade['our_status'] !== 'sold');
                    echo $formatted_time . ($is_estimated ? ' <small class="text-warning">(est)</small>' : '');
                } else {
                    echo '--';
                }
                ?>
            </span>
            <input type="datetime-local" value="<?php echo $trade['our_exit_timestamp'] ? date('Y-m-d\TH:i:s', strtotime($trade['our_exit_timestamp'])) : ''; ?>">
        </div>
    </div>
    <div class="trade-info-item">
        <div class="trade-info-label">Duration</div>
        <div class="trade-info-value"><?php echo $duration_text; ?></div>
    </div>
    <div class="trade-info-item">
        <div class="trade-info-label">Entry Price</div>
        <div class="trade-info-value editable" data-field="our_entry_price">
            <span class="display-value"><?php echo $trade['our_entry_price'] ? '$' . number_format($trade['our_entry_price'], 6) : '--'; ?></span>
            <input type="number" step="0.000001" value="<?php echo $trade['our_entry_price'] ?? ''; ?>">
        </div>
    </div>
    <div class="trade-info-item">
        <div class="trade-info-label">Exit Price</div>
        <div class="trade-info-value editable" data-field="our_exit_price">
            <span class="display-value">
                <?php 
                if ($trade['our_exit_price']) {
                    echo '$' . number_format($trade['our_exit_price'], 6);
                } elseif ($effective_exit_price) {
                    echo '$' . number_format($effective_exit_price, 6) . ' <small class="text-warning">(est)</small>';
                } else {
                    echo '--';
                }
                ?>
            </span>
            <input type="number" step="0.000001" value="<?php echo $trade['our_exit_price'] ?? ''; ?>">
        </div>
    </div>
    <div class="trade-info-item">
        <div class="trade-info-label">Profit/Loss (%)</div>
        <div class="trade-info-value editable" data-field="our_profit_loss">
            <span class="display-value <?php 
                if (!empty($play['short_play'])) {
                    echo $trade['our_profit_loss'] > 0 ? 'text-danger' : ($trade['our_profit_loss'] < 0 ? 'text-success' : '');
                } else {
                    echo $trade['our_profit_loss'] > 0 ? 'text-success' : ($trade['our_profit_loss'] < 0 ? 'text-danger' : '');
                }
            ?>">
                <?php 
                if ($trade['our_profit_loss'] !== null) {
                    echo ($trade['our_profit_loss'] > 0 ? '+' : '') . number_format($trade['our_profit_loss'], 2) . '%';
                } else {
                    echo '--';
                }
                ?>
            </span>
            <input type="number" step="0.01" value="<?php echo $trade['our_profit_loss'] ?? ''; ?>">
        </div>
    </div>
    <div class="trade-info-item">
        <div class="trade-info-label">Status</div>
        <div class="trade-info-value editable" data-field="our_status">
            <span class="display-value"><?php echo strtoupper($trade['our_status'] === 'pending' ? 'active' : $trade['our_status']); ?></span>
            <select>
                <option value="pending" <?php echo $trade['our_status'] === 'pending' ? 'selected' : ''; ?>>Active</option>
                <option value="sold" <?php echo $trade['our_status'] === 'sold' ? 'selected' : ''; ?>>Sold</option>
                <option value="completed" <?php echo $trade['our_status'] === 'completed' ? 'selected' : ''; ?>>Completed</option>
                <option value="cancelled" <?php echo $trade['our_status'] === 'cancelled' ? 'selected' : ''; ?>>Cancelled</option>
            </select>
        </div>
    </div>

</div>

<!-- Price Chart -->
<div class="card custom-card mb-3">
    <div class="card-header">
        <div class="card-title">Price Chart</div>
        <span class="badge bg-info-transparent ms-auto">Green = Entry | Yellow = Exit</span>
    </div>
    <div class="card-body">
        <div id="tradeDetailChart"></div>
    </div>
</div>

<!-- Manual Price Analysis -->
<div class="card custom-card mb-3">
    <div class="card-header d-flex justify-content-between align-items-center">
        <div class="card-title mb-0">Manual Price Analysis</div>
        <button id="clearAnalysisBtn" class="btn btn-sm btn-danger">Clear All Points</button>
    </div>
    <div class="card-body">
        <p class="text-muted fs-13 mb-3">Click on the chart to record price points for analysis.</p>
        <div class="timeline-table-wrapper">
            <table class="table table-bordered text-nowrap mb-0">
                <thead>
                    <tr>
                        <th class="text-center">#</th>
                        <th>Time</th>
                        <th class="text-center">Price</th>
                        <th class="text-center">% Change</th>
                        <th class="text-center">Time Diff</th>
                    </tr>
                </thead>
                <tbody id="analysisTableBody">
                    <tr><td colspan="5" class="text-center text-muted">Click on the chart to add price points</td></tr>
                </tbody>
            </table>
        </div>
    </div>
</div>

<!-- Price Checks Timeline -->
<?php if (!empty($price_checks) || $trade['price_movements']): ?>
<div class="card custom-card mb-3">
    <div class="card-header">
        <div class="card-title">Price Checks Timeline</div>
        <div class="ms-auto">
            <?php if (!empty($price_checks)): ?>
                <span class="badge bg-primary-transparent"><?php echo count($price_checks); ?> checks</span>
                <?php if ($price_checks_source === 'archive'): ?>
                    <span class="badge bg-secondary-transparent">from archive</span>
                <?php else: ?>
                    <span class="badge bg-success-transparent">from live</span>
                <?php endif; ?>
            <?php else: ?>
                <span class="badge bg-warning-transparent">from legacy JSON</span>
            <?php endif; ?>
        </div>
    </div>
    <div class="card-body">
        <div class="timeline-table-wrapper">
            <table class="table table-bordered text-nowrap mb-0">
                <thead>
                    <tr>
                        <th class="text-center">#</th>
                        <th>Time</th>
                        <th class="text-center">Current</th>
                        <th class="text-center">Entry</th>
                        <th class="text-center">High</th>
                        <th class="text-center">Gain %</th>
                        <th class="text-center">Drop from High %</th>
                        <th class="text-center">Tolerance %</th>
                        <th class="text-center">Basis</th>
                        <th class="text-center">Should Sell</th>
                    </tr>
                </thead>
                <tbody id="timelineTableBody">
                    <!-- Will be populated by JavaScript -->
                </tbody>
            </table>
        </div>
    </div>
</div>
<?php endif; ?>

<!-- Filter Validation Results -->
<?php if (!empty($filter_results_summary)): ?>
<div class="card custom-card mb-3">
    <div class="card-header">
        <div class="card-title">Filter Validation Results</div>
        <?php 
        $any_passed = false;
        foreach ($filter_results_summary as $pg) {
            if ($pg['all_passed']) { $any_passed = true; break; }
        }
        ?>
        <span class="badge <?php echo $any_passed ? 'bg-success-transparent' : 'bg-danger-transparent'; ?> ms-auto">
            <?php echo $any_passed ? 'PASSED' : 'FAILED'; ?>
        </span>
    </div>
    <div class="card-body">
        <p class="text-muted small mb-3">
            <?php echo count($filter_results_summary); ?> project(s) evaluated. 
            Trade passes if <strong>any</strong> project's filters <strong>all</strong> pass.
        </p>
        
        <?php foreach ($filter_results_summary as $project): ?>
        <div class="card custom-card mb-3" style="border: 1px solid <?php echo $project['all_passed'] ? 'rgba(38, 191, 148, 0.5)' : 'rgba(230, 83, 60, 0.5)'; ?>;">
            <div class="card-header py-2" style="background: <?php echo $project['all_passed'] ? 'rgba(38, 191, 148, 0.1)' : 'rgba(230, 83, 60, 0.1)'; ?>;">
                <div class="d-flex align-items-center justify-content-between w-100">
                    <span class="fw-semibold">
                        <?php echo htmlspecialchars($project['project_name']); ?>
                        <span class="text-muted">(ID: <?php echo $project['project_id']; ?>)</span>
                    </span>
                    <span>
                        <span class="badge <?php echo $project['all_passed'] ? 'bg-success' : 'bg-danger'; ?>">
                            <?php echo $project['all_passed'] ? 'ALL PASSED' : 'FAILED'; ?>
                        </span>
                        <span class="badge bg-secondary-transparent ms-1">
                            <?php echo $project['passed']; ?>/<?php echo $project['total']; ?> filters
                        </span>
                    </span>
                </div>
            </div>
            <div class="card-body p-0">
                <div class="table-responsive">
                    <table class="table table-sm table-borderless mb-0">
                        <thead>
                            <tr class="text-muted small" style="border-bottom: 1px solid rgba(255,255,255,0.1);">
                                <th style="width: 30px;"></th>
                                <th>Filter</th>
                                <th>Field</th>
                                <th class="text-end">Min</th>
                                <th class="text-center">Actual</th>
                                <th>Max</th>
                            </tr>
                        </thead>
                        <tbody>
                            <?php foreach ($project['filters'] as $filter): ?>
                            <tr style="<?php echo $filter['passed'] ? '' : 'background: rgba(230, 83, 60, 0.15);'; ?>">
                                <td class="text-center">
                                    <?php if ($filter['passed']): ?>
                                        <i class="ri-checkbox-circle-fill text-success"></i>
                                    <?php else: ?>
                                        <i class="ri-close-circle-fill text-danger"></i>
                                    <?php endif; ?>
                                </td>
                                <td>
                                    <span class="fw-medium"><?php echo htmlspecialchars($filter['filter_name'] ?? 'Filter #' . $filter['filter_id']); ?></span>
                                    <?php if ($filter['error']): ?>
                                        <br><small class="text-danger"><?php echo htmlspecialchars($filter['error']); ?></small>
                                    <?php endif; ?>
                                </td>
                                <td>
                                    <code class="text-info small"><?php echo htmlspecialchars($filter['field_column'] ?? '-'); ?></code>
                                    <?php if ($filter['minute'] > 0): ?>
                                        <span class="badge bg-secondary-transparent small">min <?php echo $filter['minute']; ?></span>
                                    <?php endif; ?>
                                </td>
                                <td class="text-end text-muted">
                                    <?php echo $filter['from_value'] !== null ? number_format($filter['from_value'], 4) : '-'; ?>
                                </td>
                                <td class="text-center fw-semibold <?php echo $filter['passed'] ? 'text-success' : 'text-danger'; ?>">
                                    <?php echo $filter['actual_value'] !== null ? number_format($filter['actual_value'], 4) : 'NULL'; ?>
                                </td>
                                <td class="text-muted">
                                    <?php echo $filter['to_value'] !== null ? number_format($filter['to_value'], 4) : '-'; ?>
                                </td>
                            </tr>
                            <?php endforeach; ?>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
        <?php endforeach; ?>
    </div>
</div>
<?php endif; ?>

<!-- Trade Entry Log -->
<div class="card custom-card mb-3">
    <div class="card-header">
        <div class="card-title">Trade Entry Log</div>
        <span class="badge bg-secondary-transparent ms-auto"><?php echo htmlspecialchars($trade_table_name, ENT_QUOTES | ENT_SUBSTITUTE, 'UTF-8'); ?></span>
    </div>
    <div class="card-body">
        <div class="trade-log-wrapper">
            <?php if ($entry_log_pretty !== null): ?>
                <pre><?php echo htmlspecialchars($entry_log_pretty); ?></pre>
            <?php elseif ($entry_log_raw !== null && $entry_log_raw !== ''): ?>
                <pre><?php echo htmlspecialchars($entry_log_raw); ?></pre>
            <?php else: ?>
                <p class="text-muted mb-0">No entry log recorded for this trade.</p>
            <?php endif; ?>
        </div>
    </div>
</div>

<?php endif; ?>

<?php
$content = ob_get_clean();

// --- Page Scripts ---
ob_start();
?>
<!-- Apex Charts JS -->
<script src="<?php echo $baseUrl; ?>/assets/libs/apexcharts/apexcharts.min.js"></script>

<script>
    // Inject PHP data into JavaScript
    const jsData = <?php echo json_encode($js_data); ?>;
    const tradeData = jsData.trade;
    const returnUrl = <?php echo json_encode($return_url); ?>;
    
    // Debug: Show raw values from PHP
    console.log('=== DEBUG: Trade Timestamps ===');
    console.log('Raw followed_at from DB:', tradeData.followed_at);
    console.log('Raw our_exit_timestamp from DB:', tradeData.our_exit_timestamp);
    console.log('PHP followed_at_ms:', jsData.followed_at_ms, '→', jsData.followed_at_ms ? new Date(jsData.followed_at_ms).toISOString() : null);
    console.log('PHP effective_exit_ms:', jsData.effective_exit_ms, '→', jsData.effective_exit_ms ? new Date(jsData.effective_exit_ms).toISOString() : null);
    console.log('Browser timezone offset (minutes):', new Date().getTimezoneOffset());
    
    // Store for manual analysis points
    let analysisPoints = [];
    let chartInstance = null;
    let allPricesData = [];
    let currentHoveredPoint = null;
    
    // Load and render price chart
    async function loadTradeChart() {
        try {
            if (!jsData.followed_at_ms) {
                document.getElementById('tradeDetailChart').innerHTML = '<div class="text-center text-muted py-5">No trade timestamp data available</div>';
                return;
            }
            
            const followedAtTime = jsData.followed_at_ms;
            let exitTime = jsData.effective_exit_ms || followedAtTime;
            let exitPrice = jsData.effective_exit_price;
            const entryPrice = parseFloat(tradeData.our_entry_price) || null;
            
            if (exitPrice !== null && exitPrice !== undefined) {
                exitPrice = parseFloat(exitPrice);
                if (isNaN(exitPrice)) exitPrice = null;
            }
            
            const bufferMs = 10 * 60 * 1000;
            const startTimeMs = followedAtTime - bufferMs;
            const endTimeMs = Math.max(exitTime, followedAtTime) + bufferMs;
            
            const startTimeSec = Math.floor(startTimeMs / 1000);
            const endTimeSec = Math.floor(endTimeMs / 1000);
            
            console.log('Trade Entry Time:', new Date(followedAtTime).toLocaleString());
            console.log('Trade Exit Time:', new Date(exitTime).toLocaleString());
            console.log('Fetching prices from:', new Date(startTimeMs).toLocaleString(), 'to:', new Date(endTimeMs).toLocaleString());
            
            const response = await fetch(`/chart/plays/get_trade_prices.php?start=${startTimeSec}&end=${endTimeSec}`);
            const data = await response.json();
            
            console.log('Price data received:', data.prices ? data.prices.length : 0, 'points');
            
            if (!data.success) {
                document.getElementById('tradeDetailChart').innerHTML = '<div class="text-center text-danger py-5">Error loading chart data</div>';
                return;
            }
            
            if (!data.prices || data.prices.length === 0) {
                document.getElementById('tradeDetailChart').innerHTML = '<div class="text-center text-warning py-5">No price data available for this time period</div>';
                return;
            }
            
            console.log('Rendering chart with entry:', new Date(followedAtTime).toLocaleString(), 'exit:', new Date(exitTime).toLocaleString());
            renderChart(data.prices, followedAtTime, exitTime, entryPrice, exitPrice);
            
        } catch (error) {
            console.error('Error loading trade chart:', error);
            document.getElementById('tradeDetailChart').innerHTML = '<div class="text-center text-danger py-5">Error loading chart</div>';
        }
    }
    
    function renderChart(pricePoints, followedAtTime, exitTime, entryPrice, exitPrice) {
        const allPrices = pricePoints.map(item => {
            let timestampMs;
            if (typeof item.x === 'number') {
                timestampMs = item.x < 10000000000 ? item.x * 1000 : item.x;
            } else {
                let timestamp = item.x;
                if (timestamp.includes(' ')) {
                    timestamp = timestamp.replace(' ', 'T');
                }
                timestampMs = new Date(timestamp).getTime();
            }
            return { x: timestampMs, y: item.y };
        });
        
        allPricesData = allPrices;
        
        // Debug: Compare price data range with entry/exit times
        if (allPrices.length > 0) {
            const priceStart = allPrices[0].x;
            const priceEnd = allPrices[allPrices.length - 1].x;
            console.log('Price data range:', new Date(priceStart).toLocaleString(), 'to', new Date(priceEnd).toLocaleString());
            console.log('Entry time:', new Date(followedAtTime).toLocaleString(), '| Exit time:', new Date(exitTime).toLocaleString());
            console.log('Entry within price range:', followedAtTime >= priceStart && followedAtTime <= priceEnd);
            console.log('Exit within price range:', exitTime >= priceStart && exitTime <= priceEnd);
        }
        
        // Calculate chart x-axis range to include entry/exit times with buffer
        const bufferMs = 2 * 60 * 1000; // 2 minute buffer
        let xMin = followedAtTime - bufferMs;
        let xMax = (exitTime || followedAtTime) + bufferMs;
        
        // Also include price data range
        if (allPrices.length > 0) {
            xMin = Math.min(xMin, allPrices[0].x);
            xMax = Math.max(xMax, allPrices[allPrices.length - 1].x);
        }
        
        // Build annotations
        const annotations = { xaxis: [] };
        
        // Entry marker (green)
        annotations.xaxis.push({
            x: followedAtTime,
            borderColor: '#10b981',
            strokeDashArray: 0,
            borderWidth: 3,
            label: {
                borderColor: '#10b981',
                style: {
                    color: '#fff',
                    background: '#10b981',
                    fontSize: '11px'
                },
                text: 'ENTRY'
            }
        });
        
        // Exit marker and range
        if (exitTime && exitTime !== followedAtTime) {
            annotations.xaxis.push({
                x: exitTime,
                borderColor: '#f59e0b',
                strokeDashArray: 0,
                borderWidth: 3,
                label: {
                    borderColor: '#f59e0b',
                    style: {
                        color: '#fff',
                        background: '#f59e0b',
                        fontSize: '11px'
                    },
                    text: 'EXIT'
                }
            });
            
            // Trade period range (light red highlight)
            annotations.xaxis.push({
                x: followedAtTime,
                x2: exitTime,
                fillColor: 'rgba(239, 68, 68, 0.15)',
                opacity: 1
            });
        }
        
        console.log('Chart annotations:', JSON.stringify(annotations, null, 2));
        
        var options = {
            series: [{
                name: 'SOL Price',
                data: allPrices
            }],
            chart: {
                type: 'line',
                height: 500,
                background: 'transparent',
                toolbar: {
                    show: true,
                    tools: {
                        download: true,
                        selection: true,
                        zoom: true,
                        zoomin: true,
                        zoomout: true,
                        pan: true,
                        reset: true
                    }
                },
                animations: { enabled: false },
                events: {
                    click: function(event, chartContext, config) {
                        handleChartClick(event, chartContext, config);
                    },
                    dataPointSelection: function(event, chartContext, config) {
                        handleDataPointClick(event, chartContext, config);
                    }
                }
            },
            stroke: {
                curve: 'smooth',
                width: 2.5
            },
            colors: ['#6366f1'],
            markers: {
                size: 0,
                hover: {
                    size: 6,
                    sizeOffset: 3
                }
            },
            grid: {
                borderColor: 'rgba(255,255,255,0.1)',
                strokeDashArray: 3
            },
            xaxis: {
                type: 'datetime',
                min: xMin,
                max: xMax,
                labels: {
                    datetimeUTC: true,
                    style: {
                        colors: '#9ca3af',
                        fontSize: '11px'
                    },
                    datetimeFormatter: {
                        hour: 'HH:mm',
                        minute: 'HH:mm:ss'
                    }
                }
            },
            yaxis: {
                labels: {
                    style: {
                        colors: '#9ca3af',
                        fontSize: '11px'
                    },
                    formatter: function(val) {
                        return '$' + val.toFixed(6);
                    }
                }
            },
            tooltip: {
                theme: 'dark',
                x: { format: 'MMM dd, HH:mm:ss' },
                custom: function({ series, seriesIndex, dataPointIndex, w }) {
                    const timestamp = w.globals.seriesX[seriesIndex][dataPointIndex];
                    const price = series[seriesIndex][dataPointIndex];
                    
                    currentHoveredPoint = { timestamp, price };
                    
                    return '<div class="p-2">' +
                        '<div class="mb-1"><strong>' + new Date(timestamp).toLocaleTimeString() + '</strong></div>' +
                        '<div>Price: <span class="text-primary fw-semibold">$' + price.toFixed(6) + '</span></div>' +
                        '<div class="fs-11 text-muted mt-1">Click to record point</div>' +
                        '</div>';
                }
            },
            annotations: annotations
        };

        chartInstance = new ApexCharts(document.querySelector("#tradeDetailChart"), options);
        chartInstance.render();
    }
    
    function handleChartClick(event, chartContext, config) {
        if (currentHoveredPoint && currentHoveredPoint.price !== undefined && !isNaN(currentHoveredPoint.price)) {
            analysisPoints.push({
                timestamp: currentHoveredPoint.timestamp,
                price: currentHoveredPoint.price
            });
            updateAnalysisTable();
        }
    }
    
    function handleDataPointClick(event, chartContext, config) {
        if (config.dataPointIndex !== undefined && chartInstance) {
            const timestamp = chartInstance.w.globals.seriesX[0][config.dataPointIndex];
            const price = chartInstance.w.globals.series[0][config.dataPointIndex];
            
            if (timestamp && price !== undefined && !isNaN(price)) {
                analysisPoints.push({ timestamp, price });
                updateAnalysisTable();
            }
        }
    }
    
    function updateAnalysisTable() {
        const tbody = document.getElementById('analysisTableBody');
        if (!tbody) return;
        
        tbody.innerHTML = '';
        
        if (analysisPoints.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" class="text-center text-muted">Click on the chart to add price points</td></tr>';
            return;
        }
        
        for (let i = analysisPoints.length - 1; i >= 0; i--) {
            const point = analysisPoints[i];
            
            if (!point || point.price === undefined || isNaN(point.price)) continue;
            
            const tr = document.createElement('tr');
            tr.className = 'analysis-row';
            
            const pointNumber = i + 1;
            const timeStr = new Date(point.timestamp).toLocaleString('en-US', {
                month: 'short', day: 'numeric', year: 'numeric',
                hour: '2-digit', minute: '2-digit', second: '2-digit'
            });
            const priceStr = '$' + point.price.toFixed(6);
            
            let changeStr = '--';
            let timeDiffStr = '--';
            
            if (i > 0) {
                const prevPoint = analysisPoints[i - 1];
                const priceDiff = point.price - prevPoint.price;
                const changePercent = (priceDiff / prevPoint.price) * 100;
                
                const changeClass = changePercent >= 0 ? 'positive-change' : 'negative-change';
                const changeSign = changePercent >= 0 ? '+' : '';
                changeStr = `<span class="${changeClass}">${changeSign}${changePercent.toFixed(4)}%</span>`;
                
                const timeDiffMs = point.timestamp - prevPoint.timestamp;
                const timeDiffSec = timeDiffMs / 1000;
                const timeDiffMin = timeDiffSec / 60;
                const timeDiffHour = timeDiffMin / 60;
                
                if (timeDiffHour >= 1) {
                    timeDiffStr = timeDiffHour.toFixed(2) + ' hours';
                } else if (timeDiffMin >= 1) {
                    timeDiffStr = timeDiffMin.toFixed(2) + ' min';
                } else {
                    timeDiffStr = timeDiffSec.toFixed(2) + ' sec';
                }
            }
            
            tr.innerHTML = `
                <td class="text-center">${pointNumber}</td>
                <td>${timeStr}</td>
                <td class="text-center">${priceStr}</td>
                <td class="text-center">${changeStr}</td>
                <td class="text-center">${timeDiffStr}</td>
            `;
            
            tbody.appendChild(tr);
        }
    }
    
    function clearAnalysisPoints() {
        analysisPoints = [];
        updateAnalysisTable();
    }
    
    // Render price checks timeline
    function renderTimeline() {
        try {
            const tbody = document.getElementById('timelineTableBody');
            if (!tbody) return;
            
            tbody.innerHTML = '';
            
            const fmtPrice = (v) => (v === null || v === undefined || isNaN(v)) ? '--' : parseFloat(v).toFixed(6);
            const fmtPct = (v) => (v === null || v === undefined || isNaN(v)) ? '--' : `${(parseFloat(v) * 100).toFixed(4)}%`;
            const fmtPctRaw = (v) => (v === null || v === undefined || isNaN(v)) ? '--' : `${parseFloat(v).toFixed(4)}%`;
            
            const priceChecks = jsData.price_checks || [];
            
            if (priceChecks.length > 0) {
                priceChecks.forEach((item, index) => {
                    const tr = document.createElement('tr');
                    const sell = item.should_sell == 1;
                    const isBackfill = item.is_backfill == 1;
                    
                    const timeStr = item.checked_at ? new Date(item.checked_at.replace(' ', 'T')).toLocaleString('en-US', {
                        month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit'
                    }) : '--';
                    
                    const basisBadge = item.basis 
                        ? `<span class="badge ${item.basis === 'highest' ? 'bg-primary-transparent' : 'bg-warning-transparent'}">${item.basis}</span>`
                        : '--';
                    
                    if (isBackfill) {
                        tr.style.background = 'rgba(var(--primary-rgb), 0.1)';
                    }
                    
                    tr.innerHTML = `
                        <td class="text-center">${index + 1}${isBackfill ? ' <small class="text-primary">(backfill)</small>' : ''}</td>
                        <td>${timeStr}</td>
                        <td class="text-center">$${fmtPrice(item.current_price)}</td>
                        <td class="text-center">$${fmtPrice(item.entry_price)}</td>
                        <td class="text-center">$${fmtPrice(item.highest_price)}</td>
                        <td class="text-center">${fmtPct(item.gain_from_entry)}</td>
                        <td class="text-center">${fmtPct(item.drop_from_high)}</td>
                        <td class="text-center">${fmtPct(item.tolerance)}</td>
                        <td class="text-center">${basisBadge}</td>
                        <td class="text-center ${sell ? 'sell-true' : 'sell-false'}">${sell ? 'TRUE' : 'FALSE'}</td>
                    `;
                    tbody.appendChild(tr);
                });
                return;
            }
            
            // Fallback to legacy JSON data
            let movements = tradeData.price_movements;
            
            if (!movements) {
                tbody.innerHTML = '<tr><td colspan="10" class="text-center text-muted">No price checks recorded</td></tr>';
                return;
            }
            
            if (typeof movements === 'string') {
                movements = movements.trim();
                if (movements.startsWith('"[') || movements.startsWith("'[")) {
                    movements = movements.replace(/^"|"$/g, '');
                    movements = movements.replace(/^'|'$/g, '');
                    movements = movements.replace(/\\"/g, '"');
                }
                movements = JSON.parse(movements);
            }
            
            if (!Array.isArray(movements) || movements.length === 0) {
                tbody.innerHTML = '<tr><td colspan="10" class="text-center text-muted">No price checks recorded</td></tr>';
                return;
            }
            
            movements.forEach((item, index) => {
                const tr = document.createElement('tr');
                const sell = !!item.should_sell;
                const isBackfill = !!item.backfilled;
                
                const basisBadge = item.basis 
                    ? `<span class="badge ${item.basis === 'highest' ? 'bg-primary-transparent' : 'bg-warning-transparent'}">${item.basis}</span>`
                    : '--';
                
                if (isBackfill) {
                    tr.style.background = 'rgba(var(--primary-rgb), 0.1)';
                }
                
                tr.innerHTML = `
                    <td class="text-center">${index + 1}${isBackfill ? ' <small class="text-primary">(backfill)</small>' : ''}</td>
                    <td>${item.timestamp || ''}</td>
                    <td class="text-center">$${fmtPrice(item.current_price)}</td>
                    <td class="text-center">$${fmtPrice(item.entry_price)}</td>
                    <td class="text-center">$${fmtPrice(item.highest_price)}</td>
                    <td class="text-center">${fmtPctRaw(item.gain_from_entry_pct)}</td>
                    <td class="text-center">${fmtPctRaw(item.drop_from_high_pct)}</td>
                    <td class="text-center">${fmtPctRaw(item.tolerance_pct)}</td>
                    <td class="text-center">${basisBadge}</td>
                    <td class="text-center ${sell ? 'sell-true' : 'sell-false'}">${sell ? 'TRUE' : 'FALSE'}</td>
                `;
                tbody.appendChild(tr);
            });
        } catch (e) {
            console.error('Failed to render timeline:', e);
            const tbody = document.getElementById('timelineTableBody');
            if (tbody) {
                tbody.innerHTML = '<tr><td colspan="10" class="text-center text-danger">Error parsing price checks</td></tr>';
            }
        }
    }
    
    // Initialize on document ready
    document.addEventListener('DOMContentLoaded', function() {
        loadTradeChart();
        renderTimeline();
        
        const clearBtn = document.getElementById('clearAnalysisBtn');
        if (clearBtn) {
            clearBtn.addEventListener('click', clearAnalysisPoints);
        }
    });
    
    // Edit mode functions
    let isEditMode = false;
    
    function toggleEditMode() {
        isEditMode = true;
        
        document.getElementById('editBtn').style.display = 'none';
        document.getElementById('saveBtn').style.display = 'inline-block';
        document.getElementById('cancelBtn').style.display = 'inline-block';
        
        document.querySelectorAll('.trade-info-value.editable').forEach(field => {
            field.classList.add('edit-mode');
        });
    }
    
    function cancelEdit() {
        isEditMode = false;
        
        document.getElementById('editBtn').style.display = 'inline-block';
        document.getElementById('saveBtn').style.display = 'none';
        document.getElementById('cancelBtn').style.display = 'none';
        
        document.querySelectorAll('.trade-info-value.editable').forEach(field => {
            field.classList.remove('edit-mode');
            const input = field.querySelector('input, select');
            if (input) {
                input.value = input.defaultValue;
            }
        });
    }
    
    async function saveTrade() {
        const updates = {};
        let hasChanges = false;
        
        document.querySelectorAll('.trade-info-value.editable').forEach(field => {
            const fieldName = field.getAttribute('data-field');
            const input = field.querySelector('input, select');
            
            if (input) {
                let value = input.value.trim();
                if (value !== '') {
                    if (input.type === 'datetime-local' && value) {
                        value = value.replace('T', ' ');
                    }
                    updates[fieldName] = value;
                    hasChanges = true;
                }
            }
        });
        
        if (!hasChanges) {
            alert('No changes to save');
            return;
        }
        
        const saveBtn = document.getElementById('saveBtn');
        saveBtn.disabled = true;
        saveBtn.innerHTML = '<i class="ri-loader-4-line me-1"></i>Saving...';
        
        try {
            const response = await fetch('save_trade.php', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    trade_id: <?php echo $trade_id; ?>,
                    updates: updates
                })
            });
            
            const data = await response.json();
            
            if (data.success) {
                saveBtn.innerHTML = '<i class="ri-check-line me-1"></i>Saved!';
                saveBtn.classList.remove('btn-success');
                saveBtn.classList.add('btn-success');
                
                setTimeout(() => {
                    window.location.href = returnUrl;
                }, 500);
            } else {
                alert('Error saving trade: ' + (data.error || 'Unknown error'));
                saveBtn.disabled = false;
                saveBtn.innerHTML = '<i class="ri-save-line me-1"></i>Save Changes';
            }
        } catch (error) {
            console.error('Error saving trade:', error);
            alert('Error saving trade. Please try again.');
            saveBtn.disabled = false;
            saveBtn.innerHTML = '<i class="ri-save-line me-1"></i>Save Changes';
        }
    }
</script>
<?php
$scripts = ob_get_clean();

// Include the base layout
include __DIR__ . '/../../../pages/layouts/base.php';
?>

