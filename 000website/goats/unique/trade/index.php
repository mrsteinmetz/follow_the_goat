<?php
/**
 * Trade Details Page - Individual Trade View
 * Migrated to use DuckDBClient API
 */

// Set timezone to UTC (server time) - never use browser time
date_default_timezone_set('UTC');

// --- Load DuckDB Client ---
require_once __DIR__ . '/../../../includes/DuckDBClient.php';
// API Base URL - uses PHP proxy to reach Flask API on server
$API_BASE = dirname($_SERVER["SCRIPT_NAME"]) . '/../../../api/proxy.php';

// Get trade ID, play ID, and return URL from query parameters
$trade_id = (int)($_GET['id'] ?? 0);
$play_id = (int)($_GET['play_id'] ?? 0);
$return_url = $_GET['return_url'] ?? '../?id=' . $play_id;
$requested_source = strtolower($_GET['source'] ?? 'live');
$source = $requested_source;

if ($trade_id <= 0) {
    header('Location: ../../?error=' . urlencode('Invalid trade ID'));
    exit;
}

// --- Base URL for v2 template ---
$rootFolder = basename($_SERVER['DOCUMENT_ROOT']);
$baseUrl = (isset($_SERVER['HTTPS']) && $_SERVER['HTTPS'] === 'on' ? 'https://' : 'http://') . $_SERVER['HTTP_HOST'] . dirname(dirname(dirname(dirname($_SERVER['SCRIPT_NAME']))));

// --- Initialize API Client ---
$client = new DuckDBClient();
$api_available = $client->isAvailable();

$trade = null;
$play = null;
$error_message = '';
$trade_table_name = 'follow_the_goat_buyins';  // Archive is deprecated

if (!$api_available) {
    $error_message = 'API server is not available. Please ensure master.py is running.';
} else {
    // Fetch trade details via API (always from live table)
    $buyin_result = $client->getSingleBuyin($trade_id);
    
    if ($buyin_result && isset($buyin_result['buyin'])) {
        $trade = $buyin_result['buyin'];
        $source = 'live';
    }
    
    if (!$trade) {
        header('Location: ../../?error=' . urlencode('Trade not found'));
        exit;
    }
    
    // Update play_id if not provided
    if ($play_id <= 0) {
        $play_id = $trade['play_id'] ?? 0;
    }
    
    // Fetch play details via API
    $play_result = $client->getPlay($play_id);
    $play = $play_result['play'] ?? null;
    
    // Fetch price checks from API (use 'all' to get all historical checks)
    $price_checks_result = $client->getPriceChecks($trade_id, 'all', 1000);
    $price_checks = [];
    $price_checks_source = 'live';
    
    if ($price_checks_result && isset($price_checks_result['price_checks'])) {
        $price_checks = $price_checks_result['price_checks'];
        $price_checks_source = $price_checks_result['source'] ?? 'live';
        
        // Sort by checked_at ASC for chronological timeline display
        usort($price_checks, function($a, $b) {
            $timeA = strtotime($a['checked_at'] ?? '1970-01-01');
            $timeB = strtotime($b['checked_at'] ?? '1970-01-01');
            return $timeA <=> $timeB; // ASC order
        });
    }
}

// Filter validation results - parse from pattern_validator_log JSON field
$filter_results = [];
$filter_results_summary = [];
$validation_status = 'unknown';
$validation_message = '';
$any_project_passed = false;

// Cache for project names
$project_name_cache = [];

// Parse pattern_validator_log from trade data
if ($trade && !empty($trade['pattern_validator_log'])) {
    $validator_log_raw = $trade['pattern_validator_log'];
    $validator_log = null;
    
    // Try to decode the JSON
    if (is_string($validator_log_raw)) {
        $validator_log = json_decode($validator_log_raw, true);
        
        // Handle double-encoded JSON
        if (json_last_error() !== JSON_ERROR_NONE) {
            $validator_log_raw = stripslashes($validator_log_raw);
            $validator_log = json_decode($validator_log_raw, true);
        }
    } elseif (is_array($validator_log_raw)) {
        $validator_log = $validator_log_raw;
    }
    
    if ($validator_log && is_array($validator_log)) {
        // Extract project_results for display
        $project_results = $validator_log['project_results'] ?? [];
        
        if (!empty($project_results)) {
            // Collect project IDs for name lookup
            $project_ids = array_map(fn($pr) => $pr['project_id'] ?? 0, $project_results);
            
            // Try to fetch project names from API
            foreach ($project_ids as $pid) {
                if ($pid > 0 && !isset($project_name_cache[$pid]) && $api_available) {
                    $project_data = $client->getPatternProject($pid);
                    if ($project_data && isset($project_data['project']['name'])) {
                        $project_name_cache[$pid] = $project_data['project']['name'];
                    } else {
                        $project_name_cache[$pid] = "Project #$pid";
                    }
                }
            }
            
            foreach ($project_results as $pr) {
                $project_id = $pr['project_id'] ?? 0;
                $filters = $pr['filter_results'] ?? [];
                $project_decision = $pr['decision'] ?? 'NO_GO';
                
                $passed_count = 0;
                $failed_count = 0;
                $formatted_filters = [];
                
                foreach ($filters as $f) {
                    $is_passed = !empty($f['passed']);
                    if ($is_passed) {
                        $passed_count++;
                    } else {
                        $failed_count++;
                    }
                    
                    $formatted_filters[] = [
                        'filter_id' => $f['filter_id'] ?? 0,
                        'filter_name' => $f['filter_name'] ?? 'Unknown Filter',
                        'field_column' => $f['field'] ?? '',
                        'section' => $f['section'] ?? '',
                        'minute' => $f['minute'] ?? 0,
                        'from_value' => $f['from_value'],
                        'to_value' => $f['to_value'],
                        'actual_value' => $f['actual_value'],
                        'passed' => $is_passed,
                        'error' => $f['error'] ?? null,
                    ];
                }
                
                $all_passed = ($failed_count === 0 && $passed_count > 0);
                
                // Get project name from cache or use default
                $project_name = $project_name_cache[$project_id] ?? "Project #$project_id";
                
                $filter_results_summary[] = [
                    'project_id' => $project_id,
                    'project_name' => $project_name,
                    'total' => count($filters),
                    'passed' => $passed_count,
                    'failed' => $failed_count,
                    'all_passed' => $all_passed,
                    'filters' => $formatted_filters,
                ];
                
                if ($all_passed) {
                    $any_project_passed = true;
                }
            }
            
            $validation_status = $any_project_passed ? 'passed' : 'failed';
            $validation_message = $any_project_passed 
                ? 'Trade PASSED filter validation (at least one project passed all filters)'
                : 'Trade FAILED filter validation (no project had all filters pass)';
        } else {
            // Check for schema-based validation (stages)
            $stages = $validator_log['stages'] ?? [];
            if (!empty($stages)) {
                $validation_status = ($validator_log['decision'] ?? '') === 'GO' ? 'passed' : 'failed';
                $validation_message = ($validator_log['decision'] ?? '') === 'GO'
                    ? 'Trade PASSED schema validation'
                    : 'Trade FAILED schema validation';
            } else {
                $validation_status = 'no_data';
                $validation_message = 'No filter validation results recorded for this trade';
            }
        }
    } else {
        $validation_status = 'no_data';
        $validation_message = 'Could not parse validation log';
    }
} else {
    $validation_status = 'no_data';
    $validation_message = 'No filter validation results recorded for this trade';
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
    // Parse followed_at - handle both ISO format and plain datetime
    $followed_at_str = $trade['followed_at'];
    if (strpos($followed_at_str, 'T') !== false) {
        // ISO format: ensure it has timezone
        if (!preg_match('/[+-]\d{2}:\d{2}|Z$/', $followed_at_str)) {
            $followed_at_str .= 'Z'; // Add Z if no timezone
        }
        $entry_time = strtotime($followed_at_str);
    } else {
        // Plain datetime format: treat as UTC
        $entry_time = strtotime($followed_at_str . ' UTC');
    }
    
    $status = strtolower($trade['our_status']);
    
    if ($status !== 'completed' && $status !== 'sold') {
        // For no_go/pending trades, try to get the actual exit time from entry_log
        $exit_time_from_log = null;
        if (!empty($entry_log_raw)) {
            // Parse entry_log_raw (could be JSON string or already decoded)
            $entry_log_data = null;
            if (is_string($entry_log_raw)) {
                $entry_log_data = json_decode($entry_log_raw, true);
                // Handle double-encoded JSON
                if (json_last_error() !== JSON_ERROR_NONE) {
                    $entry_log_data = json_decode(stripslashes($entry_log_raw), true);
                }
            } elseif (is_array($entry_log_raw)) {
                $entry_log_data = $entry_log_raw;
            }
            
            if (is_array($entry_log_data)) {
                // Find the last timestamp in the log (usually when status was updated)
                foreach ($entry_log_data as $log_entry) {
                    if (isset($log_entry['timestamp'])) {
                        $log_timestamp = $log_entry['timestamp'];
                        // Parse timestamp - handle ISO format with timezone
                        if (strpos($log_timestamp, 'T') !== false) {
                            if (!preg_match('/[+-]\d{2}:\d{2}|Z$/', $log_timestamp)) {
                                $log_timestamp .= 'Z';
                            }
                            $log_ts = strtotime($log_timestamp);
                        } else {
                            $log_ts = strtotime($log_timestamp . ' UTC');
                        }
                        if ($log_ts && $log_ts > $entry_time) {
                            $exit_time_from_log = $log_ts;
                        }
                    }
                }
            }
        }
        
        if ($exit_time_from_log) {
            // Use the timestamp from entry log (when trade was actually processed)
            $effective_exit_timestamp = gmdate('Y-m-d H:i:s', $exit_time_from_log);
            $effective_exit_ms = $exit_time_from_log;
        } else {
            // Fallback: Use PHP's current UTC time
            $effective_exit_timestamp = gmdate('Y-m-d H:i:s');
            $effective_exit_ms = time();
        }
        
        $price_movements = json_decode($trade['price_movements'] ?? '[]', true);
        if (is_array($price_movements) && count($price_movements) > 0) {
            $last = end($price_movements);
            $effective_exit_price = $last['current_price'] ?? null;
        }
    } elseif ($trade['our_exit_timestamp']) {
        // Parse exit timestamp - handle both ISO format and plain datetime
        $exit_timestamp_str = $trade['our_exit_timestamp'];
        if (strpos($exit_timestamp_str, 'T') !== false) {
            if (!preg_match('/[+-]\d{2}:\d{2}|Z$/', $exit_timestamp_str)) {
                $exit_timestamp_str .= 'Z';
            }
            $effective_exit_ms = strtotime($exit_timestamp_str);
        } else {
            $effective_exit_ms = strtotime($exit_timestamp_str . ' UTC');
        }
        $effective_exit_timestamp = $trade['our_exit_timestamp'];
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
// Parse timestamps as UTC (database stores UTC)
$followed_at_str = $trade['followed_at'] ?? '';
if ($followed_at_str) {
    if (strpos($followed_at_str, 'T') !== false) {
        if (!preg_match('/[+-]\d{2}:\d{2}|Z$/', $followed_at_str)) {
            $followed_at_str .= 'Z';
        }
        $followed_at_ms = strtotime($followed_at_str) * 1000;
    } else {
        $followed_at_ms = strtotime($followed_at_str . ' UTC') * 1000;
    }
} else {
    $followed_at_ms = null;
}

$block_timestamp_str = $trade['block_timestamp'] ?? '';
if ($block_timestamp_str) {
    if (strpos($block_timestamp_str, 'T') !== false) {
        if (!preg_match('/[+-]\d{2}:\d{2}|Z$/', $block_timestamp_str)) {
            $block_timestamp_str .= 'Z';
        }
        $block_timestamp_ms = strtotime($block_timestamp_str) * 1000;
    } else {
        $block_timestamp_ms = strtotime($block_timestamp_str . ' UTC') * 1000;
    }
} else {
    $block_timestamp_ms = null;
}

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
            <?php 
            if ($trade['followed_at']) {
                $followed_at_str = $trade['followed_at'];
                if (strpos($followed_at_str, 'T') !== false) {
                    if (!preg_match('/[+-]\d{2}:\d{2}|Z$/', $followed_at_str)) {
                        $followed_at_str .= 'Z';
                    }
                    $entry_ts = strtotime($followed_at_str);
                } else {
                    $entry_ts = strtotime($followed_at_str . ' UTC');
                }
                echo gmdate('M d, Y H:i:s', $entry_ts) . ' UTC';
            } else {
                echo '--';
            }
            ?>
        </div>
    </div>
    <div class="trade-info-item">
        <div class="trade-info-label">Exit Time</div>
        <div class="trade-info-value editable" data-field="our_exit_timestamp">
            <span class="display-value">
                <?php 
                if ($effective_exit_timestamp) {
                    date_default_timezone_set('UTC');
                    $formatted_time = gmdate('M d, Y H:i:s', strtotime($effective_exit_timestamp));
                    $is_estimated = ($trade['our_status'] !== 'completed' && $trade['our_status'] !== 'sold');
                    echo $formatted_time . ' UTC' . ($is_estimated ? ' <small class="text-warning">(est)</small>' : '');
                } else {
                    echo '--';
                }
                ?>
            </span>
            <input type="datetime-local" value="<?php 
                if ($trade['our_exit_timestamp']) {
                    date_default_timezone_set('UTC');
                    echo gmdate('Y-m-d\TH:i:s', strtotime($trade['our_exit_timestamp']));
                } else {
                    echo '';
                }
            ?>">
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

<!-- Filter Validation Status -->
<?php
$status_colors = [
    'passed' => ['bg' => 'bg-success', 'border' => 'border-success', 'icon' => 'ri-checkbox-circle-fill'],
    'failed' => ['bg' => 'bg-danger', 'border' => 'border-danger', 'icon' => 'ri-close-circle-fill'],
    'error' => ['bg' => 'bg-warning', 'border' => 'border-warning', 'icon' => 'ri-error-warning-fill'],
    'no_data' => ['bg' => 'bg-secondary', 'border' => 'border-secondary', 'icon' => 'ri-question-line'],
    'unknown' => ['bg' => 'bg-secondary', 'border' => 'border-secondary', 'icon' => 'ri-question-line'],
];
$status_style = $status_colors[$validation_status] ?? $status_colors['unknown'];
?>
<div class="card custom-card mb-3 <?php echo $status_style['border']; ?>" style="border-width: 2px;">
    <div class="card-header <?php echo $status_style['bg']; ?>-transparent py-2">
        <div class="d-flex align-items-center justify-content-between w-100">
            <div class="d-flex align-items-center">
                <i class="<?php echo $status_style['icon']; ?> fs-20 me-2 <?php echo str_replace('bg-', 'text-', $status_style['bg']); ?>"></i>
                <div>
                    <h6 class="mb-0 fw-semibold">Filter Validation Status</h6>
                    <small class="text-muted"><?php echo htmlspecialchars($validation_message); ?></small>
                </div>
            </div>
            <div class="text-end">
                <?php if (!empty($filter_results_summary)): ?>
                    <span class="badge <?php echo $any_project_passed ? 'bg-success' : 'bg-danger'; ?> fs-12">
                        <?php echo $any_project_passed ? 'PASSED' : 'FAILED'; ?>
                    </span>
                    <br>
                    <small class="text-muted">
                        <?php 
                        $total_filters = array_sum(array_column($filter_results_summary, 'total'));
                        echo $total_filters; ?> filters across <?php echo count($filter_results_summary); ?> project(s)
                    </small>
                <?php elseif ($validation_status === 'no_data'): ?>
                    <span class="badge bg-secondary fs-11">NO VALIDATION DATA</span>
                    <br>
                    <small class="text-muted">Trade ID: <?php echo $trade_id; ?></small>
                <?php endif; ?>
            </div>
        </div>
    </div>
    
    <?php if ($validation_status === 'no_data'): ?>
    <div class="card-body py-3">
        <p class="mb-2 text-muted small">
            <strong>Why might filter results be missing?</strong>
        </p>
        <ul class="mb-0 small text-muted ps-3">
            <li>This trade may have been created before filter validation was enabled</li>
            <li>The play may not have any projects/filters configured</li>
            <li>The pattern validator may not have run for this trade</li>
            <li>The trade may have been manually created or imported</li>
        </ul>
    </div>
    <?php endif; ?>
    
    <?php if (!empty($filter_results_summary)): ?>
    <!-- Detailed Filter Results (Expandable) -->
    <div class="card-body border-top pt-3">
        <p class="text-muted small mb-3">
            <i class="ri-information-line me-1"></i>
            <?php echo count($filter_results_summary); ?> project(s) evaluated. 
            Trade passes if <strong>any</strong> project's filters <strong>all</strong> pass.
        </p>
        
        <?php foreach ($filter_results_summary as $project): ?>
        <div class="card mb-2" style="border: 2px solid <?php echo $project['all_passed'] ? 'rgba(38, 191, 148, 0.5)' : 'rgba(230, 83, 60, 0.5)'; ?>;">
            <div class="card-header py-2" style="background: <?php echo $project['all_passed'] ? 'rgba(38, 191, 148, 0.15)' : 'rgba(230, 83, 60, 0.15)'; ?>;">
                <div class="d-flex align-items-center justify-content-between w-100">
                    <span class="fw-semibold">
                        <i class="<?php echo $project['all_passed'] ? 'ri-checkbox-circle-fill text-success' : 'ri-close-circle-fill text-danger'; ?> me-1"></i>
                        <?php echo htmlspecialchars($project['project_name']); ?>
                        <span class="text-muted fs-11">(Project #<?php echo $project['project_id']; ?>)</span>
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
                    <table class="table table-sm table-hover mb-0">
                        <thead>
                            <tr class="text-muted small" style="border-bottom: 1px solid var(--default-border);">
                                <th style="width: 30px;"></th>
                                <th>Filter</th>
                                <th>Field Column</th>
                                <th class="text-center">Min</th>
                                <th class="text-center fw-bold">Actual</th>
                                <th class="text-center">Max</th>
                            </tr>
                        </thead>
                        <tbody>
                            <?php foreach ($project['filters'] as $filter): ?>
                            <tr style="<?php echo $filter['passed'] ? '' : 'background: rgba(230, 83, 60, 0.1);'; ?>">
                                <td class="text-center">
                                    <?php if ($filter['passed']): ?>
                                        <i class="ri-checkbox-circle-fill text-success fs-16"></i>
                                    <?php else: ?>
                                        <i class="ri-close-circle-fill text-danger fs-16"></i>
                                    <?php endif; ?>
                                </td>
                                <td>
                                    <span class="fw-medium"><?php echo htmlspecialchars($filter['filter_name'] ?? 'Filter #' . $filter['filter_id']); ?></span>
                                    <?php if (!empty($filter['error'])): ?>
                                        <br><small class="text-danger"><?php echo htmlspecialchars($filter['error']); ?></small>
                                    <?php endif; ?>
                                </td>
                                <td>
                                    <code class="text-info small"><?php echo htmlspecialchars($filter['field_column'] ?? '-'); ?></code>
                                    <?php if (($filter['minute'] ?? 0) > 0): ?>
                                        <span class="badge bg-purple-transparent small ms-1">M<?php echo $filter['minute']; ?></span>
                                    <?php endif; ?>
                                </td>
                                <td class="text-center text-muted font-monospace">
                                    <?php echo $filter['from_value'] !== null ? number_format($filter['from_value'], 4) : '-'; ?>
                                </td>
                                <td class="text-center fw-bold font-monospace <?php echo $filter['passed'] ? 'text-success' : 'text-danger'; ?>">
                                    <?php echo $filter['actual_value'] !== null ? number_format($filter['actual_value'], 4) : 'NULL'; ?>
                                </td>
                                <td class="text-center text-muted font-monospace">
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
    <?php endif; ?>
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
<?php if (!empty($price_checks) || !empty($trade['price_movements'])): ?>
<div class="card custom-card mb-3">
    <div class="card-header">
        <div class="card-title">Price Checks Timeline</div>
        <div class="ms-auto">
            <?php if (!empty($price_checks)): ?>
                <span class="badge bg-primary-transparent"><?php echo count($price_checks); ?> checks</span>
                <span class="badge bg-success-transparent">from <?php echo $price_checks_source === 'duckdb' ? 'DuckDB' : 'MySQL'; ?></span>
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
    // UTC Date Formatting Utilities (Server Time - Never Browser Time)
    const formatUTC = {
        // Format timestamp to UTC string: "Jan 15, 2024 12:33:45"
        toUTCString: function(timestampMs) {
            if (!timestampMs) return '--';
            const d = new Date(timestampMs);
            const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
            const year = d.getUTCFullYear();
            const month = months[d.getUTCMonth()];
            const day = d.getUTCDate();
            const hours = String(d.getUTCHours()).padStart(2, '0');
            const minutes = String(d.getUTCMinutes()).padStart(2, '0');
            const seconds = String(d.getUTCSeconds()).padStart(2, '0');
            return `${month} ${day}, ${year} ${hours}:${minutes}:${seconds}`;
        },
        
        // Format timestamp to UTC time string: "12:33:45"
        toUTCTimeString: function(timestampMs) {
            if (!timestampMs) return '--';
            const d = new Date(timestampMs);
            const hours = String(d.getUTCHours()).padStart(2, '0');
            const minutes = String(d.getUTCMinutes()).padStart(2, '0');
            const seconds = String(d.getUTCSeconds()).padStart(2, '0');
            return `${hours}:${minutes}:${seconds}`;
        },
        
        // Format timestamp to UTC date/time: "Jan 15, 12:33"
        toUTCDateTime: function(timestampMs) {
            if (!timestampMs) return '--';
            const d = new Date(timestampMs);
            const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
            const month = months[d.getUTCMonth()];
            const day = d.getUTCDate();
            const hours = String(d.getUTCHours()).padStart(2, '0');
            const minutes = String(d.getUTCMinutes()).padStart(2, '0');
            return `${month} ${day}, ${hours}:${minutes}`;
        },
        
        // Format timestamp to UTC ISO string for debugging
        toISOString: function(timestampMs) {
            if (!timestampMs) return null;
            return new Date(timestampMs).toISOString();
        }
    };
    
    // Inject PHP data into JavaScript
    const jsData = <?php echo json_encode($js_data); ?>;
    const tradeData = jsData.trade;
    const returnUrl = <?php echo json_encode($return_url); ?>;
    
    // Debug: Show raw values from PHP (all in UTC)
    console.log('=== DEBUG: Trade Timestamps (UTC) ===');
    console.log('Raw followed_at from DB:', tradeData.followed_at);
    console.log('Raw our_exit_timestamp from DB:', tradeData.our_exit_timestamp);
    console.log('PHP followed_at_ms:', jsData.followed_at_ms, '→ UTC:', formatUTC.toISOString(jsData.followed_at_ms));
    console.log('PHP effective_exit_ms:', jsData.effective_exit_ms, '→ UTC:', formatUTC.toISOString(jsData.effective_exit_ms));
    
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
            
            // Use larger buffer to ensure we capture all price data
            // Buffer: 30 minutes before entry, and ensure we go well past exit
            const bufferBeforeMs = 30 * 60 * 1000; // 30 minutes before
            const bufferAfterMs = 30 * 60 * 1000;  // 30 minutes after
            
            const startTimeMs = followedAtTime - bufferBeforeMs;
            // Ensure end time covers the full trade period plus buffer
            const tradeDurationMs = exitTime - followedAtTime;
            const endTimeMs = exitTime + Math.max(bufferAfterMs, tradeDurationMs * 0.5); // At least 30min after, or 50% of trade duration
            
            const startTimeSec = Math.floor(startTimeMs / 1000);
            const endTimeSec = Math.floor(endTimeMs / 1000);
            
            console.log('=== Trade Chart Debug (UTC) ===');
            console.log('Trade Entry Time:', formatUTC.toUTCString(followedAtTime), `(${followedAtTime})`);
            console.log('Trade Exit Time:', formatUTC.toUTCString(exitTime), `(${exitTime})`);
            console.log('Trade Duration:', ((exitTime - followedAtTime) / 1000 / 60).toFixed(1), 'minutes');
            console.log('Fetching prices from:', formatUTC.toUTCString(startTimeMs), 'to:', formatUTC.toUTCString(endTimeMs));
            console.log('Time range:', ((endTimeMs - startTimeMs) / 1000 / 60).toFixed(1), 'minutes');
            
            const apiUrl = `<?php echo $baseUrl; ?>/chart/plays/get_trade_prices.php?start=${startTimeSec}&end=${endTimeSec}`;
            console.log('API URL:', apiUrl);
            
            const response = await fetch(apiUrl);
            console.log('Response status:', response.status, response.statusText);
            
            const data = await response.json();
            
            console.log('Price data received:', data.prices ? data.prices.length : 0, 'points');
            if (data.debug) {
                console.log('API Debug Info:', data.debug);
            }
            if (data.prices && data.prices.length > 0) {
                const firstPrice = data.prices[0];
                const lastPrice = data.prices[data.prices.length - 1];
                console.log('First price point:', firstPrice.x, '$' + firstPrice.y);
                console.log('Last price point:', lastPrice.x, '$' + lastPrice.y);
            }
            
            if (!data.success) {
                const errorMsg = data.error || 'Unknown error';
                document.getElementById('tradeDetailChart').innerHTML = `<div class="text-center text-danger py-5">Error loading chart data: ${errorMsg}</div>`;
                return;
            }
            
            if (!data.prices || data.prices.length === 0) {
                document.getElementById('tradeDetailChart').innerHTML = '<div class="text-center text-warning py-5">No price data available for this time period</div>';
                return;
            }
            
            console.log('Rendering chart with entry:', formatUTC.toUTCString(followedAtTime), 'exit:', formatUTC.toUTCString(exitTime));
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
        
        // Debug: Compare price data range with entry/exit times (all UTC)
        if (allPrices.length > 0) {
            const priceStart = allPrices[0].x;
            const priceEnd = allPrices[allPrices.length - 1].x;
            console.log('Price data range:', formatUTC.toUTCString(priceStart), 'to', formatUTC.toUTCString(priceEnd));
            console.log('Entry time:', formatUTC.toUTCString(followedAtTime), '| Exit time:', formatUTC.toUTCString(exitTime));
            console.log('Entry within price range:', followedAtTime >= priceStart && followedAtTime <= priceEnd);
            console.log('Exit within price range:', exitTime >= priceStart && exitTime <= priceEnd);
        }
        
        // Calculate chart x-axis range to include entry/exit times with buffer
        // Ensure we show the full trade period even if price data doesn't extend that far
        const bufferMs = 5 * 60 * 1000; // 5 minute buffer
        let xMin = followedAtTime - bufferMs;
        let xMax = (exitTime || followedAtTime) + bufferMs;
        
        // Include price data range, but don't let it shrink the view if trade period is larger
        if (allPrices.length > 0) {
            const priceDataMin = allPrices[0].x;
            const priceDataMax = allPrices[allPrices.length - 1].x;
            // Expand range to include both trade period and price data
            xMin = Math.min(xMin, priceDataMin);
            xMax = Math.max(xMax, priceDataMax);
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
                    datetimeUTC: true,  // Always use UTC (server time)
                    style: {
                        colors: '#9ca3af',
                        fontSize: '11px'
                    },
                    datetimeFormatter: {
                        hour: 'HH:mm UTC',
                        minute: 'HH:mm:ss UTC',
                        day: 'MMM dd HH:mm UTC'
                    },
                    formatter: function(value, timestamp) {
                        // Format in UTC and append UTC label
                        const d = new Date(timestamp);
                        const hours = String(d.getUTCHours()).padStart(2, '0');
                        const minutes = String(d.getUTCMinutes()).padStart(2, '0');
                        return hours + ':' + minutes + ' UTC';
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
                    
                    // Format time in UTC (server time)
                    const timeStr = formatUTC.toUTCTimeString(timestamp);
                    const dateStr = formatUTC.toUTCDateTime(timestamp);
                    
                    return '<div class="p-2">' +
                        '<div class="mb-1"><strong>' + dateStr + ' UTC</strong></div>' +
                        '<div>Time: <span class="text-muted">' + timeStr + ' UTC</span></div>' +
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
            // Format in UTC (server time)
            const timeStr = formatUTC.toUTCString(point.timestamp);
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
                    // Handle both boolean and integer values
                    const sell = item.should_sell === true || item.should_sell == 1 || item.should_sell === '1';
                    const isBackfill = item.is_backfill === true || item.is_backfill == 1 || item.is_backfill === '1';
                    
                    // Format in UTC (server time) - parse the datetime string and convert to UTC
                    let timeStr = '--';
                    if (item.checked_at) {
                        // Parse the datetime string (assumes it's already in UTC from server)
                        const dt = new Date(item.checked_at.replace(' ', 'T') + 'Z');
                        timeStr = formatUTC.toUTCDateTime(dt.getTime());
                    }
                    
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

