<?php
/**
 * Price Cycles Monitor
 * 
 * Displays price cycle data from the cycle_tracker table.
 * Shows when cycles start, end, and their performance metrics.
 */

// --- DuckDB API Client ---
require_once __DIR__ . '/../../includes/DuckDBClient.php';
define('DUCKDB_API_URL', 'http://127.0.0.1:5051');
$duckdb = new DuckDBClient(DUCKDB_API_URL);
$use_duckdb = $duckdb->isAvailable();

// --- Base URL for template ---
$baseUrl = '../..';

// --- Parameters ---
$threshold_filter = isset($_GET['threshold']) ? floatval($_GET['threshold']) : null;
$hours = isset($_GET['hours']) ? $_GET['hours'] : '24';  // Default to 24h (uses DuckDB engine)
$limit = isset($_GET['limit']) ? intval($_GET['limit']) : 50;
$refresh_interval = isset($_GET['refresh']) ? intval($_GET['refresh']) : 30; // Default 30 seconds

// --- Fetch Cycle Data ---
function fetchCycleData($duckdb, $threshold, $hours, $limit) {
    $cycles = [];
    $error_message = null;
    $data_source = "No Data";
    $actual_source = null;
    $total_count = 0;
    $missing_cycles = 0;

    if ($duckdb->isAvailable()) {
        $response = $duckdb->getCycleTracker($threshold, $hours, $limit);
        
        if ($response && isset($response['cycles'])) {
            $cycles = $response['cycles'];
            $actual_source = $response['source'] ?? 'unknown';
            $total_count = $response['total_count'] ?? count($cycles);
            $missing_cycles = $response['missing_cycles'] ?? 0;
            
            switch ($actual_source) {
                case 'engine':
                    $data_source = "🦆 Engine";
                    break;
                case 'mysql':
                    $data_source = "🗄️ MySQL";
                    break;
                default:
                    $data_source = "🦆 DuckDB";
            }
        } else {
            $data_source = "No Data";
        }
    } else {
        $error_message = "DuckDB API is not available. Please start the services: python scheduler/master2.py (and python scheduler/website_api.py)";
    }
    
    return [
        'cycles' => $cycles,
        'error_message' => $error_message,
        'data_source' => $data_source,
        'actual_source' => $actual_source,
        'total_count' => $total_count,
        'missing_cycles' => $missing_cycles
    ];
}

// Fetch ALL cycles (no threshold filter) for the threshold range table
function fetchAllCyclesForRangeTable($duckdb, $hours, $limit) {
    if ($duckdb->isAvailable()) {
        // Fetch without threshold filter (null = all thresholds)
        $response = $duckdb->getCycleTracker(null, $hours, $limit);
        if ($response && isset($response['cycles'])) {
            return $response['cycles'];
        }
    }
    return [];
}

// Check if this is an AJAX request for data refresh
if (isset($_GET['ajax']) && $_GET['ajax'] === 'refresh') {
    header('Content-Type: application/json');
    $result = fetchCycleData($duckdb, $threshold_filter, $hours, $limit);
    
    echo json_encode([
        'success' => true,
        'cycles' => $result['cycles'],
        'error_message' => $result['error_message'],
        'data_source' => $result['data_source'],
        'actual_source' => $result['actual_source'],
        'count' => count($result['cycles']),
        'total_count' => $result['total_count'],
        'missing_cycles' => $result['missing_cycles']
    ]);
    exit;
}

// Regular page load
$result = fetchCycleData($duckdb, $threshold_filter, $hours, $limit);
$cycles = $result['cycles'];
$error_message = $result['error_message'];
$data_source = $result['data_source'];
$actual_source = $result['actual_source'];
$total_count = $result['total_count'];
$missing_cycles = $result['missing_cycles'];

// Fetch ALL cycles for the threshold range table (ignoring threshold filter)
$all_cycles_for_table = fetchAllCyclesForRangeTable($duckdb, $hours, 1000);

// Calculate stats
$active_cycles = 0;
$completed_cycles = 0;
$avg_max_increase = 0;
$total_increase = 0;

foreach ($cycles as $cycle) {
    if (empty($cycle['cycle_end_time'])) {
        $active_cycles++;
    } else {
        $completed_cycles++;
    }
    $total_increase += floatval($cycle['max_percent_increase'] ?? 0);
}

if (count($cycles) > 0) {
    $avg_max_increase = $total_increase / count($cycles);
}

// Calculate cycle counts by threshold and percent increase range
// Rows: percent increase ranges, Columns: threshold values
// Use STRING keys to avoid PHP float comparison issues
$thresholds = ['0.2', '0.25', '0.3', '0.35', '0.4', '0.45', '0.5'];
$ranges = [
    '0.0 - 0.1' => [0.0, 0.1],
    '0.1 - 0.2' => [0.1, 0.2],
    '0.2 - 0.3' => [0.2, 0.3],
    '0.3 - 0.4' => [0.3, 0.4],
    '0.4 - 0.5' => [0.4, 0.5],
    '0.5+' => [0.5, 999]
];

$threshold_range_counts = [];
foreach ($ranges as $range_name => $range_bounds) {
    $threshold_range_counts[$range_name] = [];
    foreach ($thresholds as $th) {
        $threshold_range_counts[$range_name][$th] = 0;
    }
}

// Use all_cycles_for_table instead of filtered cycles
foreach ($all_cycles_for_table as $cycle) {
    $threshold = floatval($cycle['threshold'] ?? 0);
    $max_increase = floatval($cycle['max_percent_increase'] ?? 0);
    
    // Convert threshold to string key (e.g., 0.25 -> "0.25")
    // Use 2 decimal places to handle thresholds like 0.25, 0.35, 0.45
    $threshold_key = rtrim(rtrim(number_format($threshold, 2), '0'), '.');
    
    // Only count if this is one of our tracked thresholds
    if (!in_array($threshold_key, $thresholds)) {
        continue;
    }
    
    // Find which range this cycle's max_percent_increase falls into
    foreach ($ranges as $range_name => $range_bounds) {
        if ($max_increase >= $range_bounds[0] && ($range_bounds[1] == 999 || $max_increase < $range_bounds[1])) {
            $threshold_range_counts[$range_name][$threshold_key]++;
            break;
        }
    }
}
?>

<!-- Styles for this page -->
<?php ob_start(); ?>

<style>
    .stat-card {
        background: rgba(var(--body-bg-rgb2), 1);
        border: 1px solid rgba(255,255,255,0.1);
        border-radius: 0.5rem;
        padding: 1.25rem;
        transition: all 0.2s ease;
    }
    .stat-card:hover {
        border-color: rgba(var(--primary-rgb), 0.3);
        transform: translateY(-2px);
    }
    .stat-value {
        font-size: 1.75rem;
        font-weight: 700;
        line-height: 1.2;
        color: #ffffff;
    }
    .stat-label {
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        color: rgba(255,255,255,0.6);
        margin-bottom: 0.25rem;
    }
    .stat-icon {
        width: 48px;
        height: 48px;
        border-radius: 0.5rem;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 1.5rem;
    }
    .threshold-badge {
        font-size: 0.8rem;
        padding: 0.35rem 0.65rem;
        font-weight: 600;
    }
    .price-up { color: #10b981; }
    .price-down { color: #ef4444; }
    .data-table th {
        font-size: 0.7rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: rgba(255,255,255,0.5);
        font-weight: 600;
        padding: 0.75rem 0.5rem;
        border-bottom: 1px solid rgba(255,255,255,0.1);
    }
    .data-table td {
        padding: 0.75rem 0.5rem;
        border-bottom: 1px solid rgba(255,255,255,0.05);
        vertical-align: middle;
    }
    .data-table tbody tr:hover {
        background: rgba(var(--primary-rgb), 0.05);
    }
    .filter-btn {
        padding: 0.4rem 0.75rem;
        font-size: 0.8rem;
        border-radius: 0.375rem;
        transition: all 0.15s ease;
    }
    .filter-btn.active {
        background: rgba(var(--primary-rgb), 0.2) !important;
        border-color: rgb(var(--primary-rgb)) !important;
        color: rgb(var(--primary-rgb)) !important;
    }
    .connection-badge {
        display: inline-flex;
        align-items: center;
        gap: 0.5rem;
        padding: 0.35rem 0.75rem;
        border-radius: 0.375rem;
        font-size: 0.75rem;
        font-weight: 600;
    }
    .connection-badge.connected {
        background: rgba(var(--success-rgb), 0.15);
        color: rgb(var(--success-rgb));
    }
    .connection-badge.disconnected {
        background: rgba(var(--danger-rgb), 0.15);
        color: rgb(var(--danger-rgb));
    }
    .pulse-dot {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        background: currentColor;
        animation: pulse 1.5s infinite;
    }
    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.4; }
    }
    .data-source-info {
        font-size: 0.75rem;
        color: rgba(255,255,255,0.5);
    }
</style>

<?php $styles = ob_get_clean(); ?>

<!-- Content for this page -->
<?php ob_start(); ?>

                    <!-- Page Header -->
                    <div class="d-flex align-items-center justify-content-between flex-wrap gap-2 mb-4">
                        <div>
                            <h1 class="page-title fw-semibold fs-18 mb-0">Price Cycles Monitor</h1>
                            <p class="text-muted mb-0 fs-13">Track SOL price cycle patterns across thresholds</p>
                        </div>
                        <div class="d-flex gap-2 align-items-center">
                            <span class="connection-badge <?php echo $use_duckdb ? 'connected' : 'disconnected'; ?>">
                                <span class="pulse-dot"></span>
                                <?php echo $use_duckdb ? 'Connected' : 'Disconnected'; ?>
                            </span>
                            <select id="refreshInterval" class="form-select form-select-sm" style="width: auto;" onchange="updateRefreshInterval(this.value)">
                                <option value="5" <?php echo $refresh_interval == 5 ? 'selected' : ''; ?>>5 sec</option>
                                <option value="10" <?php echo $refresh_interval == 10 ? 'selected' : ''; ?>>10 sec</option>
                                <option value="30" <?php echo $refresh_interval == 30 ? 'selected' : ''; ?>>30 sec</option>
                                <option value="60" <?php echo $refresh_interval == 60 ? 'selected' : ''; ?>>1 min</option>
                                <option value="300" <?php echo $refresh_interval == 300 ? 'selected' : ''; ?>>5 min</option>
                                <option value="0" <?php echo $refresh_interval == 0 ? 'selected' : ''; ?>>Off</option>
                            </select>
                            <button class="btn btn-sm btn-primary" onclick="refreshData()">
                                <i class="ri-refresh-line me-1"></i> Refresh
                            </button>
                        </div>
                    </div>

                    <?php if ($error_message): ?>
                    <div class="alert alert-danger mb-4" role="alert">
                        <i class="ri-error-warning-line me-2"></i><?php echo htmlspecialchars($error_message); ?>
                    </div>
                    <?php endif; ?>

                    <!-- Stats Cards -->
                    <div class="row g-3 mb-4">
                        <div class="col-sm-6 col-xl-3">
                            <div class="stat-card">
                                <div class="d-flex justify-content-between align-items-start">
                                    <div>
                                        <p class="stat-label">Total Cycles</p>
                                        <p class="stat-value" id="stat-total" style="color: #fff;"><?php echo $total_count; ?></p>
                                        <?php if ($total_count > $limit): ?>
                                        <p class="text-muted mb-0" style="font-size: 0.7rem;">Showing <?php echo count($cycles); ?></p>
                                        <?php endif; ?>
                                    </div>
                                    <div class="stat-icon bg-primary-transparent text-primary">
                                        <i class="ri-loop-right-line"></i>
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div class="col-sm-6 col-xl-3">
                            <div class="stat-card">
                                <div class="d-flex justify-content-between align-items-start">
                                    <div>
                                        <p class="stat-label">Active Cycles</p>
                                        <p class="stat-value price-up" id="stat-active"><?php echo $active_cycles; ?></p>
                                    </div>
                                    <div class="stat-icon bg-success-transparent text-success">
                                        <i class="ri-play-circle-line"></i>
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div class="col-sm-6 col-xl-3">
                            <div class="stat-card">
                                <div class="d-flex justify-content-between align-items-start">
                                    <div>
                                        <p class="stat-label">Completed</p>
                                        <p class="stat-value text-secondary" id="stat-completed"><?php echo $completed_cycles; ?></p>
                                    </div>
                                    <div class="stat-icon bg-secondary-transparent text-secondary">
                                        <i class="ri-check-double-line"></i>
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div class="col-sm-6 col-xl-3">
                            <div class="stat-card">
                                <div class="d-flex justify-content-between align-items-start">
                                    <div>
                                        <p class="stat-label">Missing Cycles</p>
                                        <p class="stat-value <?php echo $missing_cycles > 0 ? 'text-warning' : 'text-muted'; ?>" id="stat-missing">
                                            <?php echo $missing_cycles; ?>
                                        </p>
                                        <p class="text-muted mb-0" style="font-size: 0.7rem;">ID sequence gaps</p>
                                    </div>
                                    <div class="stat-icon bg-warning-transparent text-warning">
                                        <i class="ri-alert-line"></i>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>

                    <!-- Avg Max Increase moved to a separate row -->
                    <div class="row g-3 mb-4">
                        <div class="col-12">
                            <div class="stat-card">
                                <div class="d-flex justify-content-between align-items-start">
                                    <div>
                                        <p class="stat-label">Average Max Increase</p>
                                        <p class="stat-value <?php echo $avg_max_increase >= 0 ? 'price-up' : 'price-down'; ?>" id="stat-avg">
                                            <?php echo number_format($avg_max_increase, 4); ?>%
                                        </p>
                                    </div>
                                    <div class="stat-icon bg-info-transparent text-info">
                                        <i class="ri-line-chart-line"></i>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>

                    <!-- Threshold Range Counts Table -->
                    <div class="card custom-card mb-4">
                        <div class="card-header">
                            <h5 class="card-title mb-0">Cycles by Threshold Range</h5>
                        </div>
                        <div class="card-body">
                            <div class="table-responsive">
                                <table class="table mb-0">
                                    <thead>
                                        <tr>
                                            <th>Range</th>
                                            <th class="text-center">0.2%</th>
                                            <th class="text-center">0.25%</th>
                                            <th class="text-center">0.3%</th>
                                            <th class="text-center">0.35%</th>
                                            <th class="text-center">0.4%</th>
                                            <th class="text-center">0.45%</th>
                                            <th class="text-center">0.5%</th>
                                        </tr>
                                    </thead>
                                    <tbody id="threshold-range-body">
                                        <?php foreach ($threshold_range_counts as $range_name => $counts): 
                                            // Check if this row has any non-zero values
                                            $row_total = ($counts['0.2'] ?? 0) + ($counts['0.25'] ?? 0) + ($counts['0.3'] ?? 0) + ($counts['0.35'] ?? 0) + ($counts['0.4'] ?? 0) + ($counts['0.45'] ?? 0) + ($counts['0.5'] ?? 0);
                                            if ($row_total == 0) continue;
                                        ?>
                                        <tr>
                                            <td class="fw-medium"><?php echo htmlspecialchars($range_name); ?>%</td>
                                            <td class="text-center">
                                                <?php if (($counts['0.2'] ?? 0) > 0): ?>
                                                <span class="badge bg-primary-transparent"><?php echo $counts['0.2']; ?></span>
                                                <?php else: ?>
                                                <span class="text-muted">-</span>
                                                <?php endif; ?>
                                            </td>
                                            <td class="text-center">
                                                <?php if (($counts['0.25'] ?? 0) > 0): ?>
                                                <span class="badge bg-primary-transparent"><?php echo $counts['0.25']; ?></span>
                                                <?php else: ?>
                                                <span class="text-muted">-</span>
                                                <?php endif; ?>
                                            </td>
                                            <td class="text-center">
                                                <?php if (($counts['0.3'] ?? 0) > 0): ?>
                                                <span class="badge bg-primary-transparent"><?php echo $counts['0.3']; ?></span>
                                                <?php else: ?>
                                                <span class="text-muted">-</span>
                                                <?php endif; ?>
                                            </td>
                                            <td class="text-center">
                                                <?php if (($counts['0.35'] ?? 0) > 0): ?>
                                                <span class="badge bg-primary-transparent"><?php echo $counts['0.35']; ?></span>
                                                <?php else: ?>
                                                <span class="text-muted">-</span>
                                                <?php endif; ?>
                                            </td>
                                            <td class="text-center">
                                                <?php if (($counts['0.4'] ?? 0) > 0): ?>
                                                <span class="badge bg-primary-transparent"><?php echo $counts['0.4']; ?></span>
                                                <?php else: ?>
                                                <span class="text-muted">-</span>
                                                <?php endif; ?>
                                            </td>
                                            <td class="text-center">
                                                <?php if (($counts['0.45'] ?? 0) > 0): ?>
                                                <span class="badge bg-primary-transparent"><?php echo $counts['0.45']; ?></span>
                                                <?php else: ?>
                                                <span class="text-muted">-</span>
                                                <?php endif; ?>
                                            </td>
                                            <td class="text-center">
                                                <?php if (($counts['0.5'] ?? 0) > 0): ?>
                                                <span class="badge bg-primary-transparent"><?php echo $counts['0.5']; ?></span>
                                                <?php else: ?>
                                                <span class="text-muted">-</span>
                                                <?php endif; ?>
                                            </td>
                                        </tr>
                                        <?php endforeach; ?>
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>

                    <!-- Filters -->
                    <div class="card custom-card mb-4">
                        <div class="card-body py-3">
                            <div class="d-flex flex-wrap gap-2 align-items-center">
                                <span class="text-muted fs-13 me-1">Threshold:</span>
                                <a href="?hours=<?php echo $hours; ?>&limit=<?php echo $limit; ?>" 
                                   class="btn btn-sm btn-outline-light filter-btn <?php echo $threshold_filter === null ? 'active' : ''; ?>">
                                    All
                                </a>
                                <?php foreach ([0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5] as $t): ?>
                                <a href="?threshold=<?php echo $t; ?>&hours=<?php echo $hours; ?>&limit=<?php echo $limit; ?>" 
                                   class="btn btn-sm btn-outline-light filter-btn <?php echo $threshold_filter == $t ? 'active' : ''; ?>">
                                    <?php echo $t; ?>%
                                </a>
                                <?php endforeach; ?>
                                
                            <span class="text-muted fs-13 ms-3 me-1">Created:</span>
                            <a href="?<?php echo $threshold_filter !== null ? "threshold=$threshold_filter&" : ''; ?>hours=1&limit=<?php echo $limit; ?>" 
                               class="btn btn-sm btn-outline-light filter-btn <?php echo $hours == '1' ? 'active' : ''; ?>">
                                1h
                            </a>
                            <a href="?<?php echo $threshold_filter !== null ? "threshold=$threshold_filter&" : ''; ?>hours=24&limit=<?php echo $limit; ?>" 
                               class="btn btn-sm btn-outline-light filter-btn <?php echo $hours == '24' ? 'active' : ''; ?>">
                                24h
                            </a>
                            <a href="?<?php echo $threshold_filter !== null ? "threshold=$threshold_filter&" : ''; ?>hours=all&limit=<?php echo $limit; ?>" 
                               class="btn btn-sm btn-outline-light filter-btn <?php echo $hours == 'all' ? 'active' : ''; ?>"
                               title="All data in DuckDB (24h rolling window)">
                                All
                            </a>
                            <a href="?<?php echo $threshold_filter !== null ? "threshold=$threshold_filter&" : ''; ?>hours=48&limit=<?php echo $limit; ?>" 
                               class="btn btn-sm btn-outline-light filter-btn <?php echo $hours == '48' ? 'active' : ''; ?>"
                               title="Historical data from MySQL archive">
                                48h (Archive)
                            </a>

                                <span class="ms-auto data-source-info" id="data-source">
                                    Data: <?php echo $data_source; ?>
                                    <?php if ($actual_source == 'engine' && $hours == 'all'): ?>
                                        <span class="text-muted ms-1" style="font-size: 0.7rem;">(24h max)</span>
                                    <?php endif; ?>
                                </span>
                            </div>
                        </div>
                    </div>

                    <!-- Cycles Table -->
                    <div class="card custom-card">
                        <div class="card-header">
                            <h5 class="card-title mb-0">Recent Cycles</h5>
                        </div>
                        <div class="card-body p-0">
                            <div class="table-responsive">
                                <table class="table mb-0 data-table">
                                    <thead>
                                        <tr>
                                            <th>ID</th>
                                            <th>Threshold</th>
                                            <th>Status</th>
                                            <th>Start Time (UTC)</th>
                                            <th>End Time (UTC)</th>
                                            <th>Start Price</th>
                                            <th>Highest</th>
                                            <th>Lowest</th>
                                            <th>Max Increase</th>
                                            <th>From Lowest</th>
                                            <th>Data Points</th>
                                        </tr>
                                    </thead>
                                    <tbody id="cycles-table-body">
                                        <?php if (empty($cycles)): ?>
                                        <tr>
                                            <td colspan="11" class="text-center py-4 text-muted">
                                                <i class="ri-database-2-line fs-24 d-block mb-2"></i>
                                                No cycles found. Waiting for data...
                                            </td>
                                        </tr>
                                        <?php else: ?>
                                        <?php foreach ($cycles as $cycle): ?>
                                        <?php 
                                            $is_active = empty($cycle['cycle_end_time']);
                                            $max_increase = floatval($cycle['max_percent_increase'] ?? 0);
                                            $from_lowest = floatval($cycle['max_percent_increase_from_lowest'] ?? 0);
                                        ?>
                                        <tr>
                                            <td class="fw-medium">#<?php echo htmlspecialchars($cycle['id'] ?? '-'); ?></td>
                                            <td>
                                                <span class="badge bg-primary-transparent threshold-badge">
                                                    <?php echo number_format(floatval($cycle['threshold'] ?? 0), 2); ?>%
                                                </span>
                                            </td>
                                            <td>
                                                <?php if ($is_active): ?>
                                                <span class="badge bg-success">Active</span>
                                                <?php else: ?>
                                                <span class="badge bg-secondary">Closed</span>
                                                <?php endif; ?>
                                            </td>
                                            <td class="fs-12">
                                                <?php echo $cycle['cycle_start_time'] ? gmdate('M j, H:i:s', strtotime($cycle['cycle_start_time'])) : '-'; ?>
                                            </td>
                                            <td class="fs-12">
                                                <?php echo $cycle['cycle_end_time'] ? gmdate('M j, H:i:s', strtotime($cycle['cycle_end_time'])) : '-'; ?>
                                            </td>
                                            <td class="font-monospace">$<?php echo number_format(floatval($cycle['sequence_start_price'] ?? 0), 4); ?></td>
                                            <td class="font-monospace price-up">$<?php echo number_format(floatval($cycle['highest_price_reached'] ?? 0), 4); ?></td>
                                            <td class="font-monospace price-down">$<?php echo number_format(floatval($cycle['lowest_price_reached'] ?? 0), 4); ?></td>
                                            <td class="<?php echo $max_increase >= 0 ? 'price-up' : 'price-down'; ?>">
                                                <?php echo number_format($max_increase, 4); ?>%
                                            </td>
                                            <td class="<?php echo $from_lowest >= 0 ? 'price-up' : 'price-down'; ?>">
                                                <?php echo number_format($from_lowest, 4); ?>%
                                            </td>
                                            <td class="text-center">
                                                <span class="badge bg-light text-dark"><?php echo number_format(intval($cycle['total_data_points'] ?? 0)); ?></span>
                                            </td>
                                        </tr>
                                        <?php endforeach; ?>
                                        <?php endif; ?>
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>

<?php $content = ob_get_clean(); ?>

<!-- Scripts for this page -->
<?php ob_start(); ?>

<script>
    // Auto-refresh interval (default 30 seconds)
    let autoRefreshInterval = null;
    let refreshIntervalSeconds = <?php echo $refresh_interval; ?>;
    
    function updateRefreshInterval(seconds) {
        refreshIntervalSeconds = parseInt(seconds);
        
        // Update URL parameter without reload
        const url = new URL(window.location);
        if (refreshIntervalSeconds > 0) {
            url.searchParams.set('refresh', refreshIntervalSeconds);
        } else {
            url.searchParams.delete('refresh');
        }
        window.history.replaceState({}, '', url);
        
        // Restart auto-refresh with new interval
        if (autoRefreshInterval) {
            clearInterval(autoRefreshInterval);
        }
        
        if (refreshIntervalSeconds > 0) {
            autoRefreshInterval = setInterval(refreshData, refreshIntervalSeconds * 1000);
        }
    }
    
    function refreshData() {
        const params = new URLSearchParams(window.location.search);
        params.set('ajax', 'refresh');
        
            fetch('?' + params.toString())
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        updateStats(data);
                        updateTable(data.cycles);
                        const hours = new URLSearchParams(window.location.search).get('hours') || 'all';
                        updateDataSource(data.data_source, data.actual_source, hours);
                    }
                })
            .catch(err => {
                console.error('Refresh failed:', err);
            });
    }
    
    function updateStats(data) {
        let active = 0, completed = 0, totalIncrease = 0;
        data.cycles.forEach(cycle => {
            if (!cycle.cycle_end_time) active++;
            else completed++;
            totalIncrease += parseFloat(cycle.max_percent_increase || 0);
        });
        
        // Update total count (from database, not just paginated results)
        const totalCount = data.total_count || data.count;
        document.getElementById('stat-total').textContent = totalCount.toLocaleString();
        
        // Update missing cycles
        const missingEl = document.getElementById('stat-missing');
        if (missingEl) {
            const missingCount = data.missing_cycles || 0;
            missingEl.textContent = missingCount;
            missingEl.className = 'stat-value ' + (missingCount > 0 ? 'text-warning' : 'text-muted');
        }
        
        document.getElementById('stat-active').textContent = active;
        document.getElementById('stat-completed').textContent = completed;
        
        const avg = data.count > 0 ? totalIncrease / data.count : 0;
        const avgEl = document.getElementById('stat-avg');
        avgEl.textContent = avg.toFixed(4) + '%';
        avgEl.className = 'stat-value ' + (avg >= 0 ? 'price-up' : 'price-down');
        
        // Update threshold range table
        updateThresholdRanges(data.cycles);
    }
    
    function updateThresholdRanges(cycles) {
        const thresholds = ['0.2', '0.25', '0.3', '0.35', '0.4', '0.45', '0.5'];
        const ranges = {
            '0.0 - 0.1': [0.0, 0.1],
            '0.1 - 0.2': [0.1, 0.2],
            '0.2 - 0.3': [0.2, 0.3],
            '0.3 - 0.4': [0.3, 0.4],
            '0.4 - 0.5': [0.4, 0.5],
            '0.5+': [0.5, 999]
        };
        
        const counts = {};
        Object.keys(ranges).forEach(rangeName => {
            counts[rangeName] = {};
            thresholds.forEach(th => {
                counts[rangeName][th] = 0;
            });
        });
        
        cycles.forEach(cycle => {
            let threshold = parseFloat(cycle.threshold || 0);
            const maxIncrease = parseFloat(cycle.max_percent_increase || 0);
            
            // Convert threshold to string key (handles 0.25, 0.35, 0.45)
            const thresholdKey = parseFloat(threshold.toFixed(2)).toString();
            
            // Only count if this is one of our tracked thresholds
            if (!thresholds.includes(thresholdKey)) {
                return;
            }
            
            // Find which range this cycle falls into
            for (const [rangeName, rangeBounds] of Object.entries(ranges)) {
                if (maxIncrease >= rangeBounds[0] && (rangeBounds[1] == 999 || maxIncrease < rangeBounds[1])) {
                    if (counts[rangeName] && counts[rangeName].hasOwnProperty(thresholdKey)) {
                        counts[rangeName][thresholdKey]++;
                    }
                    break;
                }
            }
        });
        
        const tbody = document.getElementById('threshold-range-body');
        if (tbody) {
            // Filter out rows with all zeros
            const rows = Object.entries(counts)
                .filter(([rangeName, rangeCounts]) => {
                    const total = thresholds.reduce((sum, th) => sum + rangeCounts[th], 0);
                    return total > 0;
                })
                .map(([rangeName, rangeCounts]) => `
                <tr>
                    <td class="fw-medium">${rangeName}%</td>
                    <td class="text-center">
                        ${rangeCounts['0.2'] > 0 ? `<span class="badge bg-primary-transparent">${rangeCounts['0.2']}</span>` : '<span class="text-muted">-</span>'}
                    </td>
                    <td class="text-center">
                        ${rangeCounts['0.25'] > 0 ? `<span class="badge bg-primary-transparent">${rangeCounts['0.25']}</span>` : '<span class="text-muted">-</span>'}
                    </td>
                    <td class="text-center">
                        ${rangeCounts['0.3'] > 0 ? `<span class="badge bg-primary-transparent">${rangeCounts['0.3']}</span>` : '<span class="text-muted">-</span>'}
                    </td>
                    <td class="text-center">
                        ${rangeCounts['0.35'] > 0 ? `<span class="badge bg-primary-transparent">${rangeCounts['0.35']}</span>` : '<span class="text-muted">-</span>'}
                    </td>
                    <td class="text-center">
                        ${rangeCounts['0.4'] > 0 ? `<span class="badge bg-primary-transparent">${rangeCounts['0.4']}</span>` : '<span class="text-muted">-</span>'}
                    </td>
                    <td class="text-center">
                        ${rangeCounts['0.45'] > 0 ? `<span class="badge bg-primary-transparent">${rangeCounts['0.45']}</span>` : '<span class="text-muted">-</span>'}
                    </td>
                    <td class="text-center">
                        ${rangeCounts['0.5'] > 0 ? `<span class="badge bg-primary-transparent">${rangeCounts['0.5']}</span>` : '<span class="text-muted">-</span>'}
                    </td>
                </tr>
            `).join('');
            tbody.innerHTML = rows;
        }
    }
    
    function updateDataSource(source, actualSource, hours) {
        const el = document.getElementById('data-source');
        if (el) {
            let text = 'Data: ' + source;
            // Show 24h max indicator when using DuckDB engine with "all" filter
            if (actualSource === 'engine' && hours === 'all') {
                text += ' <span class="text-muted ms-1" style="font-size: 0.7rem;">(24h max)</span>';
            }
            el.innerHTML = text;
        }
    }
    
    function updateTable(cycles) {
        const tbody = document.getElementById('cycles-table-body');
        
        if (!cycles || cycles.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="11" class="text-center py-4 text-muted">
                        <i class="ri-database-2-line fs-24 d-block mb-2"></i>
                        No cycles found. Waiting for data...
                    </td>
                </tr>
            `;
            return;
        }
        
        tbody.innerHTML = cycles.map(cycle => {
            const isActive = !cycle.cycle_end_time;
            const maxIncrease = parseFloat(cycle.max_percent_increase || 0);
            const fromLowest = parseFloat(cycle.max_percent_increase_from_lowest || 0);
            
            return `
                <tr>
                    <td class="fw-medium">#${cycle.id || '-'}</td>
                    <td>
                        <span class="badge bg-primary-transparent threshold-badge">
                            ${parseFloat(cycle.threshold || 0).toFixed(2)}%
                        </span>
                    </td>
                    <td>
                        ${isActive 
                            ? '<span class="badge bg-success">Active</span>' 
                            : '<span class="badge bg-secondary">Closed</span>'}
                    </td>
                    <td class="fs-12">${formatDate(cycle.cycle_start_time)}</td>
                    <td class="fs-12">${formatDate(cycle.cycle_end_time)}</td>
                    <td class="font-monospace">$${parseFloat(cycle.sequence_start_price || 0).toFixed(4)}</td>
                    <td class="font-monospace price-up">$${parseFloat(cycle.highest_price_reached || 0).toFixed(4)}</td>
                    <td class="font-monospace price-down">$${parseFloat(cycle.lowest_price_reached || 0).toFixed(4)}</td>
                    <td class="${maxIncrease >= 0 ? 'price-up' : 'price-down'}">${maxIncrease.toFixed(4)}%</td>
                    <td class="${fromLowest >= 0 ? 'price-up' : 'price-down'}">${fromLowest.toFixed(4)}%</td>
                    <td class="text-center">
                        <span class="badge bg-light text-dark">${parseInt(cycle.total_data_points || 0).toLocaleString()}</span>
                    </td>
                </tr>
            `;
        }).join('');
    }
    
    function formatDate(dateStr) {
        if (!dateStr) return '-';
        const date = new Date(dateStr);
        const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
        // Use UTC methods to display server time (UTC)
        return `${months[date.getUTCMonth()]} ${date.getUTCDate()}, ${String(date.getUTCHours()).padStart(2,'0')}:${String(date.getUTCMinutes()).padStart(2,'0')}:${String(date.getUTCSeconds()).padStart(2,'0')}`;
    }
    
    // Start auto-refresh with configured interval
    if (refreshIntervalSeconds > 0) {
        autoRefreshInterval = setInterval(refreshData, refreshIntervalSeconds * 1000);
    }
</script>

<?php $scripts = ob_get_clean(); ?>

<!-- Render using base layout -->
<?php include __DIR__ . '/../layouts/base.php'; ?>

