<?php
/**
 * SOL Price Dashboard - Candlestick Chart with Price Cycles
 * Migrated from: 000old_code/solana_node/v2/index.php
 * 
 * This page displays the 24-hour SOL price chart using DuckDB data
 * and shows price cycle analysis with configurable thresholds.
 */

// --- DuckDB API Client ---
require_once __DIR__ . '/includes/DuckDBClient.php';
define('DUCKDB_API_URL', 'http://127.0.0.1:5050');
$duckdb = new DuckDBClient(DUCKDB_API_URL);
$use_duckdb = $duckdb->isAvailable();

// --- Base URL for template ---
$baseUrl = '';  // Root of 000website

// --- Parameters ---
$price_cycle_id = $_GET['price_cycle_id'] ?? null;
$threshold = $_GET['threshold'] ?? 0.3;
$increase = $_GET['increase'] ?? 0.5;
$investment = $_GET['investment'] ?? 500;
// Candle interval in minutes (0.5 = 30 seconds, 1-10 = minutes)
$candle_interval = isset($_GET['candle_interval']) ? max(0.5, min(10, floatval($_GET['candle_interval']))) : 0.5;
$token = 'SOL';
$coin_id = 5;

// Use 24-hour interval from now
$end_datetime = date('Y-m-d H:i:s');
$start_datetime = date('Y-m-d H:i:s', strtotime('-24 hours'));

// --- Chart Data ---
$chart_data = [
    'labels' => [],
    'prices' => [],
    'candles' => [],
    'cycle_prices' => [],
    'coin_name' => 'SOL',
];

/**
 * Aggregate raw price points into OHLC candles
 * @param array $prices Array of ['x' => timestamp, 'y' => price]
 * @param float $intervalMinutes Candle interval in minutes (0.5 = 30 seconds, 1+ = minutes)
 * @return array Array of candles in ApexCharts format
 */
function aggregateToCandles(array $prices, float $intervalMinutes = 1): array {
    if (empty($prices)) {
        return [];
    }
    
    $candles = [];
    $intervalSeconds = $intervalMinutes * 60; // Handles 0.5 (30 sec) correctly
    
    // Group prices by interval
    $buckets = [];
    foreach ($prices as $point) {
        $timestamp = strtotime($point['x'] . ' UTC');
        $bucketTime = floor($timestamp / $intervalSeconds) * $intervalSeconds;
        
        if (!isset($buckets[$bucketTime])) {
            $buckets[$bucketTime] = [];
        }
        $buckets[$bucketTime][] = $point['y'];
    }
    
    // Calculate OHLC for each bucket
    ksort($buckets);
    foreach ($buckets as $bucketTime => $pricesInBucket) {
        $open = $pricesInBucket[0];
        $high = max($pricesInBucket);
        $low = min($pricesInBucket);
        $close = end($pricesInBucket);
        
        $candles[] = [
            'x' => $bucketTime * 1000, // JavaScript timestamp (milliseconds)
            'y' => [$open, $high, $low, $close]
        ];
    }
    
    return $candles;
}

// --- Fetch Price Data ---
$data_source = "No Data";
$error_message = null;

if ($use_duckdb) {
    $data_source = "DuckDB API";
    $price_response = $duckdb->getPricePoints($token, $start_datetime, $end_datetime);
    
    if ($price_response && isset($price_response['prices'])) {
        $chart_data['prices'] = $price_response['prices'];
    }
} else {
    $error_message = "DuckDB API is not available. Please start the scheduler: python scheduler/master.py";
}

// Aggregate prices into OHLC candles based on selected interval
$chart_data['candles'] = aggregateToCandles($chart_data['prices'], $candle_interval);

// --- Fetch Cycle Tracker Data ---
$analysis_data = [];
$selected_cycle = null;
$cycle_start_times = [];

if ($use_duckdb) {
    // Get cycle tracker data from API
    $cycle_response = $duckdb->getCycleTracker($threshold, '24', 100);
    
    if ($cycle_response && isset($cycle_response['cycles'])) {
        // Filter cycles by minimum increase
        foreach ($cycle_response['cycles'] as $cycle) {
            $percent_change = $cycle['max_percent_increase_from_lowest'] ?? 0;
            if ($percent_change > $increase) {
                $analysis_data[] = [
                    'price_cycle' => $cycle['id'],
                    'cycle_start_time' => $cycle['cycle_start_time'],
                    'cycle_end_time' => $cycle['cycle_end_time'],
                    'sequence_start_price' => $cycle['sequence_start_price'],
                    'highest_price_reached' => $cycle['highest_price_reached'],
                    'percent_change' => $percent_change,
                    'total_data_points' => $cycle['total_data_points'] ?? 0,
                ];
                $cycle_start_times[] = $cycle['cycle_start_time'];
            }
        }
    }
    
    // If a specific cycle is selected, get its details
    if ($price_cycle_id) {
        foreach ($analysis_data as $cycle) {
            if ($cycle['price_cycle'] == $price_cycle_id) {
                $selected_cycle = $cycle;
                break;
            }
        }
        
        // If not found in filtered data, fetch it directly
        if (!$selected_cycle) {
            $all_cycles = $duckdb->getCycleTracker(null, 'all', 1000);
            if ($all_cycles && isset($all_cycles['cycles'])) {
                foreach ($all_cycles['cycles'] as $cycle) {
                    if ($cycle['id'] == $price_cycle_id) {
                        $selected_cycle = [
                            'price_cycle' => $cycle['id'],
                            'cycle_start_time' => $cycle['cycle_start_time'],
                            'cycle_end_time' => $cycle['cycle_end_time'],
                            'sequence_start_price' => $cycle['sequence_start_price'],
                            'highest_price_reached' => $cycle['highest_price_reached'],
                            'percent_change' => $cycle['max_percent_increase_from_lowest'] ?? 0,
                        ];
                        break;
                    }
                }
            }
        }
    }
}

// --- Status Data ---
$status_data = [
    'api_status' => $use_duckdb ? 'connected' : 'disconnected',
    'last_price_time' => null,
    'price_count' => count($chart_data['prices']),
    'active_cycle' => null,
    'price_analysis' => null,
];

if ($use_duckdb && !empty($chart_data['prices'])) {
    $last_price = end($chart_data['prices']);
    $status_data['last_price_time'] = $last_price['x'];
}

// Get latest price analysis time
if ($use_duckdb) {
    $analysis_response = $duckdb->getPriceAnalysis(5, '1', 1);
    if ($analysis_response && isset($analysis_response['price_analysis']) && !empty($analysis_response['price_analysis'])) {
        $status_data['price_analysis'] = $analysis_response['price_analysis'][0]['created_at'] ?? null;
    }
    
    // Get active cycle (most recent for threshold 0.3)
    $active_cycle_response = $duckdb->getCycleTracker(0.3, '24', 1);
    if ($active_cycle_response && isset($active_cycle_response['cycles']) && !empty($active_cycle_response['cycles'])) {
        $status_data['active_cycle'] = $active_cycle_response['cycles'][0]['cycle_start_time'] ?? null;
    }
}

// --- Scheduler Status Data ---
$scheduler_status = null;
$scheduler_jobs = [];
if ($use_duckdb) {
    $scheduler_status = $duckdb->getSchedulerStatus();
    if ($scheduler_status && isset($scheduler_status['jobs'])) {
        $scheduler_jobs = $scheduler_status['jobs'];
    }
}

$json_chart_data = json_encode($chart_data);
$json_status_data = json_encode($status_data);
$json_cycle_start_times = json_encode($cycle_start_times);
$json_selected_cycle = json_encode($selected_cycle);
$json_scheduler_jobs = json_encode($scheduler_jobs);
// Validate scheduler_started is a valid date string before JSON encoding
$scheduler_started_raw = $scheduler_status['scheduler_started'] ?? null;
$json_scheduler_started = 'null';
if ($scheduler_started_raw && is_string($scheduler_started_raw) && preg_match('/\d{4}[-\/]\d{2}[-\/]\d{2}/', $scheduler_started_raw)) {
    $json_scheduler_started = json_encode($scheduler_started_raw);
}

?>

<!-- This code is useful for internal styles -->
<?php ob_start(); ?>

<style>
    .status-grid {
        display: grid;
        grid-template-columns: repeat(4, 1fr);
        gap: 0.75rem;
    }
    @media (max-width: 992px) {
        .status-grid {
            grid-template-columns: repeat(2, 1fr);
        }
    }
    @media (max-width: 576px) {
        .status-grid {
            grid-template-columns: 1fr;
        }
    }
    .status-btn {
        padding: 0.6rem 0.75rem;
        border-radius: 0.5rem;
        border: 1px solid rgba(255,255,255,0.1);
        background: rgba(var(--body-bg-rgb2), 1);
        cursor: pointer;
        transition: all 0.2s ease;
        text-align: center;
    }
    .status-btn:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(0,0,0,0.3);
    }
    .status-btn.status-good {
        border-color: rgb(var(--success-rgb));
        background: rgba(var(--success-rgb), 0.1);
    }
    .status-btn.status-warning {
        border-color: rgb(var(--warning-rgb));
        background: rgba(var(--warning-rgb), 0.1);
    }
    .status-btn.status-bad {
        border-color: rgb(var(--danger-rgb));
        background: rgba(var(--danger-rgb), 0.1);
    }
    .status-btn.status-unknown {
        border-color: rgba(255,255,255,0.2);
    }
    .status-title {
        font-size: 0.7rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 0.25rem;
        color: rgba(255,255,255,0.7);
    }
    .status-info {
        font-size: 0.75rem;
        color: rgba(255,255,255,0.9);
        font-weight: 500;
    }
    #sol-candlestick-chart {
        min-height: 450px;
    }
    .data-source-badge {
        position: fixed;
        top: 70px;
        right: 20px;
        z-index: 9999;
        padding: 4px 12px;
        border-radius: 4px;
        font-size: 11px;
        font-weight: 600;
    }
    .link-cycle {
        color: rgb(var(--primary-rgb));
        text-decoration: none;
        font-weight: 600;
    }
    .link-cycle:hover {
        text-decoration: underline;
    }
    
    /* Scheduler Job Status Styles */
    .job-status-grid {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
        gap: 0.75rem;
    }
    .job-card {
        padding: 0.875rem;
        border-radius: 0.5rem;
        border: 1px solid rgba(255,255,255,0.1);
        background: rgba(var(--body-bg-rgb2), 1);
        transition: all 0.2s ease;
    }
    .job-card:hover {
        transform: translateY(-1px);
        box-shadow: 0 2px 8px rgba(0,0,0,0.2);
    }
    .job-card.job-running {
        border-left: 3px solid rgb(var(--info-rgb));
    }
    .job-card.job-success {
        border-left: 3px solid rgb(var(--success-rgb));
    }
    .job-card.job-error {
        border-left: 3px solid rgb(var(--danger-rgb));
    }
    .job-card.job-stale {
        border-left: 3px solid rgb(var(--warning-rgb));
    }
    .job-card.job-service {
        border-left: 3px solid rgb(var(--primary-rgb));
    }
    .job-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: 0.5rem;
    }
    .job-name {
        font-weight: 600;
        font-size: 0.8rem;
        color: rgba(255,255,255,0.9);
    }
    .job-status-badge {
        font-size: 0.65rem;
        padding: 0.2rem 0.5rem;
        border-radius: 3px;
        font-weight: 600;
        text-transform: uppercase;
    }
    .job-desc {
        font-size: 0.7rem;
        color: rgba(255,255,255,0.5);
        margin-bottom: 0.5rem;
    }
    .job-meta {
        display: flex;
        flex-wrap: wrap;
        gap: 0.75rem;
        font-size: 0.7rem;
    }
    .job-meta-item {
        display: flex;
        align-items: center;
        gap: 0.25rem;
    }
    .job-meta-label {
        color: rgba(255,255,255,0.5);
    }
    .job-meta-value {
        color: rgba(255,255,255,0.8);
        font-weight: 500;
    }
    .job-meta-value.fresh {
        color: rgb(var(--success-rgb));
    }
    .job-meta-value.stale {
        color: rgb(var(--warning-rgb));
    }
    .job-meta-value.old {
        color: rgb(var(--danger-rgb));
    }
    .scheduler-uptime {
        font-size: 0.75rem;
        color: rgba(255,255,255,0.6);
    }
</style>

<?php $styles = ob_get_clean(); ?>
<!-- This code is useful for internal styles -->

<!-- This code is useful for content -->
<?php ob_start(); ?>

                    <!-- Start::page-header -->
                    <div class="page-header-breadcrumb mb-3">
                        <div class="d-flex align-center justify-content-between flex-wrap">
                            <h1 class="page-title fw-medium fs-18 mb-0">SOL Price Dashboard</h1>
                            <ol class="breadcrumb mb-0">
                                <li class="breadcrumb-item"><a href="javascript:void(0);">Dashboards</a></li>
                                <li class="breadcrumb-item active" aria-current="page">SOL Candlestick</li>
                            </ol>
                        </div>
                    </div>
                    <!-- End::page-header -->

                    <!-- Data Source Badge -->
                    <div class="data-source-badge" style="background: <?php echo $use_duckdb ? 'rgb(var(--success-rgb))' : 'rgb(var(--danger-rgb))'; ?>; color: white;">
                        🦆 <?php echo $data_source; ?>
                    </div>

                    <?php if ($error_message): ?>
                    <!-- API Error Alert -->
                    <div class="alert alert-danger mb-3">
                        <div class="d-flex align-items-center">
                            <i class="ti ti-alert-circle fs-4 me-2"></i>
                            <div>
                                <h6 class="mb-0">API Connection Error</h6>
                                <p class="mb-0"><?php echo htmlspecialchars($error_message); ?></p>
                            </div>
                        </div>
                    </div>
                    <?php endif; ?>

                    <!-- Start:: Filters Card -->
                    <div class="row mb-3">
                        <div class="col-xl-12">
                            <div class="card custom-card">
                                <div class="card-header">
                                    <div class="card-title">Filters</div>
                                </div>
                                <div class="card-body">
                                    <form method="get" class="row g-3 align-items-end">
                                        <div class="col-auto">
                                            <span class="badge bg-info-transparent fs-14 py-2 px-3">
                                                <i class="ti ti-clock me-1"></i> Last 24 Hours
                                            </span>
                                        </div>
                                        <div class="col-xl-2 col-lg-3 col-md-4">
                                            <label for="threshold" class="form-label">Threshold</label>
                                            <select id="threshold" name="threshold" class="form-select">
                                                <option value="0.1" <?php echo $threshold == 0.1 ? 'selected' : ''; ?>>0.1%</option>
                                                <option value="0.2" <?php echo $threshold == 0.2 ? 'selected' : ''; ?>>0.2%</option>
                                                <option value="0.3" <?php echo $threshold == 0.3 ? 'selected' : ''; ?>>0.3%</option>
                                                <option value="0.4" <?php echo $threshold == 0.4 ? 'selected' : ''; ?>>0.4%</option>
                                                <option value="0.5" <?php echo $threshold == 0.5 ? 'selected' : ''; ?>>0.5%</option>
                                            </select>
                                        </div>
                                        <div class="col-xl-2 col-lg-3 col-md-4">
                                            <label for="increase" class="form-label">Min Increase %</label>
                                            <input type="number" id="increase" name="increase" class="form-control" value="<?php echo htmlspecialchars($increase); ?>" min="0" step="0.1">
                                        </div>
                                        <div class="col-xl-2 col-lg-3 col-md-4">
                                            <label for="candle_interval" class="form-label">Candle Grouping</label>
                                            <select id="candle_interval" name="candle_interval" class="form-select">
                                                <option value="0.5" <?php echo $candle_interval == 0.5 ? 'selected' : ''; ?>>30 sec</option>
                                                <?php for ($i = 1; $i <= 10; $i++): ?>
                                                <option value="<?php echo $i; ?>" <?php echo abs($candle_interval - $i) < 0.01 ? 'selected' : ''; ?>><?php echo $i; ?> min</option>
                                                <?php endfor; ?>
                                            </select>
                                        </div>
                                        <div class="col-xl-2 col-lg-2 col-md-4">
                                            <button type="submit" class="btn btn-primary w-100">
                                                <i class="ti ti-filter me-1"></i> Apply Filters
                                            </button>
                                        </div>
                                        <input type="hidden" name="investment" value="<?php echo htmlspecialchars($investment); ?>">
                                    </form>
                                </div>
                            </div>
                        </div>
                    </div>
                    <!-- End:: Filters Card -->

                    <!-- Start:: Status Buttons -->
                    <div class="status-grid mb-3">
                        <div id="apiStatusBtn" class="status-btn <?php echo $use_duckdb ? 'status-good' : 'status-bad'; ?>">
                            <div class="status-title">API Status</div>
                            <div class="status-info"><?php echo $use_duckdb ? 'Connected' : 'Disconnected'; ?></div>
                        </div>
                        <div id="priceDataBtn" class="status-btn <?php echo count($chart_data['prices']) > 0 ? 'status-good' : 'status-warning'; ?>">
                            <div class="status-title">Price Data</div>
                            <div class="status-info"><?php echo number_format(count($chart_data['prices'])); ?> points</div>
                        </div>
                        <div id="priceAnalysisBtn" class="status-btn status-unknown" onclick="location.reload()">
                            <div class="status-title">Price Analysis</div>
                            <div class="status-info" id="priceAnalysisInfo">Loading...</div>
                        </div>
                        <div id="activeCycleBtn" class="status-btn status-unknown" onclick="location.reload()">
                            <div class="status-title">Active Cycle</div>
                            <div class="status-info" id="activeCycleInfo">Loading...</div>
                        </div>
                    </div>
                    <!-- End:: Status Buttons -->

                    <!-- Start:: Candlestick Chart -->
                    <div class="row mb-3">
                        <div class="col-xl-12">
                            <div class="card custom-card">
                                <div class="card-header">
                                    <div class="card-title">
                                        SOL Price Chart - Candlestick (<?php echo $candle_interval == 0.5 ? '30 sec' : $candle_interval . ' min'; ?>)
                                    </div>
                                    <div class="ms-auto">
                                        <span class="badge bg-primary-transparent"><?php echo count($chart_data['candles']); ?> candles</span>
                                        <span class="badge bg-secondary-transparent"><?php echo count($chart_data['prices']); ?> price points</span>
                                    </div>
                                </div>
                                <div class="card-body">
                                    <?php if (!empty($chart_data['candles'])): ?>
                                    <?php if ($selected_cycle): ?>
                                    <!-- Selected Cycle Info Banner -->
                                    <div class="alert alert-primary d-flex align-items-center gap-3 mb-3" role="alert">
                                        <i class="ti ti-chart-line fs-3"></i>
                                        <div class="flex-fill">
                                            <h6 class="mb-1">Viewing Cycle #<?php echo htmlspecialchars($price_cycle_id); ?></h6>
                                            <div class="d-flex flex-wrap gap-3 fs-13">
                                                <span><strong>Start:</strong> <?php echo date('M d, H:i:s', strtotime($selected_cycle['cycle_start_time'])); ?></span>
                                                <?php if ($selected_cycle['cycle_end_time']): ?>
                                                <span><strong>End:</strong> <?php echo date('H:i:s', strtotime($selected_cycle['cycle_end_time'])); ?></span>
                                                <?php else: ?>
                                                <span><strong>Status:</strong> <span class="badge bg-success">Active</span></span>
                                                <?php endif; ?>
                                                <span><strong>Change:</strong> <span class="text-success fw-semibold">+<?php echo number_format($selected_cycle['percent_change'], 2); ?>%</span></span>
                                                <span><strong>Start Price:</strong> $<?php echo number_format($selected_cycle['sequence_start_price'], 4); ?></span>
                                                <span><strong>High:</strong> $<?php echo number_format($selected_cycle['highest_price_reached'], 4); ?></span>
                                            </div>
                                        </div>
                                        <a href="?threshold=<?php echo $threshold; ?>&increase=<?php echo $increase; ?>&candle_interval=<?php echo $candle_interval; ?>" class="btn btn-sm btn-outline-light">
                                            <i class="ti ti-x me-1"></i>Clear
                                        </a>
                                    </div>
                                    <?php endif; ?>
                                    <!-- Price Info Bar -->
                                    <div class="p-3 d-flex align-items-center gap-4 border border-dashed rounded flex-wrap mb-3">
                                        <div class="d-flex align-items-center gap-2 flex-wrap">
                                            <span class="avatar avatar-sm bg-primary-transparent">
                                                <i class="ti ti-currency-solana fs-16"></i>
                                            </span>
                                            <div>
                                                <span class="fw-medium">Solana</span> - <span class="text-muted">SOL</span>
                                            </div>
                                        </div>
                                        <?php 
                                        $lastCandle = end($chart_data['candles']);
                                        $firstCandle = reset($chart_data['candles']);
                                        if ($lastCandle && $firstCandle):
                                            $currentPrice = $lastCandle['y'][3]; // close
                                            $openPrice = $firstCandle['y'][0]; // open
                                            $dayChange = (($currentPrice - $openPrice) / $openPrice) * 100;
                                            $changeClass = $dayChange >= 0 ? 'text-success' : 'text-danger';
                                            $changeIcon = $dayChange >= 0 ? 'ti-trending-up' : 'ti-trending-down';
                                        ?>
                                        <h6 class="fw-medium mb-0">
                                            $<?php echo number_format($currentPrice, 4); ?> USD
                                            <span class="<?php echo $changeClass; ?> mx-2">
                                                <i class="ti <?php echo $changeIcon; ?> me-1"></i><?php echo number_format(abs($dayChange), 2); ?>%
                                            </span>
                                            (Day)
                                        </h6>
                                        <div class="d-flex gap-4 align-items-center flex-wrap">
                                            <div>Open - <span class="text-success fw-medium">$<?php echo number_format($lastCandle['y'][0], 4); ?></span></div>
                                            <div>High - <span class="text-success fw-medium">$<?php echo number_format($lastCandle['y'][1], 4); ?></span></div>
                                            <div>Low - <span class="text-danger fw-medium">$<?php echo number_format($lastCandle['y'][2], 4); ?></span></div>
                                            <div>Close - <span class="text-success fw-medium">$<?php echo number_format($lastCandle['y'][3], 4); ?></span></div>
                                        </div>
                                        <?php endif; ?>
                                    </div>
                                    <div id="sol-candlestick-chart"></div>
                                    <?php else: ?>
                                    <div class="text-center py-5">
                                        <i class="ti ti-chart-candle fs-1 text-muted mb-3 d-block"></i>
                                        <h5 class="text-muted">No price data available</h5>
                                        <p class="text-muted">Make sure the scheduler is running to collect price data.</p>
                                        <code class="d-block mt-3">python scheduler/master.py</code>
                                    </div>
                                    <?php endif; ?>
                                </div>
                            </div>
                        </div>
                    </div>
                    <!-- End:: Candlestick Chart -->

                    <!-- Start:: Scheduler Job Status -->
                    <div class="row mb-3">
                        <div class="col-xl-12">
                            <div class="card custom-card">
                                <div class="card-header">
                                    <div class="card-title">
                                        <i class="ti ti-clock-play me-2"></i>Scheduler Jobs Status
                                    </div>
                                    <div class="ms-auto d-flex gap-2 align-items-center">
                                        <span class="scheduler-uptime" id="schedulerUptime">-</span>
                                        <button class="btn btn-sm btn-outline-light" onclick="refreshSchedulerStatus()" title="Refresh">
                                            <i class="ti ti-refresh"></i>
                                        </button>
                                    </div>
                                </div>
                                <div class="card-body">
                                    <?php if (empty($scheduler_jobs)): ?>
                                    <div class="text-center py-4">
                                        <i class="ti ti-clock-off fs-1 text-muted mb-3 d-block"></i>
                                        <h6 class="text-muted">No scheduler data available</h6>
                                        <p class="text-muted fs-13">Start the scheduler to see job status: <code>python scheduler/master.py</code></p>
                                    </div>
                                    <?php else: ?>
                                    <div class="job-status-grid" id="jobStatusGrid">
                                        <?php foreach ($scheduler_jobs as $job_id => $job): ?>
                                        <div class="job-card job-<?php echo htmlspecialchars($job['status'] ?? 'unknown'); ?><?php echo !empty($job['is_service']) || !empty($job['is_stream']) ? ' job-service' : ''; ?>" data-job-id="<?php echo htmlspecialchars($job_id); ?>">
                                            <div class="job-header">
                                                <span class="job-name"><?php echo htmlspecialchars($job_id); ?></span>
                                                <span class="job-status-badge bg-<?php 
                                                    $status = $job['status'] ?? 'unknown';
                                                    echo match($status) {
                                                        'running' => 'info',
                                                        'success' => 'success',
                                                        'error' => 'danger',
                                                        'stopped' => 'secondary',
                                                        default => 'secondary'
                                                    };
                                                ?>-transparent"><?php echo htmlspecialchars($status); ?></span>
                                            </div>
                                            <div class="job-desc"><?php echo htmlspecialchars($job['description'] ?? ''); ?></div>
                                            <div class="job-meta">
                                                <?php if (!empty($job['last_success'])): ?>
                                                <div class="job-meta-item">
                                                    <span class="job-meta-label">Last OK:</span>
                                                    <span class="job-meta-value job-time" data-time="<?php echo htmlspecialchars($job['last_success']); ?>">-</span>
                                                </div>
                                                <?php endif; ?>
                                                <?php if (!empty($job['run_count'])): ?>
                                                <div class="job-meta-item">
                                                    <span class="job-meta-label">Runs:</span>
                                                    <span class="job-meta-value"><?php echo number_format($job['run_count']); ?></span>
                                                </div>
                                                <?php endif; ?>
                                                <?php if (!empty($job['error_message'])): ?>
                                                <div class="job-meta-item" style="flex-basis: 100%;">
                                                    <span class="job-meta-label">Error:</span>
                                                    <span class="job-meta-value text-danger"><?php echo htmlspecialchars(substr($job['error_message'], 0, 50)); ?></span>
                                                </div>
                                                <?php endif; ?>
                                            </div>
                                        </div>
                                        <?php endforeach; ?>
                                    </div>
                                    <?php endif; ?>
                                </div>
                            </div>
                        </div>
                    </div>
                    <!-- End:: Scheduler Job Status -->

                    <!-- Start:: Cycle Tracker Table -->
                    <?php if (!empty($analysis_data)): ?>
                    <div class="row">
                        <div class="col-xl-12">
                            <div class="card custom-card">
                                <div class="card-header">
                                    <div class="card-title">
                                        <i class="ti ti-chart-arrows me-2"></i>Price Cycle Results
                                    </div>
                                    <div class="ms-auto d-flex gap-3">
                                        <span class="badge bg-info-transparent">Threshold: <?php echo htmlspecialchars($threshold); ?>%</span>
                                        <span class="badge bg-info-transparent"><i class="ti ti-clock me-1"></i>Last 24 Hours</span>
                                        <span class="badge bg-success-transparent"><?php echo count($analysis_data); ?> cycles with ><?php echo htmlspecialchars($increase); ?>% increase</span>
                                    </div>
                                </div>
                                <div class="card-body">
                                    <div class="table-responsive">
                                        <table class="table table-bordered text-nowrap">
                                            <thead>
                                                <tr>
                                                    <th>Cycle ID</th>
                                                    <th>Start Price</th>
                                                    <th>Highest Price</th>
                                                    <th>Percent Change</th>
                                                    <th>Start Time</th>
                                                    <th>End Time</th>
                                                    <th>Duration</th>
                                                    <th>Investment Value</th>
                                                </tr>
                                            </thead>
                                            <tbody>
                                                <?php 
                                                $current_investment = $investment;
                                                foreach ($analysis_data as $index => $row): 
                                                ?>
                                                <tr>
                                                    <td>
                                                        <a href="?price_cycle_id=<?php echo htmlspecialchars($row['price_cycle']); ?>&threshold=<?php echo htmlspecialchars($threshold); ?>&increase=<?php echo htmlspecialchars($increase); ?>&investment=<?php echo htmlspecialchars($investment); ?>&candle_interval=<?php echo htmlspecialchars($candle_interval); ?>" class="link-cycle">
                                                            #<?php echo htmlspecialchars($row['price_cycle']); ?>
                                                        </a>
                                                    </td>
                                                    <td>$<?php echo number_format($row['sequence_start_price'], 4); ?></td>
                                                    <td>$<?php echo number_format($row['highest_price_reached'], 4); ?></td>
                                                    <td>
                                                        <span class="badge bg-success">+<?php echo number_format($row['percent_change'], 2); ?>%</span>
                                                    </td>
                                                    <td><?php echo date('M d, H:i:s', strtotime($row['cycle_start_time'])); ?></td>
                                                    <td>
                                                        <?php if ($row['cycle_end_time']): ?>
                                                        <?php echo date('H:i:s', strtotime($row['cycle_end_time'])); ?>
                                                        <?php else: ?>
                                                        <span class="badge bg-warning text-dark">Active</span>
                                                        <?php endif; ?>
                                                    </td>
                                                    <td>
                                                        <?php if ($row['cycle_end_time']): ?>
                                                        <span class="badge bg-info">
                                                            <?php 
                                                            $start_ts = strtotime($row['cycle_start_time']);
                                                            $end_ts = strtotime($row['cycle_end_time']);
                                                            echo round(($end_ts - $start_ts) / 60) . ' min';
                                                            ?>
                                                        </span>
                                                        <?php else: ?>
                                                        <span class="badge bg-secondary">Ongoing</span>
                                                        <?php endif; ?>
                                                    </td>
                                                    <td>
                                                        <span class="badge bg-warning text-dark">
                                                            $<?php 
                                                            $gain_multiplier = 1 + ($row['percent_change'] / 100);
                                                            $new_investment = $current_investment * $gain_multiplier;
                                                            echo number_format($new_investment, 2);
                                                            $current_investment = $new_investment;
                                                            ?>
                                                        </span>
                                                    </td>
                                                </tr>
                                                <?php endforeach; ?>
                                            </tbody>
                                        </table>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                    <?php else: ?>
                    <div class="row">
                        <div class="col-xl-12">
                            <div class="alert alert-info">
                                <div class="d-flex align-items-center">
                                    <i class="ti ti-info-circle fs-4 me-2"></i>
                                    <div>
                                        <h6 class="mb-0">No Cycles Found</h6>
                                        <p class="mb-0">No price cycles found with ><?php echo htmlspecialchars($increase); ?>% increase for threshold <?php echo htmlspecialchars($threshold); ?>% in the last 24 hours.</p>
                                        <?php if (!$use_duckdb): ?>
                                        <p class="mb-0 mt-2"><strong>Tip:</strong> Make sure the scheduler is running: <code>python scheduler/master.py</code></p>
                                        <?php endif; ?>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                    <?php endif; ?>
                    <!-- End:: Cycle Tracker Table -->

<?php $content = ob_get_clean(); ?>
<!-- This code is useful for content -->

<!-- This code is useful for internal scripts -->
<?php ob_start(); ?>

        <!-- Apex Charts JS -->
        <script src="<?php echo $baseUrl; ?>/assets/libs/apexcharts/apexcharts.min.js"></script>

        <script>
            // Status data from PHP
            window.statusData = <?php echo $json_status_data; ?>;
            window.chartData = <?php echo $json_chart_data; ?>;
            window.cycleStartTimes = <?php echo $json_cycle_start_times; ?>;
            window.selectedCycle = <?php echo $json_selected_cycle; ?>;
            window.schedulerJobs = <?php echo $json_scheduler_jobs; ?>;
            window.schedulerStarted = <?php echo $json_scheduler_started; ?>;

            // Update status buttons based on timestamp freshness
            function updateStatusButtons() {
                const now = new Date();
                const statusData = window.statusData;

                function updateButton(btnId, infoId, timestamp) {
                    const btn = document.getElementById(btnId);
                    const info = document.getElementById(infoId);
                    
                    if (!btn || !info) return;
                    
                    if (!timestamp) {
                        btn.className = 'status-btn status-warning';
                        info.textContent = 'No data';
                        return;
                    }

                    const dataTime = new Date(timestamp + ' UTC');
                    const diffSeconds = Math.floor((now - dataTime) / 1000);
                    
                    let statusClass = 'status-good';
                    if (diffSeconds > 60) statusClass = 'status-warning';
                    if (diffSeconds > 300) statusClass = 'status-bad';

                    btn.className = 'status-btn ' + statusClass;

                    // Format time display
                    if (diffSeconds < 60) {
                        info.textContent = diffSeconds + 's ago';
                    } else if (diffSeconds < 3600) {
                        info.textContent = Math.floor(diffSeconds / 60) + 'm ago';
                    } else {
                        info.textContent = Math.floor(diffSeconds / 3600) + 'h ago';
                    }
                }

                updateButton('priceAnalysisBtn', 'priceAnalysisInfo', statusData.price_analysis);
                updateButton('activeCycleBtn', 'activeCycleInfo', statusData.active_cycle);
            }

            // Initialize on page load
            document.addEventListener('DOMContentLoaded', function() {
                updateStatusButtons();
                setInterval(updateStatusButtons, 5000);
            });

            // Candlestick Chart
            document.addEventListener('DOMContentLoaded', function() {
                const chartData = window.chartData;
                const cycleStartTimes = window.cycleStartTimes;
                const selectedCycle = window.selectedCycle;

                if (!chartData.candles || chartData.candles.length === 0) {
                    return;
                }

                // Build annotations for cycle start times
                const xAxisAnnotations = cycleStartTimes.map(function(timestamp, index) {
                    const timeValue = new Date(timestamp + ' UTC').getTime();
                    return {
                        x: timeValue,
                        borderColor: '#50cd89',
                        strokeDashArray: 0,
                        label: {
                            borderColor: '#50cd89',
                            style: {
                                color: '#fff',
                                background: '#50cd89',
                                fontSize: '10px'
                            },
                            text: 'Cycle ' + (index + 1)
                        }
                    };
                });

                // Add highlighted region for selected cycle
                if (selectedCycle && selectedCycle.cycle_start_time) {
                    const cycleStart = new Date(selectedCycle.cycle_start_time + ' UTC').getTime();
                    const cycleEnd = selectedCycle.cycle_end_time 
                        ? new Date(selectedCycle.cycle_end_time + ' UTC').getTime()
                        : new Date().getTime();
                    
                    // Add the highlighted region annotation
                    xAxisAnnotations.push({
                        x: cycleStart,
                        x2: cycleEnd,
                        fillColor: 'rgba(16, 185, 129, 0.15)',
                        borderColor: 'rgb(16, 185, 129)',
                        strokeDashArray: 0,
                        label: {
                            borderColor: 'rgb(16, 185, 129)',
                            style: {
                                color: '#fff',
                                background: 'rgb(16, 185, 129)',
                                fontSize: '11px',
                                fontWeight: 600
                            },
                            text: 'Selected Cycle (+' + parseFloat(selectedCycle.percent_change).toFixed(2) + '%)',
                            position: 'top'
                        }
                    });
                    
                    // Add vertical lines at start and end
                    xAxisAnnotations.push({
                        x: cycleStart,
                        borderColor: 'rgb(16, 185, 129)',
                        borderWidth: 2,
                        strokeDashArray: 0,
                        label: {
                            borderColor: 'rgb(16, 185, 129)',
                            style: {
                                color: '#fff',
                                background: 'rgb(16, 185, 129)',
                                fontSize: '10px'
                            },
                            text: 'START',
                            position: 'bottom'
                        }
                    });
                    
                    if (selectedCycle.cycle_end_time) {
                        xAxisAnnotations.push({
                            x: cycleEnd,
                            borderColor: 'rgb(239, 68, 68)',
                            borderWidth: 2,
                            strokeDashArray: 0,
                            label: {
                                borderColor: 'rgb(239, 68, 68)',
                                style: {
                                    color: '#fff',
                                    background: 'rgb(239, 68, 68)',
                                    fontSize: '10px'
                                },
                                text: 'END',
                                position: 'bottom'
                            }
                        });
                    }
                }

                var options = {
                    series: [{
                        name: 'SOL Price',
                        data: chartData.candles
                    }],
                    chart: {
                        type: 'candlestick',
                        height: 450,
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
                        animations: {
                            enabled: true,
                            speed: 500
                        }
                    },
                    tooltip: {
                        enabled: true,
                        theme: 'dark',
                        custom: function({ seriesIndex, dataPointIndex, w }) {
                            const o = w.globals.seriesCandleO[seriesIndex][dataPointIndex];
                            const h = w.globals.seriesCandleH[seriesIndex][dataPointIndex];
                            const l = w.globals.seriesCandleL[seriesIndex][dataPointIndex];
                            const c = w.globals.seriesCandleC[seriesIndex][dataPointIndex];
                            const timestamp = new Date(w.globals.seriesX[seriesIndex][dataPointIndex]);
                            
                            return '<div class="apexcharts-tooltip-candlestick p-2">' +
                                '<div class="mb-1"><strong>' + timestamp.toLocaleTimeString() + '</strong></div>' +
                                '<div>Open: <span class="text-success">$' + o.toFixed(4) + '</span></div>' +
                                '<div>High: <span class="text-success">$' + h.toFixed(4) + '</span></div>' +
                                '<div>Low: <span class="text-danger">$' + l.toFixed(4) + '</span></div>' +
                                '<div>Close: <span class="text-info">$' + c.toFixed(4) + '</span></div>' +
                                '</div>';
                        }
                    },
                    plotOptions: {
                        candlestick: {
                            colors: {
                                upward: 'rgb(50, 212, 132)',
                                downward: 'rgb(255, 103, 87)'
                            },
                            wick: {
                                useFillColor: true
                            }
                        }
                    },
                    grid: {
                        borderColor: 'rgba(255,255,255,0.1)',
                        strokeDashArray: 3,
                    },
                    xaxis: {
                        type: 'datetime',
                        labels: {
                            datetimeUTC: false,
                            style: {
                                colors: 'rgb(161, 165, 183)',
                                fontSize: '11px'
                            },
                            datetimeFormatter: {
                                hour: 'HH:mm'
                            }
                        },
                        axisBorder: {
                            color: 'rgba(255,255,255,0.1)'
                        },
                        axisTicks: {
                            color: 'rgba(255,255,255,0.1)'
                        }
                    },
                    yaxis: {
                        tooltip: {
                            enabled: true
                        },
                        labels: {
                            style: {
                                colors: 'rgb(161, 165, 183)',
                                fontSize: '11px'
                            },
                            formatter: function(val) {
                                return '$' + val.toFixed(4);
                            }
                        }
                    },
                    annotations: {
                        xaxis: xAxisAnnotations
                    }
                };

                var chart = new ApexCharts(document.querySelector("#sol-candlestick-chart"), options);
                chart.render();
            });

            // =============================================================
            // SCHEDULER STATUS FUNCTIONS
            // =============================================================
            
            /**
             * Format a timestamp as relative time (e.g., "5s ago", "2m ago")
             * All timestamps are treated as UTC (server time).
             */
            function formatRelativeTime(isoTimestamp) {
                if (!isoTimestamp) return '-';
                
                // Get current UTC time
                const now = new Date();
                const nowUTC = Date.UTC(
                    now.getUTCFullYear(),
                    now.getUTCMonth(),
                    now.getUTCDate(),
                    now.getUTCHours(),
                    now.getUTCMinutes(),
                    now.getUTCSeconds()
                );
                
                // Parse server timestamp as UTC
                let dataTimeUTC;
                if (isoTimestamp.includes('T')) {
                    // ISO format - ensure it's treated as UTC
                    if (isoTimestamp.endsWith('Z')) {
                        dataTimeUTC = new Date(isoTimestamp).getTime();
                    } else if (isoTimestamp.includes('+') || isoTimestamp.slice(-6).match(/[+-]\d{2}:\d{2}/)) {
                        // Has timezone offset
                        dataTimeUTC = new Date(isoTimestamp).getTime();
                    } else {
                        // No timezone - treat as UTC
                        dataTimeUTC = new Date(isoTimestamp + 'Z').getTime();
                    }
                } else {
                    // "YYYY-MM-DD HH:MM:SS" format - treat as UTC
                    dataTimeUTC = new Date(isoTimestamp.replace(' ', 'T') + 'Z').getTime();
                }
                
                const diffSeconds = Math.floor((nowUTC - dataTimeUTC) / 1000);
                
                if (diffSeconds < 0) return 'now';
                if (diffSeconds < 60) return diffSeconds + 's ago';
                if (diffSeconds < 3600) return Math.floor(diffSeconds / 60) + 'm ago';
                if (diffSeconds < 86400) return Math.floor(diffSeconds / 3600) + 'h ago';
                return Math.floor(diffSeconds / 86400) + 'd ago';
            }
            
            /**
             * Get freshness class based on time difference
             * All timestamps are treated as UTC (server time).
             */
            function getFreshnessClass(isoTimestamp, thresholdSeconds = 30) {
                if (!isoTimestamp) return '';
                
                // Get current UTC time
                const now = new Date();
                const nowUTC = Date.UTC(
                    now.getUTCFullYear(),
                    now.getUTCMonth(),
                    now.getUTCDate(),
                    now.getUTCHours(),
                    now.getUTCMinutes(),
                    now.getUTCSeconds()
                );
                
                // Parse server timestamp as UTC
                let dataTimeUTC;
                if (isoTimestamp.includes('T')) {
                    // ISO format - ensure it's treated as UTC
                    if (isoTimestamp.endsWith('Z')) {
                        dataTimeUTC = new Date(isoTimestamp).getTime();
                    } else if (isoTimestamp.includes('+') || isoTimestamp.slice(-6).match(/[+-]\d{2}:\d{2}/)) {
                        // Has timezone offset
                        dataTimeUTC = new Date(isoTimestamp).getTime();
                    } else {
                        // No timezone - treat as UTC
                        dataTimeUTC = new Date(isoTimestamp + 'Z').getTime();
                    }
                } else {
                    // "YYYY-MM-DD HH:MM:SS" format - treat as UTC
                    dataTimeUTC = new Date(isoTimestamp.replace(' ', 'T') + 'Z').getTime();
                }
                
                const diffSeconds = Math.floor((nowUTC - dataTimeUTC) / 1000);
                
                if (diffSeconds <= thresholdSeconds) return 'fresh';
                if (diffSeconds <= thresholdSeconds * 3) return 'stale';
                return 'old';
            }
            
            /**
             * Update all job time displays
             */
            function updateJobTimes() {
                document.querySelectorAll('.job-time').forEach(function(el) {
                    const timestamp = el.dataset.time;
                    el.textContent = formatRelativeTime(timestamp);
                    
                    // Update freshness class
                    el.classList.remove('fresh', 'stale', 'old');
                    const freshness = getFreshnessClass(timestamp);
                    if (freshness) el.classList.add(freshness);
                });
                
                // Update scheduler uptime (all times in UTC)
                const uptimeEl = document.getElementById('schedulerUptime');
                if (uptimeEl) {
                    if (window.schedulerStarted && typeof window.schedulerStarted === 'string' && window.schedulerStarted.length > 0) {
                        try {
                            // Parse scheduler start time as UTC
                            let startedUTC;
                            const startedStr = window.schedulerStarted;
                            if (startedStr.includes('T')) {
                                startedUTC = startedStr.endsWith('Z') 
                                    ? new Date(startedStr).getTime()
                                    : new Date(startedStr + 'Z').getTime();
                            } else {
                                startedUTC = new Date(startedStr.replace(' ', 'T') + 'Z').getTime();
                            }
                            
                            // Validate the parsed date
                            if (isNaN(startedUTC)) {
                                uptimeEl.textContent = 'Uptime: -';
                                return;
                            }
                            
                            // Get current UTC time
                            const now = new Date();
                            const nowUTC = Date.UTC(
                                now.getUTCFullYear(),
                                now.getUTCMonth(),
                                now.getUTCDate(),
                                now.getUTCHours(),
                                now.getUTCMinutes(),
                                now.getUTCSeconds()
                            );
                            
                            const uptimeSeconds = Math.floor((nowUTC - startedUTC) / 1000);
                            
                            // Validate uptime is reasonable (not negative or NaN)
                            if (isNaN(uptimeSeconds) || uptimeSeconds < 0) {
                                uptimeEl.textContent = 'Uptime: -';
                                return;
                            }
                            
                            let uptimeText = 'Uptime: ';
                            if (uptimeSeconds < 60) {
                                uptimeText += uptimeSeconds + 's';
                            } else if (uptimeSeconds < 3600) {
                                uptimeText += Math.floor(uptimeSeconds / 60) + 'm ' + (uptimeSeconds % 60) + 's';
                            } else if (uptimeSeconds < 86400) {
                                const hours = Math.floor(uptimeSeconds / 3600);
                                const mins = Math.floor((uptimeSeconds % 3600) / 60);
                                uptimeText += hours + 'h ' + mins + 'm';
                            } else {
                                const days = Math.floor(uptimeSeconds / 86400);
                                const hours = Math.floor((uptimeSeconds % 86400) / 3600);
                                uptimeText += days + 'd ' + hours + 'h';
                            }
                            uptimeEl.textContent = uptimeText;
                        } catch (e) {
                            uptimeEl.textContent = 'Uptime: -';
                        }
                    } else {
                        uptimeEl.textContent = 'Uptime: -';
                    }
                }
            }
            
            /**
             * Refresh scheduler status from API
             */
            async function refreshSchedulerStatus() {
                try {
                    const response = await fetch('<?php echo DUCKDB_API_URL; ?>/scheduler_status');
                    const data = await response.json();
                    
                    if (data.status === 'ok' && data.jobs) {
                        window.schedulerJobs = data.jobs;
                        // Only set schedulerStarted if it's a valid non-empty string
                        if (data.scheduler_started && typeof data.scheduler_started === 'string' && data.scheduler_started.length > 0) {
                            window.schedulerStarted = data.scheduler_started;
                        }
                        updateJobStatusUI(data.jobs);
                    }
                } catch (error) {
                    console.error('Failed to refresh scheduler status:', error);
                }
            }
            
            /**
             * Update the job status UI with new data
             */
            function updateJobStatusUI(jobs) {
                const grid = document.getElementById('jobStatusGrid');
                if (!grid) return;
                
                // Update existing cards
                Object.entries(jobs).forEach(function([jobId, job]) {
                    let card = grid.querySelector('[data-job-id="' + jobId + '"]');
                    
                    if (!card) {
                        // Create new card for new jobs
                        card = document.createElement('div');
                        card.className = 'job-card';
                        card.dataset.jobId = jobId;
                        grid.appendChild(card);
                    }
                    
                    // Update card class
                    card.className = 'job-card job-' + (job.status || 'unknown');
                    if (job.is_service || job.is_stream) card.classList.add('job-service');
                    
                    // Get status badge color
                    const statusColors = {
                        'running': 'info',
                        'success': 'success',
                        'error': 'danger',
                        'stopped': 'secondary'
                    };
                    const statusColor = statusColors[job.status] || 'secondary';
                    
                    // Update card content
                    card.innerHTML = `
                        <div class="job-header">
                            <span class="job-name">${jobId}</span>
                            <span class="job-status-badge bg-${statusColor}-transparent">${job.status || 'unknown'}</span>
                        </div>
                        <div class="job-desc">${job.description || ''}</div>
                        <div class="job-meta">
                            ${job.last_success ? `
                            <div class="job-meta-item">
                                <span class="job-meta-label">Last OK:</span>
                                <span class="job-meta-value job-time" data-time="${job.last_success}">-</span>
                            </div>
                            ` : ''}
                            ${job.run_count ? `
                            <div class="job-meta-item">
                                <span class="job-meta-label">Runs:</span>
                                <span class="job-meta-value">${job.run_count.toLocaleString()}</span>
                            </div>
                            ` : ''}
                            ${job.error_message ? `
                            <div class="job-meta-item" style="flex-basis: 100%;">
                                <span class="job-meta-label">Error:</span>
                                <span class="job-meta-value text-danger">${job.error_message.substring(0, 50)}</span>
                            </div>
                            ` : ''}
                        </div>
                    `;
                });
                
                // Update times immediately after updating UI
                updateJobTimes();
            }
            
            // Initialize on page load
            document.addEventListener('DOMContentLoaded', function() {
                // Update job times immediately and every second
                updateJobTimes();
                setInterval(updateJobTimes, 1000);
                
                // Refresh scheduler status every 5 seconds
                setInterval(refreshSchedulerStatus, 5000);
            });
        </script>

<?php $scripts = ob_get_clean(); ?>
<!-- This code is useful for internal scripts -->

<!-- This code use for render base file -->
<?php include __DIR__ . '/pages/layouts/base.php'; ?>
<!-- This code use for render base file -->
