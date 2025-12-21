<?php
/**
 * SOL Price Dashboard - Candlestick Chart
 * Ported from chart/index.php to v2 template system
 */

// --- Load Configuration from .env ---
require_once __DIR__ . '/../chart/config.php';

// --- DuckDB API Client for faster queries ---
require_once __DIR__ . '/../chart/build_pattern_config/DuckDBClient.php';
define('DUCKDB_API_URL', 'http://127.0.0.1:5050');
$duckdb = new DuckDBClient(DUCKDB_API_URL);
$use_duckdb = $duckdb->isAvailable();

// --- Base URL for v2 template ---
$rootFolder = basename($_SERVER['DOCUMENT_ROOT']);
$baseUrl = (isset($_SERVER['HTTPS']) && $_SERVER['HTTPS'] === 'on' ? 'https://' : 'http://') . $_SERVER['HTTP_HOST'] . dirname($_SERVER['SCRIPT_NAME']);

// --- Data Fetching ---
$dsn = "mysql:host=$db_host;dbname=$db_name;charset=$db_charset";
$options = [
    PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
    PDO::ATTR_EMULATE_PREPARES   => false,
];

$chart_data = [
    'labels' => [],
    'prices' => [],
    'candles' => [],
    'cycle_prices' => [],
    'markers_good' => [],
    'markers_bad' => [],
    'coin_name' => 'SOL',
];

$price_cycle_id = $_GET['price_cycle_id'] ?? null;
$threshold = $_GET['threshold'] ?? 0.3;
$increase = $_GET['increase'] ?? 0.8;
$investment = $_GET['investment'] ?? 500;
$candle_interval = isset($_GET['candle_interval']) ? max(1, min(10, (int)$_GET['candle_interval'])) : 5;
$coin_id = 5;

// Use 24-hour interval from now
$end_datetime = date('Y-m-d H:i:s');
$start_datetime = date('Y-m-d H:i:s', strtotime('-24 hours'));

/**
 * Aggregate raw price points into OHLC candles
 * @param array $prices Array of ['x' => timestamp, 'y' => price]
 * @param int $intervalMinutes Candle interval in minutes
 * @return array Array of candles in ApexCharts format
 */
function aggregateToCandles(array $prices, int $intervalMinutes = 1): array {
    if (empty($prices)) {
        return [];
    }
    
    $candles = [];
    $intervalSeconds = $intervalMinutes * 60;
    
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

try {
    $pdo = new PDO($dsn, $db_user, $db_pass, $options);

    // If price_cycle_id is provided, get the date range from the cycle
    $selected_cycle = null;
    if ($price_cycle_id) {
        // Get the actual cycle start/end times for highlighting
        $cycle_info_stmt = $pdo->prepare("SELECT cycle_start_time, cycle_end_time, sequence_start_price, highest_price_reached, max_percent_increase_from_lowest as percent_change FROM solcatcher.cycle_tracker WHERE id = :price_cycle");
        $cycle_info_stmt->execute(['price_cycle' => $price_cycle_id]);
        $selected_cycle = $cycle_info_stmt->fetch();
        
        $cycle_stmt = $pdo->prepare("SELECT MIN(created_at) as start_date, MAX(created_at) as end_date FROM solcatcher.price_analysis WHERE price_cycle = :price_cycle");
        $cycle_stmt->execute(['price_cycle' => $price_cycle_id]);
        $cycle_dates = $cycle_stmt->fetch();
        
        if ($cycle_dates && $cycle_dates['start_date'] && $cycle_dates['end_date']) {
            $cycle_start = new DateTime($cycle_dates['start_date']);
            $cycle_start->sub(new DateInterval('PT1H'));
            
            $cycle_end = new DateTime($cycle_dates['end_date']);
            $cycle_end->add(new DateInterval('PT1H'));
            
            $start_datetime = $cycle_start->format('Y-m-d H:i:s');
            $end_datetime = $cycle_end->format('Y-m-d H:i:s');
        }
    }

    // ==========================================================================
    // PRICE POINTS - Try DuckDB first, fallback to MySQL (always use price_points for live data)
    // ==========================================================================
    $used_duckdb_prices = false;
    if ($use_duckdb) {
        $duckdb_prices = $duckdb->getPricePoints($coin_id, $start_datetime, $end_datetime);
        if ($duckdb_prices && isset($duckdb_prices['prices'])) {
            $chart_data['prices'] = $duckdb_prices['prices'];
            $used_duckdb_prices = true;
        }
    }
    
    if (!$used_duckdb_prices) {
        $price_stmt = $pdo->prepare("SELECT value, created_at FROM solcatcher.price_points WHERE coin_id = :coin_id AND created_at BETWEEN :start_date AND :end_date ORDER BY created_at ASC");
        $price_stmt->execute([
            'coin_id' => $coin_id,
            'start_date' => $start_datetime,
            'end_date' => $end_datetime,
        ]);
        while ($row = $price_stmt->fetch()) {
            $chart_data['prices'][] = ['x' => $row['created_at'], 'y' => (float)$row['value']];
        }
    }

    // Aggregate prices into OHLC candles based on selected interval
    $chart_data['candles'] = aggregateToCandles($chart_data['prices'], $candle_interval);

    // ==========================================================================
    // PRICE CYCLE DATA (if price_cycle_id provided)
    // ==========================================================================
    if ($price_cycle_id) {
        $cycle_price_stmt = $pdo->prepare("SELECT created_at, current_price FROM solcatcher.price_analysis WHERE price_cycle = :price_cycle AND created_at BETWEEN :start_date AND :end_date ORDER BY created_at ASC");
        $cycle_price_stmt->execute([
            'price_cycle' => $price_cycle_id,
            'start_date' => $start_datetime,
            'end_date' => $end_datetime,
        ]);
        while ($cycle_row = $cycle_price_stmt->fetch()) {
            $chart_data['cycle_prices'][] = ['x' => $cycle_row['created_at'], 'y' => (float)$cycle_row['current_price']];
        }
    }

    // ==========================================================================
    // STATUS DATA
    // ==========================================================================
    $status_data = [
        'get_prices' => null,
        'price_analysis' => null,
        'stream_trades' => null,
        'profiles' => null,
        'active_cycle' => null,
    ];

    // Get Prices - latest created_at
    $get_prices_stmt = $pdo->prepare("SELECT created_at FROM solcatcher.price_points_archive ORDER BY id DESC LIMIT 1");
    $get_prices_stmt->execute();
    $get_prices_result = $get_prices_stmt->fetch();
    if ($get_prices_result) {
        $status_data['get_prices'] = $get_prices_result['created_at'];
    }

    // Price Analysis - latest created_at
    $price_analysis_stmt = $pdo->prepare("SELECT created_at FROM solcatcher.price_analysis ORDER BY created_at DESC LIMIT 1");
    $price_analysis_stmt->execute();
    $price_analysis_result = $price_analysis_stmt->fetch();
    if ($price_analysis_result) {
        $status_data['price_analysis'] = $price_analysis_result['created_at'];
    }

    // Stream Trades - latest trade_timestamp
    try {
        $stream_trades_stmt = $pdo->prepare("SELECT trade_timestamp FROM solcatcher.sol_stablecoin_trades ORDER BY trade_timestamp DESC LIMIT 1");
        $stream_trades_stmt->execute();
        $stream_trades_result = $stream_trades_stmt->fetch();
        if ($stream_trades_result) {
            $status_data['stream_trades'] = $stream_trades_result['trade_timestamp'];
        }
    } catch (\PDOException $e) {
        error_log("Error fetching stream trades: " . $e->getMessage());
    }

    // Profiles - latest trade_timestamp
    try {
        $profiles_stmt = $pdo->prepare("SELECT trade_timestamp FROM solcatcher.sol_stablecoin_profiles ORDER BY trade_timestamp DESC LIMIT 1");
        $profiles_stmt->execute();
        $profiles_result = $profiles_stmt->fetch();
        if ($profiles_result) {
            $status_data['profiles'] = $profiles_result['trade_timestamp'];
        }
    } catch (\PDOException $e) {
        error_log("Error fetching profiles: " . $e->getMessage());
    }

    // ==========================================================================
    // ACTIVE CYCLE - Try DuckDB first
    // ==========================================================================
    try {
        $active_cycle_fetched = false;
        if ($use_duckdb) {
            $latest_cycle = $duckdb->getLatestCycle(0.3);
            if ($latest_cycle !== null) {
                $status_data['active_cycle'] = $latest_cycle;
                $active_cycle_fetched = true;
            }
        }
        
        if (!$active_cycle_fetched) {
            $active_cycle_stmt = $pdo->prepare("SELECT cycle_start_time FROM solcatcher.cycle_tracker WHERE threshold = 0.3 ORDER BY id DESC LIMIT 1");
            $active_cycle_stmt->execute();
            $active_cycle_result = $active_cycle_stmt->fetch();
            if ($active_cycle_result) {
                $status_data['active_cycle'] = $active_cycle_result['cycle_start_time'];
            }
        }
    } catch (\PDOException $e) {
        error_log("Error fetching active cycle: " . $e->getMessage());
    }

    // ==========================================================================
    // CYCLE TRACKER DATA - Always use MySQL with 24 hour interval
    // ==========================================================================
    $analysis_data = [];
    $analysis_stmt = $pdo->prepare("
        SELECT 
            id as price_cycle,
            cycle_start_time,
            cycle_end_time,
            sequence_start_price,
            highest_price_reached,
            max_percent_increase_from_lowest as percent_change
        FROM solcatcher.cycle_tracker
        WHERE threshold = :threshold 
            AND created_at >= NOW() - INTERVAL 24 HOUR
            AND max_percent_increase_from_lowest > :increase
        ORDER BY id DESC
    ");
    $analysis_stmt->execute([
        'threshold' => $threshold,
        'increase' => $increase
    ]);
    $analysis_data = $analysis_stmt->fetchAll();

    // Extract cycle start times for chart markers
    $cycle_start_times = [];
    foreach ($analysis_data as $row) {
        $cycle_start_times[] = $row['cycle_start_time'];
    }

} catch (\PDOException $e) {
    die("Database connection failed: " . $e->getMessage());
}

$json_chart_data = json_encode($chart_data);
$json_status_data = json_encode($status_data);
$json_cycle_start_times = json_encode($cycle_start_times);
$json_selected_cycle = json_encode($selected_cycle);

// Debug info
$debug_source = $use_duckdb ? "DuckDB" : "MySQL";
?>

<!-- This code is useful for internal styles -->
<?php ob_start(); ?>

<style>
    .status-grid {
        display: grid;
        grid-template-columns: repeat(5, 1fr);
        gap: 1rem;
    }
    @media (max-width: 1200px) {
        .status-grid {
            grid-template-columns: repeat(3, 1fr);
        }
    }
    @media (max-width: 768px) {
        .status-grid {
            grid-template-columns: repeat(2, 1fr);
        }
    }
    .status-btn {
        padding: 1rem;
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
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 0.5rem;
        color: rgba(255,255,255,0.7);
    }
    .status-info {
        font-size: 0.8rem;
        color: rgba(255,255,255,0.9);
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
                        🦆 <?php echo $debug_source; ?>
                    </div>

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
                                                <option value="0.1" <?php echo $threshold == 0.1 ? 'selected' : ''; ?>>0.1</option>
                                                <option value="0.2" <?php echo $threshold == 0.2 ? 'selected' : ''; ?>>0.2</option>
                                                <option value="0.3" <?php echo $threshold == 0.3 ? 'selected' : ''; ?>>0.3</option>
                                                <option value="0.4" <?php echo $threshold == 0.4 ? 'selected' : ''; ?>>0.4</option>
                                                <option value="0.5" <?php echo $threshold == 0.5 ? 'selected' : ''; ?>>0.5</option>
                                            </select>
                                        </div>
                                        <div class="col-xl-2 col-lg-3 col-md-4">
                                            <label for="increase" class="form-label">Min Increase %</label>
                                            <input type="number" id="increase" name="increase" class="form-control" value="<?php echo htmlspecialchars($increase); ?>" min="0" step="0.1">
                                        </div>
                                        <div class="col-xl-2 col-lg-3 col-md-4">
                                            <label for="candle_interval" class="form-label">Candle Grouping</label>
                                            <select id="candle_interval" name="candle_interval" class="form-select">
                                                <?php for ($i = 1; $i <= 10; $i++): ?>
                                                <option value="<?php echo $i; ?>" <?php echo $candle_interval == $i ? 'selected' : ''; ?>><?php echo $i; ?> min</option>
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
                        <div id="getPricesBtn" class="status-btn status-unknown" onclick="refreshStatus()">
                            <div class="status-title">Get Prices</div>
                            <div class="status-info" id="getPricesInfo">Loading...</div>
                        </div>
                        <div id="priceAnalysisBtn" class="status-btn status-unknown" onclick="refreshStatus()">
                            <div class="status-title">Enrich Price Data</div>
                            <div class="status-info" id="priceAnalysisInfo">Loading...</div>
                        </div>
                        <div id="streamTradesBtn" class="status-btn status-unknown" onclick="refreshStatus()">
                            <div class="status-title">Stream Trades</div>
                            <div class="status-info" id="streamTradesInfo">Loading...</div>
                        </div>
                        <div id="profilesBtn" class="status-btn status-unknown" onclick="refreshStatus()">
                            <div class="status-title">Profiles</div>
                            <div class="status-info" id="profilesInfo">Loading...</div>
                        </div>
                        <div id="activeCycleBtn" class="status-btn status-unknown" onclick="refreshStatus()">
                            <div class="status-title">Active Open Cycle</div>
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
                                        SOL Price Chart - Candlestick (<?php echo $candle_interval; ?> min)
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
                                                <span><strong>End:</strong> <?php echo date('H:i:s', strtotime($selected_cycle['cycle_end_time'])); ?></span>
                                                <span><strong>Change:</strong> <span class="text-success fw-semibold">+<?php echo number_format($selected_cycle['percent_change'], 2); ?>%</span></span>
                                                <span><strong>Start Price:</strong> $<?php echo number_format($selected_cycle['sequence_start_price'], 4); ?></span>
                                                <span><strong>High:</strong> $<?php echo number_format($selected_cycle['highest_price_reached'], 4); ?></span>
                                            </div>
                                        </div>
                                        <a href="?" class="btn btn-sm btn-outline-light">
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
                                        <p class="text-muted">Select a different date or check the data source</p>
                                    </div>
                                    <?php endif; ?>
                                </div>
                            </div>
                        </div>
                    </div>
                    <!-- End:: Candlestick Chart -->

                    <!-- Start:: Cycle Tracker Table -->
                    <?php if (!empty($analysis_data)): ?>
                    <div class="row">
                        <div class="col-xl-12">
                            <div class="card custom-card">
                                <div class="card-header">
                                    <div class="card-title">Cycle Tracker Results</div>
                                    <div class="ms-auto d-flex gap-3">
                                        <span class="badge bg-info-transparent">Threshold: <?php echo htmlspecialchars($threshold); ?></span>
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
                                                    <td><?php echo date('H:i:s', strtotime($row['cycle_end_time'])); ?></td>
                                                    <td>
                                                        <span class="badge bg-info">
                                                            <?php 
                                                            $start_ts = strtotime($row['cycle_start_time']);
                                                            $end_ts = strtotime($row['cycle_end_time']);
                                                            echo round(($end_ts - $start_ts) / 60) . ' min';
                                                            ?>
                                                        </span>
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
                                        <h6 class="mb-0">No Results Found</h6>
                                        <p class="mb-0">No price cycles found with ><?php echo htmlspecialchars($increase); ?>% increase for threshold <?php echo htmlspecialchars($threshold); ?> in the last 24 hours</p>
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

            // Update status buttons based on timestamp freshness
            function updateStatusButtons() {
                const now = new Date();
                const statusData = window.statusData;

                function updateButton(btnId, infoId, timestamp) {
                    const btn = document.getElementById(btnId);
                    const info = document.getElementById(infoId);
                    
                    if (!timestamp) {
                        btn.className = 'status-btn status-unknown';
                        info.textContent = 'No data';
                        return;
                    }

                    const dataTime = new Date(timestamp + ' UTC');
                    const diffSeconds = Math.floor((now - dataTime) / 1000);
                    
                    let statusClass = 'status-good';
                    if (diffSeconds > 120) statusClass = 'status-warning';
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

                updateButton('getPricesBtn', 'getPricesInfo', statusData.get_prices);
                updateButton('priceAnalysisBtn', 'priceAnalysisInfo', statusData.price_analysis);
                updateButton('streamTradesBtn', 'streamTradesInfo', statusData.stream_trades);
                updateButton('profilesBtn', 'profilesInfo', statusData.profiles);
                updateButton('activeCycleBtn', 'activeCycleInfo', statusData.active_cycle);
            }

            function refreshStatus() {
                location.reload();
            }

            // Initialize status on page load
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
                if (selectedCycle && selectedCycle.cycle_start_time && selectedCycle.cycle_end_time) {
                    const cycleStart = new Date(selectedCycle.cycle_start_time + ' UTC').getTime();
                    const cycleEnd = new Date(selectedCycle.cycle_end_time + ' UTC').getTime();
                    
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
        </script>

<?php $scripts = ob_get_clean(); ?>
<!-- This code is useful for internal scripts -->

<!-- This code use for render base file -->
<?php include 'pages/layouts/base.php'; ?>
<!-- This code use for render base file -->

