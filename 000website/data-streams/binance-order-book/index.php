<?php
/**
 * Binance Order Book Data Page - Market Depth Analysis
 * Migrated from: 000old_code/solana_node/v2/order-book-data/index.php
 * 
 * Displays real-time order book features from Binance WebSocket stream.
 * Data is sourced from DuckDB API (24hr hot storage).
 */

// --- DuckDB API Client ---
require_once __DIR__ . '/../../includes/DuckDBClient.php';
define('DUCKDB_API_URL', 'http://127.0.0.1:5050');
$duckdb = new DuckDBClient(DUCKDB_API_URL);
$use_duckdb = $duckdb->isAvailable();

// --- Base URL for template ---
$baseUrl = '../..';

// --- Fetch Order Book Data via API ---
function fetchOrderBookData($duckdb) {
    $orderbook_data = [];
    $error_message = null;
    $data_source = "No Data";
    $actual_source = null;

    if ($duckdb->isAvailable()) {
        // Query order book features from the API using structured query
        $response = $duckdb->query(
            'order_book_features',
            [
                'symbol', 'ts', 'best_bid', 'best_ask', 'mid_price',
                'relative_spread_bps', 'bid_depth_10', 'ask_depth_10',
                'total_depth_10', 'volume_imbalance', 'bid_slope', 'ask_slope',
                'bid_depth_bps_5', 'ask_depth_bps_5', 'bid_depth_bps_10',
                'ask_depth_bps_10', 'bid_depth_bps_25', 'ask_depth_bps_25',
                'net_liquidity_change_1s', 'microprice', 'microprice_dev_bps'
            ],
            null,  // no where clause
            'ts DESC',
            100
        );
        
        if ($response && isset($response['results'])) {
            $orderbook_data = $response['results'];
            // Capture the actual data source from API response
            $actual_source = $response['source'] ?? 'unknown';
            
            // Format data source display name
            switch ($actual_source) {
                case 'engine':
                    $data_source = "🦆 In-Memory DuckDB (TradingDataEngine)";
                    break;
                case 'duckdb':
                    $data_source = "🦆 File-Based DuckDB";
                    break;
                case 'mysql':
                    $data_source = "🗄️ MySQL (Historical)";
                    break;
                default:
                    $data_source = "🦆 DuckDB API";
            }
        } else {
            $data_source = "DuckDB API (No Data)";
        }
    } else {
        $error_message = "DuckDB API is not available. Please start the scheduler: python scheduler/master.py";
    }
    
    return [
        'orderbook_data' => $orderbook_data,
        'error_message' => $error_message,
        'data_source' => $data_source,
        'actual_source' => $actual_source
    ];
}

// Check if this is an AJAX request for data refresh
if (isset($_GET['ajax']) && $_GET['ajax'] === 'refresh') {
    header('Content-Type: application/json');
    $result = fetchOrderBookData($duckdb);
    $use_duckdb = $duckdb->isAvailable();
    
    // Build status data
    $status_data = [
        'api_status' => $use_duckdb ? 'connected' : 'disconnected',
        'orderbook_records' => count($result['orderbook_data']),
        'last_update' => null,
    ];
    
    if (!empty($result['orderbook_data'])) {
        $status_data['last_update'] = $result['orderbook_data'][0]['ts'] ?? null;
    }
    
    // Get latest price and cycle info for status buttons
    if ($use_duckdb) {
        $price_response = $duckdb->getLatestPrices();
        if ($price_response && isset($price_response['prices']['SOL'])) {
            $status_data['get_prices'] = $price_response['prices']['SOL']['ts'] ?? null;
        }
        
        $analysis_response = $duckdb->getPriceAnalysis(5, '1', 1);
        if ($analysis_response && isset($analysis_response['price_analysis']) && !empty($analysis_response['price_analysis'])) {
            $status_data['price_analysis'] = $analysis_response['price_analysis'][0]['created_at'] ?? null;
        }
        
        $cycle_response = $duckdb->getCycleTracker(0.3, '24', 1);
        if ($cycle_response && isset($cycle_response['cycles']) && !empty($cycle_response['cycles'])) {
            $status_data['active_cycle'] = $cycle_response['cycles'][0]['cycle_start_time'] ?? null;
        }
    }
    
    echo json_encode([
        'success' => true,
        'orderbook_data' => $result['orderbook_data'],
        'status_data' => $status_data,
        'error_message' => $result['error_message'],
        'data_source' => $result['data_source'],
        'actual_source' => $result['actual_source']
    ]);
    exit;
}

// Regular page load - fetch data normally
$result = fetchOrderBookData($duckdb);
$orderbook_data = $result['orderbook_data'];
$error_message = $result['error_message'];
$data_source = $result['data_source'];
$use_duckdb = $duckdb->isAvailable();

// --- Status Data ---
$status_data = [
    'api_status' => $use_duckdb ? 'connected' : 'disconnected',
    'orderbook_records' => count($orderbook_data),
    'last_update' => null,
    'stream_status' => null,
];

if (!empty($orderbook_data)) {
    $status_data['last_update'] = $orderbook_data[0]['ts'] ?? null;
}

// Get latest price and cycle info for status buttons
if ($use_duckdb) {
    // Get latest price time from latest_prices endpoint
    $price_response = $duckdb->getLatestPrices();
    if ($price_response && isset($price_response['prices']['SOL'])) {
        $status_data['get_prices'] = $price_response['prices']['SOL']['ts'] ?? null;
    }
    
    // Get latest price analysis time
    $analysis_response = $duckdb->getPriceAnalysis(5, '1', 1);
    if ($analysis_response && isset($analysis_response['price_analysis']) && !empty($analysis_response['price_analysis'])) {
        $status_data['price_analysis'] = $analysis_response['price_analysis'][0]['created_at'] ?? null;
    }
    
    // Get active cycle
    $cycle_response = $duckdb->getCycleTracker(0.3, '24', 1);
    if ($cycle_response && isset($cycle_response['cycles']) && !empty($cycle_response['cycles'])) {
        $status_data['active_cycle'] = $cycle_response['cycles'][0]['cycle_start_time'] ?? null;
    }
}

$json_status_data = json_encode($status_data);
$json_orderbook_data = json_encode($orderbook_data);
?>

<!-- This code is useful for internal styles -->
<?php ob_start(); ?>

<style>
    /* Trader-focused color scheme */
    :root {
        --bid-color: #00d4aa;
        --ask-color: #ff4444;
        --bid-bg: rgba(0, 212, 170, 0.15);
        --ask-bg: rgba(255, 68, 68, 0.15);
        --spread-tight: #00d4aa;
        --spread-normal: #ffa500;
        --spread-wide: #ff4444;
        --depth-high: rgba(0, 212, 170, 0.3);
        --depth-low: rgba(255, 255, 255, 0.05);
    }
    
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
    
    /* Trader table styling */
    .orderbook-table {
        background: rgba(0, 0, 0, 0.3);
        border-radius: 8px;
    }
    .orderbook-table thead th {
        background: rgba(0, 0, 0, 0.5) !important;
        border-bottom: 2px solid rgba(255, 255, 255, 0.1);
        font-weight: 700;
        text-transform: uppercase;
        font-size: 0.75rem;
        letter-spacing: 0.5px;
        padding: 1rem 0.75rem;
    }
    .orderbook-table tbody tr {
        transition: all 0.15s ease;
        border-bottom: 1px solid rgba(255, 255, 255, 0.05);
    }
    .orderbook-table tbody tr:hover {
        background: rgba(255, 255, 255, 0.05);
        transform: scale(1.01);
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.3);
    }
    .orderbook-table tbody tr:first-child {
        background: rgba(0, 212, 170, 0.1);
        border-left: 3px solid var(--bid-color);
    }
    
    /* Bid/Ask color coding */
    .cell-bid {
        color: var(--bid-color);
        font-weight: 600;
        background: var(--bid-bg);
        padding: 0.5rem;
        border-radius: 4px;
    }
    .cell-ask {
        color: var(--ask-color);
        font-weight: 600;
        background: var(--ask-bg);
        padding: 0.5rem;
        border-radius: 4px;
    }
    .cell-mid-price {
        color: #fff;
        font-weight: 700;
        font-size: 0.95rem;
        background: linear-gradient(135deg, rgba(0, 212, 170, 0.2), rgba(255, 68, 68, 0.2));
        padding: 0.5rem;
        border-radius: 4px;
    }
    
    /* Spread visualization */
    .spread-cell {
        position: relative;
        padding: 0.5rem;
        border-radius: 4px;
        font-weight: 600;
    }
    .spread-tight {
        color: var(--spread-tight);
        background: rgba(0, 212, 170, 0.15);
    }
    .spread-normal {
        color: var(--spread-normal);
        background: rgba(255, 165, 0, 0.15);
    }
    .spread-wide {
        color: var(--spread-wide);
        background: rgba(255, 68, 68, 0.15);
    }
    
    /* Depth visualization */
    .depth-bar-container {
        display: flex;
        align-items: center;
        gap: 0.5rem;
        min-width: 100px;
    }
    .depth-bar {
        height: 20px;
        border-radius: 3px;
        min-width: 4px;
        transition: all 0.3s ease;
    }
    .depth-bar-bid {
        background: linear-gradient(90deg, var(--bid-color), rgba(0, 212, 170, 0.6));
    }
    .depth-bar-ask {
        background: linear-gradient(90deg, var(--ask-color), rgba(255, 68, 68, 0.6));
    }
    .depth-value {
        font-weight: 600;
        min-width: 60px;
        text-align: right;
    }
    
    /* Imbalance visualization */
    .imbalance-cell {
        position: relative;
        padding: 0.5rem;
        border-radius: 4px;
        font-weight: 600;
    }
    .imbalance-bullish {
        color: var(--bid-color);
        background: rgba(0, 212, 170, 0.2);
    }
    .imbalance-bearish {
        color: var(--ask-color);
        background: rgba(255, 68, 68, 0.2);
    }
    .imbalance-neutral {
        color: rgba(255, 255, 255, 0.6);
        background: rgba(255, 255, 255, 0.05);
    }
    
    /* Liquidity change indicators */
    .liquidity-positive {
        color: var(--bid-color);
        font-weight: 700;
        background: rgba(0, 212, 170, 0.15);
        padding: 0.25rem 0.5rem;
        border-radius: 4px;
    }
    .liquidity-positive::before {
        content: '↑';
        margin-right: 0.25rem;
    }
    .liquidity-negative {
        color: var(--ask-color);
        font-weight: 700;
        background: rgba(255, 68, 68, 0.15);
        padding: 0.25rem 0.5rem;
        border-radius: 4px;
    }
    .liquidity-negative::before {
        content: '↓';
        margin-right: 0.25rem;
    }
    
    /* Microprice deviation */
    .microprice-positive {
        color: var(--bid-color);
        font-weight: 600;
    }
    .microprice-negative {
        color: var(--ask-color);
        font-weight: 600;
    }
    
    .mono-cell {
        font-family: 'Courier New', monospace;
        font-size: 0.85rem;
        letter-spacing: 0.5px;
    }
    .value-positive {
        color: var(--bid-color);
        font-weight: 600;
    }
    .value-negative {
        color: var(--ask-color);
        font-weight: 600;
    }
    .value-warning {
        color: var(--spread-normal);
        font-weight: 600;
    }
    
    .table-responsive {
        max-height: 70vh;
        overflow-y: auto;
        border-radius: 8px;
    }
    .table-responsive::-webkit-scrollbar {
        width: 8px;
        height: 8px;
    }
    .table-responsive::-webkit-scrollbar-track {
        background: rgba(0, 0, 0, 0.2);
        border-radius: 4px;
    }
    .table-responsive::-webkit-scrollbar-thumb {
        background: rgba(255, 255, 255, 0.2);
        border-radius: 4px;
    }
    .table-responsive::-webkit-scrollbar-thumb:hover {
        background: rgba(255, 255, 255, 0.3);
    }
    
    .data-source-badge {
        position: fixed;
        top: 70px;
        right: 20px;
        z-index: 9999;
        padding: 6px 14px;
        border-radius: 6px;
        font-size: 11px;
        font-weight: 700;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.4);
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    .live-indicator {
        display: inline-flex;
        align-items: center;
        gap: 0.5rem;
    }
    .live-dot {
        width: 10px;
        height: 10px;
        background: var(--bid-color);
        border-radius: 50%;
        animation: pulse 1.5s infinite;
        box-shadow: 0 0 8px var(--bid-color);
    }
    @keyframes pulse {
        0%, 100% { opacity: 1; transform: scale(1); }
        50% { opacity: 0.6; transform: scale(1.3); }
    }
    
    /* Price change indicator */
    .price-change-up {
        color: var(--bid-color);
    }
    .price-change-down {
        color: var(--ask-color);
    }
    
    /* Enhanced card header */
    .card-header {
        background: linear-gradient(135deg, rgba(0, 0, 0, 0.4), rgba(0, 0, 0, 0.2));
        border-bottom: 2px solid rgba(255, 255, 255, 0.1);
    }
</style>

<?php $styles = ob_get_clean(); ?>
<!-- This code is useful for internal styles -->

<!-- This code is useful for content -->
<?php ob_start(); ?>

                    <!-- Start::page-header -->
                    <div class="page-header-breadcrumb mb-3">
                        <div class="d-flex align-center justify-content-between flex-wrap">
                            <h1 class="page-title fw-medium fs-18 mb-0">Binance Order Book</h1>
                            <ol class="breadcrumb mb-0">
                                <li class="breadcrumb-item"><a href="<?php echo $baseUrl; ?>/">Dashboard</a></li>
                                <li class="breadcrumb-item">Data Streams</li>
                                <li class="breadcrumb-item active" aria-current="page">Binance Order Book</li>
                            </ol>
                        </div>
                    </div>
                    <!-- End::page-header -->

                    <!-- Data Source Badge -->
                    <div id="dataSourceBadge" class="data-source-badge" style="background: <?php echo $use_duckdb ? 'rgb(var(--success-rgb))' : 'rgb(var(--danger-rgb))'; ?>; color: white;">
                        <?php echo $data_source; ?>
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

                    <!-- Start:: Status Buttons -->
                    <div class="status-grid mb-3">
                        <div id="orderbookStatusBtn" class="status-btn <?php echo count($orderbook_data) > 0 ? 'status-good' : 'status-warning'; ?>">
                            <div class="status-title">Order Book Stream</div>
                            <div class="status-info" id="orderbookStatusInfo">
                                <?php echo count($orderbook_data) > 0 ? 'Active' : 'No Data'; ?>
                            </div>
                        </div>
                        <div id="getPricesBtn" class="status-btn status-unknown">
                            <div class="status-title">Get Prices</div>
                            <div class="status-info" id="getPricesInfo">Loading...</div>
                        </div>
                        <div id="priceAnalysisBtn" class="status-btn status-unknown">
                            <div class="status-title">Price Analysis</div>
                            <div class="status-info" id="priceAnalysisInfo">Loading...</div>
                        </div>
                        <div id="activeCycleBtn" class="status-btn status-unknown">
                            <div class="status-title">Active Cycle</div>
                            <div class="status-info" id="activeCycleInfo">Loading...</div>
                        </div>
                        <div id="recordCountBtn" class="status-btn status-good">
                            <div class="status-title">Records Displayed</div>
                            <div class="status-info" id="recordCountInfo"><?php echo count($orderbook_data); ?></div>
                        </div>
                    </div>
                    <!-- End:: Status Buttons -->

                    <!-- Start:: Order Book Table -->
                    <div class="row">
                        <div class="col-xl-12">
                            <div class="card custom-card">
                                <div class="card-header">
                                    <div class="card-title">
                                        <i class="ti ti-chart-bar me-2"></i>Market Depth Analysis - SOLUSDT
                                    </div>
                                    <div class="ms-auto d-flex gap-2 align-items-center">
                                        <span class="badge bg-info-transparent" id="recordCount"><?php echo count($orderbook_data); ?> Records</span>
                                        <span class="badge bg-success-transparent live-indicator">
                                            <span class="live-dot"></span>
                                            Real-time
                                        </span>
                                    </div>
                                </div>
                                <div class="card-body">
                                    <div class="table-responsive">
                                        <table class="table table-bordered text-nowrap table-sm orderbook-table">
                                            <thead class="table-dark">
                                                <tr>
                                                    <th>Time</th>
                                                    <th class="text-end">Best Bid</th>
                                                    <th class="text-end">Best Ask</th>
                                                    <th class="text-end">Mid Price</th>
                                                    <th class="text-end">Spread (BPS)</th>
                                                    <th class="text-end">Bid Depth</th>
                                                    <th class="text-end">Ask Depth</th>
                                                    <th class="text-end">Total Depth</th>
                                                    <th class="text-end">Vol Imbalance</th>
                                                    <th class="text-end">Microprice Dev</th>
                                                    <th class="text-end">Net Liq Δ 1s</th>
                                                </tr>
                                            </thead>
                                            <tbody id="orderbookTableBody">
                                                <?php if (!empty($orderbook_data)): ?>
                                                    <?php 
                                                    $prevMidPrice = null;
                                                    foreach ($orderbook_data as $idx => $row): 
                                                        $spread = $row['relative_spread_bps'] ?? 0;
                                                        $imbalance = $row['volume_imbalance'] ?? 0;
                                                        $micropriceDev = $row['microprice_dev_bps'] ?? 0;
                                                        $netLiq = $row['net_liquidity_change_1s'] ?? 0;
                                                        $bidDepth = $row['bid_depth_10'] ?? 0;
                                                        $askDepth = $row['ask_depth_10'] ?? 0;
                                                        $totalDepth = $row['total_depth_10'] ?? 0;
                                                        $midPrice = $row['mid_price'] ?? 0;
                                                        
                                                        // Spread classification
                                                        if ($spread < 1.0) {
                                                            $spreadClass = 'spread-tight';
                                                        } elseif ($spread < 2.0) {
                                                            $spreadClass = 'spread-normal';
                                                        } else {
                                                            $spreadClass = 'spread-wide';
                                                        }
                                                        
                                                        // Imbalance classification
                                                        if ($imbalance > 0.1) {
                                                            $imbalanceClass = 'imbalance-bullish';
                                                        } elseif ($imbalance < -0.1) {
                                                            $imbalanceClass = 'imbalance-bearish';
                                                        } else {
                                                            $imbalanceClass = 'imbalance-neutral';
                                                        }
                                                        
                                                        // Microprice class
                                                        $micropriceClass = $micropriceDev > 0 ? 'microprice-positive' : ($micropriceDev < 0 ? 'microprice-negative' : '');
                                                        
                                                        // Net liquidity class
                                                        $netLiqClass = '';
                                                        $netLiqDisplay = '--';
                                                        if ($netLiq !== null && $netLiq != 0) {
                                                            $netLiqClass = $netLiq > 0 ? 'liquidity-positive' : 'liquidity-negative';
                                                            $netLiqDisplay = number_format(abs($netLiq), 2);
                                                        }
                                                        
                                                        // Depth bar calculations
                                                        $maxDepth = max($bidDepth, $askDepth, 1);
                                                        $bidDepthPercent = ($bidDepth / $maxDepth) * 100;
                                                        $askDepthPercent = ($askDepth / $maxDepth) * 100;
                                                        
                                                        // Price change indicator
                                                        $priceChangeClass = '';
                                                        if ($prevMidPrice !== null && $midPrice > 0) {
                                                            if ($midPrice > $prevMidPrice) {
                                                                $priceChangeClass = 'price-change-up';
                                                            } elseif ($midPrice < $prevMidPrice) {
                                                                $priceChangeClass = 'price-change-down';
                                                            }
                                                        }
                                                        $prevMidPrice = $midPrice;
                                                        
                                                        $timestamp = isset($row['ts']) ? date('H:i:s', strtotime($row['ts'])) : '--';
                                                    ?>
                                                    <tr>
                                                        <td class="mono-cell"><?php echo $timestamp; ?></td>
                                                        <td class="text-end mono-cell cell-bid"><?php echo number_format($row['best_bid'] ?? 0, 4); ?></td>
                                                        <td class="text-end mono-cell cell-ask"><?php echo number_format($row['best_ask'] ?? 0, 4); ?></td>
                                                        <td class="text-end mono-cell cell-mid-price <?php echo $priceChangeClass; ?>"><?php echo number_format($midPrice, 4); ?></td>
                                                        <td class="text-end mono-cell spread-cell <?php echo $spreadClass; ?>"><?php echo number_format($spread, 2); ?></td>
                                                        <td class="text-end">
                                                            <div class="depth-bar-container">
                                                                <div class="depth-bar depth-bar-bid" style="width: <?php echo min($bidDepthPercent, 100); ?>%"></div>
                                                                <span class="mono-cell depth-value"><?php echo number_format($bidDepth, 2); ?></span>
                                                            </div>
                                                        </td>
                                                        <td class="text-end">
                                                            <div class="depth-bar-container">
                                                                <div class="depth-bar depth-bar-ask" style="width: <?php echo min($askDepthPercent, 100); ?>%"></div>
                                                                <span class="mono-cell depth-value"><?php echo number_format($askDepth, 2); ?></span>
                                                            </div>
                                                        </td>
                                                        <td class="text-end mono-cell"><?php echo number_format($totalDepth, 2); ?></td>
                                                        <td class="text-end mono-cell imbalance-cell <?php echo $imbalanceClass; ?>"><?php echo number_format($imbalance, 4); ?></td>
                                                        <td class="text-end mono-cell <?php echo $micropriceClass; ?>"><?php echo number_format($micropriceDev, 2); ?></td>
                                                        <td class="text-end mono-cell <?php echo $netLiqClass; ?>"><?php echo $netLiqDisplay; ?></td>
                                                    </tr>
                                                    <?php endforeach; ?>
                                                <?php else: ?>
                                                    <tr>
                                                        <td colspan="11" class="text-center py-4">
                                                            <div class="d-flex flex-column align-items-center">
                                                                <i class="ti ti-database-off fs-1 text-muted mb-2"></i>
                                                                <h6 class="text-muted mb-1">No order book data</h6>
                                                                <p class="text-muted mb-0 fs-13">Make sure the scheduler is running:</p>
                                                                <code class="mt-2">python scheduler/master.py</code>
                                                            </div>
                                                        </td>
                                                    </tr>
                                                <?php endif; ?>
                                            </tbody>
                                        </table>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                    <!-- End:: Order Book Table -->

                    <!-- Start:: Feature Explanation -->
                    <div class="row">
                        <div class="col-xl-12">
                            <div class="card custom-card">
                                <div class="card-header">
                                    <div class="card-title">
                                        <i class="ti ti-info-circle me-2"></i>Feature Glossary
                                    </div>
                                </div>
                                <div class="card-body">
                                    <div class="row">
                                        <div class="col-md-4">
                                            <h6 class="fw-semibold text-primary">Price Metrics</h6>
                                            <ul class="list-unstyled fs-13">
                                                <li><strong>Best Bid/Ask:</strong> Top of book prices</li>
                                                <li><strong>Mid Price:</strong> (Bid + Ask) / 2</li>
                                                <li><strong>Spread (BPS):</strong> Relative spread in basis points</li>
                                            </ul>
                                        </div>
                                        <div class="col-md-4">
                                            <h6 class="fw-semibold text-primary">Depth Metrics</h6>
                                            <ul class="list-unstyled fs-13">
                                                <li><strong>Bid/Ask Depth:</strong> Sum of top 10 levels</li>
                                                <li><strong>Total Depth:</strong> Bid + Ask depth</li>
                                                <li><strong>Vol Imbalance:</strong> (Bid - Ask) / Total</li>
                                            </ul>
                                        </div>
                                        <div class="col-md-4">
                                            <h6 class="fw-semibold text-primary">Advanced Metrics</h6>
                                            <ul class="list-unstyled fs-13">
                                                <li><strong>Microprice Dev:</strong> Size-weighted price deviation</li>
                                                <li><strong>Net Liq Δ 1s:</strong> Liquidity change over 1 second</li>
                                            </ul>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                    <!-- End:: Feature Explanation -->

<?php $content = ob_get_clean(); ?>
<!-- This code is useful for content -->

<!-- This code is useful for internal scripts -->
<?php ob_start(); ?>

        <script>
            // Status data from PHP
            window.statusData = <?php echo $json_status_data; ?>;
            window.orderbookData = <?php echo $json_orderbook_data; ?>;
            
            // Status refresh functionality
            function refreshStatus(statusData) {
                const now = new Date();
                
                function updateButton(btnId, infoId, timestamp) {
                    const btn = document.getElementById(btnId);
                    const info = document.getElementById(infoId);
                    
                    if (!btn || !info) return;
                    
                    if (!timestamp) {
                        btn.className = 'status-btn status-warning';
                        info.textContent = 'No data';
                        return;
                    }
                    
                    const dataTime = new Date(timestamp.includes('T') ? timestamp : timestamp + ' UTC');
                    const diffMs = now - dataTime;
                    const diffSecs = Math.floor(diffMs / 1000);
                    const diffMins = Math.floor(diffMs / 60000);
                    
                    let statusClass = 'status-bad';
                    let statusText = '';
                    
                    if (diffSecs < 30) {
                        statusClass = 'status-good';
                        statusText = diffSecs + 's ago';
                    } else if (diffMins < 5) {
                        statusClass = 'status-good';
                        statusText = diffMins < 1 ? diffSecs + 's ago' : diffMins + 'm ago';
                    } else if (diffMins < 15) {
                        statusClass = 'status-warning';
                        statusText = diffMins + 'm ago';
                    } else {
                        statusText = diffMins + 'm ago';
                    }
                    
                    btn.className = 'status-btn ' + statusClass;
                    info.textContent = statusText;
                }
                
                // Update order book stream status
                if (statusData.last_update) {
                    updateButton('orderbookStatusBtn', 'orderbookStatusInfo', statusData.last_update);
                }
                
                updateButton('getPricesBtn', 'getPricesInfo', statusData.get_prices);
                updateButton('priceAnalysisBtn', 'priceAnalysisInfo', statusData.price_analysis);
                updateButton('activeCycleBtn', 'activeCycleInfo', statusData.active_cycle);
            }
            
            // Format number with decimals
            function formatNumber(num, decimals) {
                return parseFloat(num || 0).toFixed(decimals);
            }
            
            // Format timestamp
            function formatTimestamp(ts) {
                if (!ts) return '--';
                const date = new Date(ts.includes('T') ? ts : ts + ' UTC');
                return date.toLocaleTimeString('en-US', { hour12: false });
            }
            
            // Get CSS class for value
            function getValueClass(value, thresholds) {
                if (thresholds.positive !== undefined && value > thresholds.positive) return 'value-positive';
                if (thresholds.negative !== undefined && value < thresholds.negative) return 'value-negative';
                if (thresholds.warning !== undefined && value >= thresholds.warning) return 'value-warning';
                return '';
            }
            
            // Update table with new data
            function updateOrderBookTable(orderbookData) {
                const tbody = document.getElementById('orderbookTableBody');
                const recordCount = document.getElementById('recordCount');
                const recordCountInfo = document.getElementById('recordCountInfo');
                
                if (!tbody) return;
                
                // Update record count
                const count = orderbookData.length;
                if (recordCount) recordCount.textContent = count + ' Records';
                if (recordCountInfo) recordCountInfo.textContent = count;
                
                if (count === 0) {
                    tbody.innerHTML = `
                        <tr>
                            <td colspan="11" class="text-center py-4">
                                <div class="d-flex flex-column align-items-center">
                                    <i class="ti ti-database-off fs-1 text-muted mb-2"></i>
                                    <h6 class="text-muted mb-1">No order book data</h6>
                                    <p class="text-muted mb-0 fs-13">Make sure the scheduler is running:</p>
                                    <code class="mt-2">python scheduler/master.py</code>
                                </div>
                            </td>
                        </tr>
                    `;
                    return;
                }
                
                // Build table rows with trader-focused styling
                let html = '';
                let prevMidPrice = null;
                
                orderbookData.forEach((row, idx) => {
                    const spread = parseFloat(row.relative_spread_bps || 0);
                    const imbalance = parseFloat(row.volume_imbalance || 0);
                    const micropriceDev = parseFloat(row.microprice_dev_bps || 0);
                    const netLiq = parseFloat(row.net_liquidity_change_1s || 0);
                    const bidDepth = parseFloat(row.bid_depth_10 || 0);
                    const askDepth = parseFloat(row.ask_depth_10 || 0);
                    const totalDepth = parseFloat(row.total_depth_10 || 0);
                    const midPrice = parseFloat(row.mid_price || 0);
                    
                    // Spread classification
                    let spreadClass = 'spread-normal';
                    if (spread < 1.0) {
                        spreadClass = 'spread-tight';
                    } else if (spread >= 2.0) {
                        spreadClass = 'spread-wide';
                    }
                    
                    // Imbalance classification
                    let imbalanceClass = 'imbalance-neutral';
                    if (imbalance > 0.1) {
                        imbalanceClass = 'imbalance-bullish';
                    } else if (imbalance < -0.1) {
                        imbalanceClass = 'imbalance-bearish';
                    }
                    
                    // Microprice class
                    let micropriceClass = '';
                    if (micropriceDev > 0) {
                        micropriceClass = 'microprice-positive';
                    } else if (micropriceDev < 0) {
                        micropriceClass = 'microprice-negative';
                    }
                    
                    // Net liquidity class and display
                    let netLiqClass = '';
                    let netLiqDisplay = '--';
                    if (netLiq !== null && netLiq != 0) {
                        netLiqClass = netLiq > 0 ? 'liquidity-positive' : 'liquidity-negative';
                        netLiqDisplay = formatNumber(Math.abs(netLiq), 2);
                    }
                    
                    // Depth bar calculations
                    const maxDepth = Math.max(bidDepth, askDepth, 1);
                    const bidDepthPercent = Math.min((bidDepth / maxDepth) * 100, 100);
                    const askDepthPercent = Math.min((askDepth / maxDepth) * 100, 100);
                    
                    // Price change indicator
                    let priceChangeClass = '';
                    if (prevMidPrice !== null && midPrice > 0) {
                        if (midPrice > prevMidPrice) {
                            priceChangeClass = 'price-change-up';
                        } else if (midPrice < prevMidPrice) {
                            priceChangeClass = 'price-change-down';
                        }
                    }
                    prevMidPrice = midPrice;
                    
                    html += `
                        <tr>
                            <td class="mono-cell">${formatTimestamp(row.ts)}</td>
                            <td class="text-end mono-cell cell-bid">${formatNumber(row.best_bid, 4)}</td>
                            <td class="text-end mono-cell cell-ask">${formatNumber(row.best_ask, 4)}</td>
                            <td class="text-end mono-cell cell-mid-price ${priceChangeClass}">${formatNumber(midPrice, 4)}</td>
                            <td class="text-end mono-cell spread-cell ${spreadClass}">${formatNumber(spread, 2)}</td>
                            <td class="text-end">
                                <div class="depth-bar-container">
                                    <div class="depth-bar depth-bar-bid" style="width: ${bidDepthPercent}%"></div>
                                    <span class="mono-cell depth-value">${formatNumber(bidDepth, 2)}</span>
                                </div>
                            </td>
                            <td class="text-end">
                                <div class="depth-bar-container">
                                    <div class="depth-bar depth-bar-ask" style="width: ${askDepthPercent}%"></div>
                                    <span class="mono-cell depth-value">${formatNumber(askDepth, 2)}</span>
                                </div>
                            </td>
                            <td class="text-end mono-cell">${formatNumber(totalDepth, 2)}</td>
                            <td class="text-end mono-cell imbalance-cell ${imbalanceClass}">${formatNumber(imbalance, 4)}</td>
                            <td class="text-end mono-cell ${micropriceClass}">${formatNumber(micropriceDev, 2)}</td>
                            <td class="text-end mono-cell ${netLiqClass}">${netLiqDisplay}</td>
                        </tr>
                    `;
                });
                
                tbody.innerHTML = html;
            }
            
            // Update data source badge
            function updateDataSourceBadge(dataSource, actualSource) {
                const badge = document.getElementById('dataSourceBadge');
                if (!badge) return;
                
                badge.textContent = dataSource || 'No Data';
                
                // Update badge color based on source
                if (actualSource === 'engine') {
                    badge.style.background = 'rgb(var(--success-rgb))';
                } else if (actualSource === 'duckdb') {
                    badge.style.background = 'rgb(var(--info-rgb))';
                } else if (actualSource === 'mysql') {
                    badge.style.background = 'rgb(var(--warning-rgb))';
                } else {
                    badge.style.background = 'rgb(var(--danger-rgb))';
                }
            }
            
            // Fetch fresh data from server
            async function fetchOrderBookData() {
                try {
                    const response = await fetch('?ajax=refresh');
                    const data = await response.json();
                    
                    if (data.success) {
                        // Update global data
                        window.statusData = data.status_data;
                        window.orderbookData = data.orderbook_data;
                        
                        // Update table
                        updateOrderBookTable(data.orderbook_data);
                        
                        // Update status buttons
                        refreshStatus(data.status_data);
                        
                        // Update data source badge
                        if (data.data_source) {
                            updateDataSourceBadge(data.data_source, data.actual_source);
                        }
                    }
                } catch (error) {
                    console.error('Error fetching order book data:', error);
                }
            }
            
            // Initial status refresh
            refreshStatus(window.statusData);
            
            // Refresh data every 1 second
            setInterval(fetchOrderBookData, 1000);
            
            // Also refresh status every 5 seconds (for time-based updates)
            setInterval(() => refreshStatus(window.statusData), 5000);
        </script>

<?php $scripts = ob_get_clean(); ?>
<!-- This code is useful for internal scripts -->

<!-- This code use for render base file -->
<?php include __DIR__ . '/../../pages/layouts/base.php'; ?>
<!-- This code use for render base file -->

