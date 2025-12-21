<?php
/**
 * SOL Price Dashboard - Candlestick Chart
 * Migrated from: 000old_code/solana_node/v2/index.php
 * 
 * This page displays the 24-hour SOL price chart using DuckDB data.
 * Price cycles feature is coming soon (placeholder added).
 */

// --- DuckDB API Client ---
require_once __DIR__ . '/includes/DuckDBClient.php';
define('DUCKDB_API_URL', 'http://127.0.0.1:5050');
$duckdb = new DuckDBClient(DUCKDB_API_URL);
$use_duckdb = $duckdb->isAvailable();

// --- Base URL for template ---
$baseUrl = '';  // Root of 000website

// --- Parameters ---
$candle_interval = isset($_GET['candle_interval']) ? max(1, min(10, (int)$_GET['candle_interval'])) : 5;
$token = 'SOL';

// Use 24-hour interval from now
$end_datetime = date('Y-m-d H:i:s');
$start_datetime = date('Y-m-d H:i:s', strtotime('-24 hours'));

// --- Chart Data ---
$chart_data = [
    'labels' => [],
    'prices' => [],
    'candles' => [],
    'coin_name' => 'SOL',
];

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
    $error_message = "DuckDB API is not available. Please start the API server: python features/price_api/api.py";
}

// Aggregate prices into OHLC candles based on selected interval
$chart_data['candles'] = aggregateToCandles($chart_data['prices'], $candle_interval);

// --- Status Data ---
$status_data = [
    'api_status' => $use_duckdb ? 'connected' : 'disconnected',
    'last_price_time' => null,
    'price_count' => count($chart_data['prices']),
];

if ($use_duckdb && !empty($chart_data['prices'])) {
    $last_price = end($chart_data['prices']);
    $status_data['last_price_time'] = $last_price['x'];
}

$json_chart_data = json_encode($chart_data);
$json_status_data = json_encode($status_data);

?>

<!-- This code is useful for internal styles -->
<?php ob_start(); ?>

<style>
    .status-grid {
        display: grid;
        grid-template-columns: repeat(3, 1fr);
        gap: 1rem;
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
    .coming-soon-card {
        background: linear-gradient(135deg, rgba(var(--warning-rgb), 0.1), rgba(var(--primary-rgb), 0.1));
        border: 2px dashed rgba(var(--warning-rgb), 0.3);
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
                        <div id="lastUpdateBtn" class="status-btn status-unknown" onclick="location.reload()">
                            <div class="status-title">Last Update</div>
                            <div class="status-info" id="lastUpdateInfo">Loading...</div>
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
                                        <p class="text-muted">Make sure the DuckDB API is running and price data is being collected.</p>
                                        <code class="d-block mt-3">python features/price_api/api.py</code>
                                    </div>
                                    <?php endif; ?>
                                </div>
                            </div>
                        </div>
                    </div>
                    <!-- End:: Candlestick Chart -->

                    <!-- Start:: Price Cycles Placeholder -->
                    <div class="row">
                        <div class="col-xl-12">
                            <div class="card custom-card coming-soon-card">
                                <div class="card-body text-center py-5">
                                    <div class="mb-3">
                                        <span class="avatar avatar-xl avatar-rounded bg-warning-transparent">
                                            <i class="ti ti-chart-arrows fs-2"></i>
                                        </span>
                                    </div>
                                    <h4 class="fw-semibold mb-2">Price Cycles - Coming Soon</h4>
                                    <p class="text-muted mb-0">
                                        Cycle detection and analysis will be available in a future update.<br>
                                        This feature will help identify trading opportunities based on price patterns.
                                    </p>
                                    <div class="mt-3">
                                        <span class="badge bg-warning-transparent fs-12">
                                            <i class="ti ti-clock me-1"></i> Feature in Development
                                        </span>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                    <!-- End:: Price Cycles Placeholder -->

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

            // Update last update time
            function updateLastUpdateInfo() {
                const statusData = window.statusData;
                const info = document.getElementById('lastUpdateInfo');
                const btn = document.getElementById('lastUpdateBtn');
                
                if (!statusData.last_price_time) {
                    btn.className = 'status-btn status-warning';
                    info.textContent = 'No data';
                    return;
                }

                const dataTime = new Date(statusData.last_price_time + ' UTC');
                const now = new Date();
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

            // Initialize on page load
            document.addEventListener('DOMContentLoaded', function() {
                updateLastUpdateInfo();
                setInterval(updateLastUpdateInfo, 5000);
            });

            // Candlestick Chart
            document.addEventListener('DOMContentLoaded', function() {
                const chartData = window.chartData;

                if (!chartData.candles || chartData.candles.length === 0) {
                    return;
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
                    }
                };

                var chart = new ApexCharts(document.querySelector("#sol-candlestick-chart"), options);
                chart.render();
            });
        </script>

<?php $scripts = ob_get_clean(); ?>
<!-- This code is useful for internal scripts -->

<!-- This code use for render base file -->
<?php include __DIR__ . '/pages/layouts/base.php'; ?>
<!-- This code use for render base file -->
