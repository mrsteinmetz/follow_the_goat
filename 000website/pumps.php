<?php
/**
 * Pump Training Analytics Chart
 *
 * Same SOL candlestick chart as the main dashboard, with markers for the
 * **training/analytics points** the pump engine uses: path-aware "clean_pump"
 * entries (minute-0 points where price rose >= 0.2% within 4 min without
 * crashing). These are the best-entry points the model is trained on.
 *
 * Data: pump_training_labels, written when refresh_pump_model runs.
 * URL: http://195.201.84.5/pumps.php
 */

date_default_timezone_set('UTC');

require_once __DIR__ . '/includes/DatabaseClient.php';
require_once __DIR__ . '/includes/config.php';
$db = new DatabaseClient(DATABASE_API_URL);
$api_available = $db->isAvailable();

$baseUrl = '';

$candle_interval = isset($_GET['candle_interval']) ? max(1/60, min(10, floatval($_GET['candle_interval']))) : 0.5;
$hours = isset($_GET['hours']) ? max(6, min(168, (int)$_GET['hours'])) : 24;
$token = 'SOL';

$end_datetime = gmdate('Y-m-d H:i:s');
$start_datetime = gmdate('Y-m-d H:i:s', strtotime("-{$hours} hours"));

$chart_data = [
    'labels' => [],
    'prices' => [],
    'candles' => [],
    'coin_name' => 'SOL',
];

function aggregateToCandlesPumps(array $prices, float $intervalMinutes = 1): array {
    if (empty($prices)) return [];
    $candles = [];
    $intervalSeconds = $intervalMinutes * 60;
    $buckets = [];
    foreach ($prices as $point) {
        $timestamp = strtotime($point['x'] . ' UTC');
        $bucketTime = floor($timestamp / $intervalSeconds) * $intervalSeconds;
        if (!isset($buckets[$bucketTime])) $buckets[$bucketTime] = [];
        $buckets[$bucketTime][] = $point['y'];
    }
    ksort($buckets);
    foreach ($buckets as $bucketTime => $pricesInBucket) {
        $open = $pricesInBucket[0];
        $high = max($pricesInBucket);
        $low = min($pricesInBucket);
        $close = end($pricesInBucket);
        $candles[] = ['x' => $bucketTime * 1000, 'y' => [$open, $high, $low, $close]];
    }
    return $candles;
}

if ($api_available) {
    $price_response = $db->getPricePoints($token, $start_datetime, $end_datetime);
    if ($price_response && isset($price_response['prices']) && count($price_response['prices']) > 0) {
        $chart_data['prices'] = $price_response['prices'];
    }
}
$chart_data['candles'] = aggregateToCandlesPumps($chart_data['prices'], $candle_interval);

// Training/analytics points: load directly from PostgreSQL (pump_training_labels)
$pump_markers = [];
$pump_db_error = null;
$hours_int = (int) $hours;
try {
    $pdo = new PDO(
        'pgsql:host=' . PG_HOST . ';port=' . PG_PORT . ';dbname=' . PG_DATABASE,
        PG_USER,
        PG_PASSWORD,
        [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
    );
    $sql = "SELECT followed_at, entry_price, max_fwd_pct
        FROM pump_training_labels
        WHERE followed_at >= NOW() - (" . $hours_int . " || ' hours')::interval
        ORDER BY followed_at ASC";
    $stmt = $pdo->query($sql);
    $rows = $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
    foreach ($rows as $e) {
        $followed = $e['followed_at'] ?? null;
        if (!$followed) continue;
        $ts = is_object($followed) ? $followed->format('Y-m-d H:i:s') : (string)$followed;
        $ts_utc = (int) round(strtotime($ts . ' UTC') * 1000);
        $max_fwd = isset($e['max_fwd_pct']) ? floatval($e['max_fwd_pct']) : null;
        $label = $max_fwd !== null ? ('Good +' . number_format($max_fwd, 2) . '%') : 'Good entry';
        $pump_markers[] = [
            'x' => $ts_utc,
            'price' => isset($e['entry_price']) ? floatval($e['entry_price']) : 0,
            'label' => $label,
            'max_fwd_pct' => $max_fwd,
        ];
    }
} catch (Throwable $e) {
    $pump_db_error = $e->getMessage();
}

$json_chart_data = json_encode($chart_data);
$json_pump_markers = json_encode($pump_markers);
?>
<?php ob_start(); ?>
<style>
    #sol-pumps-chart { min-height: 450px; }
    .pump-legend { display: flex; gap: 1rem; flex-wrap: wrap; align-items: center; margin-bottom: 0.75rem; }
    .pump-legend span { font-size: 0.8rem; color: rgba(255,255,255,0.8); }
    .pump-legend .dot { width: 10px; height: 10px; border-radius: 50%; display: inline-block; margin-right: 0.35rem; }
    .pump-legend .dot-good { background: #22c55e; }
</style>
<?php $styles = ob_get_clean(); ?>

<?php ob_start(); ?>

<div class="page-header-breadcrumb mb-3">
    <div class="d-flex align-center justify-content-between flex-wrap">
        <h1 class="page-title fw-medium fs-18 mb-0">Pump Training Analytics — Best Entry Points</h1>
        <ol class="breadcrumb mb-0">
            <li class="breadcrumb-item"><a href="<?php echo $baseUrl ? $baseUrl . '/' : ''; ?>">Dashboards</a></li>
            <li class="breadcrumb-item active" aria-current="page">Pumps</li>
        </ol>
    </div>
</div>

<?php if (!$api_available): ?>
<div class="alert alert-danger">
    <i class="ti ti-alert-circle me-2"></i>Website API is not available. Start: <code>python scheduler/website_api.py</code>
</div>
<?php else: ?>

<div class="card custom-card mb-3">
    <div class="card-header">
        <div class="card-title"><i class="ti ti-chart-candle me-2"></i>SOL Price with Training Entry Points</div>
        <div class="ms-auto">
            <span class="badge bg-primary-transparent"><?php echo count($chart_data['candles']); ?> candles</span>
            <span class="badge bg-success-transparent"><?php echo count($pump_markers); ?> good-entry points</span>
        </div>
    </div>
    <div class="card-body">
        <div class="pump-legend">
            <span><span class="dot dot-good"></span>Clean-pump entry (price rose ≥0.2% within 4 min — training labels)</span>
            <span class="text-muted ms-2"><strong>Zoom:</strong> Ctrl + scroll over chart, or drag to select a range. Scroll (no Ctrl) to pan. Bottom bar: drag to move in time.</span>
        </div>
        <?php if (count($pump_markers) === 0): ?>
        <div class="alert alert-info py-2 mb-3 small">
            No training entry points shown for the last <?php echo (int)$hours; ?>h.
            <?php if ($pump_db_error): ?>
            <br><strong>DB:</strong> <?php echo htmlspecialchars($pump_db_error); ?> — check PHP can connect to PostgreSQL (PG_* / DB_*).
            <?php else: ?>
            The table <code>pump_training_labels</code> has 1600+ rows (clean-pump entries). Try <strong>48h</strong> or <strong>72h</strong>. If still none, PHP may be using a different DB than Python (same host/user/db).
            <?php endif; ?>
        </div>
        <?php endif; ?>
        <form method="get" class="row g-2 align-items-center mb-3">
            <div class="col-auto">
                <label class="form-label mb-0">Hours</label>
                <select name="hours" class="form-select form-select-sm" style="width: auto;">
                    <?php foreach ([12, 24, 48, 72, 168] as $h): ?>
                    <option value="<?php echo $h; ?>" <?php echo $hours == $h ? 'selected' : ''; ?>><?php echo $h; ?>h</option>
                    <?php endforeach; ?>
                </select>
            </div>
            <div class="col-auto">
                <label class="form-label mb-0">Candle</label>
                <select name="candle_interval" class="form-select form-select-sm" style="width: auto;">
                    <?php
                    $interval_options = [
                        [1/60, '1s'], [5/60, '5s'], [10/60, '10s'], [15/60, '15s'], [20/60, '20s'], [25/60, '25s'],
                        [0.5, '30s'],
                    ];
                    foreach ($interval_options as $opt):
                        $val = $opt[0]; $label = $opt[1];
                        $sel = abs($candle_interval - $val) < 0.001 ? 'selected' : '';
                    ?>
                    <option value="<?php echo $val; ?>" <?php echo $sel; ?>><?php echo $label; ?></option>
                    <?php endforeach; ?>
                    <?php for ($i = 1; $i <= 10; $i++): ?>
                    <option value="<?php echo $i; ?>" <?php echo abs($candle_interval - $i) < 0.01 ? 'selected' : ''; ?>><?php echo $i; ?>m</option>
                    <?php endfor; ?>
                </select>
            </div>
            <div class="col-auto">
                <button type="submit" class="btn btn-primary btn-sm">Apply</button>
            </div>
        </form>
        <?php if (!empty($chart_data['candles'])): ?>
        <div id="sol-pumps-chart"></div>
        <?php else: ?>
        <div class="text-center py-5 text-muted">
            <i class="ti ti-chart-candle fs-1 d-block mb-2"></i>
            No price data in this range.
        </div>
        <?php endif; ?>
    </div>
</div>

<?php endif; ?>

<?php $content = ob_get_clean(); ?>

<?php ob_start(); ?>
<script src="https://cdn.amcharts.com/lib/5/index.js"></script>
<script src="https://cdn.amcharts.com/lib/5/xy.js"></script>
<script src="https://cdn.amcharts.com/lib/5/stock.js"></script>
<script src="https://cdn.amcharts.com/lib/5/themes/Dark.js"></script>
<script>
document.addEventListener('DOMContentLoaded', function() {
    const chartData = <?php echo $json_chart_data; ?>;
    const pumpMarkers = <?php echo $json_pump_markers; ?>;

    if (!chartData.candles || chartData.candles.length === 0) return;

    var container = document.getElementById("sol-pumps-chart");
    if (!container) return;

    var am5Data = chartData.candles.map(function(c) {
        var o = c.y[0], h = c.y[1], l = c.y[2], cl = c.y[3];
        return { date: c.x, open: o, high: h, low: l, close: cl };
    });

    var root = am5.Root.new(container);
    root.setThemes([am5themes_Dark.new(root)]);

    var stockChart = root.container.children.push(am5stock.StockChart.new(root, {}));

    var mainPanel = stockChart.panels.push(am5stock.StockPanel.new(root, {
        wheelY: "zoomX",
        wheelX: "panX",
        panX: true,
        panY: true,
        pinchZoomX: true
    }));

    var valueAxis = mainPanel.yAxes.push(am5xy.ValueAxis.new(root, {
        renderer: am5xy.AxisRendererY.new(root, { pan: "zoom" }),
        numberFormat: "#'$'#.00##",
        tooltip: am5.Tooltip.new(root, {})
    }));

    var dateAxis = mainPanel.xAxes.push(am5xy.GaplessDateAxis.new(root, {
        baseInterval: { timeUnit: "minute", count: 1 },
        renderer: am5xy.AxisRendererX.new(root, { pan: "zoom" }),
        tooltip: am5.Tooltip.new(root, {})
    }));

    var candleSeries = mainPanel.series.push(am5xy.CandlestickSeries.new(root, {
        name: "SOL",
        openValueYField: "open",
        highValueYField: "high",
        lowValueYField: "low",
        valueYField: "close",
        valueXField: "date",
        xAxis: dateAxis,
        yAxis: valueAxis,
        risingFill: am5.color("#32d484"),
        risingStroke: am5.color("#32d484"),
        fallingFill: am5.color("#ff6757"),
        fallingStroke: am5.color("#ff6757")
    }));

    candleSeries.data.setAll(am5Data);

    stockChart.set("stockSeries", candleSeries);

    var cursor = mainPanel.set("cursor", am5xy.XYCursor.new(root, {
        behavior: "zoomX",
        xAxis: dateAxis,
        yAxis: valueAxis,
        snapToSeries: [candleSeries],
        snapToSeriesBy: "y!"
    }));

    function setWheelBehavior(zoom) {
        mainPanel.set("wheelX", "panX");
        mainPanel.set("wheelY", zoom ? "zoomX" : "panX");
    }
    document.addEventListener("keydown", function(ev) { if (ev.ctrlKey) setWheelBehavior(true); });
    document.addEventListener("keyup", function(ev) { if (!ev.ctrlKey) setWheelBehavior(false); });
    mainPanel.plotContainer.events.on("wheel", function(ev) {
        if (ev.originalEvent.ctrlKey) ev.originalEvent.preventDefault();
    });

    if (pumpMarkers && pumpMarkers.length > 0) {
        var markerData = pumpMarkers.map(function(m) {
            return { date: m.x, value: m.price, label: m.label || "Good entry" };
        });
        var markerSeries = mainPanel.series.push(am5xy.LineSeries.new(root, {
            name: "Entry",
            valueYField: "value",
            valueXField: "date",
            xAxis: dateAxis,
            yAxis: valueAxis,
            stroke: am5.color(0x22c55e),
            strokeWidth: 2
        }));
        markerSeries.strokes.template.set("visible", false);
        markerSeries.bullets.push(function() {
            var circle = am5.Circle.new(root, {
                radius: 6,
                fill: am5.color(0x22c55e),
                stroke: am5.color(0xffffff),
                strokeWidth: 2
            });
            circle.set("tooltipText", "{label}");
            return am5.Bullet.new(root, { sprite: circle });
        });
        markerSeries.data.setAll(markerData);
    }

    var scrollbar = mainPanel.set("scrollbarX", am5xy.XYChartScrollbar.new(root, {
        orientation: "horizontal",
        height: 50
    }));
    stockChart.toolsContainer.children.push(scrollbar);

    var sbDateAxis = scrollbar.chart.xAxes.push(am5xy.GaplessDateAxis.new(root, {
        baseInterval: { timeUnit: "minute", count: 1 },
        renderer: am5xy.AxisRendererX.new(root, {})
    }));
    var sbValueAxis = scrollbar.chart.yAxes.push(am5xy.ValueAxis.new(root, {
        renderer: am5xy.AxisRendererY.new(root, {})
    }));
    var sbSeries = scrollbar.chart.series.push(am5xy.LineSeries.new(root, {
        valueYField: "close",
        valueXField: "date",
        xAxis: sbDateAxis,
        yAxis: sbValueAxis
    }));
    sbSeries.data.setAll(am5Data);

    candleSeries.appear(1000);
    mainPanel.appear(1000, 100);
});
</script>
<?php $scripts = ob_get_clean(); ?>

<?php include __DIR__ . '/pages/layouts/base.php'; ?>
