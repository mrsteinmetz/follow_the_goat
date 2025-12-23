<?php
/**
 * Price Cycles Monitor
 * 
 * Displays price cycle data from the cycle_tracker table.
 * Shows when cycles start, end, and their performance metrics.
 */

// --- DuckDB API Client ---
require_once __DIR__ . '/../../includes/DuckDBClient.php';
define('DUCKDB_API_URL', 'http://127.0.0.1:5050');
$duckdb = new DuckDBClient(DUCKDB_API_URL);
$use_duckdb = $duckdb->isAvailable();

// --- Base URL for template ---
$baseUrl = '../..';

// --- Parameters ---
$threshold_filter = isset($_GET['threshold']) ? floatval($_GET['threshold']) : null;
$hours = isset($_GET['hours']) ? $_GET['hours'] : 'all';
$limit = isset($_GET['limit']) ? intval($_GET['limit']) : 50;

// --- Fetch Cycle Data ---
function fetchCycleData($duckdb, $threshold, $hours, $limit) {
    $cycles = [];
    $error_message = null;
    $data_source = "No Data";
    $actual_source = null;

    if ($duckdb->isAvailable()) {
        $response = $duckdb->getCycleTracker($threshold, $hours, $limit);
        
        if ($response && isset($response['cycles'])) {
            $cycles = $response['cycles'];
            $actual_source = $response['source'] ?? 'unknown';
            
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
        $error_message = "DuckDB API is not available. Please start the scheduler: python scheduler/master.py";
    }
    
    return [
        'cycles' => $cycles,
        'error_message' => $error_message,
        'data_source' => $data_source,
        'actual_source' => $actual_source
    ];
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
        'count' => count($result['cycles'])
    ]);
    exit;
}

// Regular page load
$result = fetchCycleData($duckdb, $threshold_filter, $hours, $limit);
$cycles = $result['cycles'];
$error_message = $result['error_message'];
$data_source = $result['data_source'];
$actual_source = $result['actual_source'];

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
                                        <p class="stat-value" id="stat-total" style="color: #fff;"><?php echo count($cycles); ?></p>
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
                                        <p class="stat-label">Avg Max Increase</p>
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

                    <!-- Filters -->
                    <div class="card custom-card mb-4">
                        <div class="card-body py-3">
                            <div class="d-flex flex-wrap gap-2 align-items-center">
                                <span class="text-muted fs-13 me-1">Threshold:</span>
                                <a href="?hours=<?php echo $hours; ?>&limit=<?php echo $limit; ?>" 
                                   class="btn btn-sm btn-outline-light filter-btn <?php echo $threshold_filter === null ? 'active' : ''; ?>">
                                    All
                                </a>
                                <?php foreach ([0.1, 0.2, 0.3, 0.4, 0.5] as $t): ?>
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
                                   class="btn btn-sm btn-outline-light filter-btn <?php echo $hours == 'all' ? 'active' : ''; ?>">
                                    All
                                </a>

                                <span class="ms-auto data-source-info" id="data-source">
                                    Data: <?php echo $data_source; ?>
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
    // Auto-refresh every 5 seconds
    let autoRefreshInterval = null;
    
    function refreshData() {
        const params = new URLSearchParams(window.location.search);
        params.set('ajax', 'refresh');
        
        fetch('?' + params.toString())
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    updateStats(data);
                    updateTable(data.cycles);
                    updateDataSource(data.data_source);
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
        
        document.getElementById('stat-total').textContent = data.count;
        document.getElementById('stat-active').textContent = active;
        document.getElementById('stat-completed').textContent = completed;
        
        const avg = data.count > 0 ? totalIncrease / data.count : 0;
        const avgEl = document.getElementById('stat-avg');
        avgEl.textContent = avg.toFixed(4) + '%';
        avgEl.className = 'stat-value ' + (avg >= 0 ? 'price-up' : 'price-down');
    }
    
    function updateDataSource(source) {
        const el = document.getElementById('data-source');
        if (el) el.textContent = 'Data: ' + source;
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
    
    // Start auto-refresh
    autoRefreshInterval = setInterval(refreshData, 5000);
</script>

<?php $scripts = ob_get_clean(); ?>

<!-- Render using base layout -->
<?php include __DIR__ . '/../layouts/base.php'; ?>
