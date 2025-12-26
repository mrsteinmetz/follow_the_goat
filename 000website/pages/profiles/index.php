<?php
/**
 * Wallet Profiles Monitor
 * 
 * Displays wallet profile data from the wallet_profiles table.
 * Shows trade-to-cycle mappings with filtering by threshold and ordering.
 */

// --- DuckDB API Client ---
require_once __DIR__ . '/../../includes/DuckDBClient.php';
define('DUCKDB_API_URL', 'http://127.0.0.1:5050');
$duckdb = new DuckDBClient(DUCKDB_API_URL);
$use_duckdb = $duckdb->isAvailable();

// --- Base URL for template ---
$baseUrl = '../..';

// --- Parameters ---
$threshold_filter = isset($_GET['threshold']) ? floatval($_GET['threshold']) : 0.3;
$hours = isset($_GET['hours']) ? $_GET['hours'] : '24';
$limit = isset($_GET['limit']) ? intval($_GET['limit']) : 100;
$order_by = isset($_GET['order_by']) ? $_GET['order_by'] : 'avg_gain';
$refresh_interval = isset($_GET['refresh']) ? intval($_GET['refresh']) : 30;

// --- Available thresholds (from cycle_tracker) ---
$available_thresholds = [0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5];

// --- Fetch Profile Data ---
function fetchProfileData($duckdb, $threshold, $hours, $limit, $order_by) {
    $profiles = [];
    $error_message = null;
    $data_source = "No Data";
    $actual_source = null;
    $aggregated = false;

    if ($duckdb->isAvailable()) {
        $response = $duckdb->getProfiles($threshold, $hours, $limit, $order_by);
        
        if ($response && isset($response['profiles'])) {
            $profiles = $response['profiles'];
            $actual_source = $response['source'] ?? 'unknown';
            $aggregated = $response['aggregated'] ?? false;
            
            switch ($actual_source) {
                case 'engine':
                    $data_source = "Engine";
                    break;
                case 'mysql':
                    $data_source = "MySQL";
                    break;
                default:
                    $data_source = "DuckDB";
            }
        } else {
            $data_source = "No Data";
        }
    } else {
        $error_message = "DuckDB API is not available. Please start the scheduler: python scheduler/master.py";
    }
    
    return [
        'profiles' => $profiles,
        'error_message' => $error_message,
        'data_source' => $data_source,
        'actual_source' => $actual_source,
        'aggregated' => $aggregated
    ];
}

// --- Fetch Statistics ---
function fetchProfileStats($duckdb, $threshold, $hours) {
    if ($duckdb->isAvailable()) {
        $response = $duckdb->getProfilesStats($threshold, $hours);
        if ($response && isset($response['stats'])) {
            return $response['stats'];
        }
    }
    return null;
}

// Check if this is an AJAX request for data refresh
if (isset($_GET['ajax']) && $_GET['ajax'] === 'refresh') {
    header('Content-Type: application/json');
    $result = fetchProfileData($duckdb, $threshold_filter, $hours, $limit, $order_by);
    $stats = fetchProfileStats($duckdb, $threshold_filter, $hours);
    
    echo json_encode([
        'success' => true,
        'profiles' => $result['profiles'],
        'error_message' => $result['error_message'],
        'data_source' => $result['data_source'],
        'actual_source' => $result['actual_source'],
        'aggregated' => $result['aggregated'],
        'stats' => $stats,
        'count' => count($result['profiles'])
    ]);
    exit;
}

// Regular page load
$result = fetchProfileData($duckdb, $threshold_filter, $hours, $limit, $order_by);
$profiles = $result['profiles'];
$error_message = $result['error_message'];
$data_source = $result['data_source'];
$actual_source = $result['actual_source'];
$aggregated = $result['aggregated'];

// Fetch stats
$stats = fetchProfileStats($duckdb, $threshold_filter, $hours);
$total_profiles = $stats['total_profiles'] ?? 0;
$unique_wallets = $stats['unique_wallets'] ?? 0;
$unique_cycles = $stats['unique_cycles'] ?? 0;
$avg_entry_price = $stats['avg_entry_price'] ?? 0;
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
    .wallet-address {
        font-family: monospace;
        font-size: 0.8rem;
        max-width: 120px;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
    }
    .short-badge {
        font-size: 0.7rem;
        padding: 0.2rem 0.4rem;
    }
</style>

<?php $styles = ob_get_clean(); ?>

<!-- Content for this page -->
<?php ob_start(); ?>

                    <!-- Page Header -->
                    <div class="d-flex align-items-center justify-content-between flex-wrap gap-2 mb-4">
                        <div>
                            <h1 class="page-title fw-semibold fs-18 mb-0">Wallet Profiles</h1>
                            <p class="text-muted mb-0 fs-13">Track wallet trading patterns across price cycles</p>
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
                                        <p class="stat-label">Total Profiles</p>
                                        <p class="stat-value" id="stat-total" style="color: #fff;"><?php echo number_format($total_profiles); ?></p>
                                    </div>
                                    <div class="stat-icon bg-primary-transparent text-primary">
                                        <i class="ri-user-line"></i>
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div class="col-sm-6 col-xl-3">
                            <div class="stat-card">
                                <div class="d-flex justify-content-between align-items-start">
                                    <div>
                                        <p class="stat-label">Unique Wallets</p>
                                        <p class="stat-value price-up" id="stat-wallets"><?php echo number_format($unique_wallets); ?></p>
                                    </div>
                                    <div class="stat-icon bg-success-transparent text-success">
                                        <i class="ri-wallet-line"></i>
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div class="col-sm-6 col-xl-3">
                            <div class="stat-card">
                                <div class="d-flex justify-content-between align-items-start">
                                    <div>
                                        <p class="stat-label">Unique Cycles</p>
                                        <p class="stat-value text-info" id="stat-cycles"><?php echo number_format($unique_cycles); ?></p>
                                    </div>
                                    <div class="stat-icon bg-info-transparent text-info">
                                        <i class="ri-loop-right-line"></i>
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div class="col-sm-6 col-xl-3">
                            <div class="stat-card">
                                <div class="d-flex justify-content-between align-items-start">
                                    <div>
                                        <p class="stat-label">Total Invested</p>
                                        <p class="stat-value text-secondary" id="stat-total-invested">
                                            $<?php echo number_format(floatval($stats['total_invested'] ?? 0), 0); ?>
                                        </p>
                                    </div>
                                    <div class="stat-icon bg-secondary-transparent text-secondary">
                                        <i class="ri-money-dollar-circle-line"></i>
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
                                <?php foreach ($available_thresholds as $t): ?>
                                <a href="?threshold=<?php echo $t; ?>&hours=<?php echo $hours; ?>&limit=<?php echo $limit; ?>&order_by=<?php echo $order_by; ?>" 
                                   class="btn btn-sm btn-outline-light filter-btn <?php echo $threshold_filter == $t ? 'active' : ''; ?>">
                                    <?php echo $t; ?>%
                                </a>
                                <?php endforeach; ?>
                                
                                <span class="text-muted fs-13 ms-3 me-1">Time:</span>
                                <a href="?threshold=<?php echo $threshold_filter; ?>&hours=1&limit=<?php echo $limit; ?>&order_by=<?php echo $order_by; ?>" 
                                   class="btn btn-sm btn-outline-light filter-btn <?php echo $hours == '1' ? 'active' : ''; ?>">
                                    1h
                                </a>
                                <a href="?threshold=<?php echo $threshold_filter; ?>&hours=24&limit=<?php echo $limit; ?>&order_by=<?php echo $order_by; ?>" 
                                   class="btn btn-sm btn-outline-light filter-btn <?php echo $hours == '24' ? 'active' : ''; ?>">
                                    24h
                                </a>
                                <a href="?threshold=<?php echo $threshold_filter; ?>&hours=all&limit=<?php echo $limit; ?>&order_by=<?php echo $order_by; ?>" 
                                   class="btn btn-sm btn-outline-light filter-btn <?php echo $hours == 'all' ? 'active' : ''; ?>">
                                    All
                                </a>

                                <span class="text-muted fs-13 ms-3 me-1">Order:</span>
                                <a href="?threshold=<?php echo $threshold_filter; ?>&hours=<?php echo $hours; ?>&limit=<?php echo $limit; ?>&order_by=avg_gain" 
                                   class="btn btn-sm btn-outline-light filter-btn <?php echo $order_by == 'avg_gain' ? 'active' : ''; ?>">
                                    Avg Gain
                                </a>
                                <a href="?threshold=<?php echo $threshold_filter; ?>&hours=<?php echo $hours; ?>&limit=<?php echo $limit; ?>&order_by=trade_count" 
                                   class="btn btn-sm btn-outline-light filter-btn <?php echo $order_by == 'trade_count' ? 'active' : ''; ?>">
                                    Trade Count
                                </a>
                                <a href="?threshold=<?php echo $threshold_filter; ?>&hours=<?php echo $hours; ?>&limit=<?php echo $limit; ?>&order_by=recent" 
                                   class="btn btn-sm btn-outline-light filter-btn <?php echo $order_by == 'recent' ? 'active' : ''; ?>">
                                    Recent
                                </a>

                                <span class="ms-auto data-source-info" id="data-source">
                                    Data: <?php echo $data_source; ?>
                                </span>
                            </div>
                        </div>
                    </div>

                    <!-- Profiles Table -->
                    <div class="card custom-card">
                        <div class="card-header">
                            <h5 class="card-title mb-0">Wallet Summary</h5>
                        </div>
                        <div class="card-body p-0">
                            <div class="table-responsive">
                                <table class="table mb-0 data-table">
                                    <thead>
                                        <tr>
                                            <th>Wallet</th>
                                            <th>Avg Gain</th>
                                            <th>&lt; <?php echo number_format($threshold_filter, 2); ?>%</th>
                                            <th>&gt;= <?php echo number_format($threshold_filter, 2); ?>%</th>
                                            <th>Total Trades</th>
                                            <th>Total Invested</th>
                                            <th>Latest Trade</th>
                                        </tr>
                                    </thead>
                                    <tbody id="profiles-table-body">
                                        <?php if (empty($profiles)): ?>
                                        <tr>
                                            <td colspan="7" class="text-center py-4 text-muted">
                                                <i class="ri-user-search-line fs-24 d-block mb-2"></i>
                                                No profiles found. Waiting for data...
                                            </td>
                                        </tr>
                                        <?php else: ?>
                                        <?php foreach ($profiles as $profile): 
                                            $avg_potential_gain = floatval($profile['avg_potential_gain'] ?? 0);
                                            $total_invested = floatval($profile['total_invested'] ?? 0);
                                            $trades_below = intval($profile['trades_below_threshold'] ?? 0);
                                            $trades_at_above = intval($profile['trades_at_above_threshold'] ?? 0);
                                            $trade_count = intval($profile['trade_count'] ?? 0);
                                        ?>
                                        <tr>
                                            <td class="wallet-address" title="<?php echo htmlspecialchars($profile['wallet_address'] ?? '-'); ?>">
                                                <?php echo htmlspecialchars(substr($profile['wallet_address'] ?? '-', 0, 8) . '...'); ?>
                                            </td>
                                            <td class="font-monospace <?php echo $avg_potential_gain >= 0 ? 'price-up' : 'price-down'; ?>">
                                                <?php echo ($avg_potential_gain >= 0 ? '+' : '') . number_format($avg_potential_gain, 3); ?>%
                                            </td>
                                            <td class="fw-medium">
                                                <span class="badge bg-danger-transparent text-danger"><?php echo number_format($trades_below); ?></span>
                                            </td>
                                            <td class="fw-medium">
                                                <span class="badge bg-success-transparent text-success"><?php echo number_format($trades_at_above); ?></span>
                                            </td>
                                            <td class="fw-medium">
                                                <span class="badge bg-primary-transparent text-primary"><?php echo number_format($trade_count); ?></span>
                                            </td>
                                            <td class="font-monospace">
                                                $<?php echo number_format($total_invested, 2); ?>
                                            </td>
                                            <td class="fs-12">
                                                <?php 
                                                $latest = $profile['latest_trade'] ?? $profile['last_trade_time'] ?? null;
                                                echo $latest ? gmdate('M j, H:i', strtotime($latest)) : '-'; 
                                                ?>
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
    // Auto-refresh interval
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
                    updateStats(data.stats);
                    updateTable(data.profiles);
                    updateDataSource(data.data_source);
                }
            })
            .catch(err => {
                console.error('Refresh failed:', err);
            });
    }
    
    function updateStats(stats) {
        if (!stats) return;
        
        document.getElementById('stat-total').textContent = (stats.total_profiles || 0).toLocaleString();
        document.getElementById('stat-wallets').textContent = (stats.unique_wallets || 0).toLocaleString();
        document.getElementById('stat-cycles').textContent = (stats.unique_cycles || 0).toLocaleString();
        document.getElementById('stat-total-invested').textContent = '$' + Math.round(parseFloat(stats.total_invested || 0)).toLocaleString();
    }
    
    function updateDataSource(source) {
        const el = document.getElementById('data-source');
        if (el) {
            el.innerHTML = 'Data: ' + source;
        }
    }
    
    function updateTable(profiles) {
        const tbody = document.getElementById('profiles-table-body');
        
        if (!profiles || profiles.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="7" class="text-center py-4 text-muted">
                        <i class="ri-user-search-line fs-24 d-block mb-2"></i>
                        No profiles found. Waiting for data...
                    </td>
                </tr>
            `;
            return;
        }
        
        tbody.innerHTML = profiles.map(profile => {
            const avgPotentialGain = parseFloat(profile.avg_potential_gain || 0);
            const totalInvested = parseFloat(profile.total_invested || 0);
            const tradesBelow = parseInt(profile.trades_below_threshold || 0);
            const tradesAtAbove = parseInt(profile.trades_at_above_threshold || 0);
            const tradeCount = parseInt(profile.trade_count || 0);
            const gainClass = avgPotentialGain >= 0 ? 'price-up' : 'price-down';
            const gainSign = avgPotentialGain >= 0 ? '+' : '';
            
            return `
                <tr>
                    <td class="wallet-address" title="${profile.wallet_address || '-'}">
                        ${(profile.wallet_address || '-').substring(0, 8)}...
                    </td>
                    <td class="font-monospace ${gainClass}">${gainSign}${avgPotentialGain.toFixed(3)}%</td>
                    <td class="fw-medium">
                        <span class="badge bg-danger-transparent text-danger">${tradesBelow.toLocaleString()}</span>
                    </td>
                    <td class="fw-medium">
                        <span class="badge bg-success-transparent text-success">${tradesAtAbove.toLocaleString()}</span>
                    </td>
                    <td class="fw-medium">
                        <span class="badge bg-primary-transparent text-primary">${tradeCount.toLocaleString()}</span>
                    </td>
                    <td class="font-monospace">$${totalInvested.toFixed(2)}</td>
                    <td class="fs-12">${formatDateShort(profile.latest_trade)}</td>
                </tr>
            `;
        }).join('');
    }
    
    function formatDate(dateStr) {
        if (!dateStr) return '-';
        const date = new Date(dateStr);
        const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
        return `${months[date.getUTCMonth()]} ${date.getUTCDate()}, ${String(date.getUTCHours()).padStart(2,'0')}:${String(date.getUTCMinutes()).padStart(2,'0')}:${String(date.getUTCSeconds()).padStart(2,'0')}`;
    }
    
    function formatDateShort(dateStr) {
        if (!dateStr) return '-';
        const date = new Date(dateStr);
        const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
        return `${months[date.getUTCMonth()]} ${date.getUTCDate()}, ${String(date.getUTCHours()).padStart(2,'0')}:${String(date.getUTCMinutes()).padStart(2,'0')}`;
    }
    
    // Start auto-refresh with configured interval
    if (refreshIntervalSeconds > 0) {
        autoRefreshInterval = setInterval(refreshData, refreshIntervalSeconds * 1000);
    }
</script>

<?php $scripts = ob_get_clean(); ?>

<!-- Render using base layout -->
<?php include __DIR__ . '/../layouts/base.php'; ?>

