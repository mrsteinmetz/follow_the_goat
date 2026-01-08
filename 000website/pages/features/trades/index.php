<?php
/**
 * Trades List Page - Recent Trades with Trail Data
 * Shows 100 recent trades and links to detail view with 15-minute trail data
 */

// --- DuckDB API Client ---
require_once __DIR__ . '/../../../includes/DuckDBClient.php';
define('DUCKDB_API_URL', 'http://127.0.0.1:5051');
$duckdb = new DuckDBClient(DUCKDB_API_URL);
$use_duckdb = $duckdb->isAvailable();

// --- Base URL for template ---
$baseUrl = '';

$error_message = '';
$trades = [];

// Parameters
$play_id = isset($_GET['play_id']) ? (int)$_GET['play_id'] : null;
$status = $_GET['status'] ?? null;
$hours = $_GET['hours'] ?? '24';
$limit = isset($_GET['limit']) ? (int)$_GET['limit'] : 100;
$has_potential_gains = $_GET['has_potential_gains'] ?? null;

// Fetch trades
if ($use_duckdb) {
    $response = $duckdb->getBuyins($play_id, $status, $hours, $limit);
    if ($response && isset($response['buyins'])) {
        $trades = $response['buyins'];
        
        // Filter by potential_gains if requested
        if ($has_potential_gains === 'yes') {
            $trades = array_filter($trades, function($trade) {
                return isset($trade['potential_gains']) && $trade['potential_gains'] !== null;
            });
        } elseif ($has_potential_gains === 'no') {
            $trades = array_filter($trades, function($trade) {
                return !isset($trade['potential_gains']) || $trade['potential_gains'] === null;
            });
        }
    }
} else {
    $error_message = "DuckDB API is not available. Please start the scheduler: python scheduler/master.py";
}

// Fetch plays for filter dropdown
$plays = [];
if ($use_duckdb) {
    $plays_response = $duckdb->getPlays();
    if ($plays_response && isset($plays_response['plays'])) {
        $plays = $plays_response['plays'];
    }
}

// --- Page Styles ---
ob_start();
?>
<style>
    .trades-table {
        font-size: 0.85rem;
    }
    
    .trades-table th {
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        color: var(--text-muted);
        font-weight: 600;
        border-bottom: 2px solid var(--default-border);
    }
    
    .trades-table td {
        vertical-align: middle;
        padding: 0.75rem 0.5rem;
    }
    
    .trades-table tbody tr {
        cursor: pointer;
        transition: all 0.15s ease;
    }
    
    .trades-table tbody tr:hover {
        background: rgba(var(--primary-rgb), 0.08) !important;
    }
    
    .status-badge {
        padding: 0.25rem 0.6rem;
        border-radius: 4px;
        font-size: 0.7rem;
        font-weight: 600;
        text-transform: uppercase;
    }
    
    .status-pending { background: rgba(var(--warning-rgb), 0.15); color: rgb(var(--warning-rgb)); }
    .status-sold { background: rgba(var(--success-rgb), 0.15); color: rgb(var(--success-rgb)); }
    .status-no_go { background: rgba(var(--danger-rgb), 0.15); color: rgb(var(--danger-rgb)); }
    .status-validating { background: rgba(var(--info-rgb), 0.15); color: rgb(var(--info-rgb)); }
    .status-error { background: rgba(var(--danger-rgb), 0.15); color: rgb(var(--danger-rgb)); }
    
    .profit-positive { color: rgb(var(--success-rgb)); font-weight: 600; }
    .profit-negative { color: rgb(var(--danger-rgb)); font-weight: 600; }
    .profit-zero { color: var(--text-muted); }
    
    .price-value {
        font-family: 'SF Mono', 'Monaco', 'Consolas', monospace;
        font-size: 0.85rem;
    }
    
    .wallet-address {
        font-family: 'SF Mono', 'Monaco', 'Consolas', monospace;
        font-size: 0.75rem;
        color: var(--text-muted);
    }
    
    .time-relative {
        font-size: 0.75rem;
        color: var(--text-muted);
    }
    
    .filter-card {
        background: var(--custom-white);
        border: 1px solid var(--default-border);
        border-radius: 0.5rem;
        padding: 1rem;
        margin-bottom: 1rem;
    }
    
    .stats-row {
        display: flex;
        gap: 1.5rem;
        flex-wrap: wrap;
        margin-bottom: 1rem;
    }
    
    .stat-item {
        display: flex;
        flex-direction: column;
        gap: 0.2rem;
    }
    
    .stat-label {
        font-size: 0.65rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        color: var(--text-muted);
    }
    
    .stat-value {
        font-size: 1.2rem;
        font-weight: 700;
        color: var(--default-text-color);
    }
    
    .stat-value.positive { color: rgb(var(--success-rgb)); }
    .stat-value.negative { color: rgb(var(--danger-rgb)); }
    
    /* API Status Badge */
    .api-status-badge {
        position: fixed;
        top: 70px;
        right: 20px;
        z-index: 9999;
        padding: 4px 12px;
        border-radius: 4px;
        font-size: 11px;
        font-weight: 600;
    }
    
    .empty-state {
        text-align: center;
        padding: 3rem 2rem;
        border: 1px dashed var(--default-border);
        border-radius: 0.5rem;
    }
    
    .trail-indicator {
        display: inline-flex;
        align-items: center;
        gap: 0.25rem;
        padding: 0.2rem 0.5rem;
        border-radius: 3px;
        font-size: 0.65rem;
        font-weight: 600;
    }
    
    .trail-indicator.has-trail {
        background: rgba(var(--success-rgb), 0.15);
        color: rgb(var(--success-rgb));
    }
    
    .trail-indicator.no-trail {
        background: rgba(var(--secondary-rgb), 0.15);
        color: var(--text-muted);
    }
    
    /* Live current price indicator for pending trades */
    .current-price-live {
        position: relative;
        color: rgb(var(--info-rgb));
    }
    
    .current-price-live::after {
        content: '';
        display: inline-block;
        width: 6px;
        height: 6px;
        background: rgb(var(--info-rgb));
        border-radius: 50%;
        margin-left: 4px;
        animation: pulse-dot 1.5s ease-in-out infinite;
    }
    
    @keyframes pulse-dot {
        0%, 100% { opacity: 1; transform: scale(1); }
        50% { opacity: 0.5; transform: scale(0.8); }
    }
</style>
<?php
$styles = ob_get_clean();

// --- Calculate Stats ---
$stats = [
    'total' => count($trades),
    'pending' => 0,
    'sold' => 0,
    'no_go' => 0,
    'total_profit' => 0,
];

foreach ($trades as $trade) {
    $status_val = $trade['our_status'] ?? '';
    if ($status_val === 'pending') $stats['pending']++;
    elseif ($status_val === 'sold') $stats['sold']++;
    elseif ($status_val === 'no_go') $stats['no_go']++;
    
    if (isset($trade['our_profit_loss'])) {
        $stats['total_profit'] += floatval($trade['our_profit_loss']);
    }
}

// --- Page Content ---
ob_start();
?>

<!-- API Status Badge -->
<div class="api-status-badge" style="background: <?php echo $use_duckdb ? 'rgb(var(--success-rgb))' : 'rgb(var(--danger-rgb))'; ?>; color: white;">
    🦆 <?php echo $use_duckdb ? 'API Connected' : 'API Disconnected'; ?>
</div>

<!-- Page Header -->
<div class="d-flex align-items-center justify-content-between page-header-breadcrumb flex-wrap gap-2 mb-3">
    <div>
        <nav>
            <ol class="breadcrumb mb-1">
                <li class="breadcrumb-item"><a href="<?php echo $baseUrl; ?>/">Dashboard</a></li>
                <li class="breadcrumb-item"><a href="#">Features</a></li>
                <li class="breadcrumb-item active" aria-current="page">Trades</li>
            </ol>
        </nav>
        <h1 class="page-title fw-medium fs-18 mb-0">Recent Trades</h1>
    </div>
</div>

<!-- Messages -->
<?php if ($error_message): ?>
<div class="alert alert-danger alert-dismissible fade show" role="alert">
    <i class="ri-error-warning-line me-2"></i><?php echo htmlspecialchars($error_message); ?>
    <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
</div>
<?php endif; ?>

<!-- Filters -->
<div class="filter-card">
    <form method="GET" action="" class="row g-3 align-items-end">
        <div class="col-md-2">
            <label for="play_id" class="form-label">Play</label>
            <select name="play_id" id="play_id" class="form-select form-select-sm">
                <option value="">All Plays</option>
                <?php foreach ($plays as $p): ?>
                <option value="<?php echo $p['id']; ?>" <?php echo $play_id == $p['id'] ? 'selected' : ''; ?>>
                    <?php echo htmlspecialchars($p['name']); ?>
                </option>
                <?php endforeach; ?>
            </select>
        </div>
        
        <div class="col-md-2">
            <label for="status" class="form-label">Status</label>
            <select name="status" id="status" class="form-select form-select-sm">
                <option value="">All Status</option>
                <option value="pending" <?php echo $status === 'pending' ? 'selected' : ''; ?>>Pending</option>
                <option value="sold" <?php echo $status === 'sold' ? 'selected' : ''; ?>>Sold</option>
                <option value="no_go" <?php echo $status === 'no_go' ? 'selected' : ''; ?>>No Go</option>
                <option value="validating" <?php echo $status === 'validating' ? 'selected' : ''; ?>>Validating</option>
            </select>
        </div>
        
        <div class="col-md-2">
            <label for="hours" class="form-label">Time Window</label>
            <select name="hours" id="hours" class="form-select form-select-sm">
                <option value="1" <?php echo $hours === '1' ? 'selected' : ''; ?>>Last Hour</option>
                <option value="6" <?php echo $hours === '6' ? 'selected' : ''; ?>>Last 6 Hours</option>
                <option value="12" <?php echo $hours === '12' ? 'selected' : ''; ?>>Last 12 Hours</option>
                <option value="24" <?php echo $hours === '24' ? 'selected' : ''; ?>>Last 24 Hours</option>
                <option value="all" <?php echo $hours === 'all' ? 'selected' : ''; ?>>All (MySQL)</option>
            </select>
        </div>
        
        <div class="col-md-2">
            <label for="limit" class="form-label">Limit</label>
            <select name="limit" id="limit" class="form-select form-select-sm">
                <option value="25" <?php echo $limit === 25 ? 'selected' : ''; ?>>25</option>
                <option value="50" <?php echo $limit === 50 ? 'selected' : ''; ?>>50</option>
                <option value="100" <?php echo $limit === 100 ? 'selected' : ''; ?>>100</option>
                <option value="200" <?php echo $limit === 200 ? 'selected' : ''; ?>>200</option>
            </select>
        </div>
        
        <div class="col-md-2">
            <label for="has_potential_gains" class="form-label">Potential Gains</label>
            <select name="has_potential_gains" id="has_potential_gains" class="form-select form-select-sm">
                <option value="">All Trades</option>
                <option value="yes" <?php echo $has_potential_gains === 'yes' ? 'selected' : ''; ?>>Has Potential Gains</option>
                <option value="no" <?php echo $has_potential_gains === 'no' ? 'selected' : ''; ?>>No Potential Gains</option>
            </select>
        </div>
        
        <div class="col-md-2">
            <button type="submit" class="btn btn-primary btn-sm w-100">
                <i class="ri-filter-line me-1"></i>Filter
            </button>
        </div>
        
        <div class="col-md-2">
            <a href="/pages/features/trades/" class="btn btn-outline-secondary btn-sm w-100">
                <i class="ri-refresh-line me-1"></i>Reset
            </a>
        </div>
    </form>
</div>

<!-- Stats -->
<div class="stats-row">
    <div class="stat-item">
        <span class="stat-label">Total Trades</span>
        <span class="stat-value"><?php echo $stats['total']; ?></span>
    </div>
    <div class="stat-item">
        <span class="stat-label">Pending</span>
        <span class="stat-value" style="color: rgb(var(--warning-rgb));"><?php echo $stats['pending']; ?></span>
    </div>
    <div class="stat-item">
        <span class="stat-label">Sold</span>
        <span class="stat-value positive"><?php echo $stats['sold']; ?></span>
    </div>
    <div class="stat-item">
        <span class="stat-label">No Go</span>
        <span class="stat-value negative"><?php echo $stats['no_go']; ?></span>
    </div>
    <div class="stat-item">
        <span class="stat-label">Total P/L</span>
        <span class="stat-value <?php echo $stats['total_profit'] >= 0 ? 'positive' : 'negative'; ?>">
            <?php echo $stats['total_profit'] >= 0 ? '+' : ''; ?><?php echo number_format($stats['total_profit'], 2); ?>%
        </span>
    </div>
</div>

<!-- Trades Table -->
<?php if (empty($trades)): ?>
<div class="empty-state">
    <div class="mb-3">
        <i class="ri-exchange-line text-muted" style="font-size: 3rem;"></i>
    </div>
    <h4 class="text-muted">No trades found</h4>
    <p class="text-muted mb-0">Try adjusting your filters or time window.</p>
</div>
<?php else: ?>
<div class="card custom-card">
    <div class="card-body p-0">
        <div class="table-responsive">
            <table class="table trades-table mb-0">
                <thead>
                    <tr>
                        <th>ID</th>
                        <th>Play</th>
                        <th>Status</th>
                        <th>Cycle ID</th>
                        <th>Entry Price</th>
                        <th>Current/Exit</th>
                        <th>P/L %</th>
                        <th>Potential Gains</th>
                        <th>Wallet</th>
                        <th>Trail</th>
                        <th>Time</th>
                    </tr>
                </thead>
                <tbody>
                    <?php foreach ($trades as $trade): ?>
                    <?php
                    // Calculate P/L for pending trades from current_price, use our_profit_loss for sold
                    $trade_status = $trade['our_status'] ?? '';
                    $entry_price = floatval($trade['our_entry_price'] ?? 0);
                    $current_price = floatval($trade['current_price'] ?? 0);
                    $exit_price = floatval($trade['our_exit_price'] ?? 0);
                    
                    // Determine display price and P/L based on status
                    if ($trade_status === 'pending') {
                        // For pending: show current tracked price and calculate live P/L
                        $display_price = $current_price;
                        if ($entry_price > 0 && $current_price > 0) {
                            $calculated_pl = (($current_price - $entry_price) / $entry_price) * 100;
                        } else {
                            $calculated_pl = null;
                        }
                    } elseif ($trade_status === 'sold') {
                        // For sold: show exit price and final P/L
                        $display_price = $exit_price > 0 ? $exit_price : $current_price;
                        $calculated_pl = $trade['our_profit_loss'] ?? null;
                    } else {
                        // For other statuses (no_go, validating, etc.)
                        $display_price = $current_price > 0 ? $current_price : null;
                        $calculated_pl = $trade['our_profit_loss'] ?? null;
                    }
                    ?>
                    <tr onclick="window.location.href='/pages/features/trades/detail.php?id=<?php echo $trade['id']; ?>'" data-trade-id="<?php echo $trade['id']; ?>">
                        <td>
                            <span class="fw-semibold">#<?php echo $trade['id']; ?></span>
                        </td>
                        <td>
                            <span class="badge bg-primary-transparent">
                                Play <?php echo $trade['play_id'] ?? '-'; ?>
                            </span>
                        </td>
                        <td>
                            <span class="status-badge status-<?php echo strtolower($trade['our_status'] ?? 'unknown'); ?>">
                                <?php echo htmlspecialchars($trade['our_status'] ?? 'Unknown'); ?>
                            </span>
                        </td>
                        <td>
                            <?php 
                            $cycle_id = $trade['price_cycle'] ?? null;
                            if ($cycle_id !== null): 
                            ?>
                            <span class="badge bg-info-transparent" style="font-family: 'SF Mono', monospace;">
                                #<?php echo $cycle_id; ?>
                            </span>
                            <?php else: ?>
                            <span class="text-muted">-</span>
                            <?php endif; ?>
                        </td>
                        <td>
                            <span class="price-value">
                                $<?php echo number_format($entry_price, 4); ?>
                            </span>
                        </td>
                        <td>
                            <?php if ($display_price > 0): ?>
                            <span class="price-value <?php echo $trade_status === 'pending' ? 'current-price-live' : ''; ?>">
                                $<?php echo number_format($display_price, 4); ?>
                            </span>
                            <?php else: ?>
                            <span class="text-muted">-</span>
                            <?php endif; ?>
                        </td>
                        <td>
                            <?php 
                            if ($calculated_pl !== null):
                                $pl_class = $calculated_pl > 0 ? 'profit-positive' : ($calculated_pl < 0 ? 'profit-negative' : 'profit-zero');
                                $pl_sign = $calculated_pl > 0 ? '+' : '';
                            ?>
                            <span class="<?php echo $pl_class; ?>">
                                <?php echo $pl_sign . number_format($calculated_pl, 2); ?>%
                            </span>
                            <?php else: ?>
                            <span class="profit-zero">-</span>
                            <?php endif; ?>
                        </td>
                        <td>
                            <?php 
                            $potential_gains = $trade['potential_gains'] ?? null;
                            if ($potential_gains !== null):
                                $pg_class = $potential_gains > 0 ? 'profit-positive' : ($potential_gains < 0 ? 'profit-negative' : 'profit-zero');
                                $pg_sign = $potential_gains > 0 ? '+' : '';
                            ?>
                            <span class="<?php echo $pg_class; ?>">
                                <?php echo $pg_sign . number_format($potential_gains, 2); ?>%
                            </span>
                            <?php else: ?>
                            <span class="text-muted" style="font-size: 0.7rem;">pending</span>
                            <?php endif; ?>
                        </td>
                        <td>
                            <span class="wallet-address" title="<?php echo htmlspecialchars($trade['wallet_address'] ?? ''); ?>">
                                <?php echo substr($trade['wallet_address'] ?? '', 0, 8); ?>...
                            </span>
                        </td>
                        <td>
                            <?php 
                            // Check if this trade might have trail data
                            $has_trail = !empty($trade['fifteen_min_trail']) || $trade['our_status'] !== 'validating';
                            ?>
                            <span class="trail-indicator <?php echo $has_trail ? 'has-trail' : 'no-trail'; ?>">
                                <i class="ri-line-chart-line"></i>
                                <?php echo $has_trail ? '15m' : '-'; ?>
                            </span>
                        </td>
                        <td>
                            <span class="time-relative" title="<?php echo $trade['followed_at'] ?? ''; ?>">
                                <?php 
                                if (!empty($trade['followed_at'])) {
                                    $timestamp = strtotime($trade['followed_at']);
                                    $diff = time() - $timestamp;
                                    if ($diff < 60) echo $diff . 's ago';
                                    elseif ($diff < 3600) echo floor($diff/60) . 'm ago';
                                    elseif ($diff < 86400) echo floor($diff/3600) . 'h ago';
                                    else echo date('M j, g:ia', $timestamp);
                                } else {
                                    echo '-';
                                }
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
<?php endif; ?>

<script>
    // Auto-refresh every 30 seconds
    setTimeout(function() {
        location.reload();
    }, 30000);
</script>

<?php
$content = ob_get_clean();
$scripts = '';

// Include the base layout
include __DIR__ . '/../../layouts/base.php';
?>

