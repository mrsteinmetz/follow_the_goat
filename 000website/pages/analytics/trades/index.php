<?php
/**
 * Analytics - Trades
 * 
 * Displays all live trades from follow_the_goat_buyins ordered by DESC.
 * Allows clicking on each trade to view detailed validation information.
 */

// --- Database API Client ---
require_once __DIR__ . '/../../../includes/DatabaseClient.php';
require_once __DIR__ . '/../../../includes/config.php';
$db = new DatabaseClient(DATABASE_API_URL);
$api_available = $db->isAvailable();

// --- Base URL for template ---
$baseUrl = '../../..';

// --- Parameters ---
$play_filter = isset($_GET['play_id']) ? intval($_GET['play_id']) : null;
$status_filter = isset($_GET['status']) ? $_GET['status'] : null;
$hours = isset($_GET['hours']) ? $_GET['hours'] : '72';
$limit = isset($_GET['limit']) ? intval($_GET['limit']) : 200;
$refresh_interval = isset($_GET['refresh']) ? intval($_GET['refresh']) : 30;

// --- Fetch Trades Data ---
function fetchTradesData($db, $play_id, $status, $hours, $limit) {
    $trades = [];
    $error_message = null;
    $data_source = "No Data";

    if ($db->isAvailable()) {
        $response = $db->getBuyins($play_id, $status, $hours, $limit);
        
        if ($response && isset($response['buyins'])) {
            $trades = $response['buyins'];
            $data_source = $response['source'] ?? 'DuckDB';
        } else {
            $data_source = "No Data";
        }
    } else {
        $error_message = "Website API is not available. Please start the API: python scheduler/website_api.py";
    }
    
    return [
        'trades' => $trades,
        'error_message' => $error_message,
        'data_source' => $data_source,
    ];
}

// --- Fetch All Plays for Filter ---
function fetchPlays($db) {
    if ($db->isAvailable()) {
        $response = $db->getPlays();
        if ($response && isset($response['plays'])) {
            return $response['plays'];
        }
    }
    return [];
}

// Check if this is an AJAX request for data refresh
if (isset($_GET['ajax']) && $_GET['ajax'] === 'refresh') {
    header('Content-Type: application/json');
    $result = fetchTradesData($db, $play_filter, $status_filter, $hours, $limit);
    
    echo json_encode([
        'success' => true,
        'trades' => $result['trades'],
        'error_message' => $result['error_message'],
        'data_source' => $result['data_source'],
        'count' => count($result['trades'])
    ]);
    exit;
}

// Check if this is an AJAX request for single trade details
if (isset($_GET['ajax']) && $_GET['ajax'] === 'detail' && isset($_GET['buyin_id'])) {
    header('Content-Type: application/json');
    $buyin_id = intval($_GET['buyin_id']);
    $source = $_GET['source'] ?? 'live';
    
    $trade = $db->getSingleBuyin($buyin_id, $source);
    
    echo json_encode([
        'success' => $trade !== null,
        'trade' => $trade,
    ]);
    exit;
}

// Regular page load
$result = fetchTradesData($db, $play_filter, $status_filter, $hours, $limit);
$trades = $result['trades'];
$error_message = $result['error_message'];
$data_source = $result['data_source'];

// Fetch plays for filter dropdown
$plays = fetchPlays($db);

// Calculate stats
$total_trades = count($trades);
$pending_count = count(array_filter($trades, fn($t) => ($t['our_status'] ?? '') === 'pending'));
$sold_count = count(array_filter($trades, fn($t) => ($t['our_status'] ?? '') === 'sold'));
$no_go_count = count(array_filter($trades, fn($t) => ($t['our_status'] ?? '') === 'no_go'));
?>

<!-- Styles for this page -->
<?php ob_start(); ?>

<style>
    /* === TRADE LIST STYLES === */
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
    
    /* Status badges */
    .badge-pending {
        background: rgba(59, 130, 246, 0.2);
        color: #60a5fa;
        border: 1px solid rgba(59, 130, 246, 0.3);
    }
    .badge-sold {
        background: rgba(16, 185, 129, 0.2);
        color: #34d399;
        border: 1px solid rgba(16, 185, 129, 0.3);
    }
    .badge-no-go {
        background: rgba(239, 68, 68, 0.2);
        color: #f87171;
        border: 1px solid rgba(239, 68, 68, 0.3);
    }
    .badge-validating {
        background: rgba(251, 191, 36, 0.2);
        color: #fbbf24;
        border: 1px solid rgba(251, 191, 36, 0.3);
    }
    .badge-error {
        background: rgba(156, 163, 175, 0.2);
        color: #9ca3af;
        border: 1px solid rgba(156, 163, 175, 0.3);
    }
    
    /* Decision badges */
    .decision-go {
        background: rgba(16, 185, 129, 0.2);
        color: #34d399;
    }
    .decision-nogo {
        background: rgba(239, 68, 68, 0.2);
        color: #f87171;
    }
    .decision-error {
        background: rgba(156, 163, 175, 0.2);
        color: #9ca3af;
    }
    
    /* Table styles */
    .trades-table {
        font-size: 0.875rem;
    }
    .trades-table th {
        font-weight: 600;
        text-transform: uppercase;
        font-size: 0.7rem;
        letter-spacing: 0.5px;
        color: rgba(255,255,255,0.6);
        border-bottom: 1px solid rgba(255,255,255,0.1);
    }
    .trades-table td {
        vertical-align: middle;
        border-bottom: 1px solid rgba(255,255,255,0.05);
    }
    .trades-table tbody tr {
        cursor: pointer;
        transition: all 0.2s ease;
    }
    .trades-table tbody tr:hover {
        background: rgba(var(--primary-rgb), 0.05);
    }
    
    /* Wallet address styling */
    .wallet-addr {
        font-family: 'JetBrains Mono', 'Fira Code', monospace;
        font-size: 0.8rem;
        color: rgba(255,255,255,0.7);
    }
    .wallet-addr.training {
        color: #fbbf24;
        font-style: italic;
    }
    
    /* Price display */
    .price-display {
        font-family: 'JetBrains Mono', 'Fira Code', monospace;
    }
    
    /* Filters section */
    .filter-section {
        background: rgba(var(--body-bg-rgb2), 1);
        border: 1px solid rgba(255,255,255,0.1);
        border-radius: 0.5rem;
        padding: 1rem;
    }
    
    /* Modal styles */
    .trade-detail-modal .modal-dialog {
        max-width: 900px;
    }
    .trade-detail-modal .modal-content {
        background: rgb(var(--body-bg-rgb2));
        border: 1px solid rgba(255,255,255,0.1);
    }
    .trade-detail-modal .modal-header {
        border-bottom: 1px solid rgba(255,255,255,0.1);
    }
    .trade-detail-modal .modal-body {
        max-height: 70vh;
        overflow-y: auto;
    }
    
    /* Detail sections */
    .detail-section {
        background: rgba(0,0,0,0.2);
        border-radius: 0.5rem;
        padding: 1rem;
        margin-bottom: 1rem;
    }
    .detail-section h6 {
        color: rgba(var(--primary-rgb), 1);
        font-weight: 600;
        margin-bottom: 0.75rem;
        padding-bottom: 0.5rem;
        border-bottom: 1px solid rgba(255,255,255,0.1);
    }
    
    /* JSON viewer */
    .json-viewer {
        font-family: 'JetBrains Mono', 'Fira Code', monospace;
        font-size: 0.75rem;
        background: rgba(0,0,0,0.3);
        border-radius: 0.25rem;
        padding: 0.5rem;
        max-height: 300px;
        overflow: auto;
    }
    
    /* Filters pass/fail */
    .filter-pass {
        color: #34d399;
    }
    .filter-fail {
        color: #f87171;
    }
    
    /* Loading spinner */
    .loading-spinner {
        display: inline-block;
        width: 1rem;
        height: 1rem;
        border: 2px solid rgba(var(--primary-rgb), 0.3);
        border-radius: 50%;
        border-top-color: rgb(var(--primary-rgb));
        animation: spin 1s ease-in-out infinite;
    }
    @keyframes spin {
        to { transform: rotate(360deg); }
    }
</style>

<?php $styles = ob_get_clean(); ?>

<!-- Page Content -->
<?php ob_start(); ?>

<!-- Page Header -->
<div class="d-flex align-items-center justify-content-between page-header-breadcrumb flex-wrap gap-2 mb-4">
    <div>
        <nav aria-label="breadcrumb">
            <ol class="breadcrumb mb-1">
                <li class="breadcrumb-item"><a href="<?php echo $baseUrl; ?>">Home</a></li>
                <li class="breadcrumb-item">Analytics</li>
                <li class="breadcrumb-item active" aria-current="page">Trades</li>
            </ol>
        </nav>
        <h1 class="page-title fw-semibold fs-18 mb-0">Trade Analytics</h1>
    </div>
    <div class="d-flex gap-2 align-items-center">
        <span class="badge bg-dark px-3 py-2" id="data-source-badge">
            <i class="ri-database-2-line me-1"></i>
            <span id="data-source-text"><?php echo htmlspecialchars($data_source); ?></span>
        </span>
        <span class="badge bg-dark px-3 py-2" id="last-update-badge">
            <i class="ri-time-line me-1"></i>
            <span id="last-update-text">Just now</span>
        </span>
    </div>
</div>

<?php if ($error_message): ?>
<div class="alert alert-danger mb-4">
    <i class="ri-error-warning-line me-2"></i>
    <?php echo htmlspecialchars($error_message); ?>
</div>
<?php endif; ?>

<!-- Stats Row -->
<div class="row mb-4">
    <div class="col-xl-3 col-sm-6 col-12">
        <div class="stat-card">
            <div class="d-flex align-items-center">
                <div class="stat-icon bg-primary bg-opacity-10 text-primary me-3">
                    <i class="ri-exchange-funds-line"></i>
                </div>
                <div>
                    <div class="stat-label">Total Trades</div>
                    <div class="stat-value" id="stat-total"><?php echo number_format($total_trades); ?></div>
                </div>
            </div>
        </div>
    </div>
    <div class="col-xl-3 col-sm-6 col-12">
        <div class="stat-card">
            <div class="d-flex align-items-center">
                <div class="stat-icon bg-info bg-opacity-10 text-info me-3">
                    <i class="ri-time-line"></i>
                </div>
                <div>
                    <div class="stat-label">Pending</div>
                    <div class="stat-value" id="stat-pending"><?php echo number_format($pending_count); ?></div>
                </div>
            </div>
        </div>
    </div>
    <div class="col-xl-3 col-sm-6 col-12">
        <div class="stat-card">
            <div class="d-flex align-items-center">
                <div class="stat-icon bg-success bg-opacity-10 text-success me-3">
                    <i class="ri-check-double-line"></i>
                </div>
                <div>
                    <div class="stat-label">Sold</div>
                    <div class="stat-value" id="stat-sold"><?php echo number_format($sold_count); ?></div>
                </div>
            </div>
        </div>
    </div>
    <div class="col-xl-3 col-sm-6 col-12">
        <div class="stat-card">
            <div class="d-flex align-items-center">
                <div class="stat-icon bg-danger bg-opacity-10 text-danger me-3">
                    <i class="ri-close-circle-line"></i>
                </div>
                <div>
                    <div class="stat-label">No-Go</div>
                    <div class="stat-value" id="stat-nogo"><?php echo number_format($no_go_count); ?></div>
                </div>
            </div>
        </div>
    </div>
</div>

<!-- Filters -->
<div class="filter-section mb-4">
    <div class="row g-3">
        <div class="col-md-3">
            <label class="form-label small text-muted">Play</label>
            <select class="form-select form-select-sm" id="filter-play">
                <option value="">All Plays</option>
                <?php foreach ($plays as $play): ?>
                <option value="<?php echo $play['id']; ?>" <?php echo $play_filter == $play['id'] ? 'selected' : ''; ?>>
                    #<?php echo $play['id']; ?> - <?php echo htmlspecialchars($play['name'] ?? 'Unnamed'); ?>
                </option>
                <?php endforeach; ?>
            </select>
        </div>
        <div class="col-md-2">
            <label class="form-label small text-muted">Status</label>
            <select class="form-select form-select-sm" id="filter-status">
                <option value="" <?php echo !$status_filter ? 'selected' : ''; ?>>All Status</option>
                <option value="pending" <?php echo $status_filter === 'pending' ? 'selected' : ''; ?>>Pending</option>
                <option value="sold" <?php echo $status_filter === 'sold' ? 'selected' : ''; ?>>Sold</option>
                <option value="no_go" <?php echo $status_filter === 'no_go' ? 'selected' : ''; ?>>No-Go</option>
                <option value="validating" <?php echo $status_filter === 'validating' ? 'selected' : ''; ?>>Validating</option>
            </select>
        </div>
        <div class="col-md-2">
            <label class="form-label small text-muted">Time Window</label>
            <select class="form-select form-select-sm" id="filter-hours">
                <option value="1" <?php echo $hours === '1' ? 'selected' : ''; ?>>Last 1 Hour</option>
                <option value="6" <?php echo $hours === '6' ? 'selected' : ''; ?>>Last 6 Hours</option>
                <option value="24" <?php echo $hours === '24' ? 'selected' : ''; ?>>Last 24 Hours</option>
                <option value="72" <?php echo $hours === '72' ? 'selected' : ''; ?>>Last 72 Hours</option>
                <option value="all" <?php echo $hours === 'all' ? 'selected' : ''; ?>>All Time</option>
            </select>
        </div>
        <div class="col-md-2">
            <label class="form-label small text-muted">Limit</label>
            <select class="form-select form-select-sm" id="filter-limit">
                <option value="50" <?php echo $limit === 50 ? 'selected' : ''; ?>>50</option>
                <option value="100" <?php echo $limit === 100 ? 'selected' : ''; ?>>100</option>
                <option value="200" <?php echo $limit === 200 ? 'selected' : ''; ?>>200</option>
                <option value="500" <?php echo $limit === 500 ? 'selected' : ''; ?>>500</option>
            </select>
        </div>
        <div class="col-md-3 d-flex align-items-end">
            <button class="btn btn-primary btn-sm me-2" onclick="applyFilters()">
                <i class="ri-filter-line me-1"></i> Apply
            </button>
            <button class="btn btn-outline-secondary btn-sm" onclick="refreshData()">
                <i class="ri-refresh-line me-1"></i> Refresh
            </button>
        </div>
    </div>
</div>

<!-- Trades Table -->
<div class="card">
    <div class="card-body p-0">
        <div class="table-responsive">
            <table class="table table-hover trades-table mb-0" id="trades-table">
                <thead>
                    <tr>
                        <th>ID</th>
                        <th>Play</th>
                        <th>Wallet</th>
                        <th>Status</th>
                        <th>Decision</th>
                        <th>Entry Price</th>
                        <th>Created</th>
                    </tr>
                </thead>
                <tbody id="trades-tbody">
                    <?php foreach ($trades as $trade): ?>
                    <?php
                        $status = $trade['our_status'] ?? 'unknown';
                        $wallet = $trade['wallet_address'] ?? '';
                        $is_training = strpos($wallet, 'TRAINING_TEST_') === 0;
                        $wallet_display = $is_training ? 'Training Trade' : substr($wallet, 0, 8) . '...' . substr($wallet, -6);
                        
                        // Parse validation log for decision
                        $decision = 'N/A';
                        $validator_log = $trade['pattern_validator_log'] ?? null;
                        if ($validator_log) {
                            if (is_string($validator_log)) {
                                $parsed = json_decode($validator_log, true);
                            } else {
                                $parsed = $validator_log;
                            }
                            if ($parsed && isset($parsed['decision'])) {
                                $decision = $parsed['decision'];
                            }
                        }
                        
                        $status_class = 'badge-' . str_replace('_', '-', $status);
                        $decision_class = strtolower($decision) === 'go' ? 'decision-go' : 
                                         (strtolower($decision) === 'no_go' ? 'decision-nogo' : 'decision-error');
                    ?>
                    <tr onclick="showTradeDetail(<?php echo $trade['id']; ?>)" data-trade-id="<?php echo $trade['id']; ?>">
                        <td><strong>#<?php echo $trade['id']; ?></strong></td>
                        <td>
                            <span class="badge bg-dark">#<?php echo $trade['play_id'] ?? 'N/A'; ?></span>
                        </td>
                        <td>
                            <span class="wallet-addr <?php echo $is_training ? 'training' : ''; ?>" title="<?php echo htmlspecialchars($wallet); ?>">
                                <?php echo htmlspecialchars($wallet_display); ?>
                            </span>
                        </td>
                        <td>
                            <span class="badge <?php echo $status_class; ?>"><?php echo ucfirst(str_replace('_', ' ', $status)); ?></span>
                        </td>
                        <td>
                            <span class="badge <?php echo $decision_class; ?>"><?php echo htmlspecialchars($decision); ?></span>
                        </td>
                        <td>
                            <span class="price-display">
                                $<?php echo number_format($trade['our_entry_price'] ?? 0, 2); ?>
                            </span>
                        </td>
                        <td>
                            <span class="text-muted small">
                                <?php 
                                    $created = $trade['created_at'] ?? null;
                                    if ($created) {
                                        echo date('M d, H:i:s', strtotime($created));
                                    } else {
                                        echo 'N/A';
                                    }
                                ?>
                            </span>
                        </td>
                    </tr>
                    <?php endforeach; ?>
                    
                    <?php if (empty($trades)): ?>
                    <tr>
                        <td colspan="7" class="text-center text-muted py-5">
                            <i class="ri-inbox-line fs-1 d-block mb-2"></i>
                            No trades found for the selected filters.
                        </td>
                    </tr>
                    <?php endif; ?>
                </tbody>
            </table>
        </div>
    </div>
</div>

<!-- Trade Detail Modal -->
<div class="modal fade trade-detail-modal" id="tradeDetailModal" tabindex="-1" aria-labelledby="tradeDetailModalLabel" aria-hidden="true">
    <div class="modal-dialog modal-lg modal-dialog-scrollable">
        <div class="modal-content">
            <div class="modal-header">
                <h5 class="modal-title" id="tradeDetailModalLabel">
                    <i class="ri-exchange-funds-line me-2"></i>
                    Trade Details <span id="modal-trade-id"></span>
                </h5>
                <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
            </div>
            <div class="modal-body" id="trade-detail-content">
                <div class="text-center py-5">
                    <div class="loading-spinner"></div>
                    <div class="text-muted mt-2">Loading trade details...</div>
                </div>
            </div>
            <div class="modal-footer">
                <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
            </div>
        </div>
    </div>
</div>

<?php $content = ob_get_clean(); ?>

<!-- Scripts for this page -->
<?php ob_start(); ?>

<script>
    // Auto-refresh configuration
    const refreshInterval = <?php echo $refresh_interval * 1000; ?>;
    let refreshTimer = null;
    let lastUpdateTime = new Date();
    
    // Start auto-refresh
    function startAutoRefresh() {
        if (refreshTimer) clearInterval(refreshTimer);
        refreshTimer = setInterval(refreshData, refreshInterval);
    }
    
    // Update last updated time display
    function updateLastUpdateDisplay() {
        const now = new Date();
        const seconds = Math.floor((now - lastUpdateTime) / 1000);
        let text = 'Just now';
        if (seconds >= 60) {
            const minutes = Math.floor(seconds / 60);
            text = `${minutes}m ago`;
        } else if (seconds > 5) {
            text = `${seconds}s ago`;
        }
        document.getElementById('last-update-text').textContent = text;
    }
    
    // Refresh data via AJAX
    function refreshData() {
        const playId = document.getElementById('filter-play').value;
        const status = document.getElementById('filter-status').value;
        const hours = document.getElementById('filter-hours').value;
        const limit = document.getElementById('filter-limit').value;
        
        let url = `?ajax=refresh&hours=${hours}&limit=${limit}`;
        if (playId) url += `&play_id=${playId}`;
        if (status) url += `&status=${status}`;
        
        fetch(url)
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    updateTradesTable(data.trades);
                    updateStats(data.trades);
                    document.getElementById('data-source-text').textContent = data.data_source;
                    lastUpdateTime = new Date();
                    updateLastUpdateDisplay();
                }
            })
            .catch(error => console.error('Refresh error:', error));
    }
    
    // Update trades table with new data
    function updateTradesTable(trades) {
        const tbody = document.getElementById('trades-tbody');
        
        if (trades.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="7" class="text-center text-muted py-5">
                        <i class="ri-inbox-line fs-1 d-block mb-2"></i>
                        No trades found for the selected filters.
                    </td>
                </tr>
            `;
            return;
        }
        
        tbody.innerHTML = trades.map(trade => {
            const status = trade.our_status || 'unknown';
            const wallet = trade.wallet_address || '';
            const isTraining = wallet.indexOf('TRAINING_TEST_') === 0;
            const walletDisplay = isTraining ? 'Training Trade' : wallet.substring(0, 8) + '...' + wallet.slice(-6);
            
            let decision = 'N/A';
            if (trade.pattern_validator_log) {
                try {
                    const parsed = typeof trade.pattern_validator_log === 'string' 
                        ? JSON.parse(trade.pattern_validator_log) 
                        : trade.pattern_validator_log;
                    if (parsed && parsed.decision) {
                        decision = parsed.decision;
                    }
                } catch (e) {}
            }
            
            const statusClass = 'badge-' + status.replace('_', '-');
            const decisionClass = decision.toLowerCase() === 'go' ? 'decision-go' : 
                                 (decision.toLowerCase() === 'no_go' ? 'decision-nogo' : 'decision-error');
            
            const createdAt = trade.created_at 
                ? new Date(trade.created_at).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }) + ', ' + 
                  new Date(trade.created_at).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false })
                : 'N/A';
            
            return `
                <tr onclick="showTradeDetail(${trade.id})" data-trade-id="${trade.id}">
                    <td><strong>#${trade.id}</strong></td>
                    <td><span class="badge bg-dark">#${trade.play_id || 'N/A'}</span></td>
                    <td>
                        <span class="wallet-addr ${isTraining ? 'training' : ''}" title="${wallet}">
                            ${walletDisplay}
                        </span>
                    </td>
                    <td><span class="badge ${statusClass}">${status.replace('_', ' ').replace(/\b\w/g, l => l.toUpperCase())}</span></td>
                    <td><span class="badge ${decisionClass}">${decision}</span></td>
                    <td><span class="price-display">$${parseFloat(trade.our_entry_price || 0).toFixed(2)}</span></td>
                    <td><span class="text-muted small">${createdAt}</span></td>
                </tr>
            `;
        }).join('');
    }
    
    // Update stats
    function updateStats(trades) {
        const total = trades.length;
        const pending = trades.filter(t => t.our_status === 'pending').length;
        const sold = trades.filter(t => t.our_status === 'sold').length;
        const noGo = trades.filter(t => t.our_status === 'no_go').length;
        
        document.getElementById('stat-total').textContent = total.toLocaleString();
        document.getElementById('stat-pending').textContent = pending.toLocaleString();
        document.getElementById('stat-sold').textContent = sold.toLocaleString();
        document.getElementById('stat-nogo').textContent = noGo.toLocaleString();
    }
    
    // Apply filters
    function applyFilters() {
        const playId = document.getElementById('filter-play').value;
        const status = document.getElementById('filter-status').value;
        const hours = document.getElementById('filter-hours').value;
        const limit = document.getElementById('filter-limit').value;
        
        let url = `?hours=${hours}&limit=${limit}`;
        if (playId) url += `&play_id=${playId}`;
        if (status) url += `&status=${status}`;
        
        window.location.href = url;
    }
    
    // Show trade detail modal
    function showTradeDetail(tradeId) {
        const modal = new bootstrap.Modal(document.getElementById('tradeDetailModal'));
        document.getElementById('modal-trade-id').textContent = `#${tradeId}`;
        document.getElementById('trade-detail-content').innerHTML = `
            <div class="text-center py-5">
                <div class="loading-spinner"></div>
                <div class="text-muted mt-2">Loading trade details...</div>
            </div>
        `;
        modal.show();
        
        // Fetch trade details
        fetch(`?ajax=detail&buyin_id=${tradeId}`)
            .then(response => response.json())
            .then(data => {
                if (data.success && data.trade) {
                    renderTradeDetail(data.trade);
                } else {
                    document.getElementById('trade-detail-content').innerHTML = `
                        <div class="alert alert-danger">
                            <i class="ri-error-warning-line me-2"></i>
                            Failed to load trade details.
                        </div>
                    `;
                }
            })
            .catch(error => {
                document.getElementById('trade-detail-content').innerHTML = `
                    <div class="alert alert-danger">
                        <i class="ri-error-warning-line me-2"></i>
                        Error loading trade details: ${error.message}
                    </div>
                `;
            });
    }
    
    // Render trade detail content
    function renderTradeDetail(trade) {
        const content = document.getElementById('trade-detail-content');
        
        // Parse validation log
        let validatorLog = null;
        if (trade.pattern_validator_log) {
            try {
                validatorLog = typeof trade.pattern_validator_log === 'string' 
                    ? JSON.parse(trade.pattern_validator_log) 
                    : trade.pattern_validator_log;
            } catch (e) {}
        }
        
        // Parse entry log
        let entryLog = null;
        if (trade.entry_log) {
            try {
                entryLog = typeof trade.entry_log === 'string' 
                    ? JSON.parse(trade.entry_log) 
                    : trade.entry_log;
            } catch (e) {}
        }
        
        const status = trade.our_status || 'unknown';
        const decision = validatorLog?.decision || 'N/A';
        const schemaSource = validatorLog?.schema_source || 'N/A';
        const validatorVersion = validatorLog?.validator_version || 'N/A';
        
        let html = `
            <!-- Trade Summary -->
            <div class="detail-section">
                <h6><i class="ri-information-line me-2"></i>Trade Summary</h6>
                <div class="row">
                    <div class="col-md-6">
                        <table class="table table-sm table-borderless mb-0">
                            <tr>
                                <td class="text-muted" style="width: 40%">Trade ID:</td>
                                <td><strong>#${trade.id}</strong></td>
                            </tr>
                            <tr>
                                <td class="text-muted">Play ID:</td>
                                <td><span class="badge bg-dark">#${trade.play_id || 'N/A'}</span></td>
                            </tr>
                            <tr>
                                <td class="text-muted">Wallet:</td>
                                <td class="wallet-addr" style="word-break: break-all; font-size: 0.75rem;">
                                    ${trade.wallet_address || 'N/A'}
                                </td>
                            </tr>
                            <tr>
                                <td class="text-muted">Direction:</td>
                                <td>${trade.direction || 'N/A'}</td>
                            </tr>
                        </table>
                    </div>
                    <div class="col-md-6">
                        <table class="table table-sm table-borderless mb-0">
                            <tr>
                                <td class="text-muted" style="width: 40%">Status:</td>
                                <td><span class="badge badge-${status.replace('_', '-')}">${status.replace('_', ' ')}</span></td>
                            </tr>
                            <tr>
                                <td class="text-muted">Entry Price:</td>
                                <td class="price-display">$${parseFloat(trade.our_entry_price || 0).toFixed(6)}</td>
                            </tr>
                            <tr>
                                <td class="text-muted">Live Trade:</td>
                                <td>${trade.live_trade ? '<span class="badge bg-success">Yes</span>' : '<span class="badge bg-secondary">No</span>'}</td>
                            </tr>
                            <tr>
                                <td class="text-muted">Price Cycle:</td>
                                <td>${trade.price_cycle || 'N/A'}</td>
                            </tr>
                        </table>
                    </div>
                </div>
            </div>
            
            <!-- Validation Result -->
            <div class="detail-section">
                <h6><i class="ri-shield-check-line me-2"></i>Validation Result</h6>
                <div class="row mb-3">
                    <div class="col-md-3">
                        <div class="text-muted small mb-1">Decision</div>
                        <span class="badge fs-6 ${decision === 'GO' ? 'decision-go' : (decision === 'NO_GO' ? 'decision-nogo' : 'decision-error')}">${decision}</span>
                    </div>
                    <div class="col-md-3">
                        <div class="text-muted small mb-1">Schema Source</div>
                        <span class="badge bg-dark">${schemaSource}</span>
                    </div>
                    <div class="col-md-3">
                        <div class="text-muted small mb-1">Validator Version</div>
                        <span class="badge bg-dark">${validatorVersion}</span>
                    </div>
                    <div class="col-md-3">
                        <div class="text-muted small mb-1">Schema Play ID</div>
                        <span>#${validatorLog?.schema_play_id || 'N/A'}</span>
                    </div>
                </div>
        `;
        
        // Show filter results if available
        if (validatorLog?.filter_results && validatorLog.filter_results.length > 0) {
            const filtersTotal = validatorLog.decision_quality?.filters_total || validatorLog.filter_results.length;
            const filtersPassed = validatorLog.decision_quality?.filters_passed || 
                validatorLog.filter_results.filter(f => f.passed).length;
            const filtersFailed = validatorLog.decision_quality?.filters_failed || 
                validatorLog.filter_results.filter(f => !f.passed).length;
            
            html += `
                <div class="mb-2">
                    <strong>Filters:</strong> 
                    <span class="filter-pass">${filtersPassed} passed</span> / 
                    <span class="filter-fail">${filtersFailed} failed</span> / 
                    ${filtersTotal} total
                </div>
                <div class="table-responsive">
                    <table class="table table-sm table-borderless mb-0" style="font-size: 0.8rem;">
                        <thead>
                            <tr>
                                <th>Filter</th>
                                <th>Field</th>
                                <th>Min</th>
                                <th>Max</th>
                                <th>Actual</th>
                                <th>Result</th>
                            </tr>
                        </thead>
                        <tbody>
            `;
            
            validatorLog.filter_results.forEach(filter => {
                const passedClass = filter.passed ? 'filter-pass' : 'filter-fail';
                const passedIcon = filter.passed ? '<i class="ri-check-line"></i>' : '<i class="ri-close-line"></i>';
                html += `
                    <tr>
                        <td>${filter.filter_name || 'N/A'}</td>
                        <td><code>${filter.field || 'N/A'}</code></td>
                        <td>${filter.from_value !== null ? filter.from_value : '-'}</td>
                        <td>${filter.to_value !== null ? filter.to_value : '-'}</td>
                        <td>${filter.actual_value !== null ? parseFloat(filter.actual_value).toFixed(4) : 'N/A'}</td>
                        <td class="${passedClass}">${passedIcon}</td>
                    </tr>
                `;
            });
            
            html += `
                        </tbody>
                    </table>
                </div>
            `;
        }
        
        // Show stage results if available (schema-based validation)
        if (validatorLog?.stages && validatorLog.stages.length > 0) {
            html += `
                <div class="mb-2 mt-3">
                    <strong>Stage Results:</strong>
                </div>
            `;
            
            validatorLog.stages.forEach(stage => {
                const stagePass = stage.pass ? 'filter-pass' : 'filter-fail';
                const stageIcon = stage.pass ? '<i class="ri-check-line"></i>' : '<i class="ri-close-line"></i>';
                
                html += `
                    <div class="mb-2">
                        <span class="${stagePass}">${stageIcon}</span>
                        <strong>${stage.name}</strong>
                        ${stage.checks ? `(${stage.checks.filter(c => c.pass).length}/${stage.checks.length} checks passed)` : ''}
                    </div>
                `;
            });
        }
        
        html += `</div>`;
        
        // Entry Log (collapsed by default)
        if (entryLog) {
            html += `
                <div class="detail-section">
                    <h6><i class="ri-file-list-3-line me-2"></i>Entry Log 
                        <button class="btn btn-sm btn-link p-0 ms-2" type="button" data-bs-toggle="collapse" data-bs-target="#entryLogCollapse">
                            Toggle
                        </button>
                    </h6>
                    <div class="collapse" id="entryLogCollapse">
                        <div class="json-viewer"><pre>${JSON.stringify(entryLog, null, 2)}</pre></div>
                    </div>
                </div>
            `;
        }
        
        // Validator Log (collapsed by default)
        if (validatorLog) {
            html += `
                <div class="detail-section">
                    <h6><i class="ri-code-line me-2"></i>Validator Log 
                        <button class="btn btn-sm btn-link p-0 ms-2" type="button" data-bs-toggle="collapse" data-bs-target="#validatorLogCollapse">
                            Toggle
                        </button>
                    </h6>
                    <div class="collapse" id="validatorLogCollapse">
                        <div class="json-viewer"><pre>${JSON.stringify(validatorLog, null, 2)}</pre></div>
                    </div>
                </div>
            `;
        }
        
        // Timestamps
        html += `
            <div class="detail-section">
                <h6><i class="ri-time-line me-2"></i>Timestamps</h6>
                <table class="table table-sm table-borderless mb-0">
                    <tr>
                        <td class="text-muted" style="width: 30%">Created At:</td>
                        <td>${trade.created_at || 'N/A'}</td>
                    </tr>
                    <tr>
                        <td class="text-muted">Block Timestamp:</td>
                        <td>${trade.block_timestamp || 'N/A'}</td>
                    </tr>
                    <tr>
                        <td class="text-muted">Followed At:</td>
                        <td>${trade.followed_at || 'N/A'}</td>
                    </tr>
                </table>
            </div>
        `;
        
        content.innerHTML = html;
    }
    
    // Initialize
    document.addEventListener('DOMContentLoaded', function() {
        startAutoRefresh();
        setInterval(updateLastUpdateDisplay, 1000);
    });
</script>

<?php $scripts = ob_get_clean(); ?>

<?php include __DIR__ . '/../../layouts/base.php'; ?>

