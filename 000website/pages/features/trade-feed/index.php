<?php
/**
 * Live Trade Feed - Real-time SOL Trade Monitor
 * 
 * Shows recent buy trades from sol_stablecoin_trades with auto-refresh.
 * Highlights trades from wallets tracked by active plays.
 */

// --- Database API Client ---
require_once __DIR__ . '/../../../includes/DatabaseClient.php';
require_once __DIR__ . '/../../../includes/config.php';
$db = new DatabaseClient(DATABASE_API_URL);
$api_available = $db->isAvailable();

// --- Base URL for template ---
$baseUrl = '';

$error_message = '';
$trades = [];
$tracked_wallets = [];
$plays_data = [];

// Parameters
$minutes = isset($_GET['minutes']) ? (int)$_GET['minutes'] : 5;
$refresh = isset($_GET['refresh']) ? (int)$_GET['refresh'] : 3;

// Validate parameters
$minutes = max(1, min(60, $minutes));
$refresh = max(1, min(30, $refresh));

// Always show 100 trades in the table
$display_limit = 100;

// Fetch data
$total_in_window = 0;
$sync_diagnostic = null;
if ($use_duckdb) {
    // Get recent trades (always 100)
    $trades_response = $db->getRecentTrades($display_limit, $minutes, 'buy');
    if ($trades_response && isset($trades_response['trades'])) {
        $trades = $trades_response['trades'];
        $total_in_window = $trades_response['total_count'] ?? count($trades);
    }
    
    // Get tracked wallets from all active plays
    $wallets_response = $db->getTrackedWallets();
    if ($wallets_response && isset($wallets_response['all_wallets'])) {
        // Build a clean lookup - wallet addresses are case-sensitive
        $tracked_wallets = [];
        foreach ($wallets_response['all_wallets'] as $wallet) {
            $tracked_wallets[$wallet] = true;
        }
        $plays_data = $wallets_response['plays'] ?? [];
    }
    
    // Get sync diagnostic to compare data sources
    $sync_diagnostic = $db->getTradesDiagnostic($minutes);
} else {
    $error_message = "Website API is not available. Please start the API: python scheduler/website_api.py";
}

// Build wallet-to-plays lookup (wallet -> list of plays tracking it)
// Include perp_mode so we can filter like follow_the_goat.py does
$wallet_plays_map = [];
foreach ($plays_data as $play_id => $play_info) {
    if (!isset($play_info['wallets']) || !is_array($play_info['wallets'])) {
        continue;
    }
    $perp_mode = $play_info['perp_mode'] ?? 'any';
    foreach ($play_info['wallets'] as $wallet) {
        if (empty($wallet)) continue;
        if (!isset($wallet_plays_map[$wallet])) {
            $wallet_plays_map[$wallet] = [];
        }
        $wallet_plays_map[$wallet][] = [
            'id' => $play_id,
            'name' => $play_info['play_name'] ?? "Play $play_id",
            'perp_mode' => $perp_mode
        ];
    }
}

/**
 * Check if a trade matches a play's perp_mode filter
 * Replicates the logic from follow_the_goat.py _trade_matches_perp_mode()
 */
function trade_matches_perp_mode($trade_perp_direction, $play_perp_mode) {
    // 'any' mode accepts all trades
    if ($play_perp_mode === 'any' || empty($play_perp_mode)) {
        return true;
    }
    
    // If trade has no perp direction, it doesn't match long_only or short_only
    if (empty($trade_perp_direction)) {
        return false;
    }
    
    $trade_direction = strtolower($trade_perp_direction);
    
    if ($play_perp_mode === 'long_only') {
        return $trade_direction === 'long';
    }
    if ($play_perp_mode === 'short_only') {
        return $trade_direction === 'short';
    }
    
    return true;
}

/**
 * Get plays that would actually trigger for this trade
 * Applies the same filters as follow_the_goat.py
 */
function get_matching_plays_for_trade($wallet, $perp_direction, $wallet_plays_map) {
    if (!isset($wallet_plays_map[$wallet])) {
        return [];
    }
    
    $matching_plays = [];
    foreach ($wallet_plays_map[$wallet] as $play) {
        // Apply perp_mode filter (same as follow_the_goat.py)
        if (trade_matches_perp_mode($perp_direction, $play['perp_mode'])) {
            $matching_plays[] = $play;
        }
    }
    
    return $matching_plays;
}

// Calculate stats (using same trigger logic as follow_the_goat.py)
$tracked_count = 0;
$total_volume_sol = 0;
$total_volume_usd = 0;

foreach ($trades as $trade) {
    $wallet = $trade['wallet_address'] ?? '';
    $perp_direction = $trade['perp_direction'] ?? null;
    
    // Only count if trade would actually trigger (perp_mode filter applied)
    $matching_plays = get_matching_plays_for_trade($wallet, $perp_direction, $wallet_plays_map);
    if (!empty($matching_plays)) {
        $tracked_count++;
    }
    
    $total_volume_sol += floatval($trade['sol_amount'] ?? 0);
    $total_volume_usd += floatval($trade['stablecoin_amount'] ?? 0);
}

// --- Page Styles ---
ob_start();
?>
<style>
    .feed-container {
        width: 100%;
    }
    
    .feed-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        flex-wrap: wrap;
        gap: 1rem;
        margin-bottom: 1.5rem;
    }
    
    .feed-title {
        display: flex;
        align-items: center;
        gap: 0.75rem;
    }
    
    .feed-title h1 {
        font-size: 1.5rem;
        font-weight: 600;
        margin: 0;
    }
    
    .live-badge {
        display: inline-flex;
        align-items: center;
        gap: 0.4rem;
        padding: 0.3rem 0.7rem;
        background: rgba(var(--success-rgb), 0.15);
        color: rgb(var(--success-rgb));
        border-radius: 20px;
        font-size: 0.7rem;
        font-weight: 600;
        text-transform: uppercase;
    }
    
    .live-badge .pulse {
        width: 8px;
        height: 8px;
        background: rgb(var(--success-rgb));
        border-radius: 50%;
        animation: pulse 1.5s infinite;
    }
    
    @keyframes pulse {
        0%, 100% { opacity: 1; transform: scale(1); }
        50% { opacity: 0.5; transform: scale(1.2); }
    }
    
    .filter-bar {
        display: flex;
        align-items: center;
        gap: 1rem;
        flex-wrap: wrap;
    }
    
    .filter-group {
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    
    .filter-group label {
        font-size: 0.75rem;
        color: var(--text-muted);
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    .filter-group select {
        background: rgba(var(--secondary-rgb), 0.1);
        border: 1px solid var(--default-border);
        border-radius: 4px;
        padding: 0.35rem 0.6rem;
        font-size: 0.85rem;
        color: var(--default-text-color);
    }
    
    .stats-grid {
        display: grid;
        grid-template-columns: repeat(6, 1fr);
        gap: 1rem;
        margin-bottom: 1.5rem;
    }
    
    @media (max-width: 992px) {
        .stats-grid {
            grid-template-columns: repeat(3, 1fr);
        }
    }
    
    @media (max-width: 576px) {
        .stats-grid {
            grid-template-columns: repeat(2, 1fr);
        }
    }
    
    .stat-card {
        background: var(--custom-white);
        border: 1px solid var(--default-border);
        border-radius: 0.5rem;
        padding: 1rem;
        text-align: center;
    }
    
    .stat-card .label {
        font-size: 0.65rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        color: var(--text-muted);
        margin-bottom: 0.3rem;
    }
    
    .stat-card .value {
        font-size: 1.4rem;
        font-weight: 700;
        color: var(--default-text-color);
    }
    
    .stat-card .value.highlight {
        color: rgb(var(--primary-rgb));
    }
    
    .stat-card .value.muted {
        color: var(--text-muted);
    }
    
    .trades-table {
        font-size: 0.85rem;
        width: 100%;
    }
    
    .trades-table th {
        font-size: 0.7rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        color: var(--text-muted);
        font-weight: 600;
        border-bottom: 2px solid var(--default-border);
        padding: 0.75rem 0.5rem;
        white-space: nowrap;
    }
    
    .trades-table td {
        vertical-align: middle;
        padding: 0.6rem 0.5rem;
        border-bottom: 1px solid var(--default-border);
    }
    
    .trades-table tbody tr {
        transition: all 0.15s ease;
    }
    
    .trades-table tbody tr:hover {
        background: rgba(var(--primary-rgb), 0.05) !important;
    }
    
    /* Tracked wallet row highlight */
    .trades-table tbody tr.tracked-wallet {
        background: rgba(var(--primary-rgb), 0.08);
        border-left: 3px solid rgb(var(--primary-rgb));
    }
    
    .trades-table tbody tr.tracked-wallet:hover {
        background: rgba(var(--primary-rgb), 0.15) !important;
    }
    
    .wallet-address {
        font-family: 'SF Mono', 'Monaco', 'Consolas', monospace;
        font-size: 0.8rem;
        color: var(--text-muted);
    }
    
    .wallet-address.tracked {
        color: rgb(var(--primary-rgb));
        font-weight: 600;
    }
    
    .amount-value {
        font-family: 'SF Mono', 'Monaco', 'Consolas', monospace;
        font-size: 0.85rem;
    }
    
    .price-value {
        font-family: 'SF Mono', 'Monaco', 'Consolas', monospace;
        font-size: 0.85rem;
        color: var(--default-text-color);
    }
    
    .time-relative {
        font-size: 0.75rem;
        color: var(--text-muted);
        white-space: nowrap;
    }
    
    .play-badge {
        display: inline-block;
        padding: 0.2rem 0.5rem;
        background: rgba(var(--primary-rgb), 0.15);
        color: rgb(var(--primary-rgb));
        border-radius: 4px;
        font-size: 0.65rem;
        font-weight: 600;
        text-transform: uppercase;
        white-space: nowrap;
    }
    
    .perp-badge {
        display: inline-block;
        padding: 0.15rem 0.4rem;
        border-radius: 3px;
        font-size: 0.6rem;
        font-weight: 600;
        text-transform: uppercase;
    }
    
    .perp-badge.long {
        background: rgba(var(--success-rgb), 0.15);
        color: rgb(var(--success-rgb));
    }
    
    .perp-badge.short {
        background: rgba(var(--danger-rgb), 0.15);
        color: rgb(var(--danger-rgb));
    }
    
    .empty-state {
        text-align: center;
        padding: 4rem 2rem;
        border: 1px dashed var(--default-border);
        border-radius: 0.5rem;
    }
    
    .countdown-bar {
        height: 3px;
        background: rgba(var(--primary-rgb), 0.2);
        border-radius: 2px;
        margin-top: 1rem;
        overflow: hidden;
    }
    
    .countdown-bar .progress {
        height: 100%;
        background: rgb(var(--primary-rgb));
        width: 100%;
        animation: countdown <?php echo $refresh; ?>s linear infinite;
    }
    
    @keyframes countdown {
        from { width: 100%; }
        to { width: 0%; }
    }
    
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
    
    .signature-link {
        font-size: 0.7rem;
        color: var(--text-muted);
        text-decoration: none;
    }
    
    .signature-link:hover {
        color: rgb(var(--primary-rgb));
    }
</style>
<?php
$styles = ob_get_clean();

// --- Page Content ---
ob_start();
?>

<!-- API Status Badge -->
<div class="api-status-badge" style="background: <?php echo $use_duckdb ? 'rgb(var(--success-rgb))' : 'rgb(var(--danger-rgb))'; ?>; color: white;">
    🦆 <?php echo $use_duckdb ? 'API Connected' : 'API Disconnected'; ?>
</div>

<div class="feed-container">
    
    <!-- Page Header -->
    <div class="d-flex align-items-center justify-content-between page-header-breadcrumb flex-wrap gap-2 mb-3">
        <div>
            <nav>
                <ol class="breadcrumb mb-1">
                    <li class="breadcrumb-item"><a href="<?php echo $baseUrl; ?>/">Dashboard</a></li>
                    <li class="breadcrumb-item"><a href="#">Features</a></li>
                    <li class="breadcrumb-item active" aria-current="page">Trade Feed</li>
                </ol>
            </nav>
        </div>
    </div>
    
    <!-- Feed Header -->
    <div class="feed-header">
        <div class="feed-title">
            <h1>Live Trade Feed</h1>
            <span class="live-badge">
                <span class="pulse"></span>
                Live
            </span>
        </div>
        
        <form method="GET" action="" class="filter-bar">
            <div class="filter-group">
                <label for="minutes">Window</label>
                <select name="minutes" id="minutes" onchange="this.form.submit()">
                    <option value="1" <?php echo $minutes === 1 ? 'selected' : ''; ?>>1 min</option>
                    <option value="5" <?php echo $minutes === 5 ? 'selected' : ''; ?>>5 min</option>
                    <option value="15" <?php echo $minutes === 15 ? 'selected' : ''; ?>>15 min</option>
                    <option value="30" <?php echo $minutes === 30 ? 'selected' : ''; ?>>30 min</option>
                    <option value="60" <?php echo $minutes === 60 ? 'selected' : ''; ?>>60 min</option>
                </select>
            </div>
            
            <div class="filter-group">
                <label for="refresh">Refresh</label>
                <select name="refresh" id="refresh" onchange="this.form.submit()">
                    <option value="1" <?php echo $refresh === 1 ? 'selected' : ''; ?>>1s</option>
                    <option value="3" <?php echo $refresh === 3 ? 'selected' : ''; ?>>3s</option>
                    <option value="5" <?php echo $refresh === 5 ? 'selected' : ''; ?>>5s</option>
                    <option value="10" <?php echo $refresh === 10 ? 'selected' : ''; ?>>10s</option>
                </select>
            </div>
            
            <div class="filter-group">
                <span class="text-muted" style="font-size: 0.75rem;">Showing latest 100 trades</span>
            </div>
        </form>
    </div>
    
    <!-- Messages -->
    <?php if ($error_message): ?>
    <div class="alert alert-danger alert-dismissible fade show" role="alert">
        <i class="ri-error-warning-line me-2"></i><?php echo htmlspecialchars($error_message); ?>
        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    </div>
    <?php endif; ?>
    
    <!-- Stats Grid -->
    <div class="stats-grid">
        <div class="stat-card">
            <div class="label">Total Trades (<?php echo $minutes; ?>m)</div>
            <div class="value"><?php echo number_format($total_in_window); ?></div>
        </div>
        <div class="stat-card">
            <div class="label">Tracked in View</div>
            <div class="value highlight"><?php echo number_format($tracked_count); ?> <small style="font-size: 0.6rem;">/ <?php echo count($trades); ?></small></div>
        </div>
        <div class="stat-card">
            <div class="label">Active Plays</div>
            <div class="value"><?php echo count($plays_data); ?></div>
        </div>
        <div class="stat-card">
            <div class="label">Monitoring</div>
            <div class="value muted"><?php echo number_format(count($tracked_wallets)); ?> <small style="font-size: 0.6rem;">wallets</small></div>
        </div>
        <div class="stat-card">
            <div class="label">Volume (SOL)</div>
            <div class="value"><?php echo number_format($total_volume_sol, 2); ?></div>
        </div>
        <div class="stat-card">
            <div class="label">Volume (USD)</div>
            <div class="value">$<?php echo number_format($total_volume_usd, 0); ?></div>
        </div>
    </div>
    
    <!-- Sync Status (if diagnostic available) -->
    <?php if ($sync_diagnostic && isset($sync_diagnostic['sources'])): ?>
    <?php
        $webhook_data = $sync_diagnostic['sources']['webhook_duckdb'] ?? [];
        $db_data = $sync_diagnostic['sources']['python_duckdb'] ?? [];
        $sync_status = $sync_diagnostic['sync_status'] ?? [];
        
        $webhook_count = $webhook_data['last_' . $minutes . 'm'] ?? 0;
        $db_count = $db_data['last_' . $minutes . 'm'] ?? 0;
        $webhook_time = $webhook_data['response_time_ms'] ?? 0;
        $db_time = $db_data['query_time_ms'] ?? 0;
        
        $sync_lag = $sync_status['sync_lag'] ?? 0;
        $is_synced = $sync_status['synced'] ?? false;
        $sync_path = $sync_diagnostic['sync_mode'] ?? 'Unknown';
        $webhook_ok = ($webhook_data['status'] ?? '') === 'ok';
    ?>
    <div class="alert <?php echo $is_synced ? 'alert-success' : 'alert-warning'; ?> mb-3" style="font-size: 0.85rem;">
        <div class="d-flex align-items-center justify-content-between flex-wrap gap-2">
            <div>
                <strong><i class="ri-flashlight-line me-1"></i>⚡ Ultra-Fast Sync (500ms): <?php echo htmlspecialchars($sync_path); ?></strong>
                <span class="ms-2">
                    <?php if ($webhook_ok): ?>
                    Webhook: <strong><?php echo number_format($webhook_count); ?></strong> 
                    <small class="text-muted">(<?php echo round($webhook_time); ?>ms)</small> →
                    Local: <strong><?php echo number_format($db_count); ?></strong>
                    <small class="text-muted">(<?php echo round($db_time); ?>ms)</small>
                    <?php else: ?>
                    <span class="text-warning">⚠️ Webhook unavailable - using MySQL fallback (slower)</span>
                    <?php endif; ?>
                </span>
            </div>
            <?php if ($is_synced): ?>
            <span class="badge bg-success-transparent text-success">
                <i class="ri-check-line"></i> Synced (lag: <?php echo $sync_lag; ?>)
            </span>
            <?php else: ?>
            <span class="badge bg-warning-transparent text-warning">
                Behind by <?php echo $sync_lag; ?> trades
            </span>
            <?php endif; ?>
        </div>
    </div>
    <?php endif; ?>
    
    <!-- Countdown bar -->
    <div class="countdown-bar">
        <div class="progress"></div>
    </div>
    
    <!-- Trades Table -->
    <?php if (empty($trades)): ?>
    <div class="empty-state">
        <div class="mb-3">
            <i class="ri-exchange-line text-muted" style="font-size: 3rem;"></i>
        </div>
        <h4 class="text-muted">No trades found</h4>
        <p class="text-muted mb-0">No buy trades in the last <?php echo $minutes; ?> minute(s). Make sure the scheduler is syncing trades.</p>
    </div>
    <?php else: ?>
    <div class="card custom-card mt-3">
        <div class="card-body p-0">
            <div class="table-responsive">
                <table class="table trades-table mb-0">
                    <thead>
                        <tr>
                            <th>Time</th>
                            <th>Wallet</th>
                            <th>SOL</th>
                            <th>USD</th>
                            <th>Price</th>
                            <th>Perp</th>
                            <th>Play</th>
                            <th>Tx</th>
                        </tr>
                    </thead>
                    <tbody>
                        <?php foreach ($trades as $trade): 
                            $wallet = $trade['wallet_address'] ?? '';
                            $perp_direction = $trade['perp_direction'] ?? null;
                            $signature = $trade['signature'] ?? '';
                            
                            // Get plays that would ACTUALLY trigger for this trade
                            // (applies perp_mode filter like follow_the_goat.py)
                            $plays_for_wallet = get_matching_plays_for_trade($wallet, $perp_direction, $wallet_plays_map);
                            $is_tracked = !empty($plays_for_wallet);
                        ?>
                        <tr class="<?php echo $is_tracked ? 'tracked-wallet' : ''; ?>">
                            <td>
                                <span class="time-relative" title="<?php echo htmlspecialchars($trade['trade_timestamp'] ?? ''); ?>">
                                    <?php 
                                    if (!empty($trade['trade_timestamp'])) {
                                        $timestamp = strtotime($trade['trade_timestamp']);
                                        $diff = time() - $timestamp;
                                        if ($diff < 0) $diff = 0;
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
                            <td>
                                <span class="wallet-address <?php echo $is_tracked ? 'tracked' : ''; ?>">
                                    <?php echo htmlspecialchars($wallet); ?>
                                </span>
                            </td>
                            <td>
                                <span class="amount-value">
                                    <?php echo number_format(floatval($trade['sol_amount'] ?? 0), 4); ?>
                                </span>
                            </td>
                            <td>
                                <span class="amount-value">
                                    $<?php echo number_format(floatval($trade['stablecoin_amount'] ?? 0), 0); ?>
                                </span>
                            </td>
                            <td>
                                <span class="price-value">
                                    $<?php echo number_format(floatval($trade['price'] ?? 0), 2); ?>
                                </span>
                            </td>
                            <td>
                                <?php if ($perp_direction): ?>
                                <span class="perp-badge <?php echo strtolower($perp_direction); ?>">
                                    <?php echo htmlspecialchars($perp_direction); ?>
                                </span>
                                <?php else: ?>
                                <span class="text-muted">-</span>
                                <?php endif; ?>
                            </td>
                            <td>
                                <?php if ($is_tracked && !empty($plays_for_wallet)): ?>
                                    <?php foreach ($plays_for_wallet as $play): ?>
                                    <span class="play-badge" title="<?php echo htmlspecialchars($play['name']); ?>">
                                        #<?php echo $play['id']; ?>
                                    </span>
                                    <?php endforeach; ?>
                                <?php else: ?>
                                <span class="text-muted">-</span>
                                <?php endif; ?>
                            </td>
                            <td>
                                <?php if ($signature): ?>
                                <a href="https://solscan.io/tx/<?php echo htmlspecialchars($signature); ?>" 
                                   target="_blank" 
                                   class="signature-link"
                                   title="View on Solscan">
                                    <?php echo substr($signature, 0, 6); ?>...
                                </a>
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
    <?php endif; ?>
    
</div>

<script>
    // Auto-refresh with countdown
    const REFRESH_SECONDS = <?php echo $refresh; ?>;
    
    setTimeout(function() {
        location.reload();
    }, REFRESH_SECONDS * 1000);
</script>

<?php
$content = ob_get_clean();
$scripts = '';

// Include the base layout
include __DIR__ . '/../../layouts/base.php';
?>

