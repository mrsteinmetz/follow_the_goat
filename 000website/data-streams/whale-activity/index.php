<?php
/**
 * Whale Activity Page - Live Whale Movements
 * Displays real-time whale wallet activity from .NET Webhook DuckDB In-Memory API.
 * 
 * Data Source: .NET Webhook at 195.201.84.5/api/whale-movements
 * This reads from the in-memory DuckDB (24hr hot storage)
 */

// --- Webhook API URL (FastAPI on 8001) ---
define('WEBHOOK_API_URL', 'http://127.0.0.1:8001/webhook');

// --- Base URL for template ---
$baseUrl = '../..';

// --- Check if webhook API is available ---
function isWebhookAvailable() {
    $ch = curl_init(WEBHOOK_API_URL . '/health');
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_TIMEOUT, 3);
    curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
    $response = curl_exec($ch);
    $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);
    return $httpCode === 200;
}

$use_duckdb = isWebhookAvailable();

// --- Fetch Whale Data via .NET Webhook API ---
function fetchWhaleData() {
    $whale_data = [];
    $error_message = null;
    $data_source = "No Data";
    $actual_source = null;

    $ch = curl_init(WEBHOOK_API_URL . '/api/whale-movements?limit=100');
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_TIMEOUT, 10);
    curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
    curl_setopt($ch, CURLOPT_HTTPHEADER, ['Accept: application/json']);
    
    $response = curl_exec($ch);
    $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $curlError = curl_error($ch);
    curl_close($ch);
    
    if ($httpCode === 200 && $response) {
        $data = json_decode($response, true);
        
        if ($data && isset($data['success']) && $data['success'] === true) {
            $whale_data = $data['results'] ?? [];
            $actual_source = $data['source'] ?? 'duckdb_inmemory';
            $data_source = "🦆 DuckDB In-Memory";
        } else {
            $error_message = $data['error'] ?? 'Unknown error from API';
            $data_source = "API Error";
        }
    } else {
        $error_message = "Webhook API is not available. Error: " . ($curlError ?: "HTTP $httpCode");
        $data_source = "Offline";
    }
    
    return [
        'whale_data' => $whale_data,
        'error_message' => $error_message,
        'data_source' => $data_source,
        'actual_source' => $actual_source
    ];
}

// Check if this is an AJAX request for data refresh
if (isset($_GET['ajax']) && $_GET['ajax'] === 'refresh') {
    header('Content-Type: application/json');
    $result = fetchWhaleData();
    $use_duckdb = isWebhookAvailable();
    
    // Build status data
    $status_data = [
        'api_status' => $use_duckdb ? 'connected' : 'disconnected',
        'whale_records' => count($result['whale_data']),
        'last_update' => null,
    ];
    
    if (!empty($result['whale_data'])) {
        $status_data['last_update'] = $result['whale_data'][0]['timestamp'] ?? null;
    }
    
    // Get health data from webhook for additional status
    if ($use_duckdb) {
        $ch = curl_init(WEBHOOK_API_URL . '/health');
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_TIMEOUT, 3);
        curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
        $healthResponse = curl_exec($ch);
        curl_close($ch);
        
        if ($healthResponse) {
            $healthData = json_decode($healthResponse, true);
            if ($healthData && isset($healthData['duckdb'])) {
                $status_data['duckdb_trades'] = $healthData['duckdb']['trades_in_hot_storage'] ?? 0;
                $status_data['duckdb_whale'] = $healthData['duckdb']['whale_movements_in_hot_storage'] ?? 0;
            }
        }
    }
    
    echo json_encode([
        'success' => true,
        'whale_data' => $result['whale_data'],
        'status_data' => $status_data,
        'error_message' => $result['error_message'],
        'data_source' => $result['data_source'],
        'actual_source' => $result['actual_source']
    ]);
    exit;
}

// Regular page load - fetch data normally
$result = fetchWhaleData();
$whale_data = $result['whale_data'];
$error_message = $result['error_message'];
$data_source = $result['data_source'];
$use_duckdb = isWebhookAvailable();

// --- Status Data ---
$status_data = [
    'api_status' => $use_duckdb ? 'connected' : 'disconnected',
    'whale_records' => count($whale_data),
    'last_update' => null,
];

if (!empty($whale_data)) {
    $status_data['last_update'] = $whale_data[0]['timestamp'] ?? null;
}

// Get health data from webhook for additional status
if ($use_duckdb) {
    $ch = curl_init(WEBHOOK_API_URL . '/health');
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_TIMEOUT, 3);
    curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
    $healthResponse = curl_exec($ch);
    curl_close($ch);
    
    if ($healthResponse) {
        $healthData = json_decode($healthResponse, true);
        if ($healthData && isset($healthData['duckdb'])) {
            $status_data['duckdb_trades'] = $healthData['duckdb']['trades_in_hot_storage'] ?? 0;
            $status_data['duckdb_whale'] = $healthData['duckdb']['whale_movements_in_hot_storage'] ?? 0;
        }
    }
}

$json_status_data = json_encode($status_data);
$json_whale_data = json_encode($whale_data);
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
    .whale-card {
        border-left: 3px solid transparent;
        margin-bottom: 1rem;
        transition: all 0.3s ease;
    }
    .whale-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(0,0,0,0.3);
    }
    .whale-card.whale-mega {
        border-left-color: #ffc700;
    }
    .whale-card.whale-large {
        border-left-color: #4A9EFF;
    }
    .whale-card.whale-normal {
        border-left-color: #00D4FF;
    }
    .whale-card.whale-moderate {
        border-left-color: #9D6CFF;
    }
    .whale-card.new-row {
        animation: slideIn 0.5s ease-out;
    }
    @keyframes slideIn {
        from {
            opacity: 0;
            transform: translateY(-20px);
        }
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }
    .mono-cell {
        font-family: 'Courier New', monospace;
        font-size: 0.85rem;
    }
    .whale-details-grid {
        display: grid;
        grid-template-columns: repeat(5, 1fr);
        gap: 1rem;
        padding-top: 1rem;
        border-top: 1px solid rgba(255,255,255,0.1);
        margin-top: 1rem;
    }
    @media (max-width: 1200px) {
        .whale-details-grid {
            grid-template-columns: repeat(3, 1fr);
        }
    }
    @media (max-width: 768px) {
        .whale-details-grid {
            grid-template-columns: 1fr;
        }
    }
    .whale-detail-item {
        text-align: center;
    }
    .whale-label {
        font-size: 0.7rem;
        font-weight: 600;
        text-transform: uppercase;
        color: rgba(255,255,255,0.6);
        margin-bottom: 0.25rem;
    }
    .whale-value {
        font-size: 0.9rem;
        font-weight: 600;
        color: rgba(255,255,255,0.9);
    }
    .wallet-link, .signature-link {
        font-family: 'Courier New', monospace;
        font-size: 0.85rem;
        color: rgb(var(--primary-rgb));
        cursor: pointer;
        text-decoration: none;
    }
    .wallet-link:hover, .signature-link:hover {
        text-decoration: underline;
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
    .live-indicator {
        display: inline-flex;
        align-items: center;
        gap: 0.5rem;
    }
    .live-dot {
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
</style>

<?php $styles = ob_get_clean(); ?>
<!-- This code is useful for internal styles -->

<!-- This code is useful for content -->
<?php ob_start(); ?>

                    <!-- Start::page-header -->
                    <div class="page-header-breadcrumb mb-3">
                        <div class="d-flex align-center justify-content-between flex-wrap">
                            <h1 class="page-title fw-medium fs-18 mb-0">Live Whale Activity</h1>
                            <ol class="breadcrumb mb-0">
                                <li class="breadcrumb-item"><a href="<?php echo $baseUrl; ?>/">Dashboard</a></li>
                                <li class="breadcrumb-item">Data Streams</li>
                                <li class="breadcrumb-item active" aria-current="page">Whale Activity</li>
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
                        <div id="whaleStatusBtn" class="status-btn <?php echo count($whale_data) > 0 ? 'status-good' : 'status-warning'; ?>">
                            <div class="status-title">Whale Stream</div>
                            <div class="status-info" id="whaleStatusInfo">
                                <?php echo count($whale_data) > 0 ? 'Active' : 'No Data'; ?>
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
                            <div class="status-info" id="recordCountInfo"><?php echo count($whale_data); ?></div>
                        </div>
                    </div>
                    <!-- End:: Status Buttons -->

                    <!-- Start:: Whale Activity Cards -->
                    <div class="row">
                        <div class="col-xl-12">
                            <div class="card custom-card">
                                <div class="card-header">
                                    <div class="card-title">
                                        <i class="ti ti-whale me-2"></i>Recent Whale Movements
                                    </div>
                                    <div class="ms-auto d-flex gap-2 align-items-center">
                                        <span class="badge bg-info-transparent" id="recordCount"><?php echo count($whale_data); ?> Records</span>
                                        <span class="badge bg-success-transparent live-indicator">
                                            <span class="live-dot"></span>
                                            Real-time
                                        </span>
                                    </div>
                                </div>
                                <div class="card-body">
                                    <div id="whaleTransactionsContainer">
                                        <?php if (!empty($whale_data)): ?>
                                            <?php foreach ($whale_data as $row): 
                                                // Determine whale type class
                                                $whale_class = 'whale-normal';
                                                $whale_badge_class = 'bg-info-transparent';
                                                $whale_type_normalized = str_replace(' ', '_', strtoupper($row['whale_type'] ?? 'UNKNOWN'));
                                                
                                                if (stripos($row['whale_type'] ?? '', 'MEGA') !== false) {
                                                    $whale_class = 'whale-mega';
                                                    $whale_badge_class = 'bg-warning-transparent';
                                                } elseif (stripos($row['whale_type'] ?? '', 'LARGE') !== false) {
                                                    $whale_class = 'whale-large';
                                                    $whale_badge_class = 'bg-primary-transparent';
                                                } elseif (stripos($row['whale_type'] ?? '', 'MODERATE') !== false) {
                                                    $whale_class = 'whale-moderate';
                                                    $whale_badge_class = 'bg-secondary-transparent';
                                                }
                                                
                                                // Check if mega transaction (>100 SOL change)
                                                $sol_change = floatval($row['sol_change'] ?? 0);
                                                $is_mega_tx = abs($sol_change) > 100;
                                                $sol_class = $sol_change >= 0 ? 'text-success' : 'text-danger';
                                                
                                                // Check if large balance (>50k SOL)
                                                $current_balance = floatval($row['current_balance'] ?? 0);
                                                $is_large_balance = $current_balance > 50000;
                                                
                                                // Direction badge
                                                $direction_lower = strtolower($row['direction'] ?? 'unknown');
                                                $direction_badge_class = $direction_lower === 'in' ? 'bg-success-transparent' : 'bg-danger-transparent';
                                                $direction_text = $direction_lower === 'in' ? 'RECEIVING' : 'SENDING';
                                            ?>
                                            <div class="card custom-card whale-card <?php echo $whale_class; ?>" data-signature="<?php echo htmlspecialchars($row['signature'] ?? ''); ?>">
                                                <div class="card-body">
                                                    <div class="row align-items-center mb-3">
                                                        <div class="col-md-2 text-center">
                                                            <div class="text-muted fs-12 mb-1">Timestamp</div>
                                                            <div class="mono-cell"><?php echo date('M d H:i:s', strtotime($row['timestamp'] ?? 'now')); ?></div>
                                                        </div>
                                                        <div class="col-md-2 text-center">
                                                            <div class="text-muted fs-12 mb-1">Direction</div>
                                                            <span class="badge <?php echo $direction_badge_class; ?>"><?php echo $direction_text; ?></span>
                                                        </div>
                                                        <div class="col-md-2 text-center">
                                                            <div class="text-muted fs-12 mb-1">Whale Type</div>
                                                            <span class="badge <?php echo $whale_badge_class; ?>"><?php echo htmlspecialchars($whale_type_normalized); ?></span>
                                                        </div>
                                                        <div class="col-md-3 text-end">
                                                            <div class="text-muted fs-12 mb-1">SOL Change</div>
                                                            <div class="fw-semibold <?php echo $sol_class; ?> <?php echo $is_mega_tx ? 'fs-16' : ''; ?>">
                                                                <?php echo ($sol_change >= 0 ? '+' : '') . number_format($sol_change, 2); ?> SOL
                                                            </div>
                                                        </div>
                                                        <div class="col-md-3 text-end">
                                                            <div class="text-muted fs-12 mb-1">Current Balance</div>
                                                            <div class="fw-semibold <?php echo $is_large_balance ? 'text-success' : ''; ?>">
                                                                <?php echo number_format($current_balance, 2); ?> SOL
                                                            </div>
                                                        </div>
                                                    </div>
                                                    
                                                    <div class="whale-details-grid">
                                                        <div class="whale-detail-item">
                                                            <div class="whale-label">Previous Balance</div>
                                                            <div class="whale-value mono-cell"><?php echo number_format(floatval($row['previous_balance'] ?? 0), 2); ?> SOL</div>
                                                        </div>
                                                        <div class="whale-detail-item">
                                                            <div class="whale-label">Absolute Change</div>
                                                            <div class="whale-value mono-cell"><?php echo number_format(floatval($row['abs_change'] ?? 0), 2); ?> SOL</div>
                                                        </div>
                                                        <div class="whale-detail-item">
                                                            <div class="whale-label">Fee Paid</div>
                                                            <div class="whale-value mono-cell"><?php echo number_format(floatval($row['fee_paid'] ?? 0), 6); ?> SOL</div>
                                                        </div>
                                                        <div class="whale-detail-item">
                                                            <div class="whale-label">Wallet Address</div>
                                                            <div class="whale-value">
                                                                <span class="wallet-link" 
                                                                      onclick="copyToClipboard('<?php echo htmlspecialchars($row['wallet_address'] ?? ''); ?>', this)"
                                                                      title="Click to copy">
                                                                    <?php 
                                                                    $wallet = htmlspecialchars($row['wallet_address'] ?? '');
                                                                    echo substr($wallet, 0, 6) . '...' . substr($wallet, -4);
                                                                    ?>
                                                                </span>
                                                            </div>
                                                        </div>
                                                        <div class="whale-detail-item">
                                                            <div class="whale-label">Transaction</div>
                                                            <div class="whale-value">
                                                                <a href="https://solscan.io/tx/<?php echo htmlspecialchars($row['signature'] ?? ''); ?>" 
                                                                   target="_blank" 
                                                                   class="signature-link"
                                                                   title="View on Solscan">
                                                                    <?php 
                                                                    $sig = htmlspecialchars($row['signature'] ?? '');
                                                                    echo substr($sig, 0, 6) . '...' . substr($sig, -4);
                                                                    ?>
                                                                </a>
                                                            </div>
                                                        </div>
                                                    </div>
                                                </div>
                                            </div>
                                            <?php endforeach; ?>
                                        <?php else: ?>
                                            <div class="d-flex flex-column align-items-center py-5">
                                                <i class="ti ti-database-off fs-1 text-muted mb-2"></i>
                                                <h6 class="text-muted mb-1">No whale activity data in DuckDB hot storage</h6>
                                                <p class="text-muted mb-0 fs-13">Data will appear as new whale movements arrive via QuickNode.</p>
                                                <p class="text-muted mb-0 fs-12 mt-2">DuckDB in-memory stores last 24 hours only.</p>
                                            </div>
                                        <?php endif; ?>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                    <!-- End:: Whale Activity Cards -->

<?php $content = ob_get_clean(); ?>
<!-- This code is useful for content -->

<!-- This code is useful for internal scripts -->
<?php ob_start(); ?>

        <script>
            // Status data from PHP
            window.statusData = <?php echo $json_status_data; ?>;
            window.whaleData = <?php echo $json_whale_data; ?>;
            
            // Copy to clipboard functionality
            function copyToClipboard(text, element) {
                navigator.clipboard.writeText(text).then(() => {
                    const originalText = element.textContent;
                    element.textContent = 'Copied!';
                    setTimeout(() => {
                        element.textContent = originalText;
                    }, 2000);
                }).catch(err => {
                    console.error('Failed to copy:', err);
                });
            }
            
            // Format number with commas
            function formatNumber(num, decimals = 2) {
                return parseFloat(num || 0).toLocaleString('en-US', {
                    minimumFractionDigits: decimals,
                    maximumFractionDigits: decimals
                });
            }
            
            // Status refresh functionality - Always use UTC time
            function refreshStatus(statusData) {
                // Get current time in UTC milliseconds
                const nowUtc = Date.now();
                
                function updateButton(btnId, infoId, timestamp) {
                    const btn = document.getElementById(btnId);
                    const info = document.getElementById(infoId);
                    
                    if (!btn || !info) return;
                    
                    if (!timestamp) {
                        btn.className = 'status-btn status-warning';
                        info.textContent = 'No data';
                        return;
                    }
                    
                    // Parse timestamp - if no timezone specified, treat as local time (CET)
                    // Don't force UTC by adding 'Z' - let browser parse as local time
                    const dataTime = new Date(timestamp).getTime();
                    const diffMs = nowUtc - dataTime;
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
                
                // Update whale stream status
                if (statusData.last_update) {
                    updateButton('whaleStatusBtn', 'whaleStatusInfo', statusData.last_update);
                }
                
                updateButton('getPricesBtn', 'getPricesInfo', statusData.get_prices);
                updateButton('priceAnalysisBtn', 'priceAnalysisInfo', statusData.price_analysis);
                updateButton('activeCycleBtn', 'activeCycleInfo', statusData.active_cycle);
            }
            
            // Get whale type classes
            function getWhaleTypeClasses(whaleType) {
                const type = (whaleType || '').toUpperCase();
                let rowClass = 'whale-normal';
                let badgeClass = 'bg-info-transparent';
                
                if (type.includes('MEGA')) {
                    rowClass = 'whale-mega';
                    badgeClass = 'bg-warning-transparent';
                } else if (type.includes('LARGE')) {
                    rowClass = 'whale-large';
                    badgeClass = 'bg-primary-transparent';
                } else if (type.includes('MODERATE')) {
                    rowClass = 'whale-moderate';
                    badgeClass = 'bg-secondary-transparent';
                }
                
                return { rowClass, badgeClass };
            }
            
            // Track seen signatures
            let lastSignatures = new Set();
            document.querySelectorAll('.whale-card').forEach(card => {
                const sig = card.getAttribute('data-signature');
                if (sig) lastSignatures.add(sig);
            });
            
            // Update whale table with new data
            function updateWhaleData(whaleData) {
                const container = document.getElementById('whaleTransactionsContainer');
                const recordCount = document.getElementById('recordCount');
                const recordCountInfo = document.getElementById('recordCountInfo');
                
                if (!container) return;
                
                // Update record count
                const count = whaleData.length;
                if (recordCount) recordCount.textContent = count + ' Records';
                if (recordCountInfo) recordCountInfo.textContent = count;
                
                if (count === 0) {
                    container.innerHTML = `
                        <div class="d-flex flex-column align-items-center py-5">
                            <i class="ti ti-database-off fs-1 text-muted mb-2"></i>
                            <h6 class="text-muted mb-1">No whale activity data in DuckDB hot storage</h6>
                            <p class="text-muted mb-0 fs-13">Data will appear as new whale movements arrive via QuickNode.</p>
                            <p class="text-muted mb-0 fs-12 mt-2">DuckDB in-memory stores last 24 hours only.</p>
                        </div>
                    `;
                    return;
                }
                
                const newSignatures = new Set();
                const cards = [];
                
                whaleData.forEach(row => {
                    newSignatures.add(row.signature);
                    const isNew = !lastSignatures.has(row.signature);
                    
                    const { rowClass, badgeClass } = getWhaleTypeClasses(row.whale_type);
                    const whaleTypeNormalized = (row.whale_type || 'UNKNOWN').replace(/ /g, '_').toUpperCase();
                    
                    // Direction handling
                    const direction = (row.direction || 'out').toLowerCase();
                    const directionBadgeClass = direction === 'in' ? 'bg-success-transparent' : 'bg-danger-transparent';
                    const directionText = direction === 'in' ? 'RECEIVING' : 'SENDING';
                    
                    const timestamp = new Date(row.timestamp).toLocaleString('en-US', {
                        month: 'short',
                        day: 'numeric',
                        hour: '2-digit',
                        minute: '2-digit',
                        second: '2-digit'
                    });
                    
                    const wallet = row.wallet_address || '';
                    const walletShort = wallet.length > 10 ? wallet.substring(0, 6) + '...' + wallet.substring(wallet.length - 4) : wallet;
                    
                    const sig = row.signature || '';
                    const sigShort = sig.length > 10 ? sig.substring(0, 6) + '...' + sig.substring(sig.length - 4) : sig;
                    
                    // SOL change handling
                    const solChange = parseFloat(row.sol_change || 0);
                    const absSolChange = Math.abs(solChange);
                    const isMegaTx = absSolChange > 100;
                    const solChangeClass = solChange >= 0 ? 'text-success' : 'text-danger';
                    const solChangePrefix = solChange >= 0 ? '+' : '';
                    
                    // Balance handling
                    const currentBalance = parseFloat(row.current_balance || 0);
                    const isLargeBalance = currentBalance > 50000;
                    const balanceClass = isLargeBalance ? 'text-success' : '';
                    
                    cards.push(`
                        <div class="card custom-card whale-card ${rowClass} ${isNew ? 'new-row' : ''}" data-signature="${sig}">
                            <div class="card-body">
                                <div class="row align-items-center mb-3">
                                    <div class="col-md-2 text-center">
                                        <div class="text-muted fs-12 mb-1">Timestamp</div>
                                        <div class="mono-cell">${timestamp}</div>
                                    </div>
                                    <div class="col-md-2 text-center">
                                        <div class="text-muted fs-12 mb-1">Direction</div>
                                        <span class="badge ${directionBadgeClass}">${directionText}</span>
                                    </div>
                                    <div class="col-md-2 text-center">
                                        <div class="text-muted fs-12 mb-1">Whale Type</div>
                                        <span class="badge ${badgeClass}">${whaleTypeNormalized}</span>
                                    </div>
                                    <div class="col-md-3 text-end">
                                        <div class="text-muted fs-12 mb-1">SOL Change</div>
                                        <div class="fw-semibold ${solChangeClass} ${isMegaTx ? 'fs-16' : ''}">
                                            ${solChangePrefix}${formatNumber(solChange, 2)} SOL
                                        </div>
                                    </div>
                                    <div class="col-md-3 text-end">
                                        <div class="text-muted fs-12 mb-1">Current Balance</div>
                                        <div class="fw-semibold ${balanceClass}">
                                            ${formatNumber(currentBalance, 2)} SOL
                                        </div>
                                    </div>
                                </div>
                                
                                <div class="whale-details-grid">
                                    <div class="whale-detail-item">
                                        <div class="whale-label">Previous Balance</div>
                                        <div class="whale-value mono-cell">${formatNumber(row.previous_balance, 2)} SOL</div>
                                    </div>
                                    <div class="whale-detail-item">
                                        <div class="whale-label">Absolute Change</div>
                                        <div class="whale-value mono-cell">${formatNumber(row.abs_change, 2)} SOL</div>
                                    </div>
                                    <div class="whale-detail-item">
                                        <div class="whale-label">Fee Paid</div>
                                        <div class="whale-value mono-cell">${formatNumber(row.fee_paid, 6)} SOL</div>
                                    </div>
                                    <div class="whale-detail-item">
                                        <div class="whale-label">Wallet Address</div>
                                        <div class="whale-value">
                                            <span class="wallet-link" 
                                                  onclick="copyToClipboard('${wallet}', this)"
                                                  title="Click to copy">
                                                ${walletShort}
                                            </span>
                                        </div>
                                    </div>
                                    <div class="whale-detail-item">
                                        <div class="whale-label">Transaction</div>
                                        <div class="whale-value">
                                            <a href="https://solscan.io/tx/${sig}" 
                                               target="_blank" 
                                               class="signature-link"
                                               title="View on Solscan">
                                                ${sigShort}
                                            </a>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    `);
                });
                
                container.innerHTML = cards.join('');
                lastSignatures = newSignatures;
            }
            
            // Update data source badge
            function updateDataSourceBadge(dataSource, actualSource) {
                const badge = document.getElementById('dataSourceBadge');
                if (!badge) return;
                
                badge.textContent = dataSource || 'No Data';
                
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
            async function fetchWhaleData() {
                try {
                    const response = await fetch('?ajax=refresh');
                    const data = await response.json();
                    
                    if (data.success) {
                        window.statusData = data.status_data;
                        window.whaleData = data.whale_data;
                        
                        updateWhaleData(data.whale_data);
                        refreshStatus(data.status_data);
                        
                        if (data.data_source) {
                            updateDataSourceBadge(data.data_source, data.actual_source);
                        }
                    }
                } catch (error) {
                    console.error('Error fetching whale data:', error);
                }
            }
            
            // Initial status refresh
            refreshStatus(window.statusData);
            
            // Refresh data every 1 second
            setInterval(fetchWhaleData, 1000);
            
            // Also refresh status every 5 seconds (for time-based updates)
            setInterval(() => refreshStatus(window.statusData), 5000);
        </script>

<?php $scripts = ob_get_clean(); ?>
<!-- This code is useful for internal scripts -->

<!-- This code use for render base file -->
<?php include __DIR__ . '/../../pages/layouts/base.php'; ?>
<!-- This code use for render base file -->

