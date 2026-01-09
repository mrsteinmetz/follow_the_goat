<?php
/**
 * Whale Activity Page - Live Whale Movements
 * Displays real-time whale wallet activity from PostgreSQL via website_api.py
 * 
 * Data Source: PostgreSQL via website_api.py (port 5051)
 */

require_once __DIR__ . '/../../includes/config.php';
require_once __DIR__ . '/../../includes/DatabaseClient.php';

// --- Base URL for template ---
$baseUrl = '../..';

// --- Initialize Database Client ---
$dbClient = new DatabaseClient();

// --- Fetch Whale Data from PostgreSQL ---
function fetchWhaleData($dbClient) {
    $whale_data = [];
    $error_message = null;
    $data_source = "No Data";
    $actual_source = null;

    try {
        // Call website_api.py /whale_movements endpoint
        $result = $dbClient->get('/whale_movements?limit=100');
        
        if ($result && isset($result['success']) && $result['success'] === true) {
            $whale_data = $result['results'] ?? [];
            $actual_source = $result['source'] ?? 'postgres';
            
            // Determine source display
            switch ($actual_source) {
                case 'postgres':
                case 'postgresql':
                    $data_source = "🐘 PostgreSQL";
                    break;
                default:
                    $data_source = "Unknown Source";
            }
        } else {
            $error_message = $result['error'] ?? 'Unknown error from API';
            $data_source = "API Error";
        }
    } catch (Exception $e) {
        $error_message = "API connection failed: " . $e->getMessage();
        $data_source = "Offline";
        $actual_source = null;
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
    $result = fetchWhaleData($dbClient);
    
    // Build status data
    $status_data = [
        'api_status' => 'connected',
        'whale_records' => count($result['whale_data']),
        'last_update' => null,
    ];
    
    if (!empty($result['whale_data'])) {
        $status_data['last_update'] = $result['whale_data'][0]['timestamp'] ?? null;
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
$result = fetchWhaleData($dbClient);
$whale_data = $result['whale_data'];
$error_message = $result['error_message'];
$data_source = $result['data_source'];
$actual_source = $result['actual_source'];

// --- Status Data ---
$status_data = [
    'api_status' => 'connected',
    'whale_records' => count($whale_data),
    'last_update' => null,
];

if (!empty($whale_data)) {
    $status_data['last_update'] = $whale_data[0]['timestamp'] ?? null;
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
        background: rgb(var(--success-rgb));
        color: white;
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
                    <div id="dataSourceBadge" class="data-source-badge">
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
                                        <i class="ti ti-whale me-2"></i>Recent Whale Movements (>$10k USD)
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
                                                $whale_type = $row['whale_type'] ?? 'MODERATE_WHALE';
                                                
                                                if (stripos($whale_type, 'MEGA') !== false) {
                                                    $whale_class = 'whale-mega';
                                                    $whale_badge_class = 'bg-warning-transparent';
                                                } elseif (stripos($whale_type, 'LARGE') !== false) {
                                                    $whale_class = 'whale-large';
                                                    $whale_badge_class = 'bg-primary-transparent';
                                                } elseif (stripos($whale_type, 'WHALE') !== false && stripos($whale_type, 'MODERATE') === false) {
                                                    $whale_class = 'whale-normal';
                                                    $whale_badge_class = 'bg-info-transparent';
                                                } elseif (stripos($whale_type, 'MODERATE') !== false) {
                                                    $whale_class = 'whale-moderate';
                                                    $whale_badge_class = 'bg-secondary-transparent';
                                                }
                                                
                                                // Format amounts
                                                $stablecoin_amount = floatval($row['stablecoin_amount'] ?? 0);
                                                $sol_amount = floatval($row['sol_amount'] ?? 0);
                                                $sol_price = floatval($row['sol_price_at_trade'] ?? 0);
                                                
                                                // Direction badge
                                                $direction_lower = strtolower($row['direction'] ?? 'unknown');
                                                $direction_badge_class = $direction_lower === 'buy' ? 'bg-success-transparent' : 'bg-danger-transparent';
                                                $direction_text = $direction_lower === 'buy' ? 'BUYING' : 'SELLING';
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
                                                            <span class="badge <?php echo $whale_badge_class; ?>"><?php echo htmlspecialchars($whale_type); ?></span>
                                                        </div>
                                                        <div class="col-md-3 text-end">
                                                            <div class="text-muted fs-12 mb-1">Trade Value (USD)</div>
                                                            <div class="fw-semibold text-primary fs-16">
                                                                $<?php echo number_format($stablecoin_amount, 2); ?>
                                                            </div>
                                                        </div>
                                                        <div class="col-md-3 text-end">
                                                            <div class="text-muted fs-12 mb-1">SOL Amount</div>
                                                            <div class="fw-semibold">
                                                                <?php echo number_format($sol_amount, 2); ?> SOL
                                                            </div>
                                                        </div>
                                                    </div>
                                                    
                                                    <div class="whale-details-grid">
                                                        <div class="whale-detail-item">
                                                            <div class="whale-label">SOL Price</div>
                                                            <div class="whale-value mono-cell">$<?php echo number_format($sol_price, 2); ?></div>
                                                        </div>
                                                        <div class="whale-detail-item">
                                                            <div class="whale-label">USD Value</div>
                                                            <div class="whale-value mono-cell">$<?php echo number_format($stablecoin_amount, 2); ?></div>
                                                        </div>
                                                        <div class="whale-detail-item">
                                                            <div class="whale-label">SOL Volume</div>
                                                            <div class="whale-value mono-cell"><?php echo number_format($sol_amount, 4); ?> SOL</div>
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
                                                <h6 class="text-muted mb-1">No whale activity data available</h6>
                                                <p class="text-muted mb-0 fs-13">Whale trades (>$10k) will appear as they arrive via QuickNode.</p>
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
            
            // Status refresh functionality
            function refreshStatus(statusData) {
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
                } else if (type.includes('WHALE') && !type.includes('MODERATE')) {
                    rowClass = 'whale-normal';
                    badgeClass = 'bg-info-transparent';
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
                            <h6 class="text-muted mb-1">No whale activity data available</h6>
                            <p class="text-muted mb-0 fs-13">Whale trades (>$10k) will appear as they arrive via QuickNode.</p>
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
                    const whaleType = row.whale_type || 'MODERATE_WHALE';
                    
                    // Direction handling
                    const direction = (row.direction || 'sell').toLowerCase();
                    const directionBadgeClass = direction === 'buy' ? 'bg-success-transparent' : 'bg-danger-transparent';
                    const directionText = direction === 'buy' ? 'BUYING' : 'SELLING';
                    
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
                    
                    const stablecoinAmount = parseFloat(row.stablecoin_amount || 0);
                    const solAmount = parseFloat(row.sol_amount || 0);
                    const solPrice = parseFloat(row.sol_price_at_trade || 0);
                    
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
                                        <span class="badge ${badgeClass}">${whaleType}</span>
                                    </div>
                                    <div class="col-md-3 text-end">
                                        <div class="text-muted fs-12 mb-1">Trade Value (USD)</div>
                                        <div class="fw-semibold text-primary fs-16">
                                            $${formatNumber(stablecoinAmount, 2)}
                                        </div>
                                    </div>
                                    <div class="col-md-3 text-end">
                                        <div class="text-muted fs-12 mb-1">SOL Amount</div>
                                        <div class="fw-semibold">
                                            ${formatNumber(solAmount, 2)} SOL
                                        </div>
                                    </div>
                                </div>
                                
                                <div class="whale-details-grid">
                                    <div class="whale-detail-item">
                                        <div class="whale-label">SOL Price</div>
                                        <div class="whale-value mono-cell">$${formatNumber(solPrice, 2)}</div>
                                    </div>
                                    <div class="whale-detail-item">
                                        <div class="whale-label">USD Value</div>
                                        <div class="whale-value mono-cell">$${formatNumber(stablecoinAmount, 2)}</div>
                                    </div>
                                    <div class="whale-detail-item">
                                        <div class="whale-label">SOL Volume</div>
                                        <div class="whale-value mono-cell">${formatNumber(solAmount, 4)} SOL</div>
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
                
                switch(actualSource) {
                    case 'postgres':
                    case 'postgresql':
                        badge.style.background = 'rgb(var(--success-rgb))';
                        break;
                    default:
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
            
            // Refresh data every 2 seconds
            setInterval(fetchWhaleData, 2000);
            
            // Also refresh status every 5 seconds (for time-based updates)
            setInterval(() => refreshStatus(window.statusData), 5000);
        </script>

<?php $scripts = ob_get_clean(); ?>
<!-- This code is useful for internal scripts -->

<!-- This code use for render base file -->
<?php include __DIR__ . '/../../pages/layouts/base.php'; ?>
<!-- This code use for render base file -->
