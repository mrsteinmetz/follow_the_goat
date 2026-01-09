<?php
/**
 * Transactions Page - Live Transaction Feed
 * Displays real-time stablecoin transactions from PostgreSQL via website_api.py
 * 
 * Data Source: PostgreSQL via website_api.py (port 5051)
 */

require_once __DIR__ . '/../../includes/config.php';
require_once __DIR__ . '/../../includes/DatabaseClient.php';

// --- Base URL for template ---
$baseUrl = '../..';

// --- Initialize Database Client ---
$dbClient = new DatabaseClient();

// --- Fetch Transactions Data from PostgreSQL ---
function fetchTransactionsData($dbClient) {
    $transactions_data = [];
    $volume_data = [
        'total_volume' => 0,
        'buy_volume' => 0,
        'sell_volume' => 0
    ];
    $stats_data = [
        'total_in_db' => 0,
        'first_transaction' => null,
        'avg_size' => 0,
        'largest_tx' => 0
    ];
    $error_message = null;
    $data_source = "No Data";
    $actual_source = null;

    try {
        // Call website_api.py /trades endpoint
        $result = $dbClient->get('/trades?limit=30');
        
        if ($result && isset($result['success']) && $result['success'] === true) {
            $transactions_data = $result['results'] ?? [];
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
            
            // Calculate volume from fetched transactions
            $largest = 0;
            foreach ($transactions_data as $tx) {
                $amount = floatval($tx['stablecoin_amount'] ?? 0);
                $direction = strtolower($tx['direction'] ?? '');
                $volume_data['total_volume'] += $amount;
                if ($direction === 'buy') {
                    $volume_data['buy_volume'] += $amount;
                } else {
                    $volume_data['sell_volume'] += $amount;
                }
                
                // Track largest transaction
                if ($amount > $largest) {
                    $largest = $amount;
                }
            }
            
            // Calculate average (from displayed transactions)
            $stats_data['avg_size'] = count($transactions_data) > 0 
                ? $volume_data['total_volume'] / count($transactions_data) 
                : 0;
            $stats_data['largest_tx'] = $largest;
            $stats_data['total_in_db'] = $result['count'] ?? 0;
            
            // First transaction is oldest in the list
            if (!empty($transactions_data)) {
                $stats_data['first_transaction'] = $transactions_data[count($transactions_data) - 1]['trade_timestamp'] ?? null;
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
        'transactions_data' => $transactions_data,
        'volume_data' => $volume_data,
        'stats_data' => $stats_data,
        'error_message' => $error_message,
        'data_source' => $data_source,
        'actual_source' => $actual_source
    ];
}

// Check if this is an AJAX request for data refresh
if (isset($_GET['ajax']) && $_GET['ajax'] === 'refresh') {
    header('Content-Type: application/json');
    $result = fetchTransactionsData($dbClient);
    
    // Build status data
    $status_data = [
        'api_status' => 'connected',
        'tx_records' => count($result['transactions_data']),
        'last_update' => null,
    ];
    
    if (!empty($result['transactions_data'])) {
        $status_data['last_update'] = $result['transactions_data'][0]['trade_timestamp'] ?? null;
    }
    
    echo json_encode([
        'success' => true,
        'transactions' => $result['transactions_data'],
        'volume_data' => $result['volume_data'],
        'stats_data' => $result['stats_data'],
        'status_data' => $status_data,
        'error_message' => $result['error_message'],
        'data_source' => $result['data_source'],
        'actual_source' => $result['actual_source']
    ]);
    exit;
}

// Regular page load - fetch data normally
$result = fetchTransactionsData($dbClient);
$transactions_data = $result['transactions_data'];
$volume_data = $result['volume_data'];
$stats_data = $result['stats_data'];
$error_message = $result['error_message'];
$data_source = $result['data_source'];
$actual_source = $result['actual_source'];

// --- Status Data ---
$status_data = [
    'api_status' => 'connected',
    'tx_records' => count($transactions_data),
    'last_update' => null,
];

if (!empty($transactions_data)) {
    $status_data['last_update'] = $transactions_data[0]['trade_timestamp'] ?? null;
}

$json_status_data = json_encode($status_data);
$json_transactions_data = json_encode($transactions_data);
$json_volume_data = json_encode($volume_data);
$json_stats_data = json_encode($stats_data);
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
    .direction-badge {
        display: inline-flex;
        align-items: center;
        gap: 0.25rem;
        padding: 0.25rem 0.75rem;
        border-radius: 0.375rem;
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
    }
    .direction-badge.buy {
        background: rgba(var(--success-rgb), 0.15);
        color: rgb(var(--success-rgb));
        border: 1px solid rgba(var(--success-rgb), 0.3);
    }
    .direction-badge.sell {
        background: rgba(var(--danger-rgb), 0.15);
        color: rgb(var(--danger-rgb));
        border: 1px solid rgba(var(--danger-rgb), 0.3);
    }
    .wallet-address-link {
        font-family: 'Courier New', monospace;
        font-size: 0.85rem;
        color: rgb(var(--primary-rgb));
        cursor: pointer;
        text-decoration: none;
    }
    .wallet-address-link:hover {
        text-decoration: underline;
    }
    .signature-link {
        font-family: 'Courier New', monospace;
        font-size: 0.85rem;
        color: rgb(var(--primary-rgb));
        text-decoration: none;
    }
    .signature-link:hover {
        text-decoration: underline;
    }
    .table-responsive {
        max-height: 70vh;
        overflow-y: auto;
    }
    .mono-cell {
        font-family: 'Courier New', monospace;
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
    @keyframes fadeIn {
        from { opacity: 0; background: rgba(var(--success-rgb), 0.2); }
        to { opacity: 1; background: transparent; }
    }
    tr.new-row {
        animation: fadeIn 0.5s ease-out;
    }
</style>

<?php $styles = ob_get_clean(); ?>
<!-- This code is useful for internal styles -->

<!-- This code is useful for content -->
<?php ob_start(); ?>

                    <!-- Start::page-header -->
                    <div class="page-header-breadcrumb mb-3">
                        <div class="d-flex align-center justify-content-between flex-wrap">
                            <h1 class="page-title fw-medium fs-18 mb-0">Live Transactions</h1>
                            <ol class="breadcrumb mb-0">
                                <li class="breadcrumb-item"><a href="<?php echo $baseUrl; ?>/">Dashboard</a></li>
                                <li class="breadcrumb-item">Data Streams</li>
                                <li class="breadcrumb-item active" aria-current="page">Transactions</li>
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

                    <!-- Start:: Volume Summary Cards -->
                    <div class="row mb-3">
                        <div class="col-xl-4">
                            <div class="card custom-card">
                                <div class="card-body">
                                    <div class="d-flex align-items-center justify-content-between mb-2">
                                        <span class="text-muted fs-12">Total Volume (Recent)</span>
                                    </div>
                                    <h3 class="mb-0" id="totalVolume">$<?php echo number_format($volume_data['total_volume'], 2); ?></h3>
                                </div>
                            </div>
                        </div>
                        <div class="col-xl-4">
                            <div class="card custom-card">
                                <div class="card-body">
                                    <div class="d-flex align-items-center justify-content-between mb-2">
                                        <span class="text-muted fs-12">Buy Volume</span>
                                    </div>
                                    <h3 class="mb-0 text-success" id="buyVolume">$<?php echo number_format($volume_data['buy_volume'], 2); ?></h3>
                                </div>
                            </div>
                        </div>
                        <div class="col-xl-4">
                            <div class="card custom-card">
                                <div class="card-body">
                                    <div class="d-flex align-items-center justify-content-between mb-2">
                                        <span class="text-muted fs-12">Sell Volume</span>
                                    </div>
                                    <h3 class="mb-0 text-danger" id="sellVolume">$<?php echo number_format($volume_data['sell_volume'], 2); ?></h3>
                                </div>
                            </div>
                        </div>
                    </div>
                    <!-- End:: Volume Summary Cards -->

                    <!-- Start:: Status Buttons -->
                    <div class="status-grid mb-3">
                        <div id="txStatusBtn" class="status-btn <?php echo count($transactions_data) > 0 ? 'status-good' : 'status-warning'; ?>">
                            <div class="status-title">Transaction Stream</div>
                            <div class="status-info" id="txStatusInfo">
                                <?php echo count($transactions_data) > 0 ? 'Active' : 'No Data'; ?>
                            </div>
                        </div>
                        <div id="totalInDbBtn" class="status-btn status-good">
                            <div class="status-title">Total in Database</div>
                            <div class="status-info" id="totalInDbInfo"><?php echo number_format($stats_data['total_in_db']); ?></div>
                        </div>
                        <div id="firstTxBtn" class="status-btn status-good">
                            <div class="status-title">First Transaction</div>
                            <div class="status-info" id="firstTxInfo">
                                <?php 
                                if ($stats_data['first_transaction']) {
                                    $first_time = strtotime($stats_data['first_transaction']);
                                    $seconds_ago = time() - $first_time;
                                    $mins_ago = floor($seconds_ago / 60);
                                    
                                    if ($seconds_ago < 60) {
                                        echo $seconds_ago . 's ago';
                                    } elseif ($mins_ago < 60) {
                                        echo $mins_ago . 'm ago';
                                    } else {
                                        $hours_ago = floor($mins_ago / 60);
                                        echo $hours_ago . 'h ago';
                                    }
                                } else {
                                    echo 'N/A';
                                }
                                ?>
                            </div>
                        </div>
                        <div id="avgSizeBtn" class="status-btn status-good">
                            <div class="status-title">Avg Transaction</div>
                            <div class="status-info" id="avgSizeInfo">$<?php echo number_format($stats_data['avg_size'], 2); ?></div>
                        </div>
                        <div id="largestTxBtn" class="status-btn status-good">
                            <div class="status-title">Largest (Displayed)</div>
                            <div class="status-info" id="largestTxInfo">$<?php echo number_format($stats_data['largest_tx'], 2); ?></div>
                        </div>
                    </div>
                    <!-- End:: Status Buttons -->

                    <!-- Start:: Transactions Table -->
                    <div class="row">
                        <div class="col-xl-12">
                            <div class="card custom-card">
                                <div class="card-header">
                                    <div class="card-title">
                                        <i class="ti ti-transfer me-2"></i>Transaction Flow
                                    </div>
                                    <div class="ms-auto d-flex gap-2 align-items-center">
                                        <span class="badge bg-info-transparent" id="recordCount"><?php echo count($transactions_data); ?> Records</span>
                                        <span class="badge bg-success-transparent live-indicator">
                                            <span class="live-dot"></span>
                                            Real-time
                                        </span>
                                    </div>
                                </div>
                                <div class="card-body">
                                    <div class="table-responsive">
                                        <table class="table table-bordered text-nowrap">
                                            <thead class="table-dark">
                                                <tr>
                                                    <th>Direction</th>
                                                    <th class="text-end">Amount (USD)</th>
                                                    <th>Timestamp</th>
                                                    <th>Wallet Address</th>
                                                    <th>Signature</th>
                                                </tr>
                                            </thead>
                                            <tbody id="transactionsTableBody">
                                                <?php if (!empty($transactions_data)): ?>
                                                    <?php foreach ($transactions_data as $tx): 
                                                        $direction = strtolower($tx['direction'] ?? 'unknown');
                                                        $amount = floatval($tx['stablecoin_amount'] ?? 0);
                                                    ?>
                                                    <tr data-signature="<?php echo htmlspecialchars($tx['signature'] ?? ''); ?>">
                                                        <td>
                                                            <span class="direction-badge <?php echo $direction; ?>">
                                                                <?php echo strtoupper($direction); ?>
                                                            </span>
                                                        </td>
                                                        <td class="text-end fw-semibold <?php echo $direction === 'buy' ? 'text-success' : 'text-danger'; ?>">$<?php echo number_format($amount, 2); ?></td>
                                                        <td class="mono-cell"><?php echo date('M d, Y H:i:s', strtotime($tx['trade_timestamp'] ?? 'now')); ?></td>
                                                        <td>
                                                            <span class="wallet-address-link" 
                                                                  data-address="<?php echo htmlspecialchars($tx['wallet_address'] ?? ''); ?>"
                                                                  onclick="copyToClipboard(this)"
                                                                  title="Click to copy">
                                                                <?php 
                                                                $wallet = htmlspecialchars($tx['wallet_address'] ?? '');
                                                                echo substr($wallet, 0, 6) . '...' . substr($wallet, -4);
                                                                ?>
                                                            </span>
                                                        </td>
                                                        <td>
                                                            <a href="https://solscan.io/tx/<?php echo htmlspecialchars($tx['signature'] ?? ''); ?>" 
                                                               target="_blank" 
                                                               class="signature-link"
                                                               title="View on Solscan">
                                                                <?php 
                                                                $sig = htmlspecialchars($tx['signature'] ?? '');
                                                                echo substr($sig, 0, 8) . '...' . substr($sig, -6);
                                                                ?>
                                                            </a>
                                                        </td>
                                                    </tr>
                                                    <?php endforeach; ?>
                                                <?php else: ?>
                                                    <tr>
                                                        <td colspan="5" class="text-center py-4">
                                                            <div class="d-flex flex-column align-items-center">
                                                                <i class="ti ti-database-off fs-1 text-muted mb-2"></i>
                                                                <h6 class="text-muted mb-1">No transactions available</h6>
                                                                <p class="text-muted mb-0 fs-13">Data will appear as new trades arrive via QuickNode.</p>
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
                    <!-- End:: Transactions Table -->

<?php $content = ob_get_clean(); ?>
<!-- This code is useful for content -->

<!-- This code is useful for internal scripts -->
<?php ob_start(); ?>

        <script>
            // Status data from PHP
            window.statusData = <?php echo $json_status_data; ?>;
            window.transactionsData = <?php echo $json_transactions_data; ?>;
            window.volumeData = <?php echo $json_volume_data; ?>;
            window.statsData = <?php echo $json_stats_data; ?>;
            
            // Copy to clipboard functionality
            function copyToClipboard(element) {
                const address = element.getAttribute('data-address');
                navigator.clipboard.writeText(address).then(() => {
                    const originalText = element.textContent;
                    element.textContent = 'Copied!';
                    setTimeout(() => {
                        element.textContent = originalText;
                    }, 2000);
                }).catch(err => {
                    console.error('Failed to copy:', err);
                });
            }
            
            // Format currency with commas
            function formatCurrency(amount) {
                return parseFloat(amount || 0).toFixed(2).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
            }
            
            // Format number with commas
            function formatNumber(num) {
                return parseInt(num || 0).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
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
                
                // Update transaction stream status
                if (statusData.last_update) {
                    updateButton('txStatusBtn', 'txStatusInfo', statusData.last_update);
                }
            }
            
            // Update stats displays
            function updateStatsDisplay(statsData) {
                if (!statsData) return;
                
                // Total in DB
                const totalInDbInfo = document.getElementById('totalInDbInfo');
                if (totalInDbInfo && statsData.total_in_db !== undefined) {
                    totalInDbInfo.textContent = formatNumber(statsData.total_in_db);
                }
                
                // First transaction (oldest displayed - seconds, minutes or hours ago)
                const firstTxInfo = document.getElementById('firstTxInfo');
                if (firstTxInfo && statsData.first_transaction) {
                    const firstTime = new Date(statsData.first_transaction).getTime();
                    const secondsAgo = Math.floor((Date.now() - firstTime) / 1000);
                    
                    if (secondsAgo < 60) {
                        firstTxInfo.textContent = secondsAgo + 's ago';
                    } else {
                        const minsAgo = Math.floor(secondsAgo / 60);
                        if (minsAgo < 60) {
                            firstTxInfo.textContent = minsAgo + 'm ago';
                        } else {
                            const hoursAgo = Math.floor(minsAgo / 60);
                            firstTxInfo.textContent = hoursAgo + 'h ago';
                        }
                    }
                } else if (firstTxInfo) {
                    firstTxInfo.textContent = 'N/A';
                }
                
                // Average size
                const avgSizeInfo = document.getElementById('avgSizeInfo');
                if (avgSizeInfo && statsData.avg_size !== undefined) {
                    avgSizeInfo.textContent = '$' + formatCurrency(statsData.avg_size);
                }
                
                // Largest transaction
                const largestTxInfo = document.getElementById('largestTxInfo');
                if (largestTxInfo && statsData.largest_tx !== undefined) {
                    largestTxInfo.textContent = '$' + formatCurrency(statsData.largest_tx);
                }
            }
            
            // Track seen signatures
            let lastSignatures = new Set();
            const MAX_TRANSACTIONS = 30;
            
            // Initialize with current signatures
            document.querySelectorAll('#transactionsTableBody tr').forEach(row => {
                const sig = row.getAttribute('data-signature');
                if (sig) lastSignatures.add(sig);
            });
            
            function createTransactionRow(tx, isNew = false) {
                const direction = (tx.direction || 'unknown').toLowerCase();
                const amount = parseFloat(tx.stablecoin_amount || 0);
                
                const timestamp = new Date(tx.trade_timestamp).toLocaleString('en-US', {
                    month: 'short',
                    day: 'numeric',
                    year: 'numeric',
                    hour: '2-digit',
                    minute: '2-digit',
                    second: '2-digit'
                });
                
                const wallet = tx.wallet_address || '';
                const walletShort = wallet.length > 10 ? wallet.substring(0, 6) + '...' + wallet.substring(wallet.length - 4) : wallet;
                
                const sig = tx.signature || '';
                const sigShort = sig.length > 14 ? sig.substring(0, 8) + '...' + sig.substring(sig.length - 6) : sig;
                
                const tr = document.createElement('tr');
                tr.setAttribute('data-signature', sig);
                if (isNew) {
                    tr.classList.add('new-row');
                }
                
                tr.innerHTML = `
                    <td>
                        <span class="direction-badge ${direction}">
                            ${direction.toUpperCase()}
                        </span>
                    </td>
                    <td class="text-end fw-semibold ${direction === 'buy' ? 'text-success' : 'text-danger'}">$${formatCurrency(amount)}</td>
                    <td class="mono-cell">${timestamp}</td>
                    <td>
                        <span class="wallet-address-link" 
                              data-address="${wallet}"
                              onclick="copyToClipboard(this)"
                              title="Click to copy">
                            ${walletShort}
                        </span>
                    </td>
                    <td>
                        <a href="https://solscan.io/tx/${sig}" 
                           target="_blank" 
                           class="signature-link"
                           title="View on Solscan">
                            ${sigShort}
                        </a>
                    </td>
                `;
                
                return tr;
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
            
            async function refreshTransactions() {
                try {
                    const response = await fetch('?ajax=refresh');
                    const data = await response.json();
                    
                    if (data.success) {
                        // Update global data
                        window.statusData = data.status_data;
                        window.transactionsData = data.transactions;
                        window.volumeData = data.volume_data;
                        window.statsData = data.stats_data;
                        
                        // Update volume displays
                        if (data.volume_data) {
                            document.getElementById('totalVolume').textContent = '$' + formatCurrency(data.volume_data.total_volume);
                            document.getElementById('buyVolume').textContent = '$' + formatCurrency(data.volume_data.buy_volume);
                            document.getElementById('sellVolume').textContent = '$' + formatCurrency(data.volume_data.sell_volume);
                        }
                        
                        // Update stats displays
                        if (data.stats_data) {
                            updateStatsDisplay(data.stats_data);
                        }
                        
                        // Update record counts
                        const count = data.transactions.length;
                        const recordCount = document.getElementById('recordCount');
                        if (recordCount) recordCount.textContent = count + ' Records';
                        
                        const tbody = document.getElementById('transactionsTableBody');
                        const newTransactions = [];
                        
                        // Find only NEW transactions
                        for (const tx of data.transactions) {
                            if (!lastSignatures.has(tx.signature)) {
                                newTransactions.push(tx);
                                lastSignatures.add(tx.signature);
                            }
                        }
                        
                        // Prepend new transactions
                        if (newTransactions.length > 0) {
                            const fragment = document.createDocumentFragment();
                            
                            // Add in reverse order since we're prepending
                            for (let i = newTransactions.length - 1; i >= 0; i--) {
                                const row = createTransactionRow(newTransactions[i], true);
                                fragment.appendChild(row);
                            }
                            
                            tbody.insertBefore(fragment, tbody.firstChild);
                            
                            // Trim excess rows to maintain performance
                            while (tbody.children.length > MAX_TRANSACTIONS) {
                                const removedRow = tbody.lastChild;
                                const removedSig = removedRow.getAttribute('data-signature');
                                if (removedSig) lastSignatures.delete(removedSig);
                                tbody.removeChild(removedRow);
                            }
                        }
                        
                        // Update status buttons
                        refreshStatus(data.status_data);
                        
                        // Update data source badge
                        if (data.data_source) {
                            updateDataSourceBadge(data.data_source, data.actual_source);
                        }
                    }
                } catch (error) {
                    console.error('Error refreshing transactions:', error);
                }
            }
            
            // Initial status refresh
            refreshStatus(window.statusData);
            updateStatsDisplay(window.statsData);
            
            // Refresh data every 2 seconds
            setInterval(refreshTransactions, 2000);
            
            // Also refresh status every 5 seconds (for time-based updates)
            setInterval(() => {
                refreshStatus(window.statusData);
                updateStatsDisplay(window.statsData);
            }, 5000);
        </script>

<?php $scripts = ob_get_clean(); ?>
<!-- This code is useful for internal scripts -->

<!-- This code use for render base file -->
<?php include __DIR__ . '/../../pages/layouts/base.php'; ?>
<!-- This code use for render base file -->
