<?php
/**
 * Transactions Page - Live Transaction Feed
 * Displays real-time stablecoin transactions from .NET Webhook DuckDB In-Memory API.
 * 
 * Data Source: .NET Webhook at quicknode.smz.dk/api/trades
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

// --- Fetch Transactions Data via .NET Webhook API ---
function fetchTransactionsData() {
    $transactions_data = [];
    $volume_data = [
        'total_volume' => 0,
        'buy_volume' => 0,
        'sell_volume' => 0
    ];
    $error_message = null;
    $data_source = "No Data";
    $actual_source = null;

    $ch = curl_init(WEBHOOK_API_URL . '/api/trades?limit=30');
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
            $transactions_data = $data['results'] ?? [];
            $actual_source = $data['source'] ?? 'duckdb_inmemory';
            $data_source = "🦆 DuckDB In-Memory";
            
            // Calculate volume from fetched transactions
            foreach ($transactions_data as $tx) {
                $amount = floatval($tx['stablecoin_amount'] ?? 0);
                $direction = strtolower($tx['direction'] ?? '');
                $volume_data['total_volume'] += $amount;
                if ($direction === 'buy') {
                    $volume_data['buy_volume'] += $amount;
                } else {
                    $volume_data['sell_volume'] += $amount;
                }
            }
        } else {
            $error_message = $data['error'] ?? 'Unknown error from API';
            $data_source = "API Error";
        }
    } else {
        $error_message = "Webhook API is not available. Error: " . ($curlError ?: "HTTP $httpCode");
        $data_source = "Offline";
    }
    
    return [
        'transactions_data' => $transactions_data,
        'volume_data' => $volume_data,
        'error_message' => $error_message,
        'data_source' => $data_source,
        'actual_source' => $actual_source
    ];
}

// Check if this is an AJAX request for data refresh
if (isset($_GET['ajax']) && $_GET['ajax'] === 'refresh') {
    header('Content-Type: application/json');
    $result = fetchTransactionsData();
    $use_duckdb = isWebhookAvailable();
    
    // Build status data
    $status_data = [
        'api_status' => $use_duckdb ? 'connected' : 'disconnected',
        'tx_records' => count($result['transactions_data']),
        'last_update' => null,
    ];
    
    if (!empty($result['transactions_data'])) {
        $status_data['last_update'] = $result['transactions_data'][0]['trade_timestamp'] ?? null;
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
        'transactions' => $result['transactions_data'],
        'volume_data' => $result['volume_data'],
        'status_data' => $status_data,
        'error_message' => $result['error_message'],
        'data_source' => $result['data_source'],
        'actual_source' => $result['actual_source']
    ]);
    exit;
}

// Regular page load - fetch data normally
$result = fetchTransactionsData();
$transactions_data = $result['transactions_data'];
$volume_data = $result['volume_data'];
$error_message = $result['error_message'];
$data_source = $result['data_source'];
$use_duckdb = isWebhookAvailable();

// --- Status Data ---
$status_data = [
    'api_status' => $use_duckdb ? 'connected' : 'disconnected',
    'tx_records' => count($transactions_data),
    'last_update' => null,
];

if (!empty($transactions_data)) {
    $status_data['last_update'] = $transactions_data[0]['trade_timestamp'] ?? null;
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
$json_transactions_data = json_encode($transactions_data);
$json_volume_data = json_encode($volume_data);
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
                            <div class="status-info" id="recordCountInfo"><?php echo count($transactions_data); ?></div>
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
                                                                <h6 class="text-muted mb-1">No transactions in DuckDB hot storage</h6>
                                                                <p class="text-muted mb-0 fs-13">Data will appear as new trades arrive via QuickNode.</p>
                                                                <p class="text-muted mb-0 fs-12 mt-2">DuckDB in-memory stores last 24 hours only.</p>
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
                    
                    // Parse timestamp as UTC (ensure it ends with Z for UTC)
                    let utcTimestamp = timestamp;
                    if (!timestamp.endsWith('Z') && !timestamp.includes('+')) {
                        utcTimestamp = timestamp.includes('T') ? timestamp + 'Z' : timestamp.replace(' ', 'T') + 'Z';
                    }
                    const dataTime = new Date(utcTimestamp).getTime();
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
                
                updateButton('getPricesBtn', 'getPricesInfo', statusData.get_prices);
                updateButton('priceAnalysisBtn', 'priceAnalysisInfo', statusData.price_analysis);
                updateButton('activeCycleBtn', 'activeCycleInfo', statusData.active_cycle);
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
            
            async function refreshTransactions() {
                try {
                    const response = await fetch('?ajax=refresh');
                    const data = await response.json();
                    
                    if (data.success) {
                        // Update global data
                        window.statusData = data.status_data;
                        window.transactionsData = data.transactions;
                        window.volumeData = data.volume_data;
                        
                        // Update volume displays
                        if (data.volume_data) {
                            document.getElementById('totalVolume').textContent = '$' + formatCurrency(data.volume_data.total_volume);
                            document.getElementById('buyVolume').textContent = '$' + formatCurrency(data.volume_data.buy_volume);
                            document.getElementById('sellVolume').textContent = '$' + formatCurrency(data.volume_data.sell_volume);
                        }
                        
                        // Update record counts
                        const count = data.transactions.length;
                        const recordCount = document.getElementById('recordCount');
                        const recordCountInfo = document.getElementById('recordCountInfo');
                        if (recordCount) recordCount.textContent = count + ' Records';
                        if (recordCountInfo) recordCountInfo.textContent = count;
                        
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
            
            // Refresh data every 1 second
            setInterval(refreshTransactions, 1000);
            
            // Also refresh status every 5 seconds (for time-based updates)
            setInterval(() => refreshStatus(window.statusData), 5000);
        </script>

<?php $scripts = ob_get_clean(); ?>
<!-- This code is useful for internal scripts -->

<!-- This code use for render base file -->
<?php include __DIR__ . '/../../pages/layouts/base.php'; ?>
<!-- This code use for render base file -->

