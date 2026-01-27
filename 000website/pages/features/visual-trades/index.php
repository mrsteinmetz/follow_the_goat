<?php
/**
 * Visual Trades Page - Fast Trade Browsing with Price Charts
 * Shows sold trades with price movement visualization (10 min before, 60 min after entry)
 */

// Set timezone to UTC
date_default_timezone_set('UTC');

// --- Database API Client ---
require_once __DIR__ . '/../../../includes/DatabaseClient.php';
require_once __DIR__ . '/../../../includes/config.php';
$db = new DatabaseClient(DATABASE_API_URL);
$api_available = $db->isAvailable();

// --- Base URL for template ---
$baseUrl = '';

$error_message = '';
$trades = [];

// Fetch sold trades (status='sold' or 'completed', exclude 'no_go') for play_id = 46 only
if ($api_available) {
    // Get sold trades - fetch both 'sold' and 'completed' statuses, filtered by play_id = 46
    $all_trades = [];
    
    // Fetch sold trades for play_id = 46
    $response = $db->getBuyins(46, 'sold', 'all', 500, 'no_go');
    if ($response) {
        $trades_sold = $response['buyins'] ?? $response['results'] ?? [];
        $all_trades = array_merge($all_trades, $trades_sold);
    }
    
    // Fetch completed trades for play_id = 46
    $response2 = $db->getBuyins(46, 'completed', 'all', 500, 'no_go');
    if ($response2) {
        $trades_completed = $response2['buyins'] ?? $response2['results'] ?? [];
        $all_trades = array_merge($all_trades, $trades_completed);
    }
    
    // Remove duplicates by ID
    $unique_trades = [];
    $seen_ids = [];
    foreach ($all_trades as $trade) {
        $trade_id = $trade['id'] ?? 0;
        // Double-check play_id = 46
        if ($trade_id > 0 && ($trade['play_id'] ?? 0) == 46 && !in_array($trade_id, $seen_ids)) {
            $unique_trades[] = $trade;
            $seen_ids[] = $trade_id;
        }
    }
    
    // Sort by followed_at DESC (most recent first)
    usort($unique_trades, function($a, $b) {
        $timeA = strtotime($a['followed_at'] ?? '1970-01-01');
        $timeB = strtotime($b['followed_at'] ?? '1970-01-01');
        return $timeB <=> $timeA;
    });
    
    // Limit to 100 for the list
    $trades = array_slice($unique_trades, 0, 100);
} else {
    $error_message = "Website API is not available. Please start the API: python scheduler/website_api.py";
}

// --- Page Styles ---
ob_start();
?>
<style>
    .visual-trades-container {
        display: flex;
        flex-direction: column;
        gap: 1.5rem;
        min-height: calc(100vh - 200px);
    }
    
    .chart-section {
        background: var(--custom-white);
        border-radius: 8px;
        padding: 1.5rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }
    
    .chart-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 1rem;
        flex-wrap: wrap;
        gap: 1rem;
    }
    
    .chart-title {
        font-size: 1.25rem;
        font-weight: 600;
        color: var(--default-text-color);
    }
    
    .trade-counter {
        font-size: 0.9rem;
        color: var(--text-muted);
    }
    
    .chart-navigation {
        display: flex;
        gap: 0.5rem;
        align-items: center;
    }
    
    .chart-navigation button {
        padding: 0.5rem 1rem;
        border-radius: 6px;
        border: 1px solid var(--default-border);
        background: var(--custom-white);
        color: var(--default-text-color);
        cursor: pointer;
        transition: all 0.2s;
        font-weight: 500;
    }
    
    .chart-navigation button:hover:not(:disabled) {
        background: rgba(var(--primary-rgb), 0.1);
        border-color: rgb(var(--primary-rgb));
        color: rgb(var(--primary-rgb));
    }
    
    .chart-navigation button:disabled {
        opacity: 0.5;
        cursor: not-allowed;
    }
    
    #tradeChart {
        min-height: 500px;
    }
    
    .chart-loading {
        display: flex;
        align-items: center;
        justify-content: center;
        min-height: 500px;
        color: var(--text-muted);
    }
    
    .chart-error {
        display: flex;
        align-items: center;
        justify-content: center;
        min-height: 500px;
        color: var(--danger);
        flex-direction: column;
        gap: 1rem;
    }
    
    .trades-list-section {
        background: var(--custom-white);
        border-radius: 8px;
        padding: 1.5rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }
    
    .trades-list-header {
        font-size: 1.1rem;
        font-weight: 600;
        margin-bottom: 1rem;
        color: var(--default-text-color);
    }
    
    .trades-list {
        max-height: 400px;
        overflow-y: auto;
        border: 1px solid var(--default-border);
        border-radius: 6px;
    }
    
    .trade-item {
        display: grid;
        grid-template-columns: 120px 140px 110px 110px 100px;
        gap: 0.75rem;
        padding: 0.75rem 1rem;
        border-bottom: 1px solid var(--default-border);
        cursor: pointer;
        transition: all 0.15s;
        align-items: center;
        font-size: 0.85rem;
    }
    
    .trade-details-section {
        margin-bottom: 1.5rem;
    }
    
    .trade-details-section .card {
        border: 2px solid rgba(var(--primary-rgb), 0.3);
    }
    
    .filter-validation-card {
        margin-top: 1rem;
    }
    
    .trade-filters {
        font-size: 0.75rem;
        color: var(--text-muted);
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
    }
    
    .trade-pl {
        text-align: right;
    }
    
    .trade-item:last-child {
        border-bottom: none;
    }
    
    .trade-item:hover {
        background: rgba(var(--primary-rgb), 0.05);
    }
    
    .trade-item.active {
        background: rgba(var(--primary-rgb), 0.15);
        border-left: 3px solid rgb(var(--primary-rgb));
    }
    
    .trade-id {
        font-weight: 600;
        color: var(--default-text-color);
    }
    
    .trade-time {
        font-size: 0.85rem;
        color: var(--text-muted);
    }
    
    .trade-price {
        font-family: 'SF Mono', monospace;
        font-size: 0.9rem;
    }
    
    .trade-pl {
        font-weight: 600;
        font-family: 'SF Mono', monospace;
    }
    
    .trade-pl.positive {
        color: rgb(var(--success-rgb));
    }
    
    .trade-pl.negative {
        color: rgb(var(--danger-rgb));
    }
    
    .trade-pl.zero {
        color: var(--text-muted);
    }
    
    .no-trades {
        text-align: center;
        padding: 3rem;
        color: var(--text-muted);
    }
    
    .trade-item-header {
        display: grid;
        grid-template-columns: 120px 140px 110px 110px 100px;
        gap: 0.75rem;
        padding: 0.5rem 1rem;
        background: rgba(var(--primary-rgb), 0.1);
        border-bottom: 2px solid var(--default-border);
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
        color: var(--text-muted);
        position: sticky;
        top: 0;
        z-index: 10;
    }
    
    @media (max-width: 1200px) {
        .trade-item {
            grid-template-columns: 100px 120px 100px 100px 90px;
            font-size: 0.8rem;
        }
        
        .trade-item-header {
            grid-template-columns: 100px 120px 100px 100px 90px;
        }
    }
    
    @media (max-width: 768px) {
        .trade-item {
            grid-template-columns: 1fr;
            gap: 0.5rem;
        }
        
        .trade-item-header {
            display: none;
        }
        
        .chart-header {
            flex-direction: column;
            align-items: flex-start;
        }
    }
</style>
<?php
$styles = ob_get_clean();

// --- Page Content ---
ob_start();
?>

<div class="container-fluid">
    <div class="row">
        <div class="col-12">
            <div class="page-header d-flex justify-content-between align-items-center mb-4">
                <div>
                    <h1 class="page-title">Visual Trades</h1>
                    <p class="text-muted">Browse sold trades with price movement visualization</p>
                </div>
            </div>
            
            <?php if ($error_message): ?>
                <div class="alert alert-danger">
                    <?php echo htmlspecialchars($error_message); ?>
                </div>
            <?php elseif (empty($trades)): ?>
                <div class="alert alert-info">
                    No sold trades available. Trades must have status 'sold' or 'completed' and not be 'no_go'.
                </div>
            <?php else: ?>
                <div class="visual-trades-container">
                    <!-- Chart Section -->
                    <div class="chart-section">
                        <div class="chart-header">
                            <div>
                                <div class="chart-title">Trade Price Movement</div>
                                <div class="trade-counter" id="tradeCounter">Loading...</div>
                            </div>
                            <div class="chart-navigation">
                                <button id="prevBtn" onclick="previousTrade()" disabled>
                                    <i class="ri-arrow-left-line"></i> Previous
                                </button>
                                <button id="nextBtn" onclick="nextTrade()">
                                    Next <i class="ri-arrow-right-line"></i>
                                </button>
                            </div>
                        </div>
                        <div id="tradeChart" class="chart-loading">
                            <div>Loading chart...</div>
                        </div>
                    </div>
                    
                    <!-- Current Trade Details Section -->
                    <div class="trade-details-section" id="tradeDetailsSection" style="display: none;">
                        <div class="card custom-card mb-3">
                            <div class="card-header">
                                <div class="card-title">Current Trade Details</div>
                                <div class="ms-auto" id="tradeDetailsBadges"></div>
                            </div>
                            <div class="card-body">
                                <div class="row mb-3">
                                    <div class="col-md-4">
                                        <div class="d-flex align-items-center">
                                            <div class="me-3">
                                                <small class="text-muted d-block">Gain/Loss</small>
                                                <div class="h4 mb-0" id="tradeGain">-</div>
                                            </div>
                                        </div>
                                    </div>
                                    <div class="col-md-4">
                                        <div class="d-flex align-items-center">
                                            <div class="me-3">
                                                <small class="text-muted d-block">Potential Gain</small>
                                                <div class="h4 mb-0" id="tradePotentialGain">-</div>
                                            </div>
                                        </div>
                                    </div>
                                    <div class="col-md-4">
                                        <div class="d-flex align-items-center">
                                            <div>
                                                <small class="text-muted d-block">Trade ID</small>
                                                <div class="h5 mb-0" id="tradeIdDisplay">-</div>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                                <div id="filterValidationSection"></div>
                            </div>
                        </div>
                    </div>
                    
                    <!-- Trades List Section -->
                    <div class="trades-list-section">
                        <div class="trades-list-header">Trade List - Play ID 46 (<?php echo count($trades); ?> trades)</div>
                        <div class="trades-list" id="tradesList">
                            <div class="trade-item-header">
                                <div>ID</div>
                                <div>Time</div>
                                <div>Entry</div>
                                <div>Exit</div>
                                <div>Status</div>
                            </div>
                            <?php foreach ($trades as $index => $trade): ?>
                                <?php
                                $entry_price = floatval($trade['our_entry_price'] ?? 0);
                                $exit_price = floatval($trade['our_exit_price'] ?? 0);
                                $pl = $trade['our_profit_loss'] ?? null;
                                $potential_gains = $trade['potential_gains'] ?? null;
                                $followed_at = $trade['followed_at'] ?? '';
                                $trade_id = $trade['id'] ?? 0;
                                
                                // Format time
                                $time_str = '-';
                                if ($followed_at) {
                                    $timestamp = strtotime($followed_at);
                                    $time_str = date('M j, H:i', $timestamp);
                                }
                                
                                // P/L class
                                $pl_class = 'zero';
                                if ($pl !== null) {
                                    $pl_class = $pl > 0 ? 'positive' : ($pl < 0 ? 'negative' : 'zero');
                                }
                                
                                // Potential gains class
                                $pg_class = 'zero';
                                if ($potential_gains !== null) {
                                    $pg_class = $potential_gains > 0 ? 'positive' : ($potential_gains < 0 ? 'negative' : 'zero');
                                }
                                
                                // Parse pattern_validator_log to extract filters
                                $filters_used = [];
                                $filter_summary = '-';
                                if (!empty($trade['pattern_validator_log'])) {
                                    $validator_log = $trade['pattern_validator_log'];
                                    
                                    // Handle JSONB (PostgreSQL) - might already be array or JSON string
                                    if (is_string($validator_log)) {
                                        $validator_log = json_decode($validator_log, true);
                                    }
                                    
                                    if (is_array($validator_log)) {
                                        // Try different structures
                                        // Structure 1: { project_id: { passed: true, filters: [...] } }
                                        foreach ($validator_log as $key => $project_data) {
                                            if (is_array($project_data)) {
                                                // Check if this project passed
                                                $passed = $project_data['passed'] ?? $project_data['result'] ?? false;
                                                
                                                if ($passed) {
                                                    // Look for filters array
                                                    $filters = $project_data['filters'] ?? $project_data['filter_results'] ?? [];
                                                    
                                                    if (is_array($filters)) {
                                                        foreach ($filters as $filter) {
                                                            if (is_array($filter)) {
                                                                $filter_name = $filter['name'] ?? $filter['filter_name'] ?? $filter['column'] ?? null;
                                                                $filter_passed = $filter['passed'] ?? $filter['result'] ?? false;
                                                                
                                                                if ($filter_name && $filter_passed) {
                                                                    $filters_used[] = $filter_name;
                                                                }
                                                            } elseif (is_string($filter)) {
                                                                // Filter might just be a string name
                                                                $filters_used[] = $filter;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        
                                        // Structure 2: Direct array of filter results
                                        if (empty($filters_used) && isset($validator_log[0]) && is_array($validator_log[0])) {
                                            foreach ($validator_log as $filter_result) {
                                                if (isset($filter_result['name']) && ($filter_result['passed'] ?? false)) {
                                                    $filters_used[] = $filter_result['name'];
                                                }
                                            }
                                        }
                                        
                                        if (!empty($filters_used)) {
                                            $unique_filters = array_unique($filters_used);
                                            $filter_summary = implode(', ', array_slice($unique_filters, 0, 3));
                                            if (count($unique_filters) > 3) {
                                                $filter_summary .= ' +' . (count($unique_filters) - 3) . ' more';
                                            }
                                        }
                                    }
                                }
                                
                                // Debug: Log if data is missing
                                if ($pl === null && $potential_gains === null && $filter_summary === '-') {
                                    // This trade might not have the expected data structure
                                }
                                ?>
                                <div class="trade-item" data-trade-id="<?php echo $trade_id; ?>" data-trade-index="<?php echo $index; ?>" data-trade-data='<?php echo json_encode($trade); ?>' onclick="jumpToTrade(<?php echo $index; ?>)">
                                    <div class="trade-id">#<?php echo $trade_id; ?></div>
                                    <div class="trade-time"><?php echo htmlspecialchars($time_str); ?></div>
                                    <div class="trade-price" title="Entry Price">$<?php echo number_format($entry_price, 4); ?></div>
                                    <div class="trade-price" title="Exit Price">$<?php echo $exit_price > 0 ? number_format($exit_price, 4) : '-'; ?></div>
                                    <div class="trade-status">
                                        <span class="badge bg-success-transparent"><?php echo htmlspecialchars($trade['our_status'] ?? 'sold'); ?></span>
                                    </div>
                                </div>
                            <?php endforeach; ?>
                        </div>
                    </div>
                </div>
            <?php endif; ?>
        </div>
    </div>
</div>

<!-- ApexCharts Library -->
<script src="<?php echo $baseUrl; ?>/assets/libs/apexcharts/apexcharts.min.js"></script>

<script>
// Global state
let trades = <?php echo json_encode($trades); ?>;
let currentTradeIndex = 0;
let chartInstance = null;
let priceDataCache = {};
let isLoadingChart = false;

// Initialize on page load
document.addEventListener('DOMContentLoaded', function() {
    // Check URL hash for trade ID
    const hash = window.location.hash;
    if (hash && hash.startsWith('#trade-')) {
        const tradeId = parseInt(hash.replace('#trade-', ''));
        const index = trades.findIndex(t => t.id === tradeId);
        if (index >= 0) {
            currentTradeIndex = index;
        }
    }
    
    // Load initial trade
    if (trades.length > 0) {
        console.log('Initializing with', trades.length, 'trades');
        updateNavigationButtons(); // Set initial button states
        loadTrade(currentTradeIndex);
    } else {
        document.getElementById('tradeChart').innerHTML = '<div class="chart-error">No trades available</div>';
        updateNavigationButtons();
    }
    
    // Handle browser back/forward
    window.addEventListener('hashchange', function() {
        const hash = window.location.hash;
        if (hash && hash.startsWith('#trade-')) {
            const tradeId = parseInt(hash.replace('#trade-', ''));
            const index = trades.findIndex(t => t.id === tradeId);
            if (index >= 0 && index !== currentTradeIndex) {
                currentTradeIndex = index;
                loadTrade(currentTradeIndex);
            }
        }
    });
});

// Load trade at index
async function loadTrade(index) {
    if (index < 0 || index >= trades.length || isLoadingChart) return;
    
    isLoadingChart = true;
    currentTradeIndex = index;
    const trade = trades[index];
    
    // Update UI
    updateNavigationButtons();
    updateTradeCounter();
    updateTradeListHighlight();
    updateURL(trade.id);
    updateTradeDetails(trade);
    
    // Show loading - create a temporary div, don't replace the chart container
    const chartContainer = document.getElementById('tradeChart');
    if (chartContainer) {
        // Clear any existing chart
        if (chartInstance) {
            try {
                chartInstance.destroy();
            } catch (e) {
                console.warn('Error destroying chart:', e);
            }
            chartInstance = null;
        }
        chartContainer.innerHTML = '<div class="chart-loading">Loading price data...</div>';
    }
    
    // Set a timeout to show error if loading takes too long
    const loadingTimeout = setTimeout(() => {
        if (isLoadingChart) {
            console.warn('Loading timeout - fetch may be hanging');
            const chartContainer = document.getElementById('tradeChart');
            if (chartContainer) {
                chartContainer.innerHTML = 
                    '<div class="chart-error">Loading timeout - API may be slow or unavailable<br>' +
                    '<small>Trade ID: ' + trade.id + '</small><br>' +
                    '<small>Check browser console and ensure website API is running</small></div>';
            }
            isLoadingChart = false;
        }
    }, 35000); // 35 seconds
    
    try {
        // Get price data
        const priceDataResponse = await fetchPriceData(trade);
        clearTimeout(loadingTimeout);
        
        const chartContainer = document.getElementById('tradeChart');
        
        if (!priceDataResponse) {
            const trade = trades[index];
            const followedAt = trade.followed_at;
            const errorMsg = `No price data available for trade #${trade.id}<br>` +
                           `<small>Entry time: ${followedAt ? new Date(followedAt + ' UTC').toISOString() : 'N/A'}</small><br>` +
                           `<small>Time range: 10 min before to 60 min after entry</small><br>` +
                           `<small>Check browser console for API response details</small>`;
            if (chartContainer) chartContainer.innerHTML = `<div class="chart-error">${errorMsg}</div>`;
            isLoadingChart = false;
            return;
        }
        
        const prices = priceDataResponse.prices || [];
        if (prices.length === 0) {
            const trade = trades[index];
            const followedAt = trade.followed_at;
            const errorMsg = `No price data available for trade #${trade.id}<br>` +
                           `<small>Entry time: ${followedAt ? new Date(followedAt + ' UTC').toISOString() : 'N/A'}</small><br>` +
                           `<small>Time range: 10 min before to 60 min after entry</small><br>` +
                           `<small>API returned ${prices.length} price points</small>`;
            if (chartContainer) chartContainer.innerHTML = `<div class="chart-error">${errorMsg}</div>`;
            isLoadingChart = false;
            updateNavigationButtons();
            return;
        }
        
        console.log('Converting', prices.length, 'price points to chart format');
        
        // Convert price data to chart format
        const priceData = convertPriceDataToChartFormat(prices);
        
        if (!priceData || priceData.length === 0) {
            console.error('Failed to convert price data - all points were invalid');
            throw new Error('Failed to convert price data to chart format - check console for details');
        }
        
        console.log('Successfully converted', priceData.length, 'price points for chart');
        
        // Render chart
        renderChart(priceData, trade);
        isLoadingChart = false;
        updateNavigationButtons(); // Update buttons after chart loads
    } catch (error) {
        clearTimeout(loadingTimeout);
        console.error('Error loading trade:', error);
        const trade = trades[index];
        const chartContainer = document.getElementById('tradeChart');
        const errorDetails = `Error loading chart: ${error.message}<br>` +
                           `<small>Trade ID: ${trade.id}</small><br>` +
                           `<small>Entry time: ${trade.followed_at || 'N/A'}</small><br>` +
                           `<small>Check browser console for details</small>`;
        if (chartContainer) chartContainer.innerHTML = `<div class="chart-error">${errorDetails}</div>`;
        isLoadingChart = false;
        updateNavigationButtons(); // Update buttons after error
    }
}

// Fetch price data for trade
async function fetchPriceData(trade) {
    const tradeId = trade.id;
    
    // Check cache
    if (priceDataCache[tradeId]) {
        return priceDataCache[tradeId];
    }
    
    // Calculate time range: 10 min before, 60 min after entry
    // Entry point will be at 10/70 = ~14% from left (not centered)
    // But we show 10 min before and 60 min after as requested
    const followedAt = trade.followed_at;
    if (!followedAt) {
        throw new Error('Trade missing followed_at timestamp');
    }
    
    // Parse timestamp - handle different formats
    let entryTime;
    if (typeof followedAt === 'string') {
        // Try parsing as ISO string first
        if (followedAt.includes('T') || followedAt.includes('Z')) {
            entryTime = new Date(followedAt).getTime();
        } else {
            // Assume it's a datetime string without timezone - treat as UTC
            entryTime = new Date(followedAt + ' UTC').getTime();
        }
    } else if (typeof followedAt === 'number') {
        // Already a timestamp
        entryTime = followedAt < 10000000000 ? followedAt * 1000 : followedAt;
    } else {
        throw new Error('Invalid followed_at format: ' + typeof followedAt);
    }
    
    if (isNaN(entryTime) || entryTime <= 0) {
        throw new Error('Invalid entry time: ' + followedAt);
    }
    const startTime = entryTime - (10 * 60 * 1000);  // 10 minutes before
    const endTime = entryTime + (60 * 60 * 1000);    // 60 minutes after
    
    const startSec = Math.floor(startTime / 1000);
    const endSec = Math.floor(endTime / 1000);
    
    // Fetch from API
    const apiUrl = `<?php echo $baseUrl; ?>/chart/plays/get_trade_prices.php?start=${startSec}&end=${endSec}`;
    console.log('Fetching price data:', {
        tradeId: tradeId,
        followedAt: followedAt,
        entryTime: new Date(entryTime).toISOString(),
        startTime: new Date(startTime).toISOString(),
        endTime: new Date(endTime).toISOString(),
        startSec: startSec,
        endSec: endSec,
        apiUrl: apiUrl
    });
    
    try {
        // Create timeout controller (fallback for browsers without AbortSignal.timeout)
        let timeoutId;
        const controller = new AbortController();
        const timeoutPromise = new Promise((_, reject) => {
            timeoutId = setTimeout(() => {
                controller.abort();
                reject(new Error('Request timeout'));
            }, 30000); // 30 second timeout
        });
        
        const fetchPromise = fetch(apiUrl, {
            method: 'GET',
            headers: {
                'Accept': 'application/json'
            },
            signal: controller.signal
        });
        
        const response = await Promise.race([fetchPromise, timeoutPromise]);
        clearTimeout(timeoutId);
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        
        console.log('Price data response:', {
            success: data.success,
            count: data.prices?.length || 0,
            error: data.error,
            debug: data.debug,
            fullResponse: data
        });
        
        // Handle different response formats
        if (data.error) {
            throw new Error(data.error || 'Failed to fetch price data');
        }
        
        if (!data.success && !data.prices) {
            throw new Error('Invalid API response format');
        }
        
        // Check if we have price data
        const prices = data.prices || [];
        if (prices.length === 0) {
            console.warn('No price data returned for trade', tradeId, 'Response:', data);
            return null;
        }
        
        console.log('Successfully fetched', prices.length, 'price points');
        return { success: true, prices: prices, debug: data.debug };
    } catch (fetchError) {
        console.error('Fetch error:', fetchError);
        if (fetchError.name === 'AbortError') {
            throw new Error('Request timeout - price data fetch took too long');
        }
        throw new Error('Failed to fetch price data: ' + fetchError.message);
    }
}

// Convert price data to chart format
function convertPriceDataToChartFormat(prices) {
    // Convert to chart format
    // API returns timestamps in ISO format with 'Z' (e.g., '2024-01-01T12:00:00Z')
    const priceData = prices.map(item => {
        let timestampMs;
        if (typeof item.x === 'number') {
            timestampMs = item.x < 10000000000 ? item.x * 1000 : item.x;
        } else {
            let timestamp = item.x;
            // Handle different timestamp formats
            if (timestamp.includes(' ')) {
                // Space-separated datetime - convert to ISO
                timestamp = timestamp.replace(' ', 'T');
                if (!timestamp.includes('Z') && !timestamp.includes('+') && !timestamp.includes('-', 10)) {
                    timestamp += 'Z'; // Add Z if no timezone
                }
            } else if (!timestamp.includes('Z') && !timestamp.includes('+') && !timestamp.includes('-', 10)) {
                // Plain datetime string without timezone - assume UTC
                timestamp += 'Z';
            }
            timestampMs = new Date(timestamp).getTime();
        }
        
        if (isNaN(timestampMs)) {
            console.error('Invalid timestamp:', item.x);
            return null;
        }
        
        return { x: timestampMs, y: item.y };
    }).filter(item => item !== null);
    
    return priceData;
}

// Render chart with ApexCharts
function renderChart(priceData, trade) {
    console.log('Rendering chart with', priceData.length, 'data points');
    console.log('Trade data:', trade);
    
    // Parse entry time - handle different formats
    let entryTime;
    const followedAt = trade.followed_at;
    if (typeof followedAt === 'string') {
        if (followedAt.includes('T') || followedAt.includes('Z')) {
            entryTime = new Date(followedAt).getTime();
        } else {
            entryTime = new Date(followedAt + ' UTC').getTime();
        }
    } else {
        entryTime = new Date(followedAt + ' UTC').getTime();
    }
    
    if (isNaN(entryTime)) {
        console.error('Invalid entry time:', followedAt);
        throw new Error('Invalid entry time format');
    }
    
    const exitTime = trade.our_exit_timestamp ? (() => {
        const exit = trade.our_exit_timestamp;
        if (typeof exit === 'string') {
            if (exit.includes('T') || exit.includes('Z')) {
                return new Date(exit).getTime();
            } else {
                return new Date(exit + ' UTC').getTime();
            }
        }
        return new Date(exit + ' UTC').getTime();
    })() : null;
    
    const entryPrice = parseFloat(trade.our_entry_price) || null;
    const exitPrice = parseFloat(trade.our_exit_price) || null;
    
    console.log('Chart times:', {
        entryTime: new Date(entryTime).toISOString(),
        exitTime: exitTime ? new Date(exitTime).toISOString() : null,
        entryPrice: entryPrice,
        exitPrice: exitPrice
    });
    
    // Calculate chart range: 10 min before, 60 min after entry
    // Entry point is at 10-minute mark (10/70 = ~14% from left)
    const startTime = entryTime - (10 * 60 * 1000);
    const endTime = entryTime + (60 * 60 * 1000);
    
    // Build annotations
    const annotations = { xaxis: [] };
    
    // Entry marker (centered)
    annotations.xaxis.push({
        x: entryTime,
        borderColor: '#10b981',
        strokeDashArray: 0,
        borderWidth: 3,
        label: {
            borderColor: '#10b981',
            style: {
                color: '#fff',
                background: '#10b981',
                fontSize: '11px'
            },
            text: 'ENTRY'
        }
    });
    
    // Exit marker (if exists)
    if (exitTime && exitTime !== entryTime) {
        annotations.xaxis.push({
            x: exitTime,
            borderColor: '#f59e0b',
            strokeDashArray: 0,
            borderWidth: 3,
            label: {
                borderColor: '#f59e0b',
                style: {
                    color: '#fff',
                    background: '#f59e0b',
                    fontSize: '11px'
                },
                text: 'EXIT'
            }
        });
    }
    
    const chartOptions = {
        series: [{
            name: 'SOL Price',
            data: priceData
        }],
        chart: {
            type: 'line',
            height: 500,
            background: 'transparent',
            toolbar: {
                show: true,
                tools: {
                    download: true,
                    selection: false,
                    zoom: false,
                    zoomin: false,
                    zoomout: false,
                    pan: false,
                    reset: false
                }
            },
            animations: {
                enabled: true,
                easing: 'easeinout',
                speed: 200
            }
        },
        stroke: {
            curve: 'smooth',
            width: 2.5
        },
        colors: ['#6366f1'],
        markers: {
            size: 0,
            hover: {
                size: 6,
                sizeOffset: 3
            }
        },
        grid: {
            borderColor: 'rgba(255,255,255,0.1)',
            strokeDashArray: 3
        },
        xaxis: {
            type: 'datetime',
            min: startTime,
            max: endTime,
            labels: {
                datetimeUTC: true,
                style: {
                    colors: '#9ca3af',
                    fontSize: '11px'
                },
                formatter: function(value, timestamp) {
                    const d = new Date(timestamp);
                    const hours = String(d.getUTCHours()).padStart(2, '0');
                    const minutes = String(d.getUTCMinutes()).padStart(2, '0');
                    return hours + ':' + minutes;
                }
            }
        },
        yaxis: {
            labels: {
                style: {
                    colors: '#9ca3af',
                    fontSize: '11px'
                },
                formatter: function(val) {
                    return '$' + val.toFixed(6);
                }
            }
        },
        tooltip: {
            theme: 'dark',
            x: { format: 'MMM dd, HH:mm:ss UTC' },
            y: {
                formatter: function(val) {
                    return '$' + val.toFixed(6);
                }
            }
        },
        annotations: annotations
    };
    
    // Create or update chart
    const chartElement = document.querySelector('#tradeChart');
    if (!chartElement) {
        console.error('Chart element not found!');
        throw new Error('Chart container not found');
    }
    
    console.log('Chart element found, creating/updating chart...');
    
    if (chartInstance) {
        console.log('Updating existing chart...');
        try {
            chartInstance.updateOptions(chartOptions, false, true);
            chartInstance.updateSeries([{
                name: 'SOL Price',
                data: priceData
            }], false);
            console.log('Chart updated successfully');
        } catch (updateError) {
            console.error('Error updating chart:', updateError);
            // If update fails, recreate the chart
            chartInstance.destroy();
            chartInstance = null;
        }
    }
    
    if (!chartInstance) {
        console.log('Creating new chart...');
        try {
            chartInstance = new ApexCharts(chartElement, chartOptions);
            chartInstance.render().then(() => {
                console.log('Chart rendered successfully');
            }).catch((renderError) => {
                console.error('Chart render error:', renderError);
                throw renderError;
            });
        } catch (createError) {
            console.error('Error creating chart:', createError);
            throw new Error('Failed to create chart: ' + createError.message);
        }
    }
}

// Navigation functions
function nextTrade() {
    console.log('Next trade clicked', { currentIndex: currentTradeIndex, total: trades.length, isLoading: isLoadingChart });
    if (currentTradeIndex < trades.length - 1) {
        if (isLoadingChart) {
            console.warn('Chart is still loading, ignoring click');
            return;
        }
        loadTrade(currentTradeIndex + 1);
    } else {
        console.log('Already at last trade');
    }
}

function previousTrade() {
    console.log('Previous trade clicked', { currentIndex: currentTradeIndex, total: trades.length, isLoading: isLoadingChart });
    if (currentTradeIndex > 0) {
        if (isLoadingChart) {
            console.warn('Chart is still loading, ignoring click');
            return;
        }
        loadTrade(currentTradeIndex - 1);
    } else {
        console.log('Already at first trade');
    }
}

function jumpToTrade(index) {
    if (index >= 0 && index < trades.length && !isLoadingChart) {
        loadTrade(index);
        // Scroll trade into view
        const tradeItem = document.querySelector(`[data-trade-index="${index}"]`);
        if (tradeItem) {
            tradeItem.scrollIntoView({ behavior: 'smooth', block: 'center' });
        }
    }
}

// Update UI functions
function updateNavigationButtons() {
    const prevBtn = document.getElementById('prevBtn');
    const nextBtn = document.getElementById('nextBtn');
    
    if (prevBtn) {
        prevBtn.disabled = currentTradeIndex === 0 || isLoadingChart;
        console.log('Previous button:', { disabled: prevBtn.disabled, index: currentTradeIndex, isLoading: isLoadingChart });
    }
    
    if (nextBtn) {
        nextBtn.disabled = currentTradeIndex === trades.length - 1 || isLoadingChart;
        console.log('Next button:', { disabled: nextBtn.disabled, index: currentTradeIndex, total: trades.length, isLoading: isLoadingChart });
    }
}

function updateTradeCounter() {
    const counter = document.getElementById('tradeCounter');
    counter.textContent = `Trade ${currentTradeIndex + 1} of ${trades.length}`;
}

function updateTradeListHighlight() {
    // Remove all active classes
    document.querySelectorAll('.trade-item').forEach(item => {
        item.classList.remove('active');
    });
    
    // Add active to current trade
    const currentItem = document.querySelector(`[data-trade-index="${currentTradeIndex}"]`);
    if (currentItem) {
        currentItem.classList.add('active');
    }
}

function updateURL(tradeId) {
    const newHash = `#trade-${tradeId}`;
    if (window.location.hash !== newHash) {
        window.history.replaceState(null, null, newHash);
    }
}

// Update trade details section
function updateTradeDetails(trade) {
    const detailsSection = document.getElementById('tradeDetailsSection');
    const tradeGain = document.getElementById('tradeGain');
    const tradePotentialGain = document.getElementById('tradePotentialGain');
    const tradeIdDisplay = document.getElementById('tradeIdDisplay');
    const tradeDetailsBadges = document.getElementById('tradeDetailsBadges');
    const filterValidationSection = document.getElementById('filterValidationSection');
    
    if (!detailsSection) return;
    
    // Show the section
    detailsSection.style.display = 'block';
    
    // Update gain
    const pl = trade.our_profit_loss;
    if (pl !== null && pl !== undefined && pl !== '') {
        const plClass = pl > 0 ? 'text-success' : (pl < 0 ? 'text-danger' : 'text-muted');
        tradeGain.innerHTML = `<span class="${plClass}">${pl > 0 ? '+' : ''}${parseFloat(pl).toFixed(2)}%</span>`;
    } else {
        tradeGain.innerHTML = '<span class="text-muted">-</span>';
    }
    
    // Update potential gain
    const pg = trade.potential_gains;
    if (pg !== null && pg !== undefined && pg !== '') {
        const pgClass = pg > 0 ? 'text-success' : (pg < 0 ? 'text-danger' : 'text-muted');
        tradePotentialGain.innerHTML = `<span class="${pgClass}">${pg > 0 ? '+' : ''}${parseFloat(pg).toFixed(2)}%</span>`;
    } else {
        tradePotentialGain.innerHTML = '<span class="text-muted">-</span>';
    }
    
    // Update trade ID
    tradeIdDisplay.textContent = '#' + trade.id;
    
    // Update badges
    let badgesHtml = '';
    if (trade.our_status) {
        badgesHtml += `<span class="badge bg-success-transparent">${trade.our_status}</span>`;
    }
    tradeDetailsBadges.innerHTML = badgesHtml;
    
    // Parse and display filters
    parseAndDisplayFilters(trade, filterValidationSection);
}

// Parse pattern_validator_log and display filters like unique trade page
function parseAndDisplayFilters(trade, container) {
    if (!container) return;
    
    const validatorLog = trade.pattern_validator_log;
    if (!validatorLog) {
        container.innerHTML = '<div class="alert alert-info mb-0"><small>No filter validation data available for this trade.</small></div>';
        return;
    }
    
    let validatorData = validatorLog;
    if (typeof validatorLog === 'string') {
        try {
            validatorData = JSON.parse(validatorLog);
        } catch (e) {
            container.innerHTML = '<div class="alert alert-warning mb-0"><small>Could not parse filter validation data.</small></div>';
            return;
        }
    }
    
    if (!validatorData || typeof validatorData !== 'object') {
        container.innerHTML = '<div class="alert alert-info mb-0"><small>No filter validation data available for this trade.</small></div>';
        return;
    }
    
    // Check for project_results structure (like unique trade page)
    const projectResults = validatorData.project_results || [];
    
    if (projectResults.length === 0) {
        container.innerHTML = '<div class="alert alert-info mb-0"><small>No filter validation results recorded for this trade.</small></div>';
        return;
    }
    
    let html = '<div class="filter-validation-card">';
    
    // Overall status
    let anyProjectPassed = false;
    let totalFilters = 0;
    let totalPassed = 0;
    
    projectResults.forEach(pr => {
        const filters = pr.filter_results || [];
        const allPassed = pr.decision === 'GO';
        totalFilters += filters.length;
        filters.forEach(f => {
            if (f.passed) totalPassed++;
        });
        if (allPassed) anyProjectPassed = true;
    });
    
    html += `
        <div class="card mb-2" style="border: 2px solid ${anyProjectPassed ? 'rgba(38, 191, 148, 0.5)' : 'rgba(230, 83, 60, 0.5)'};">
            <div class="card-header py-2" style="background: ${anyProjectPassed ? 'rgba(38, 191, 148, 0.15)' : 'rgba(230, 83, 60, 0.15)'};">
                <div class="d-flex align-items-center justify-content-between w-100">
                    <div class="d-flex align-items-center">
                        <i class="${anyProjectPassed ? 'ri-checkbox-circle-fill text-success' : 'ri-close-circle-fill text-danger'} me-2"></i>
                        <div>
                            <h6 class="mb-0 fw-semibold">Filter Validation Status</h6>
                            <small class="text-muted">${anyProjectPassed ? 'Trade PASSED filter validation' : 'Trade FAILED filter validation'}</small>
                        </div>
                    </div>
                    <div class="text-end">
                        <span class="badge ${anyProjectPassed ? 'bg-success' : 'bg-danger'}">
                            ${anyProjectPassed ? 'PASSED' : 'FAILED'}
                        </span>
                        <br>
                        <small class="text-muted">${totalPassed}/${totalFilters} filters passed</small>
                    </div>
                </div>
            </div>
    `;
    
    // Display each project's filters
    projectResults.forEach((project, idx) => {
        const projectId = project.project_id || 0;
        const filters = project.filter_results || [];
        const allPassed = project.decision === 'GO';
        const passedCount = filters.filter(f => f.passed).length;
        
        html += `
            <div class="card mb-2" style="border: 2px solid ${allPassed ? 'rgba(38, 191, 148, 0.5)' : 'rgba(230, 83, 60, 0.5)'};">
                <div class="card-header py-2" style="background: ${allPassed ? 'rgba(38, 191, 148, 0.15)' : 'rgba(230, 83, 60, 0.15)'};">
                    <div class="d-flex align-items-center justify-content-between w-100">
                        <span class="fw-semibold">
                            <i class="${allPassed ? 'ri-checkbox-circle-fill text-success' : 'ri-close-circle-fill text-danger'} me-1"></i>
                            Project #${projectId}
                        </span>
                        <span>
                            <span class="badge ${allPassed ? 'bg-success' : 'bg-danger'}">
                                ${allPassed ? 'ALL PASSED' : 'FAILED'}
                            </span>
                            <span class="badge bg-secondary-transparent ms-1">
                                ${passedCount}/${filters.length} filters
                            </span>
                        </span>
                    </div>
                </div>
                <div class="card-body p-0">
                    <div class="table-responsive">
                        <table class="table table-sm table-hover mb-0">
                            <thead>
                                <tr class="text-muted small" style="border-bottom: 1px solid var(--default-border);">
                                    <th style="width: 30px;"></th>
                                    <th>Filter</th>
                                    <th>Field Column</th>
                                    <th class="text-center">Min</th>
                                    <th class="text-center fw-bold">Actual</th>
                                    <th class="text-center">Max</th>
                                </tr>
                            </thead>
                            <tbody>
        `;
        
        filters.forEach(filter => {
            const passed = filter.passed || false;
            const filterName = filter.filter_name || filter.name || 'Unknown Filter';
            const fieldColumn = filter.field || filter.field_column || '-';
            const minute = filter.minute || 0;
            const fromValue = filter.from_value !== null && filter.from_value !== undefined ? parseFloat(filter.from_value).toFixed(4) : '-';
            const toValue = filter.to_value !== null && filter.to_value !== undefined ? parseFloat(filter.to_value).toFixed(4) : '-';
            const actualValue = filter.actual_value !== null && filter.actual_value !== undefined ? parseFloat(filter.actual_value).toFixed(4) : 'NULL';
            
            html += `
                <tr style="${passed ? '' : 'background: rgba(230, 83, 60, 0.1);'}">
                    <td class="text-center">
                        <i class="${passed ? 'ri-checkbox-circle-fill text-success fs-16' : 'ri-close-circle-fill text-danger fs-16'}"></i>
                    </td>
                    <td>
                        <span class="fw-medium">${filterName}</span>
                        ${filter.error ? `<br><small class="text-danger">${filter.error}</small>` : ''}
                    </td>
                    <td>
                        <code class="text-info small">${fieldColumn}</code>
                        ${minute > 0 ? `<span class="badge bg-purple-transparent small ms-1">M${minute}</span>` : ''}
                    </td>
                    <td class="text-center text-muted font-monospace">${fromValue}</td>
                    <td class="text-center fw-bold font-monospace ${passed ? 'text-success' : 'text-danger'}">${actualValue}</td>
                    <td class="text-center text-muted font-monospace">${toValue}</td>
                </tr>
            `;
        });
        
        html += `
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        `;
    });
    
    html += '</div></div>';
    container.innerHTML = html;
}

// Keyboard shortcuts
document.addEventListener('keydown', function(e) {
    if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;
    
    if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') {
        e.preventDefault();
        previousTrade();
    } else if (e.key === 'ArrowRight' || e.key === 'ArrowDown') {
        e.preventDefault();
        nextTrade();
    }
});
</script>

<?php
$content = ob_get_clean();
$scripts = '';

// Include the base layout
include __DIR__ . '/../../layouts/base.php';
?>
