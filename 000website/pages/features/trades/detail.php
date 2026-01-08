<?php
/**
 * Trade Detail Page - View Trade with 15-Minute Trail Data
 * Shows complete trade information and loops through all 15 minutes of trail data
 */

// --- Database API Client ---
require_once __DIR__ . '/../../../includes/DatabaseClient.php';
require_once __DIR__ . '/../../../includes/config.php';
$db = new DatabaseClient(DATABASE_API_URL);
$api_available = $db->isAvailable();

// --- Base URL for template ---
$baseUrl = '';

$error_message = '';
$trade = null;
$trail_data = [];

// Get trade ID from URL
$trade_id = isset($_GET['id']) ? (int)$_GET['id'] : 0;
$source = $_GET['source'] ?? 'duckdb';

if (!$trade_id) {
    $error_message = 'Trade ID is required.';
} elseif ($use_duckdb) {
    // Fetch trade details from live table (archive is deprecated)
    $trade_response = $db->getSingleBuyin($trade_id);
    
    if ($trade_response && isset($trade_response['buyin'])) {
        $trade = $trade_response['buyin'];
    } else {
        $error_message = 'Trade not found.';
    }
    
    // Fetch trail data
    $trail_response = $db->getTrailForBuyin($trade_id, $source);
    if ($trail_response && isset($trail_response['trail_data'])) {
        $trail_data = $trail_response['trail_data'];
        // Sort by minute to ensure proper ordering
        usort($trail_data, function($a, $b) {
            return ($a['minute'] ?? 0) <=> ($b['minute'] ?? 0);
        });
    }
} else {
    $error_message = "Website API is not available. Please start the API: python scheduler/website_api.py";
}

// --- Chart Field Configurations (must match JavaScript chartConfigs) ---
$chart_field_configs = [
    'price_movements' => [
        'pm_price_change_1m' => ['label' => 'Price Change 1m', 'format' => 'percent', 'color' => 'rgb(59, 130, 246)'],
        'pm_price_change_5m' => ['label' => 'Price Change 5m', 'format' => 'percent', 'color' => 'rgb(16, 185, 129)'],
        'pm_volatility_pct' => ['label' => 'Volatility %', 'format' => 'percent', 'color' => 'rgb(239, 68, 68)'],
        'pm_momentum_volatility_ratio' => ['label' => 'Momentum/Vol Ratio', 'format' => 'decimal', 'color' => 'rgb(139, 92, 246)'],
    ],
    'order_book' => [
        'ob_mid_price' => ['label' => 'Mid Price', 'format' => 'price', 'color' => 'rgb(59, 130, 246)'],
        'ob_volume_imbalance' => ['label' => 'Volume Imbalance', 'format' => 'decimal', 'color' => 'rgb(16, 185, 129)'],
        'ob_spread_bps' => ['label' => 'Spread (bps)', 'format' => 'decimal', 'color' => 'rgb(239, 68, 68)'],
        'ob_total_liquidity' => ['label' => 'Total Liquidity', 'format' => 'number', 'color' => 'rgb(245, 158, 11)'],
    ],
    'transactions' => [
        'tx_buy_sell_pressure' => ['label' => 'Buy/Sell Pressure', 'format' => 'decimal', 'color' => 'rgb(59, 130, 246)'],
        'tx_total_volume_usd' => ['label' => 'Total Volume USD', 'format' => 'money', 'color' => 'rgb(16, 185, 129)'],
        'tx_trade_count' => ['label' => 'Trade Count', 'format' => 'number', 'color' => 'rgb(239, 68, 68)'],
        'tx_whale_volume_pct' => ['label' => 'Whale Volume %', 'format' => 'percent', 'color' => 'rgb(245, 158, 11)'],
    ],
    'whale_activity' => [
        'wh_net_flow_ratio' => ['label' => 'Net Flow Ratio', 'format' => 'decimal', 'color' => 'rgb(59, 130, 246)'],
        'wh_total_sol_moved' => ['label' => 'Total SOL Moved', 'format' => 'number', 'color' => 'rgb(16, 185, 129)'],
        'wh_inflow_sol' => ['label' => 'Inflow SOL', 'format' => 'number', 'color' => 'rgb(34, 197, 94)'],
        'wh_outflow_sol' => ['label' => 'Outflow SOL', 'format' => 'number', 'color' => 'rgb(239, 68, 68)'],
    ],
    'patterns' => [
        'pat_breakout_score' => ['label' => 'Breakout Score', 'format' => 'decimal', 'color' => 'rgb(59, 130, 246)'],
    ],
    'second_prices' => [
        'sp_price_range_pct' => ['label' => 'Price Range %', 'format' => 'percent', 'color' => 'rgb(59, 130, 246)'],
        'sp_total_change_pct' => ['label' => 'Total Change %', 'format' => 'percent', 'color' => 'rgb(16, 185, 129)'],
        'sp_volatility_pct' => ['label' => 'Volatility %', 'format' => 'percent', 'color' => 'rgb(239, 68, 68)'],
    ],
    'btc_prices' => [
        'btc_price_change_1m' => ['label' => 'BTC Change 1m', 'format' => 'percent', 'color' => 'rgb(247, 147, 26)'],
        'btc_price_change_5m' => ['label' => 'BTC Change 5m', 'format' => 'percent', 'color' => 'rgb(255, 193, 7)'],
        'btc_volatility_pct' => ['label' => 'BTC Volatility', 'format' => 'percent', 'color' => 'rgb(255, 152, 0)'],
    ],
    'eth_prices' => [
        'eth_price_change_1m' => ['label' => 'ETH Change 1m', 'format' => 'percent', 'color' => 'rgb(98, 126, 234)'],
        'eth_price_change_5m' => ['label' => 'ETH Change 5m', 'format' => 'percent', 'color' => 'rgb(130, 150, 245)'],
        'eth_volatility_pct' => ['label' => 'ETH Volatility', 'format' => 'percent', 'color' => 'rgb(156, 39, 176)'],
    ],
];

// --- Field Groups for Display ---
$field_groups = [
    'Price Movements' => [
        'pm_price_change_1m' => ['label' => 'Price Change 1m', 'format' => 'percent'],
        'pm_price_change_5m' => ['label' => 'Price Change 5m', 'format' => 'percent'],
        'pm_price_change_10m' => ['label' => 'Price Change 10m', 'format' => 'percent'],
        'pm_volatility_pct' => ['label' => 'Volatility', 'format' => 'percent'],
        'pm_momentum_volatility_ratio' => ['label' => 'Momentum/Vol Ratio', 'format' => 'decimal'],
        'pm_momentum_acceleration_1m' => ['label' => 'Momentum Accel', 'format' => 'decimal'],
        'pm_trend_consistency_3m' => ['label' => 'Trend Consistency 3m', 'format' => 'decimal'],
        'pm_cumulative_return_5m' => ['label' => 'Cumulative Return 5m', 'format' => 'percent'],
        'pm_open_price' => ['label' => 'Open', 'format' => 'price'],
        'pm_high_price' => ['label' => 'High', 'format' => 'price'],
        'pm_low_price' => ['label' => 'Low', 'format' => 'price'],
        'pm_close_price' => ['label' => 'Close', 'format' => 'price'],
    ],
    'Order Book' => [
        'ob_mid_price' => ['label' => 'Mid Price', 'format' => 'price'],
        'ob_volume_imbalance' => ['label' => 'Volume Imbalance', 'format' => 'decimal'],
        'ob_imbalance_shift_1m' => ['label' => 'Imbalance Shift 1m', 'format' => 'decimal'],
        'ob_depth_imbalance_ratio' => ['label' => 'Depth Imbalance Ratio', 'format' => 'decimal'],
        'ob_bid_liquidity_share_pct' => ['label' => 'Bid Liquidity %', 'format' => 'percent'],
        'ob_ask_liquidity_share_pct' => ['label' => 'Ask Liquidity %', 'format' => 'percent'],
        'ob_total_liquidity' => ['label' => 'Total Liquidity', 'format' => 'number'],
        'ob_spread_bps' => ['label' => 'Spread (bps)', 'format' => 'decimal'],
        'ob_net_flow_5m' => ['label' => 'Net Flow 5m', 'format' => 'number'],
    ],
    'Transactions' => [
        'tx_buy_sell_pressure' => ['label' => 'Buy/Sell Pressure', 'format' => 'decimal'],
        'tx_buy_volume_pct' => ['label' => 'Buy Volume %', 'format' => 'percent'],
        'tx_sell_volume_pct' => ['label' => 'Sell Volume %', 'format' => 'percent'],
        'tx_long_short_ratio' => ['label' => 'Long/Short Ratio', 'format' => 'decimal'],
        'tx_total_volume_usd' => ['label' => 'Total Volume USD', 'format' => 'money'],
        'tx_volume_surge_ratio' => ['label' => 'Volume Surge', 'format' => 'decimal'],
        'tx_whale_volume_pct' => ['label' => 'Whale Volume %', 'format' => 'percent'],
        'tx_trade_count' => ['label' => 'Trade Count', 'format' => 'number'],
        'tx_avg_trade_size' => ['label' => 'Avg Trade Size', 'format' => 'money'],
    ],
    'Whale Activity' => [
        'wh_net_flow_ratio' => ['label' => 'Net Flow Ratio', 'format' => 'decimal'],
        'wh_accumulation_ratio' => ['label' => 'Accumulation Ratio', 'format' => 'decimal'],
        'wh_total_sol_moved' => ['label' => 'Total SOL Moved', 'format' => 'number'],
        'wh_inflow_sol' => ['label' => 'Inflow SOL', 'format' => 'number'],
        'wh_outflow_sol' => ['label' => 'Outflow SOL', 'format' => 'number'],
        'wh_net_flow_sol' => ['label' => 'Net Flow SOL', 'format' => 'number'],
        'wh_strong_accumulation' => ['label' => 'Strong Accumulation', 'format' => 'number'],
        'wh_strong_distribution' => ['label' => 'Strong Distribution', 'format' => 'number'],
        'wh_movement_count' => ['label' => 'Movement Count', 'format' => 'number'],
    ],
    'Patterns' => [
        'pat_breakout_score' => ['label' => 'Breakout Score', 'format' => 'decimal'],
        'pat_detected_count' => ['label' => 'Patterns Detected', 'format' => 'number'],
        'pat_asc_tri_confidence' => ['label' => 'Asc Triangle Conf', 'format' => 'decimal'],
        'pat_bull_flag_confidence' => ['label' => 'Bull Flag Conf', 'format' => 'decimal'],
        'pat_bull_pennant_confidence' => ['label' => 'Bull Pennant Conf', 'format' => 'decimal'],
        'pat_fall_wedge_confidence' => ['label' => 'Falling Wedge Conf', 'format' => 'decimal'],
        'pat_cup_handle_confidence' => ['label' => 'Cup & Handle Conf', 'format' => 'decimal'],
        'pat_inv_hs_confidence' => ['label' => 'Inv H&S Conf', 'format' => 'decimal'],
    ],
    'Second Prices' => [
        'sp_min_price' => ['label' => 'Min Price', 'format' => 'price'],
        'sp_max_price' => ['label' => 'Max Price', 'format' => 'price'],
        'sp_start_price' => ['label' => 'Start Price', 'format' => 'price'],
        'sp_end_price' => ['label' => 'End Price', 'format' => 'price'],
        'sp_price_range_pct' => ['label' => 'Price Range %', 'format' => 'percent'],
        'sp_total_change_pct' => ['label' => 'Total Change %', 'format' => 'percent'],
        'sp_volatility_pct' => ['label' => 'Volatility %', 'format' => 'percent'],
        'sp_price_count' => ['label' => 'Price Count', 'format' => 'number'],
    ],
    'BTC Prices' => [
        'btc_price_change_1m' => ['label' => 'BTC Change 1m', 'format' => 'percent'],
        'btc_price_change_5m' => ['label' => 'BTC Change 5m', 'format' => 'percent'],
        'btc_price_change_10m' => ['label' => 'BTC Change 10m', 'format' => 'percent'],
        'btc_volatility_pct' => ['label' => 'BTC Volatility', 'format' => 'percent'],
        'btc_open_price' => ['label' => 'BTC Open', 'format' => 'price'],
        'btc_close_price' => ['label' => 'BTC Close', 'format' => 'price'],
    ],
    'ETH Prices' => [
        'eth_price_change_1m' => ['label' => 'ETH Change 1m', 'format' => 'percent'],
        'eth_price_change_5m' => ['label' => 'ETH Change 5m', 'format' => 'percent'],
        'eth_price_change_10m' => ['label' => 'ETH Change 10m', 'format' => 'percent'],
        'eth_volatility_pct' => ['label' => 'ETH Volatility', 'format' => 'percent'],
        'eth_open_price' => ['label' => 'ETH Open', 'format' => 'price'],
        'eth_close_price' => ['label' => 'ETH Close', 'format' => 'price'],
    ],
];

/**
 * Calculate normalized heatmap data for all fields
 * Normalizes each field's values across all 15 minutes to 0-100% scale
 */
function calculateHeatmapData($trail_data, $field_groups) {
    if (empty($trail_data)) {
        return [];
    }
    
    // Build minute lookup map
    $minute_data = [];
    foreach ($trail_data as $row) {
        $minute_data[$row['minute']] = $row;
    }
    
    $heatmap_data = [];
    
    foreach ($field_groups as $groupName => $fields) {
        $heatmap_data[$groupName] = [];
        
        foreach ($fields as $fieldKey => $fieldConfig) {
            // Collect all values for this field across all minutes
            $values = [];
            $raw_values = [];
            
            for ($m = 0; $m < 15; $m++) {
                $val = $minute_data[$m][$fieldKey] ?? null;
                $raw_values[$m] = $val;
                
                if ($val !== null && $val !== '' && is_numeric($val)) {
                    $values[] = floatval($val);
                }
            }
            
            // Calculate min and max
            $min = !empty($values) ? min($values) : 0;
            $max = !empty($values) ? max($values) : 0;
            $range = $max - $min;
            
            // Calculate normalized percentages for each minute
            $normalized = [];
            for ($m = 0; $m < 15; $m++) {
                $val = $raw_values[$m];
                
                if ($val === null || $val === '' || !is_numeric($val)) {
                    $normalized[$m] = [
                        'raw' => null,
                        'pct' => null,
                        'pct_bucket' => 'null'
                    ];
                } else {
                    $numVal = floatval($val);
                    
                    // Handle edge case where all values are the same
                    if ($range == 0) {
                        $pct = 50; // Middle of scale if all same
                    } else {
                        $pct = (($numVal - $min) / $range) * 100;
                    }
                    
                    // Round to nearest 10 for color bucket
                    $pct_bucket = min(100, max(0, round($pct / 10) * 10));
                    
                    $normalized[$m] = [
                        'raw' => $numVal,
                        'pct' => round($pct, 1),
                        'pct_bucket' => $pct_bucket
                    ];
                }
            }
            
            $heatmap_data[$groupName][$fieldKey] = [
                'label' => $fieldConfig['label'],
                'format' => $fieldConfig['format'],
                'min' => $min,
                'max' => $max,
                'minutes' => $normalized
            ];
        }
    }
    
    return $heatmap_data;
}

/**
 * Format a value based on its type
 */
function formatValue($value, $format) {
    if ($value === null || $value === '') {
        return '<span class="text-muted">-</span>';
    }
    
    $numValue = floatval($value);
    
    switch ($format) {
        case 'percent':
            $class = $numValue > 0 ? 'text-success' : ($numValue < 0 ? 'text-danger' : 'text-muted');
            $sign = $numValue > 0 ? '+' : '';
            return "<span class='{$class}'>{$sign}" . number_format($numValue, 4) . "%</span>";
        
        case 'price':
            return '$' . number_format($numValue, 4);
        
        case 'money':
            return '$' . number_format($numValue, 2);
        
        case 'number':
            return number_format($numValue, is_float($value) && fmod($numValue, 1) !== 0.0 ? 2 : 0);
        
        case 'decimal':
            $class = $numValue > 0 ? 'text-success' : ($numValue < 0 ? 'text-danger' : '');
            return "<span class='{$class}'>" . number_format($numValue, 4) . "</span>";
        
        default:
            return htmlspecialchars($value);
    }
}

// --- Page Styles ---
ob_start();
?>
<style>
    .trade-header {
        background: var(--custom-white);
        border: 1px solid var(--default-border);
        border-radius: 0.5rem;
        padding: 1.5rem;
        margin-bottom: 1.5rem;
    }
    
    .trade-id {
        font-size: 1.5rem;
        font-weight: 700;
        color: var(--default-text-color);
    }
    
    .status-badge-lg {
        padding: 0.5rem 1rem;
        border-radius: 6px;
        font-size: 0.85rem;
        font-weight: 600;
        text-transform: uppercase;
    }
    
    .status-pending { background: rgba(var(--warning-rgb), 0.15); color: rgb(var(--warning-rgb)); }
    .status-sold { background: rgba(var(--success-rgb), 0.15); color: rgb(var(--success-rgb)); }
    .status-no_go { background: rgba(var(--danger-rgb), 0.15); color: rgb(var(--danger-rgb)); }
    .status-validating { background: rgba(var(--info-rgb), 0.15); color: rgb(var(--info-rgb)); }
    
    .trade-meta {
        display: flex;
        flex-wrap: wrap;
        gap: 1.5rem;
        margin-top: 1rem;
    }
    
    .meta-item {
        display: flex;
        flex-direction: column;
        gap: 0.25rem;
    }
    
    .meta-label {
        font-size: 0.7rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        color: var(--text-muted);
    }
    
    .meta-value {
        font-size: 1rem;
        font-weight: 600;
        color: var(--default-text-color);
    }
    
    .minute-nav {
        display: flex;
        gap: 0.5rem;
        flex-wrap: wrap;
        margin-bottom: 1.5rem;
    }
    
    .minute-btn {
        padding: 0.5rem 1rem;
        border: 1px solid var(--default-border);
        border-radius: 6px;
        background: var(--custom-white);
        color: var(--default-text-color);
        font-weight: 600;
        font-size: 0.85rem;
        cursor: pointer;
        transition: all 0.15s ease;
    }
    
    .minute-btn:hover {
        border-color: rgb(var(--primary-rgb));
        background: rgba(var(--primary-rgb), 0.1);
    }
    
    .minute-btn.active {
        background: rgb(var(--primary-rgb));
        border-color: rgb(var(--primary-rgb));
        color: white;
    }
    
    
    .section-card {
        background: var(--custom-white);
        border: 1px solid var(--default-border);
        border-radius: 0.5rem;
        margin-bottom: 1rem;
    }
    
    .section-header {
        padding: 0.75rem 1rem;
        border-bottom: 1px solid var(--default-border);
        font-weight: 600;
        font-size: 0.9rem;
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    
    .section-header i {
        color: rgb(var(--primary-rgb));
    }
    
    .section-body {
        padding: 1rem;
    }
    
    .data-grid {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
        gap: 1rem;
    }
    
    .data-item {
        display: flex;
        flex-direction: column;
        gap: 0.2rem;
    }
    
    .data-label {
        font-size: 0.7rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        color: var(--text-muted);
    }
    
    .data-value {
        font-size: 0.9rem;
        font-weight: 500;
        font-family: 'SF Mono', 'Monaco', 'Consolas', monospace;
    }
    
    .no-trail-data {
        text-align: center;
        padding: 3rem 2rem;
        border: 1px dashed var(--default-border);
        border-radius: 0.5rem;
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
    
    .back-btn {
        display: inline-flex;
        align-items: center;
        gap: 0.5rem;
        padding: 0.5rem 1rem;
        border: 1px solid var(--default-border);
        border-radius: 6px;
        background: var(--custom-white);
        color: var(--default-text-color);
        text-decoration: none;
        font-size: 0.85rem;
        transition: all 0.15s ease;
    }
    
    .back-btn:hover {
        border-color: rgb(var(--primary-rgb));
        background: rgba(var(--primary-rgb), 0.1);
    }
    
    .overview-grid {
        display: grid;
        grid-template-columns: repeat(6, 1fr);
        gap: 0.5rem;
        margin-bottom: 1.5rem;
    }
    
    @media (max-width: 992px) {
        .overview-grid {
            grid-template-columns: repeat(3, 1fr);
        }
    }
    
    @media (max-width: 576px) {
        .overview-grid {
            grid-template-columns: repeat(2, 1fr);
        }
    }
    
    .overview-cell {
        padding: 0.75rem;
        background: var(--custom-white);
        border: 1px solid var(--default-border);
        border-radius: 4px;
        text-align: center;
    }
    
    .overview-minute {
        font-size: 0.65rem;
        color: var(--text-muted);
        margin-bottom: 0.25rem;
    }
    
    .overview-value {
        font-size: 0.85rem;
        font-weight: 600;
    }
    
    .chart-container {
        margin-top: 1rem;
        margin-bottom: 1rem;
        position: relative;
        height: 250px;
    }
    
    .chart-title {
        font-size: 0.75rem;
        color: var(--text-muted);
        margin-bottom: 0.5rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    .chart-wrapper {
        background: var(--custom-white);
        border: 1px solid var(--default-border);
        border-radius: 0.5rem;
        padding: 1rem;
        margin-bottom: 1rem;
    }
    
    .data-table-wrapper {
        overflow-x: auto;
        margin-top: 1rem;
    }
    
    .data-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 0.85rem;
    }
    
    .data-table th {
        background: var(--custom-white);
        border: 1px solid var(--default-border);
        padding: 0.5rem;
        text-align: center;
        font-weight: 600;
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        position: sticky;
        left: 0;
        z-index: 10;
    }
    
    .data-table th:first-child {
        text-align: left;
        min-width: 200px;
    }
    
    .data-table td {
        border: 1px solid var(--default-border);
        padding: 0.4rem 0.5rem;
        text-align: center;
        font-family: 'SF Mono', 'Monaco', 'Consolas', monospace;
    }
    
    .data-table td:first-child {
        text-align: left;
        font-weight: 600;
        background: var(--custom-white);
        position: sticky;
        left: 0;
        z-index: 5;
    }
    
    .data-table tbody tr:hover {
        background: rgba(var(--primary-rgb), 0.05);
    }
    
    .data-table .section-header-row {
        background: rgba(var(--primary-rgb), 0.1);
        font-weight: 700;
        text-transform: uppercase;
        font-size: 1rem;
    }
    
    .data-table .section-header-row td {
        padding: 0.8rem 0.5rem;
        font-size: 1rem;
        font-weight: 700;
    }
    
    .table-container {
        background: var(--custom-white);
        border: 1px solid var(--default-border);
        border-radius: 0.5rem;
        padding: 1rem;
        margin-bottom: 1.5rem;
    }
    
    /* Scatter Chart Styles */
    .scatter-chart-container {
        background: var(--custom-white);
        border: 1px solid var(--default-border);
        border-radius: 0.5rem;
        margin-bottom: 1.5rem;
        overflow: hidden;
    }
    
    .scatter-chart-wrapper {
        padding: 1rem;
        position: relative;
    }
    
    .scatter-chart-canvas {
        height: 450px;
        width: 100%;
    }
    
    .scatter-legend {
        display: flex;
        flex-wrap: wrap;
        gap: 0.75rem;
        padding: 0.75rem 1rem;
        border-top: 1px solid var(--default-border);
        background: rgba(var(--light-rgb), 0.3);
    }
    
    .scatter-legend-item {
        display: flex;
        align-items: center;
        gap: 0.4rem;
        font-size: 0.7rem;
        color: var(--default-text-color);
        cursor: pointer;
        padding: 0.25rem 0.5rem;
        border-radius: 4px;
        transition: all 0.15s ease;
    }
    
    .scatter-legend-item:hover {
        background: rgba(var(--primary-rgb), 0.1);
    }
    
    .scatter-legend-item.hidden {
        opacity: 0.4;
    }
    
    .scatter-legend-dot {
        width: 10px;
        height: 10px;
        border-radius: 50%;
    }
    
    .scatter-legend-line {
        width: 20px;
        height: 3px;
        border-radius: 2px;
    }
    
    .scatter-filter-controls {
        display: flex;
        flex-wrap: wrap;
        gap: 0.5rem;
        padding: 0.75rem 1rem;
        border-bottom: 1px solid var(--default-border);
        background: rgba(var(--light-rgb), 0.3);
    }
    
    .scatter-filter-btn {
        padding: 0.35rem 0.75rem;
        font-size: 0.7rem;
        border: 1px solid var(--default-border);
        border-radius: 4px;
        background: var(--custom-white);
        color: var(--default-text-color);
        cursor: pointer;
        transition: all 0.15s ease;
    }
    
    .scatter-filter-btn:hover {
        border-color: rgb(var(--primary-rgb));
        background: rgba(var(--primary-rgb), 0.1);
    }
    
    .scatter-filter-btn.active {
        background: rgb(var(--primary-rgb));
        border-color: rgb(var(--primary-rgb));
        color: white;
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

<!-- Page Header -->
<div class="d-flex align-items-center justify-content-between page-header-breadcrumb flex-wrap gap-2 mb-3">
    <div>
        <nav>
            <ol class="breadcrumb mb-1">
                <li class="breadcrumb-item"><a href="<?php echo $baseUrl; ?>/">Dashboard</a></li>
                <li class="breadcrumb-item"><a href="<?php echo $baseUrl; ?>/pages/features/trades/">Trades</a></li>
                <li class="breadcrumb-item active" aria-current="page">Trade #<?php echo $trade_id; ?></li>
            </ol>
        </nav>
        <h1 class="page-title fw-medium fs-18 mb-0">Trade Details</h1>
    </div>
    <a href="/pages/features/trades/" class="back-btn">
        <i class="ri-arrow-left-line"></i>
        Back to Trades
    </a>
</div>

<!-- Messages -->
<?php if ($error_message): ?>
<div class="alert alert-danger alert-dismissible fade show" role="alert">
    <i class="ri-error-warning-line me-2"></i><?php echo htmlspecialchars($error_message); ?>
    <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
</div>
<?php endif; ?>

<?php if ($trade): ?>
<!-- Trade Header -->
<div class="trade-header">
    <div class="d-flex align-items-center justify-content-between flex-wrap gap-3">
        <div>
            <span class="trade-id">Trade #<?php echo $trade['id']; ?></span>
            <span class="ms-3 status-badge-lg status-<?php echo strtolower($trade['our_status'] ?? 'unknown'); ?>">
                <?php echo htmlspecialchars($trade['our_status'] ?? 'Unknown'); ?>
            </span>
        </div>
        <div class="d-flex gap-2">
            <a href="?id=<?php echo $trade_id; ?>&source=duckdb" class="btn btn-sm <?php echo $source !== 'mysql' ? 'btn-primary' : 'btn-outline-primary'; ?>">
                DuckDB
            </a>
            <a href="?id=<?php echo $trade_id; ?>&source=mysql" class="btn btn-sm <?php echo $source === 'mysql' ? 'btn-primary' : 'btn-outline-primary'; ?>">
                MySQL
            </a>
        </div>
    </div>
    
    <div class="trade-meta">
        <div class="meta-item">
            <span class="meta-label">Play ID</span>
            <span class="meta-value"><?php echo $trade['play_id'] ?? '-'; ?></span>
        </div>
        <div class="meta-item">
            <span class="meta-label">Entry Price</span>
            <span class="meta-value">$<?php echo number_format(floatval($trade['our_entry_price'] ?? 0), 4); ?></span>
        </div>
        <div class="meta-item">
            <span class="meta-label">P/L</span>
            <?php 
            $pl = $trade['our_profit_loss'] ?? null;
            if ($pl !== null):
                $pl_num = floatval($pl);
                $pl_class = $pl_num > 0 ? 'text-success' : ($pl_num < 0 ? 'text-danger' : '');
            ?>
            <span class="meta-value <?php echo $pl_class; ?>">
                <?php echo $pl_num > 0 ? '+' : ''; ?><?php echo number_format($pl_num, 2); ?>%
            </span>
            <?php else: ?>
            <span class="meta-value text-muted">-</span>
            <?php endif; ?>
        </div>
        <div class="meta-item">
            <span class="meta-label">Potential Gain</span>
            <?php 
            $pg = $trade['potential_gain'] ?? null;
            if ($pg !== null):
                $pg_num = floatval($pg);
                $pg_class = $pg_num > 0 ? 'text-success' : ($pg_num < 0 ? 'text-danger' : '');
            ?>
            <span class="meta-value <?php echo $pg_class; ?>">
                <?php echo $pg_num > 0 ? '+' : ''; ?><?php echo number_format($pg_num, 2); ?>%
            </span>
            <?php else: ?>
            <span class="meta-value text-muted">-</span>
            <?php endif; ?>
        </div>
        <div class="meta-item">
            <span class="meta-label">Wallet</span>
            <span class="meta-value" style="font-family: monospace; font-size: 0.85rem;">
                <?php echo htmlspecialchars(substr($trade['wallet_address'] ?? '', 0, 16)); ?>...
            </span>
        </div>
        <div class="meta-item">
            <span class="meta-label">Followed At</span>
            <span class="meta-value"><?php echo $trade['followed_at'] ? date('M j, Y g:i:s A', strtotime($trade['followed_at'])) : '-'; ?></span>
        </div>
        <div class="meta-item">
            <span class="meta-label">Price Cycle</span>
            <span class="meta-value"><?php echo $trade['price_cycle'] ?? '-'; ?></span>
        </div>
    </div>
</div>

<!-- Trail Data -->
<?php if (empty($trail_data)): ?>
<div class="no-trail-data">
    <div class="mb-3">
        <i class="ri-line-chart-line text-muted" style="font-size: 3rem;"></i>
    </div>
    <h4 class="text-muted">No Trail Data Available</h4>
    <p class="text-muted mb-0">This trade does not have 15-minute trail data stored yet.</p>
    <?php if ($source === 'duckdb'): ?>
    <p class="text-muted mt-2">
        <a href="?id=<?php echo $trade_id; ?>&source=mysql">Try loading from MySQL</a>
    </p>
    <?php endif; ?>
</div>
<?php else: ?>

<?php 
// Calculate normalized scatter data
$heatmap_data = calculateHeatmapData($trail_data, $field_groups);

// Category colors for scatter chart
$category_colors = [
    'Price Movements' => '#3b82f6',   // Blue
    'Order Book' => '#10b981',        // Green
    'Transactions' => '#f59e0b',      // Amber
    'Whale Activity' => '#8b5cf6',    // Purple
    'Patterns' => '#ec4899',          // Pink
    'Second Prices' => '#06b6d4',     // Cyan
    'BTC Prices' => '#f97316',        // Orange
    'ETH Prices' => '#6366f1',        // Indigo
];
?>

<!-- Price & Filter Scatter Chart -->
<div class="scatter-chart-container">
    <div class="section-header">
        <i class="ri-bubble-chart-line"></i>
        Price Chart with Normalized Filter Scatter (0-100%)
    </div>
    <div class="scatter-filter-controls">
        <button type="button" class="scatter-filter-btn active" data-category="all">All Filters</button>
        <?php foreach ($category_colors as $cat => $color): ?>
        <button type="button" class="scatter-filter-btn" data-category="<?php echo htmlspecialchars(strtolower(str_replace(' ', '_', $cat))); ?>" style="border-left: 3px solid <?php echo $color; ?>;">
            <?php echo htmlspecialchars($cat); ?>
        </button>
        <?php endforeach; ?>
    </div>
    <div class="scatter-chart-wrapper">
        <canvas id="scatterPriceChart" class="scatter-chart-canvas"></canvas>
    </div>
    <div class="scatter-legend" id="scatterLegend">
        <div class="scatter-legend-item" data-type="price">
            <span class="scatter-legend-line" style="background: #22c55e;"></span>
            <span>Price (Close)</span>
        </div>
        <?php foreach ($category_colors as $cat => $color): ?>
        <div class="scatter-legend-item" data-category="<?php echo htmlspecialchars(strtolower(str_replace(' ', '_', $cat))); ?>">
            <span class="scatter-legend-dot" style="background: <?php echo $color; ?>;"></span>
            <span><?php echo htmlspecialchars($cat); ?></span>
        </div>
        <?php endforeach; ?>
    </div>
</div>

<!-- Pass heatmap data to JavaScript -->
<script>
    const scatterData = <?php echo json_encode($heatmap_data); ?>;
    const categoryColors = <?php echo json_encode($category_colors); ?>;
</script>

<!-- Quick Overview: Price Change per Minute -->
<div class="section-card">
    <div class="section-header">
        <i class="ri-line-chart-line"></i>
        Price Change Overview (All 15 Minutes)
    </div>
    <div class="section-body">
        <div class="overview-grid">
            <?php foreach ($trail_data as $row): ?>
            <div class="overview-cell">
                <div class="overview-minute">Minute <?php echo $row['minute']; ?></div>
                <?php 
                $pc = $row['pm_price_change_1m'] ?? null;
                if ($pc !== null):
                    $pc_num = floatval($pc);
                    $pc_class = $pc_num > 0 ? 'text-success' : ($pc_num < 0 ? 'text-danger' : 'text-muted');
                ?>
                <div class="overview-value <?php echo $pc_class; ?>">
                    <?php echo $pc_num > 0 ? '+' : ''; ?><?php echo number_format($pc_num, 3); ?>%
                </div>
                <?php else: ?>
                <div class="overview-value text-muted">-</div>
                <?php endif; ?>
            </div>
            <?php endforeach; ?>
        </div>
    </div>
</div>

<!-- Data Table View (All Fields, All Minutes) -->
<div class="table-container">
    <div class="section-header">
        <i class="ri-table-line"></i>
        Complete Data Table - All Fields Across All 15 Minutes
    </div>
    <div class="section-body">
        <div class="data-table-wrapper">
            <table class="data-table">
                <thead>
                    <tr>
                        <th>Field</th>
                        <?php for ($m = 0; $m < 15; $m++): ?>
                        <th>Min <?php echo $m; ?></th>
                        <?php endfor; ?>
                    </tr>
                </thead>
                <tbody>
                    <?php 
                    // Build a map of minute => data for quick lookup
                    $minute_data = [];
                    foreach ($trail_data as $row) {
                        $minute_data[$row['minute']] = $row;
                    }
                    
                    // Display all fields from all sections
                    foreach ($field_groups as $groupName => $fields): 
                    ?>
                    <tr class="section-header-row">
                        <td colspan="16"><?php echo $groupName; ?></td>
                    </tr>
                    <?php foreach ($fields as $fieldKey => $fieldConfig): ?>
                    <tr>
                        <td><?php echo $fieldConfig['label']; ?></td>
                        <?php for ($m = 0; $m < 15; $m++): 
                            $value = $minute_data[$m][$fieldKey] ?? null;
                        ?>
                        <td><?php echo formatValue($value, $fieldConfig['format']); ?></td>
                        <?php endfor; ?>
                    </tr>
                    <?php endforeach; ?>
                    <?php endforeach; ?>
                </tbody>
            </table>
        </div>
    </div>
</div>

<!-- Minute Navigation (for chart highlighting) -->
<div class="minute-nav" style="margin-top: 1.5rem;">
    <span style="margin-right: 1rem; font-weight: 600; color: var(--text-muted);">Highlight Minute in Charts:</span>
    <?php for ($i = 1; $i < 15; $i++): ?>
    <button type="button" class="minute-btn" onclick="showMinute(<?php echo $i; ?>)">
        Min <?php echo $i; ?>
    </button>
    <?php endfor; ?>
    <button type="button" class="minute-btn" onclick="showAllMinutes()" style="margin-left: auto;">
        <i class="ri-list-check"></i> Show All
    </button>
</div>

<!-- Charts Section (Always Visible) -->
<div id="charts-section">
    <?php 
    // Map display names to section keys
    $section_key_map = [
        'Price Movements' => 'price_movements',
        'Order Book' => 'order_book',
        'Transactions' => 'transactions',
        'Whale Activity' => 'whale_activity',
        'Patterns' => 'patterns',
        'Second Prices' => 'second_prices',
        'BTC Prices' => 'btc_prices',
        'ETH Prices' => 'eth_prices',
    ];
    
    foreach ($field_groups as $groupName => $fields): 
        $sectionKey = $section_key_map[$groupName] ?? strtolower(str_replace(' ', '_', $groupName));
        $chartFields = $chart_field_configs[$sectionKey] ?? [];
    ?>
    <div class="section-card">
        <div class="section-header">
            <i class="ri-line-chart-line"></i>
            <?php echo $groupName; ?> - Charts (All 15 Minutes)
        </div>
        <div class="section-body">
            <div class="chart-wrapper" data-section="<?php echo $sectionKey; ?>">
                <?php if (!empty($chartFields)): ?>
                    <?php foreach ($chartFields as $fieldKey => $fieldConfig): ?>
                    <div class="chart-container">
                        <div class="chart-title"><?php echo $fieldConfig['label']; ?></div>
                        <canvas id="chart_<?php echo $sectionKey; ?>_<?php echo $fieldKey; ?>" height="250"></canvas>
                    </div>
                    <?php endforeach; ?>
                <?php else: ?>
                    <p class="text-muted">No charts configured for this section.</p>
                <?php endif; ?>
            </div>
        </div>
    </div>
    <?php endforeach; ?>
</div>

<?php endif; ?>
<?php endif; ?>

<!-- Chart.js Library -->
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>

<script>
    // Trail data from PHP
    const trailData = <?php echo json_encode($trail_data); ?>;
    const charts = {};
    let currentMinute = 0;
    
    // Initialize all charts
    function initCharts() {
        if (!trailData || trailData.length === 0) return;
        
        // Chart configurations per section
        const chartConfigs = {
            'price_movements': [
                { key: 'pm_price_change_1m', label: 'Price Change 1m', color: 'rgb(59, 130, 246)' },
                { key: 'pm_price_change_5m', label: 'Price Change 5m', color: 'rgb(16, 185, 129)' },
                { key: 'pm_volatility_pct', label: 'Volatility %', color: 'rgb(239, 68, 68)' },
                { key: 'pm_momentum_volatility_ratio', label: 'Momentum/Vol Ratio', color: 'rgb(139, 92, 246)' },
            ],
            'order_book': [
                { key: 'ob_mid_price', label: 'Mid Price', color: 'rgb(59, 130, 246)' },
                { key: 'ob_volume_imbalance', label: 'Volume Imbalance', color: 'rgb(16, 185, 129)' },
                { key: 'ob_spread_bps', label: 'Spread (bps)', color: 'rgb(239, 68, 68)' },
                { key: 'ob_total_liquidity', label: 'Total Liquidity', color: 'rgb(245, 158, 11)' },
            ],
            'transactions': [
                { key: 'tx_buy_sell_pressure', label: 'Buy/Sell Pressure', color: 'rgb(59, 130, 246)' },
                { key: 'tx_total_volume_usd', label: 'Total Volume USD', color: 'rgb(16, 185, 129)' },
                { key: 'tx_trade_count', label: 'Trade Count', color: 'rgb(239, 68, 68)' },
                { key: 'tx_whale_volume_pct', label: 'Whale Volume %', color: 'rgb(245, 158, 11)' },
            ],
            'whale_activity': [
                { key: 'wh_net_flow_ratio', label: 'Net Flow Ratio', color: 'rgb(59, 130, 246)' },
                { key: 'wh_total_sol_moved', label: 'Total SOL Moved', color: 'rgb(16, 185, 129)' },
                { key: 'wh_inflow_sol', label: 'Inflow SOL', color: 'rgb(34, 197, 94)' },
                { key: 'wh_outflow_sol', label: 'Outflow SOL', color: 'rgb(239, 68, 68)' },
            ],
            'patterns': [
                { key: 'pat_breakout_score', label: 'Breakout Score', color: 'rgb(59, 130, 246)' },
            ],
            'second_prices': [
                { key: 'sp_price_range_pct', label: 'Price Range %', color: 'rgb(59, 130, 246)' },
                { key: 'sp_total_change_pct', label: 'Total Change %', color: 'rgb(16, 185, 129)' },
                { key: 'sp_volatility_pct', label: 'Volatility %', color: 'rgb(239, 68, 68)' },
            ],
            'btc_prices': [
                { key: 'btc_price_change_1m', label: 'BTC Change 1m', color: 'rgb(247, 147, 26)' },
                { key: 'btc_price_change_5m', label: 'BTC Change 5m', color: 'rgb(255, 193, 7)' },
                { key: 'btc_volatility_pct', label: 'BTC Volatility', color: 'rgb(255, 152, 0)' },
            ],
            'eth_prices': [
                { key: 'eth_price_change_1m', label: 'ETH Change 1m', color: 'rgb(98, 126, 234)' },
                { key: 'eth_price_change_5m', label: 'ETH Change 5m', color: 'rgb(130, 150, 245)' },
                { key: 'eth_volatility_pct', label: 'ETH Volatility', color: 'rgb(156, 39, 176)' },
            ],
        };
        
        // Create charts for each section
        Object.keys(chartConfigs).forEach(function(section) {
            const configs = chartConfigs[section];
            configs.forEach(function(config) {
                const canvasId = 'chart_' + section + '_' + config.key;
                const canvas = document.getElementById(canvasId);
                if (!canvas) return;
                
                // Sort trail data by minute to ensure proper ordering
                const sortedData = trailData.slice().sort(function(a, b) {
                    return (a.minute || 0) - (b.minute || 0);
                });
                
                // Extract data for all 15 minutes
                const minutes = sortedData.map(function(row) { return row.minute; });
                const values = sortedData.map(function(row) { 
                    const val = row[config.key];
                    if (val === null || val === undefined || val === '') {
                        return null;
                    }
                    const numVal = parseFloat(val);
                    return isNaN(numVal) ? null : numVal;
                });
                
                // Check if we have any valid data points
                const hasData = values.some(function(v) { return v !== null; });
                if (!hasData) {
                    console.warn('No data for chart:', canvasId);
                    return;
                }
                
                // Create chart
                const ctx = canvas.getContext('2d');
                charts[canvasId] = new Chart(ctx, {
                    type: 'line',
                    data: {
                        labels: minutes.map(function(m) { return 'Min ' + m; }),
                        datasets: [{
                            label: config.label,
                            data: values,
                            borderColor: config.color,
                            backgroundColor: config.color + '20',
                            borderWidth: 2,
                            fill: true,
                            tension: 0.4,
                            pointRadius: 4,
                            pointHoverRadius: 6,
                            pointBackgroundColor: config.color,
                            pointBorderColor: '#fff',
                            pointBorderWidth: 2,
                            spanGaps: true, // Connect across null values
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: {
                            legend: {
                                display: false
                            },
                            tooltip: {
                                mode: 'index',
                                intersect: false,
                            }
                        },
                        scales: {
                            y: {
                                beginAtZero: false,
                                grid: {
                                    color: 'rgba(0, 0, 0, 0.05)'
                                },
                                ticks: {
                                    font: {
                                        size: 11
                                    },
                                    callback: function(value) {
                                        if (value === null || value === undefined) {
                                            return '';
                                        }
                                        return value;
                                    }
                                }
                            },
                            x: {
                                grid: {
                                    display: false
                                },
                                ticks: {
                                    font: {
                                        size: 10
                                    }
                                }
                            }
                        },
                        elements: {
                            point: {
                                radius: function(context) {
                                    return context.parsed.y === null ? 0 : 4;
                                }
                            }
                        },
                        interaction: {
                            mode: 'nearest',
                            axis: 'x',
                            intersect: false
                        }
                    }
                });
            });
        });
        
        // Highlight current minute
        highlightMinute(0);
    }
    
    // Highlight a specific minute in all charts
    function highlightMinute(minute) {
        currentMinute = minute;
        
        // Update all charts to highlight the selected minute
        Object.keys(charts).forEach(function(chartId) {
            const chart = charts[chartId];
            if (!chart) return;
            
            // Reset all points
            chart.data.datasets.forEach(function(dataset) {
                if (minute === -1) {
                    // Reset all to normal
                    dataset.pointRadius = 4;
                    dataset.pointBackgroundColor = dataset.borderColor;
                } else {
                    // Highlight selected minute
                    dataset.pointRadius = dataset.data.map(function(val, idx) {
                        return idx === minute ? 8 : 4;
                    });
                    dataset.pointBackgroundColor = dataset.data.map(function(val, idx) {
                        return idx === minute ? '#fff' : dataset.borderColor;
                    });
                    dataset.pointBorderWidth = dataset.data.map(function(val, idx) {
                        return idx === minute ? 3 : 2;
                    });
                }
            });
            
            chart.update('none'); // Update without animation
        });
    }
    
    function showMinute(minute) {
        // Remove active from all buttons
        document.querySelectorAll('.minute-btn').forEach(function(btn) {
            btn.classList.remove('active');
        });
        
        // Activate button
        if (event && event.target) {
            event.target.classList.add('active');
        }
        
        // Highlight minute in charts
        highlightMinute(minute);
    }
    
    function showAllMinutes() {
        // Remove active from minute buttons
        document.querySelectorAll('.minute-btn').forEach(function(btn) {
            btn.classList.remove('active');
        });
        
        // Activate "Show All" button
        if (event && event.target) {
            event.target.classList.add('active');
        }
        
        // Reset chart highlighting
        highlightMinute(-1);
    }
    
    // Initialize charts when page loads
    document.addEventListener('DOMContentLoaded', function() {
        initCharts();
        initScatterChart();
        initScatterFilters();
    });
    
    // Scatter chart instance
    let scatterChart = null;
    let activeCategory = 'all';
    
    // Initialize the scatter chart with price line and filter points
    function initScatterChart() {
        const canvas = document.getElementById('scatterPriceChart');
        if (!canvas || !trailData || trailData.length === 0) return;
        if (typeof scatterData === 'undefined') return;
        
        const ctx = canvas.getContext('2d');
        
        // Sort trail data by minute
        const sortedData = trailData.slice().sort((a, b) => (a.minute || 0) - (b.minute || 0));
        const minutes = sortedData.map(row => row.minute);
        
        // Extract price data for the line chart (use close price)
        const priceData = sortedData.map(row => {
            const val = row['pm_close_price'];
            return val !== null && val !== undefined ? parseFloat(val) : null;
        });
        
        // Normalize price data to 0-100 scale for overlay
        const validPrices = priceData.filter(p => p !== null);
        const priceMin = Math.min(...validPrices);
        const priceMax = Math.max(...validPrices);
        const priceRange = priceMax - priceMin;
        
        const normalizedPriceData = priceData.map(p => {
            if (p === null) return null;
            return priceRange === 0 ? 50 : ((p - priceMin) / priceRange) * 100;
        });
        
        // Build scatter datasets from normalized data
        const scatterDatasets = [];
        
        // Category key mapping
        const categoryKeyMap = {
            'Price Movements': 'price_movements',
            'Order Book': 'order_book',
            'Transactions': 'transactions',
            'Whale Activity': 'whale_activity',
            'Patterns': 'patterns',
            'Second Prices': 'second_prices',
            'BTC Prices': 'btc_prices',
            'ETH Prices': 'eth_prices',
        };
        
        Object.keys(scatterData).forEach(function(categoryName) {
            const categoryKey = categoryKeyMap[categoryName] || categoryName.toLowerCase().replace(/ /g, '_');
            const color = categoryColors[categoryName] || '#888888';
            const fields = scatterData[categoryName];
            
            Object.keys(fields).forEach(function(fieldKey) {
                const fieldData = fields[fieldKey];
                const points = [];
                
                for (let m = 0; m < 15; m++) {
                    const minuteData = fieldData.minutes[m];
                    if (minuteData && minuteData.pct !== null) {
                        points.push({
                            x: m,
                            y: minuteData.pct,
                            raw: minuteData.raw,
                            field: fieldData.label,
                            min: fieldData.min,
                            max: fieldData.max
                        });
                    }
                }
                
                if (points.length > 0) {
                    scatterDatasets.push({
                        type: 'scatter',
                        label: fieldData.label,
                        data: points,
                        backgroundColor: color + 'cc',
                        borderColor: color,
                        borderWidth: 1,
                        pointRadius: 5,
                        pointHoverRadius: 8,
                        category: categoryKey,
                        hidden: false
                    });
                }
            });
        });
        
        // Create the chart
        scatterChart = new Chart(ctx, {
            type: 'scatter',
            data: {
                labels: minutes.map(m => 'Min ' + m),
                datasets: [
                    // Price line (as background reference)
                    {
                        type: 'line',
                        label: 'Price (Normalized)',
                        data: normalizedPriceData,
                        borderColor: '#22c55e',
                        backgroundColor: 'rgba(34, 197, 94, 0.1)',
                        borderWidth: 3,
                        fill: true,
                        tension: 0.4,
                        pointRadius: 0,
                        pointHoverRadius: 6,
                        order: 1, // Draw behind scatter points
                        yAxisID: 'y',
                        category: 'price'
                    },
                    ...scatterDatasets
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    mode: 'nearest',
                    intersect: true
                },
                plugins: {
                    legend: {
                        display: false // Using custom legend
                    },
                    tooltip: {
                        callbacks: {
                            title: function(context) {
                                const item = context[0];
                                if (item.dataset.type === 'line') {
                                    return 'Minute ' + item.dataIndex;
                                }
                                return 'Minute ' + item.raw.x;
                            },
                            label: function(context) {
                                if (context.dataset.type === 'line') {
                                    const originalPrice = priceData[context.dataIndex];
                                    return 'Price: $' + (originalPrice ? originalPrice.toFixed(6) : '-');
                                }
                                const point = context.raw;
                                return [
                                    point.field,
                                    'Value: ' + (point.raw !== null ? point.raw.toFixed(4) : '-'),
                                    'Normalized: ' + point.y.toFixed(1) + '%',
                                    'Range: ' + point.min.toFixed(4) + ' - ' + point.max.toFixed(4)
                                ];
                            }
                        },
                        backgroundColor: 'rgba(0, 0, 0, 0.9)',
                        titleFont: { size: 12, weight: 'bold' },
                        bodyFont: { size: 11 },
                        padding: 10,
                        cornerRadius: 6
                    }
                },
                scales: {
                    x: {
                        type: 'linear',
                        min: -0.5,
                        max: 14.5,
                        ticks: {
                            stepSize: 1,
                            callback: function(value) {
                                return 'Min ' + value;
                            }
                        },
                        grid: {
                            color: 'rgba(0, 0, 0, 0.05)'
                        },
                        title: {
                            display: true,
                            text: 'Minutes',
                            font: { size: 11 }
                        }
                    },
                    y: {
                        min: 0,
                        max: 100,
                        ticks: {
                            callback: function(value) {
                                return value + '%';
                            }
                        },
                        grid: {
                            color: 'rgba(0, 0, 0, 0.05)'
                        },
                        title: {
                            display: true,
                            text: 'Normalized Value (0-100%)',
                            font: { size: 11 }
                        }
                    }
                }
            }
        });
    }
    
    // Filter controls for scatter chart
    function initScatterFilters() {
        const filterBtns = document.querySelectorAll('.scatter-filter-btn');
        const legendItems = document.querySelectorAll('.scatter-legend-item');
        
        filterBtns.forEach(function(btn) {
            btn.addEventListener('click', function() {
                // Update active button
                filterBtns.forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                
                const category = btn.getAttribute('data-category');
                activeCategory = category;
                
                // Update chart visibility
                if (scatterChart) {
                    scatterChart.data.datasets.forEach(function(dataset) {
                        if (dataset.category === 'price') {
                            dataset.hidden = false; // Always show price line
                        } else if (category === 'all') {
                            dataset.hidden = false;
                        } else {
                            dataset.hidden = dataset.category !== category;
                        }
                    });
                    scatterChart.update();
                }
                
                // Update legend visibility
                legendItems.forEach(function(item) {
                    const itemCategory = item.getAttribute('data-category');
                    const itemType = item.getAttribute('data-type');
                    
                    if (itemType === 'price' || category === 'all' || itemCategory === category) {
                        item.classList.remove('hidden');
                    } else {
                        item.classList.add('hidden');
                    }
                });
            });
        });
        
        // Legend item click to toggle
        legendItems.forEach(function(item) {
            item.addEventListener('click', function() {
                const category = item.getAttribute('data-category');
                const itemType = item.getAttribute('data-type');
                
                if (scatterChart && category) {
                    // Toggle all datasets of this category
                    let allHidden = true;
                    scatterChart.data.datasets.forEach(function(dataset) {
                        if (dataset.category === category) {
                            if (!dataset.hidden) allHidden = false;
                        }
                    });
                    
                    scatterChart.data.datasets.forEach(function(dataset) {
                        if (dataset.category === category) {
                            dataset.hidden = !allHidden;
                        }
                    });
                    
                    item.classList.toggle('hidden', !allHidden);
                    scatterChart.update();
                } else if (scatterChart && itemType === 'price') {
                    // Toggle price line
                    const priceDataset = scatterChart.data.datasets.find(d => d.category === 'price');
                    if (priceDataset) {
                        priceDataset.hidden = !priceDataset.hidden;
                        item.classList.toggle('hidden', priceDataset.hidden);
                        scatterChart.update();
                    }
                }
            });
        });
    }
</script>

<?php
$content = ob_get_clean();
$scripts = '';

// Include the base layout
include __DIR__ . '/../../layouts/base.php';
?>

