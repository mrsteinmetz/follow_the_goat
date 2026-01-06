<?php
// Replicate the exact code from index.php
date_default_timezone_set('UTC');

require_once __DIR__ . '/includes/DuckDBClient.php';
define('DUCKDB_API_URL', 'http://127.0.0.1:5051');
$duckdb = new DuckDBClient(DUCKDB_API_URL);
$use_duckdb = $duckdb->isAvailable();

echo "use_duckdb: " . ($use_duckdb ? 'true' : 'false') . "\n";

$token = 'SOL';
$end_datetime = gmdate('Y-m-d H:i:s');
$start_datetime = gmdate('Y-m-d H:i:s', strtotime('-24 hours'));
$fallback_start_datetime = gmdate('Y-m-d H:i:s', strtotime('-2 hours'));

echo "start_datetime: $start_datetime\n";
echo "end_datetime: $end_datetime\n";
echo "fallback_start_datetime: $fallback_start_datetime\n\n";

$chart_data = [
    'labels' => [],
    'prices' => [],
    'candles' => [],
    'cycle_prices' => [],
    'coin_name' => 'SOL',
];

if ($use_duckdb) {
    echo "Calling getPricePoints...\n";
    $price_response = $duckdb->getPricePoints($token, $start_datetime, $end_datetime);
    
    echo "price_response:\n";
    print_r($price_response);
    
    if ($price_response && isset($price_response['prices'])) {
        $chart_data['prices'] = $price_response['prices'];
        echo "\nChart data prices count: " . count($chart_data['prices']) . "\n";
    }
    
    // If no data in 24h range (new deployment), try last hour
    if (empty($chart_data['prices'])) {
        echo "\nNo data in 24h range, trying fallback...\n";
        $price_response = $duckdb->getPricePoints($token, $fallback_start_datetime, $end_datetime);
        if ($price_response && isset($price_response['prices'])) {
            $chart_data['prices'] = $price_response['prices'];
            $start_datetime = $fallback_start_datetime;
            echo "Fallback successful! Count: " . count($chart_data['prices']) . "\n";
        }
    }
} else {
    echo "DuckDB API is not available\n";
}

echo "\nFinal chart_data prices count: " . count($chart_data['prices']) . "\n";

