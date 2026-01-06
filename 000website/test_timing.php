<?php
// Test with actual PHP timing
date_default_timezone_set('UTC');
require_once __DIR__ . '/includes/DuckDBClient.php';

$duckdb = new DuckDBClient('http://127.0.0.1:5051');
$token = 'SOL';
$end_datetime = gmdate('Y-m-d H:i:s');
$start_datetime = gmdate('Y-m-d H:i:s', strtotime('-24 hours'));
$fallback_start_datetime = gmdate('Y-m-d H:i:s', strtotime('-2 hours'));

echo "24h start: $start_datetime\n";
echo "fallback start: $fallback_start_datetime\n";
echo "end: $end_datetime\n\n";

echo "=== First call (24h) ===\n";
$price_response = $duckdb->getPricePoints($token, $start_datetime, $end_datetime);
echo "Response status: " . ($price_response['status'] ?? 'no status') . "\n";
echo "Prices isset: " . (isset($price_response['prices']) ? 'yes' : 'no') . "\n";
echo "Prices count: " . (isset($price_response['prices']) ? count($price_response['prices']) : 'N/A') . "\n";
echo "Prices empty: " . (empty($price_response['prices']) ? 'YES' : 'NO') . "\n\n";

$chart_data = ['prices' => []];
if ($price_response && isset($price_response['prices'])) {
    $chart_data['prices'] = $price_response['prices'];
}

echo "After first call, chart_data prices count: " . count($chart_data['prices']) . "\n";
echo "chart_data prices empty: " . (empty($chart_data['prices']) ? 'YES' : 'NO') . "\n\n";

if (empty($chart_data['prices'])) {
    echo "=== Fallback call (2h) ===\n";
    $price_response = $duckdb->getPricePoints($token, $fallback_start_datetime, $end_datetime);
    echo "Response status: " . ($price_response['status'] ?? 'no status') . "\n";
    echo "Prices isset: " . (isset($price_response['prices']) ? 'yes' : 'no') . "\n";
    echo "Prices count: " . (isset($price_response['prices']) ? count($price_response['prices']) : 'N/A') . "\n";
    echo "Prices empty: " . (empty($price_response['prices']) ? 'YES' : 'NO') . "\n";
    
    if ($price_response && isset($price_response['prices'])) {
        $chart_data['prices'] = $price_response['prices'];
        echo "\nFallback successful! Final count: " . count($chart_data['prices']) . "\n";
    }
}

echo "\n=== Final result ===\n";
echo "chart_data prices count: " . count($chart_data['prices']) . "\n";

