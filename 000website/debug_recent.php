<?php
// Debug page with recent time range
require_once __DIR__ . '/includes/DuckDBClient.php';
define('DUCKDB_API_URL', 'http://127.0.0.1:5051');
$duckdb = new DuckDBClient(DUCKDB_API_URL);

header('Content-Type: text/plain');

echo "=== Testing with LAST HOUR ===\n\n";

$token = 'SOL';
$end_datetime = date('Y-m-d H:i:s');
$start_datetime = date('Y-m-d H:i:s', strtotime('-1 hour'));

echo "Fetching prices for:\n";
echo "  Token: $token\n";
echo "  Start: $start_datetime\n";
echo "  End: $end_datetime\n\n";

$price_response = $duckdb->getPricePoints($token, $start_datetime, $end_datetime);

if ($price_response) {
    echo "  Status: SUCCESS\n";
    echo "  Count: " . ($price_response['count'] ?? 'N/A') . "\n";
    echo "  Prices array size: " . count($price_response['prices'] ?? []) . "\n\n";
    
    if (!empty($price_response['prices'])) {
        echo "✅ DATA FOUND!\n\n";
        echo "First 5 prices:\n";
        foreach (array_slice($price_response['prices'], 0, 5) as $i => $price) {
            echo "  [$i] x={$price['x']}, y={$price['y']}\n";
        }
        echo "\nLast 5 prices:\n";
        foreach (array_slice($price_response['prices'], -5) as $i => $price) {
            echo "  [$i] x={$price['x']}, y={$price['y']}\n";
        }
    } else {
        echo "❌ No prices found\n";
    }
}

