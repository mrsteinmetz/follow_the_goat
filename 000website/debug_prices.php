<?php
// Debug page to check what data PHP is getting
require_once __DIR__ . '/includes/DuckDBClient.php';
define('DUCKDB_API_URL', 'http://127.0.0.1:5051');
$duckdb = new DuckDBClient(DUCKDB_API_URL);

header('Content-Type: text/plain');

echo "=== DuckDB API Debug ===\n\n";

// Check if API is available
$use_duckdb = $duckdb->isAvailable();
echo "API Available: " . ($use_duckdb ? "YES" : "NO") . "\n\n";

if (!$use_duckdb) {
    $health = $duckdb->healthCheck();
    echo "Health Check Response:\n";
    print_r($health);
    die();
}

// Get price data
$token = 'SOL';
$end_datetime = date('Y-m-d H:i:s');
$start_datetime = date('Y-m-d H:i:s', strtotime('-24 hours'));

echo "Fetching prices for:\n";
echo "  Token: $token\n";
echo "  Start: $start_datetime\n";
echo "  End: $end_datetime\n\n";

$price_response = $duckdb->getPricePoints($token, $start_datetime, $end_datetime);

echo "Response:\n";
if ($price_response) {
    echo "  Status: SUCCESS\n";
    echo "  Count: " . ($price_response['count'] ?? 'N/A') . "\n";
    echo "  Prices array size: " . count($price_response['prices'] ?? []) . "\n\n";
    
    if (!empty($price_response['prices'])) {
        echo "First 3 prices:\n";
        foreach (array_slice($price_response['prices'], 0, 3) as $i => $price) {
            echo "  [$i] x={$price['x']}, y={$price['y']}\n";
        }
    } else {
        echo "  Prices array is EMPTY!\n";
    }
} else {
    echo "  Status: FAILED (null response)\n";
}

