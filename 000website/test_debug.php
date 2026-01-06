<?php
// Quick debug script
date_default_timezone_set('UTC');
require_once __DIR__ . '/includes/DuckDBClient.php';

$client = new DuckDBClient('http://127.0.0.1:5051');
$start = gmdate('Y-m-d H:i:s', strtotime('-24 hours'));
$end = gmdate('Y-m-d H:i:s');

echo "=== DEBUG: Testing Price Data ===\n\n";
echo "Start: $start\n";
echo "End: $end\n\n";

echo "API Health Check:\n";
$health = $client->healthCheck();
echo json_encode($health, JSON_PRETTY_PRINT) . "\n\n";

echo "Getting Price Points:\n";
$result = $client->getPricePoints('SOL', $start, $end, 100);
if ($result) {
    echo "Status: " . ($result['status'] ?? 'unknown') . "\n";
    echo "Count: " . ($result['count'] ?? 0) . "\n";
    echo "Total Available: " . ($result['total_available'] ?? 0) . "\n";
    if (isset($result['prices']) && count($result['prices']) > 0) {
        echo "First price: " . json_encode($result['prices'][0]) . "\n";
        echo "Last price: " . json_encode($result['prices'][count($result['prices'])-1]) . "\n";
    }
} else {
    echo "ERROR: No result returned\n";
}

