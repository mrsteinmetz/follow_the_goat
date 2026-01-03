<?php
header('Content-Type: text/plain');

echo "=== Timezone Debug ===\n\n";
echo "PHP default timezone: " . date_default_timezone_get() . "\n";
echo "Current PHP time: " . date('Y-m-d H:i:s') . "\n";
echo "Current system time (shell): ";
system('date "+%Y-%m-%d %H:%M:%S %Z"');
echo "\n";

// Test with current time
require_once __DIR__ . '/includes/DuckDBClient.php';
$duckdb = new DuckDBClient('http://127.0.0.1:5051');

$end = date('Y-m-d H:i:s');
$start_10min = date('Y-m-d H:i:s', strtotime('-10 minutes'));
$start_1hour = date('Y-m-d H:i:s', strtotime('-1 hour'));

echo "\nTest 1: Last 10 minutes\n";
echo "  Start: $start_10min\n";
echo "  End: $end\n";
$result = $duckdb->getPricePoints('SOL', $start_10min, $end);
echo "  Result: " . ($result['count'] ?? 0) . " prices\n";

echo "\nTest 2: Last 1 hour\n";
echo "  Start: $start_1hour\n";
echo "  End: $end\n";
$result = $duckdb->getPricePoints('SOL', $start_1hour, $end);
echo "  Result: " . ($result['count'] ?? 0) . " prices\n";

// Test with UTC
echo "\nTest 3: Last 10 minutes (UTC)\n";
$end_utc = gmdate('Y-m-d H:i:s');
$start_10min_utc = gmdate('Y-m-d H:i:s', strtotime('-10 minutes'));
echo "  Start: $start_10min_utc\n";
echo "  End: $end_utc\n";
$result = $duckdb->getPricePoints('SOL', $start_10min_utc, $end_utc);
echo "  Result: " . ($result['count'] ?? 0) . " prices\n";

