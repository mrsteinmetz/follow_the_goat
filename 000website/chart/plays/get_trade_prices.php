<?php
/**
 * Get Trade Prices API Endpoint
 * Fetches price data for trade chart visualization
 * 
 * Query Parameters:
 *   - start: Unix timestamp (seconds) for start time
 *   - end: Unix timestamp (seconds) for end time
 *   - token: Token symbol (optional, defaults to SOL)
 */

header('Content-Type: application/json');

// Load DuckDB Client
require_once __DIR__ . '/../../includes/DuckDBClient.php';

$client = new DuckDBClient();

// Validate API availability
if (!$client->isAvailable()) {
    http_response_code(503);
    echo json_encode([
        'success' => false,
        'error' => 'DuckDB API server is not available. Please ensure master.py is running.',
        'prices' => []
    ]);
    exit;
}

// Get query parameters
$start_sec = isset($_GET['start']) ? (int)$_GET['start'] : null;
$end_sec = isset($_GET['end']) ? (int)$_GET['end'] : null;
$token = isset($_GET['token']) ? strtoupper($_GET['token']) : 'SOL';

// Validate parameters
if (!$start_sec || !$end_sec) {
    http_response_code(400);
    echo json_encode([
        'success' => false,
        'error' => 'Missing required parameters: start and end timestamps',
        'prices' => []
    ]);
    exit;
}

// Convert Unix timestamps to datetime strings
$start_datetime = date('Y-m-d H:i:s', $start_sec);
$end_datetime = date('Y-m-d H:i:s', $end_sec);

try {
    // Fetch price points from DuckDB API
    $result = $client->getPricePoints($token, $start_datetime, $end_datetime);
    
    if ($result === null) {
        http_response_code(500);
        echo json_encode([
            'success' => false,
            'error' => 'Failed to fetch price data from API',
            'prices' => []
        ]);
        exit;
    }
    
    $prices = $result['prices'] ?? [];
    
    // Return success response
    echo json_encode([
        'success' => true,
        'prices' => $prices,
        'count' => count($prices),
        'debug' => [
            'token' => $token,
            'start_datetime' => $start_datetime,
            'end_datetime' => $end_datetime,
            'start_timestamp' => $start_sec,
            'end_timestamp' => $end_sec
        ]
    ]);
    
} catch (Exception $e) {
    http_response_code(500);
    echo json_encode([
        'success' => false,
        'error' => $e->getMessage(),
        'prices' => []
    ]);
}

