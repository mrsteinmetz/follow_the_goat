<?php
/**
 * Save Trade API - Handles POST requests to update trade details
 * Migrated to use DatabaseClient API
 */

// --- Load Database Client ---
require_once __DIR__ . '/../../../includes/DatabaseClient.php';

// Set response header
header('Content-Type: application/json');

// Only allow POST requests
if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    echo json_encode(['success' => false, 'error' => 'Invalid request method']);
    exit;
}

// Get JSON input
$input = file_get_contents('php://input');
$data = json_decode($input, true);

if (!$data || !isset($data['trade_id']) || !isset($data['updates'])) {
    echo json_encode(['success' => false, 'error' => 'Missing required parameters']);
    exit;
}

$trade_id = (int)$data['trade_id'];
$updates = $data['updates'];

if ($trade_id <= 0) {
    echo json_encode(['success' => false, 'error' => 'Invalid trade ID']);
    exit;
}

if (empty($updates)) {
    echo json_encode(['success' => false, 'error' => 'No updates provided']);
    exit;
}

// Define allowed fields for security
$allowed_fields = [
    'our_entry_price',
    'our_exit_price',
    'our_exit_timestamp',
    'our_profit_loss',
    'our_status',
    'tolerance'
];

// Filter to only allowed fields
$valid_updates = [];
foreach ($updates as $field => $value) {
    if (in_array($field, $allowed_fields)) {
        $valid_updates[$field] = $value;
    }
}

if (empty($valid_updates)) {
    echo json_encode(['success' => false, 'error' => 'No valid fields to update']);
    exit;
}

// Initialize API client
$client = new DatabaseClient();

if (!$client->isAvailable()) {
    echo json_encode(['success' => false, 'error' => 'API server is not available']);
    exit;
}

// Update via API (DuckDB)
$result = $client->updateBuyin($trade_id, $valid_updates);

if ($result && ($result['success'] ?? false)) {
    echo json_encode([
        'success' => true,
        'updated_fields' => count($valid_updates),
        'duckdb' => $result['duckdb'] ?? false
    ]);
} else {
    echo json_encode([
        'success' => false,
        'error' => $result['error'] ?? 'Failed to update trade'
    ]);
}
