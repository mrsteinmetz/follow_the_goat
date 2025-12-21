<?php
/**
 * Save Trade API - Handles POST requests to update trade details
 * Ported from chart/plays/unique/trade/save_trade.php
 */

// --- Load Configuration from .env ---
require_once __DIR__ . '/../../../../chart/config.php';

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

// Build update query
$set_clauses = [];
$params = ['id' => $trade_id];

foreach ($updates as $field => $value) {
    if (!in_array($field, $allowed_fields)) {
        continue; // Skip invalid fields
    }
    
    $set_clauses[] = "$field = :$field";
    $params[$field] = $value;
}

if (empty($set_clauses)) {
    echo json_encode(['success' => false, 'error' => 'No valid fields to update']);
    exit;
}

// Connect to database
$dsn = "mysql:host=$db_host;dbname=$db_name;charset=$db_charset";
$options = [
    PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
    PDO::ATTR_EMULATE_PREPARES   => false,
];

try {
    $pdo = new PDO($dsn, $db_user, $db_pass, $options);
    $tables = [
        'solcatcher.follow_the_goat_buyins_archive',
        'solcatcher.follow_the_goat_buyins'
    ];
    $updated_table = null;
    
    foreach ($tables as $table) {
        $sql = sprintf(
            "UPDATE %s SET %s WHERE id = :id",
            $table,
            implode(', ', $set_clauses)
        );
        $stmt = $pdo->prepare($sql);
        $stmt->execute($params);
        
        if ($stmt->rowCount() > 0) {
            $updated_table = $table;
            break;
        }
    }
    
    if ($updated_table === null) {
        echo json_encode(['success' => false, 'error' => 'Trade not found in archive or live tables']);
        exit;
    }
    
    echo json_encode([
        'success' => true,
        'updated_fields' => count($set_clauses),
        'table' => $updated_table
    ]);
    
} catch (\PDOException $e) {
    error_log("Error updating trade: " . $e->getMessage());
    echo json_encode(['success' => false, 'error' => 'Database error: ' . $e->getMessage()]);
}

