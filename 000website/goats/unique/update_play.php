<?php
/**
 * Update Play API - Handles POST requests to update play settings
 * Ported from chart/plays/unique/update_play.php
 */

// --- Load Configuration from .env ---
require_once __DIR__ . '/../../../chart/config.php';
require_once __DIR__ . '/../../../chart/plays/pattern_validator_defaults.php';

// Only accept POST requests
if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    header('Location: index.php?error=' . urlencode('Invalid request method'));
    exit;
}

// Get form data
$play_id = (int)($_POST['play_id'] ?? 0);
$name = trim($_POST['name'] ?? '');
$description = trim($_POST['description'] ?? '');
$find_wallets_sql = trim($_POST['find_wallets_sql'] ?? '');
$max_buys_per_cycle = (int)($_POST['max_buys_per_cycle'] ?? 5);
$short_play = isset($_POST['short_play']) ? 1 : 0;
$trigger_on_perp_mode = trim($_POST['trigger_on_perp'] ?? 'any');

// Get timing conditions data
$timing_enabled = isset($_POST['timing_enabled']) ? true : false;
$timing_price_direction = trim($_POST['timing_price_direction'] ?? 'decrease');
$timing_time_window = (int)($_POST['timing_time_window'] ?? 60);
$timing_price_threshold = (float)($_POST['timing_price_threshold'] ?? 0.005);

// Get bundle trades data
$bundle_enabled = isset($_POST['bundle_enabled']) ? true : false;
$bundle_num_trades = !empty($_POST['bundle_num_trades']) ? (int)$_POST['bundle_num_trades'] : null;
$bundle_seconds = !empty($_POST['bundle_seconds']) ? (int)$_POST['bundle_seconds'] : null;

// Get cache wallets data
$cashe_enabled = isset($_POST['cashe_enabled']) ? true : false;
$cashe_seconds = !empty($_POST['cashe_seconds']) ? (int)$_POST['cashe_seconds'] : null;

// Get pattern validator schema JSON
$pattern_validator_input = trim($_POST['pattern_validator'] ?? '');
$pattern_validator_enable = isset($_POST['pattern_validator_enable']) ? 1 : 0;
$pattern_update_by_ai = isset($_POST['pattern_update_by_ai']) ? 1 : 0;

// If AI update is enabled, force pattern_validator_enable to be 1
if ($pattern_update_by_ai === 1) {
    $pattern_validator_enable = 1;
}

if ($pattern_validator_input === '') {
    $pattern_validator_schema = getDefaultPatternValidatorSchema();
} else {
    $pattern_validator_schema = json_decode($pattern_validator_input, true);
    if (json_last_error() !== JSON_ERROR_NONE || !is_array($pattern_validator_schema)) {
        header('Location: index.php?id=' . $play_id . '&error=' . urlencode('Pattern validator schema must be valid JSON. Error: ' . json_last_error_msg()));
        exit;
    }
}

// Get project_ids array (pattern config projects - multiple selection)
$project_ids_input = $_POST['project_ids'] ?? [];
$project_ids = [];
if (is_array($project_ids_input)) {
    foreach ($project_ids_input as $pid) {
        if (!empty($pid) && is_numeric($pid)) {
            $project_ids[] = (int)$pid;
        }
    }
}
$project_ids_json = !empty($project_ids) ? json_encode($project_ids) : null;

// Get tolerance rules arrays
$decrease_range_from = $_POST['decrease_range_from'] ?? [];
$decrease_range_to = $_POST['decrease_range_to'] ?? [];
$decrease_tolerance = $_POST['decrease_tolerance'] ?? [];

$increase_range_from = $_POST['increase_range_from'] ?? [];
$increase_range_to = $_POST['increase_range_to'] ?? [];
$increase_tolerance = $_POST['increase_tolerance'] ?? [];

// Validate play ID
if ($play_id <= 0) {
    header('Location: index.php?id=' . $play_id . '&error=' . urlencode('Invalid play ID'));
    exit;
}

// Validate required fields
if (empty($name) || empty($description) || empty($find_wallets_sql)) {
    header('Location: index.php?id=' . $play_id . '&error=' . urlencode('All fields are required'));
    exit;
}

// Validate name and description length
if (strlen($name) > 60) {
    header('Location: index.php?id=' . $play_id . '&error=' . urlencode('Name must be 60 characters or less'));
    exit;
}

if (strlen($description) > 500) {
    header('Location: index.php?id=' . $play_id . '&error=' . urlencode('Description must be 500 characters or less'));
    exit;
}

// Build tolerance rules JSON structure
$decreases = [];
for ($i = 0; $i < count($decrease_range_from); $i++) {
    $range_from = $decrease_range_from[$i] === '' ? null : (float)$decrease_range_from[$i];
    $range_to = $decrease_range_to[$i] === '' ? null : (float)$decrease_range_to[$i];
    $tolerance = (float)$decrease_tolerance[$i];
    
    // Validate: decreases must be 0 or negative numbers only
    if ($range_from !== null && $range_from > 0) {
        header('Location: index.php?id=' . $play_id . '&error=' . urlencode('Decrease range values must be 0 or negative. Found positive value: ' . $range_from));
        exit;
    }
    if ($range_to !== null && $range_to > 0) {
        header('Location: index.php?id=' . $play_id . '&error=' . urlencode('Decrease range values must be 0 or negative. Found positive value: ' . $range_to));
        exit;
    }
    
    // Ensure range is in ascending order (swap if needed)
    if ($range_from !== null && $range_to !== null && $range_from > $range_to) {
        $temp = $range_from;
        $range_from = $range_to;
        $range_to = $temp;
    }
    
    $decreases[] = [
        'range' => [$range_from, $range_to],
        'tolerance' => $tolerance
    ];
}

$increases = [];
for ($i = 0; $i < count($increase_range_from); $i++) {
    $range_from = $increase_range_from[$i] === '' ? null : (float)$increase_range_from[$i];
    $range_to = $increase_range_to[$i] === '' ? null : (float)$increase_range_to[$i];
    $tolerance = (float)$increase_tolerance[$i];
    
    // Validate: increases must be 0 or positive numbers only
    if ($range_from !== null && $range_from < 0) {
        header('Location: index.php?id=' . $play_id . '&error=' . urlencode('Increase range values must be 0 or positive. Found negative value: ' . $range_from));
        exit;
    }
    if ($range_to !== null && $range_to < 0) {
        header('Location: index.php?id=' . $play_id . '&error=' . urlencode('Increase range values must be 0 or positive. Found negative value: ' . $range_to));
        exit;
    }
    
    // Ensure range is in ascending order (swap if needed)
    if ($range_from !== null && $range_to !== null && $range_from > $range_to) {
        $temp = $range_from;
        $range_from = $range_to;
        $range_to = $temp;
    }
    
    $increases[] = [
        'range' => [$range_from, $range_to],
        'tolerance' => $tolerance
    ];
}

$sell_logic = [
    'tolerance_rules' => [
        'decreases' => $decreases,
        'increases' => $increases
    ]
];

// Build find_wallets_sql JSON structure
$find_wallets_sql_json = [
    'query' => $find_wallets_sql
];

// Build trigger_on_perp JSON structure
$trigger_on_perp_json = [
    'mode' => $trigger_on_perp_mode
];

// Build timing_conditions JSON structure
$timing_conditions_json = [
    'enabled' => $timing_enabled,
    'price_direction' => $timing_price_direction,
    'time_window_seconds' => $timing_time_window,
    'price_change_threshold' => $timing_price_threshold
];

// Build bundle_trades JSON structure
$bundle_trades_json = [
    'enabled' => $bundle_enabled,
    'num_trades' => $bundle_num_trades,
    'seconds' => $bundle_seconds
];

// Build cashe_wallets JSON structure
$cashe_wallets_json = [
    'enabled' => $cashe_enabled,
    'seconds' => $cashe_seconds
];

// Database connection
$dsn = "mysql:host=$db_host;dbname=$db_name;charset=$db_charset";
$options = [
    PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
    PDO::ATTR_EMULATE_PREPARES   => false,
];

try {
    $pdo = new PDO($dsn, $db_user, $db_pass, $options);
    
    // Validate the SQL query by wrapping it in a subquery
    try {
        $validation_query = trim($find_wallets_sql);
        $validation_query = rtrim($validation_query, ';');
        
        $wrapped_query = "SELECT * FROM (" . $validation_query . ") AS validation_subquery LIMIT 0";
        
        $validation_stmt = $pdo->prepare($wrapped_query);
        $validation_stmt->execute();
        
    } catch (\PDOException $e) {
        $error_message = 'SQL query validation failed: ' . $e->getMessage();
        header('Location: index.php?id=' . $play_id . '&error=' . urlencode($error_message));
        exit;
    }
    
    // Update the play
    $stmt = $pdo->prepare("
        UPDATE solcatcher.follow_the_goat_plays 
        SET name = :name,
            description = :description,
            find_wallets_sql = :find_wallets_sql,
            sell_logic = :sell_logic,
            max_buys_per_cycle = :max_buys_per_cycle,
            short_play = :short_play,
            tricker_on_perp = :tricker_on_perp,
            timing_conditions = :timing_conditions,
            bundle_trades = :bundle_trades,
            cashe_wallets = :cashe_wallets,
            pattern_validator_enable = :pattern_validator_enable,
            pattern_update_by_ai = :pattern_update_by_ai,
            pattern_validator = :pattern_validator,
            project_ids = :project_ids
        WHERE id = :id
    ");
    
    $stmt->execute([
        'id' => $play_id,
        'name' => $name,
        'description' => $description,
        'find_wallets_sql' => json_encode($find_wallets_sql_json),
        'sell_logic' => json_encode($sell_logic),
        'max_buys_per_cycle' => $max_buys_per_cycle,
        'short_play' => $short_play,
        'tricker_on_perp' => json_encode($trigger_on_perp_json),
        'timing_conditions' => json_encode($timing_conditions_json),
        'bundle_trades' => json_encode($bundle_trades_json),
        'cashe_wallets' => json_encode($cashe_wallets_json),
        'pattern_validator_enable' => $pattern_validator_enable,
        'pattern_update_by_ai' => $pattern_update_by_ai,
        'pattern_validator' => json_encode(
            $pattern_validator_schema,
            JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRESERVE_ZERO_FRACTION
        ),
        'project_ids' => $project_ids_json
    ]);
    
    // Success - redirect back to unique play page
    header('Location: index.php?id=' . $play_id . '&success=1');
    exit;
    
} catch (\PDOException $e) {
    $error_message = 'Database error: ' . $e->getMessage();
    header('Location: index.php?id=' . $play_id . '&error=' . urlencode($error_message));
    exit;
}

