<?php
/**
 * Pattern Config Builder - Project Analysis Page
 * Migrated from: 000old_code/solana_node/chart/build_pattern_config/project_analysis.php
 * 
 * Uses DuckDB API for data operations (dual-writes to MySQL + DuckDB)
 */

// --- Database API Client ---
require_once __DIR__ . '/../../../includes/DatabaseClient.php';
require_once __DIR__ . '/../../../includes/config.php';
$db = new DatabaseClient(DATABASE_API_URL);
$use_duckdb = $db->isAvailable();

// --- Base URL for template ---
$baseUrl = '';

// Available sections in the 15-minute trail data
const TRAIL_SECTIONS = [
    'price_movements' => 'Price Movements',
    'order_book_signals' => 'Order Book Signals',
    'transactions' => 'Transactions',
    'whale_activity' => 'Whale Activity',
    'patterns' => 'Patterns'
];

// Section prefix mapping for flattened table columns
const SECTION_PREFIXES = [
    'price_movements' => 'pm_',
    'order_book_signals' => 'ob_',
    'transactions' => 'tx_',
    'whale_activity' => 'wh_',
    'patterns' => 'pat_'
];

// Status filter options
const STATUS_OPTIONS = [
    'all' => 'All Statuses',
    'sold' => 'Sold',
    'no_go' => 'No Go'
];

// Time range options (in hours)
const DEFAULT_HOURS = 6;
const HOURS_OPTIONS = [
    3 => '3 Hours',
    6 => '6 Hours',
    12 => '12 Hours',
    24 => '24 Hours',
];

// Project ID is required
$project_id = isset($_GET['id']) ? (int) $_GET['id'] : 0;
if ($project_id <= 0) {
    header('Location: ./');
    exit;
}

// Get filter parameters from URL
$selected_section = $_GET['section'] ?? 'price_movements';
$selected_minute = isset($_GET['minute']) ? (int) $_GET['minute'] : 0;
$selected_status = $_GET['status'] ?? 'all';
$selected_hours = isset($_GET['hours']) ? (int) $_GET['hours'] : DEFAULT_HOURS;
$analyse_mode = $_GET['analyse_mode'] ?? 'all'; // 'all' or 'passed'

// Validate section
if (!array_key_exists($selected_section, TRAIL_SECTIONS)) {
    $selected_section = 'price_movements';
}

// Validate minute (0-14)
if ($selected_minute < 0 || $selected_minute > 14) {
    $selected_minute = 0;
}

// Validate status
if (!array_key_exists($selected_status, STATUS_OPTIONS)) {
    $selected_status = 'all';
}

// Validate hours
if (!array_key_exists($selected_hours, HOURS_OPTIONS)) {
    $selected_hours = DEFAULT_HOURS;
}

// Validate analyse_mode
if (!in_array($analyse_mode, ['all', 'passed'])) {
    $analyse_mode = 'all';
}

/**
 * Format a value for display in the table
 */
function format_cell_value($value): string {
    if ($value === null) {
        return '<span class="text-muted">-</span>';
    }
    
    if (is_bool($value)) {
        return $value ? 'Yes' : 'No';
    }
    
    if (is_numeric($value)) {
        $float_val = (float) $value;
        if (abs($float_val) < 0.0001 && $float_val != 0) {
            return sprintf('%.6f', $float_val);
        } elseif (abs($float_val) < 1) {
            return sprintf('%.4f', $float_val);
        } elseif (abs($float_val) < 1000) {
            return sprintf('%.2f', $float_val);
        } else {
            return number_format($float_val, 2);
        }
    }
    
    return htmlspecialchars((string) $value);
}

/**
 * Get CSS class for profit/loss coloring
 */
function get_profit_class($value): string {
    if ($value === null) {
        return '';
    }
    $float_val = (float) $value;
    if ($float_val > 0) {
        return 'text-success';
    } elseif ($float_val < 0) {
        return 'text-danger';
    }
    return '';
}

/**
 * Build current filter URL for form
 */
function build_filter_url(array $overrides = []): string {
    global $project_id, $selected_section, $selected_minute, $selected_status, $selected_hours, $analyse_mode;
    
    $params = [
        'id' => $overrides['id'] ?? $project_id,
        'section' => $overrides['section'] ?? $selected_section,
        'minute' => $overrides['minute'] ?? $selected_minute,
        'status' => $overrides['status'] ?? $selected_status,
        'hours' => $overrides['hours'] ?? $selected_hours,
        'analyse_mode' => $overrides['analyse_mode'] ?? $analyse_mode,
    ];
    
    return '?' . http_build_query($params);
}

$error_message = '';
$success_message = '';
$filter_message = '';
$project = null;
$all_filters = [];
$active_filters = [];
$section_fields = [];
$field_types = [];
$field_stats = [];
$gain_ranges = [];
$total_trades = 0;
$distribution = [];
$dist_totals = [];

// Check API availability
if (!$use_duckdb) {
    $error_message = "Website API is not available. Please start the API: python scheduler/website_api.py";
} else {
    // Load project info
    $project_response = $db->getPatternProject($project_id);
    if (!$project_response || !isset($project_response['project'])) {
        header('Location: ./');
        exit;
    }
    $project = $project_response['project'];
    $all_filters = $project_response['filters'] ?? [];
    
    // Extract active filters
    $active_filters = array_filter($all_filters, function($f) {
        return $f['is_active'] == 1;
    });
    
    // Handle POST actions for filter rules
    if ($_SERVER['REQUEST_METHOD'] === 'POST') {
        $action = $_POST['filter_action'] ?? '';
        
        if ($action === 'add_filter') {
            $filter_field = $_POST['filter_field'] ?? '';
            $filter_name = trim($_POST['filter_name'] ?? '') ?: $filter_field;
            $filter_from = $_POST['filter_from'] ?? null;
            $filter_to = $_POST['filter_to'] ?? null;
            $filter_bool_value = $_POST['filter_bool_value'] ?? null;
            $include_null = isset($_POST['include_null']) ? 1 : 0;
            $exclude_mode = isset($_POST['exclude_mode']) ? 1 : 0;
            
            // Check if this is a boolean field filter
            $is_boolean_filter = ($filter_bool_value !== null && $filter_bool_value !== '' && $filter_from === '' && $filter_to === '');
            
            if ($is_boolean_filter) {
                $bool_val = (int) $filter_bool_value;
                $filter_from = $bool_val;
                $filter_to = $bool_val;
                $include_null = 0;
            }
            
            if ($filter_field && ($filter_from !== '' || $filter_to !== '')) {
                $prefix = SECTION_PREFIXES[$selected_section] ?? '';
                $db_column = $prefix . $filter_field;
                
                $result = $db->createPatternFilter([
                    'project_id' => $project_id,
                    'name' => $filter_name,
                    'section' => $selected_section,
                    'minute' => $selected_minute,
                    'field_name' => $filter_field,
                    'field_column' => $db_column,
                    'from_value' => $filter_from !== '' ? (float) $filter_from : null,
                    'to_value' => $filter_to !== '' ? (float) $filter_to : null,
                    'include_null' => $include_null,
                    'exclude_mode' => $exclude_mode,
                    'is_active' => 1,
                ]);
                
                if ($result && isset($result['success']) && $result['success']) {
                    $filter_message = 'Filter rule added successfully.';
                }
            }
        } elseif ($action === 'delete_filter') {
            $filter_id = (int) ($_POST['filter_id'] ?? 0);
            if ($filter_id > 0) {
                $db->deletePatternFilter($filter_id);
                $filter_message = 'Filter rule deleted.';
            }
        } elseif ($action === 'toggle_filter') {
            $filter_id = (int) ($_POST['filter_id'] ?? 0);
            if ($filter_id > 0) {
                // Find the filter and toggle it
                foreach ($all_filters as $filter) {
                    if ($filter['id'] == $filter_id) {
                        $db->updatePatternFilter($filter_id, [
                            'is_active' => $filter['is_active'] ? 0 : 1
                        ]);
                        $filter_message = 'Filter rule toggled.';
                        break;
                    }
                }
            }
        }
        
        // Redirect to prevent form resubmission
        header('Location: ' . build_filter_url());
        exit;
    }
    
    // Get section fields and types from API
    $section_response = $db->getTrailSections($selected_section);
    if ($section_response && isset($section_response['fields'])) {
        $section_fields = $section_response['fields'];
        $field_types = $section_response['field_types'] ?? [];
    }
    
    // Get field statistics
    $stats_response = $db->getTrailFieldStats([
        'project_id' => $project_id,
        'section' => $selected_section,
        'minute' => $selected_minute,
        'status' => $selected_status,
        'hours' => $selected_hours,
        'analyse_mode' => $analyse_mode,
    ]);
    
    if ($stats_response && $stats_response['success']) {
        $field_stats = $stats_response['field_stats'] ?? [];
        $gain_ranges = $stats_response['gain_ranges'] ?? [];
        $total_trades = $stats_response['total_trades'] ?? 0;
    }
    
    // Get gain distribution
    $dist_response = $db->getTrailGainDistribution([
        'project_id' => $project_id,
        'minute' => $selected_minute,
        'status' => $selected_status,
        'hours' => $selected_hours,
        'apply_filters' => !empty($active_filters),
    ]);
    
    if ($dist_response && $dist_response['success']) {
        $distribution = $dist_response['distribution'] ?? [];
        $dist_totals = $dist_response['totals'] ?? [];
    }
}

// Refresh filters list after POST
if (!empty($filter_message)) {
    $project_response = $db->getPatternProject($project_id);
    if ($project_response && isset($project_response['project'])) {
        $all_filters = $project_response['filters'] ?? [];
        $active_filters = array_filter($all_filters, function($f) {
            return $f['is_active'] == 1;
        });
    }
}

$analyse_passed_only = ($analyse_mode === 'passed');

// --- Page Styles ---
ob_start();
?>
<style>
    /* Filter Bar */
    .filter-bar {
        display: flex;
        flex-wrap: wrap;
        gap: 1rem;
        align-items: flex-end;
        margin-bottom: 1.5rem;
        padding: 1.25rem;
        background-color: var(--custom-white);
        border-radius: 0.5rem;
        border: 1px solid var(--default-border);
    }
    
    .filter-group {
        display: flex;
        flex-direction: column;
        gap: 0.5rem;
    }
    
    .filter-group label {
        color: var(--text-muted);
        font-size: 0.75rem;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    
    .filter-group select {
        min-width: 160px;
    }
    
    /* Filter Rules Section */
    .filter-rules-section {
        margin-bottom: 1.5rem;
        padding: 1.25rem;
        background-color: var(--custom-white);
        border-radius: 0.5rem;
        border: 1px solid var(--default-border);
    }
    
    .filter-rules-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 1rem;
    }
    
    .filter-rules-header h5 {
        margin: 0;
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    
    .active-filters-count {
        background-color: rgb(var(--success-rgb));
        color: white;
        padding: 0.25rem 0.5rem;
        border-radius: 0.25rem;
        font-size: 0.75rem;
        font-weight: 600;
    }
    
    .add-filter-form {
        display: flex;
        flex-wrap: wrap;
        gap: 0.75rem;
        align-items: flex-end;
        padding: 1rem;
        background-color: var(--light);
        border-radius: 0.375rem;
        margin-bottom: 1rem;
    }
    
    .add-filter-form .filter-input-group {
        display: flex;
        flex-direction: column;
        gap: 0.25rem;
    }
    
    .add-filter-form label {
        color: var(--text-muted);
        font-size: 0.7rem;
        font-weight: 500;
        text-transform: uppercase;
    }
    
    .add-filter-form select,
    .add-filter-form input {
        font-size: 0.85rem;
        padding: 0.4rem 0.6rem;
    }
    
    .add-filter-form input[type="number"] {
        width: 100px;
    }
    
    .add-filter-form input[type="text"] {
        width: 120px;
    }
    
    .existing-filters {
        display: flex;
        flex-direction: column;
        gap: 0.5rem;
    }
    
    .filter-rule-item {
        display: flex;
        align-items: center;
        gap: 0.75rem;
        padding: 0.75rem;
        background-color: var(--light);
        border-radius: 0.375rem;
        border-left: 3px solid rgb(var(--primary-rgb));
    }
    
    .filter-rule-item.inactive {
        opacity: 0.5;
        border-left-color: var(--text-muted);
    }
    
    .filter-rule-info {
        flex: 1;
        display: flex;
        flex-wrap: wrap;
        gap: 0.5rem;
        align-items: center;
    }
    
    .filter-rule-field {
        font-weight: 600;
        color: var(--default-text-color);
    }
    
    .filter-rule-range {
        color: var(--text-muted);
        font-size: 0.85rem;
    }
    
    .filter-rule-actions {
        display: flex;
        gap: 0.25rem;
    }
    
    .filter-rule-actions button {
        background: none;
        border: none;
        padding: 0.25rem 0.5rem;
        cursor: pointer;
        font-size: 0.9rem;
    }
    
    .no-filters-msg {
        color: var(--text-muted);
        font-style: italic;
        padding: 0.5rem 0;
    }
    
    .filter-applied-notice {
        background-color: rgba(var(--primary-rgb), 0.1);
        border: 1px solid rgb(var(--primary-rgb));
        color: rgb(var(--primary-rgb));
        padding: 0.75rem 1rem;
        border-radius: 0.375rem;
        margin-bottom: 1rem;
        display: flex;
        align-items: center;
        gap: 0.5rem;
        font-size: 0.85rem;
    }
    
    /* Summary Table Styles */
    .summary-section {
        margin-bottom: 2rem;
    }
    
    .summary-table-container {
        overflow-x: auto;
        border-radius: 0.5rem;
        border: 1px solid var(--default-border);
        margin-bottom: 1rem;
        max-height: 500px;
    }
    
    .summary-table {
        width: 100%;
        border-collapse: collapse;
        background-color: var(--custom-white);
        font-size: 0.8rem;
    }
    
    .summary-table thead th {
        background-color: var(--light);
        color: var(--default-text-color);
        padding: 0.6rem 0.5rem;
        text-align: right;
        font-weight: 600;
        border-bottom: 2px solid var(--default-border);
        font-size: 0.7rem;
        letter-spacing: 0.03em;
        white-space: nowrap;
        position: sticky;
        top: 0;
        z-index: 10;
    }
    
    .summary-table thead th:first-child {
        text-align: left;
        position: sticky;
        left: 0;
        z-index: 20;
        min-width: 140px;
    }
    
    .summary-table tbody tr {
        border-bottom: 1px solid var(--default-border);
    }
    
    .summary-table tbody tr:hover {
        background-color: var(--light);
    }
    
    .summary-table tbody td {
        padding: 0.5rem 0.5rem;
        color: var(--default-text-color);
        text-align: right;
        white-space: nowrap;
    }
    
    .summary-table tbody td:first-child {
        text-align: left;
        font-weight: 600;
        position: sticky;
        left: 0;
        background-color: inherit;
        z-index: 5;
    }
    
    .summary-table tbody tr:hover td:first-child {
        background-color: var(--light);
    }
    
    .range-count {
        color: var(--text-muted);
        font-size: 0.7rem;
        font-weight: normal;
    }
    
    /* Stats Cards */
    .stats-cards {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
        gap: 1rem;
        margin-bottom: 1.5rem;
    }
    
    .stat-card {
        background: var(--custom-white);
        border: 1px solid var(--default-border);
        border-radius: 0.5rem;
        padding: 1rem;
        text-align: center;
    }
    
    .stat-card.positive {
        border-color: rgb(var(--success-rgb));
        background: rgba(var(--success-rgb), 0.05);
    }
    
    .stat-card.negative {
        border-color: rgb(var(--danger-rgb));
        background: rgba(var(--danger-rgb), 0.05);
    }
    
    .stat-card.removed {
        border-color: rgb(var(--warning-rgb));
        background: rgba(var(--warning-rgb), 0.05);
    }
    
    .stat-card .stat-label {
        color: var(--text-muted);
        font-size: 0.7rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 0.25rem;
    }
    
    .stat-card .stat-value {
        font-size: 1.5rem;
        font-weight: 700;
        color: var(--default-text-color);
    }
    
    .stat-card.positive .stat-value {
        color: rgb(var(--success-rgb));
    }
    
    .stat-card.negative .stat-value {
        color: rgb(var(--danger-rgb));
    }
    
    .stat-card.removed .stat-value {
        color: rgb(var(--warning-rgb));
    }
    
    .stat-card .stat-suffix {
        font-size: 0.85rem;
        color: var(--text-muted);
    }
    
    /* Distribution Section */
    .distribution-section {
        margin-top: 2rem;
    }
    
    .distribution-table {
        width: 100%;
        border-collapse: collapse;
        background-color: var(--custom-white);
        border-radius: 0.5rem;
        overflow: hidden;
        border: 1px solid var(--default-border);
    }
    
    .distribution-table thead th {
        background-color: var(--light);
        color: var(--default-text-color);
        padding: 0.75rem 1rem;
        text-align: left;
        font-weight: 600;
        border-bottom: 2px solid var(--default-border);
        font-size: 0.75rem;
        text-transform: uppercase;
    }
    
    .distribution-table thead th:last-child {
        text-align: right;
    }
    
    .distribution-table tbody tr {
        border-bottom: 1px solid var(--default-border);
    }
    
    .distribution-table tbody tr:hover {
        background-color: var(--light);
    }
    
    .distribution-table tbody td {
        padding: 0.65rem 1rem;
        color: var(--default-text-color);
    }
    
    .distribution-table tbody td:last-child {
        text-align: right;
    }
    
    .distribution-bar {
        display: flex;
        align-items: center;
        gap: 0.75rem;
    }
    
    .distribution-bar-fill {
        height: 8px;
        background: linear-gradient(90deg, rgb(var(--primary-rgb)), rgb(var(--info-rgb)));
        border-radius: 4px;
        min-width: 4px;
    }
    
    .distribution-pct {
        color: var(--text-muted);
        font-size: 0.85rem;
        min-width: 45px;
        text-align: right;
    }
    
    /* Copy AI Button */
    .copy-ai-container {
        display: flex;
        align-items: center;
        gap: 1rem;
        margin-top: 1rem;
    }
    
    .copy-ai-btn {
        background: linear-gradient(135deg, rgb(var(--info-rgb)) 0%, rgb(var(--primary-rgb)) 100%);
        color: white;
        border: none;
        padding: 0.65rem 1.25rem;
        border-radius: 0.5rem;
        font-weight: 600;
        cursor: pointer;
        font-size: 0.85rem;
        display: flex;
        align-items: center;
        gap: 0.5rem;
        transition: all 0.2s ease;
    }
    
    .copy-ai-btn:hover {
        transform: translateY(-1px);
        box-shadow: 0 4px 12px rgba(var(--primary-rgb), 0.3);
    }
    
    .copy-feedback {
        color: rgb(var(--success-rgb));
        font-size: 0.85rem;
        font-weight: 500;
        opacity: 0;
        transition: opacity 0.3s ease;
    }
    
    .copy-feedback.show {
        opacity: 1;
    }
    
    /* Records Info */
    .records-info {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 1rem;
        color: var(--text-muted);
        font-size: 0.85rem;
    }
    
    .records-info strong {
        color: var(--default-text-color);
    }
    
    .section-badge {
        background-color: rgb(var(--primary-rgb));
        color: white;
        padding: 0.2rem 0.6rem;
        border-radius: 0.25rem;
        font-size: 0.75rem;
        font-weight: 500;
    }
    
    .minute-badge {
        background-color: rgb(var(--info-rgb));
        color: white;
        padding: 0.2rem 0.6rem;
        border-radius: 0.25rem;
        font-size: 0.75rem;
        font-weight: 500;
    }
    
    /* API Status Badge */
    .api-status-badge {
        position: fixed;
        top: 70px;
        right: 20px;
        z-index: 9999;
        padding: 4px 12px;
        border-radius: 4px;
        font-size: 11px;
        font-weight: 600;
    }
    
    @media (max-width: 768px) {
        .stats-cards {
            grid-template-columns: 1fr 1fr;
        }
        
        .filter-bar {
            flex-direction: column;
            align-items: stretch;
        }
        
        .filter-group select {
            width: 100%;
        }
    }
</style>
<?php
$styles = ob_get_clean();

// --- Page Content ---
ob_start();
?>

<!-- API Status Badge -->
<div class="api-status-badge" style="background: <?php echo $use_duckdb ? 'rgb(var(--success-rgb))' : 'rgb(var(--danger-rgb))'; ?>; color: white;">
    🦆 <?php echo $use_duckdb ? 'API Connected' : 'API Disconnected'; ?>
</div>

<!-- Page Header -->
<div class="d-flex align-items-center justify-content-between page-header-breadcrumb flex-wrap gap-2 mb-3">
    <div>
        <nav>
            <ol class="breadcrumb mb-1">
                <li class="breadcrumb-item"><a href="<?php echo $baseUrl; ?>/">Dashboard</a></li>
                <li class="breadcrumb-item"><a href="<?php echo $baseUrl; ?>/pages/features/patterns/">Patterns</a></li>
                <li class="breadcrumb-item active" aria-current="page"><?php echo $project ? htmlspecialchars($project['name']) : 'Project'; ?></li>
            </ol>
        </nav>
        <h1 class="page-title fw-medium fs-18 mb-0">
            📊 <?php echo $project ? htmlspecialchars($project['name']) : 'Pattern Analysis'; ?>
        </h1>
        <?php if ($project && $project['description']): ?>
            <p class="text-muted fs-12 mb-0"><?php echo htmlspecialchars($project['description']); ?></p>
        <?php endif; ?>
    </div>
    <div>
        <a href="./" class="btn btn-outline-secondary btn-sm">
            <i class="ri-arrow-left-line me-1"></i>Back to Projects
        </a>
    </div>
</div>

<!-- Messages -->
<?php if ($error_message): ?>
<div class="alert alert-danger alert-dismissible fade show" role="alert">
    <i class="ri-error-warning-line me-2"></i><?php echo htmlspecialchars($error_message); ?>
    <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
</div>
<?php endif; ?>

<?php if ($filter_message): ?>
<div class="alert alert-success alert-dismissible fade show" role="alert">
    <i class="ri-checkbox-circle-line me-2"></i><?php echo htmlspecialchars($filter_message); ?>
    <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
</div>
<?php endif; ?>

<?php if ($use_duckdb && $project): ?>

<!-- Filter Controls -->
<form method="GET" action="" class="filter-bar">
    <input type="hidden" name="id" value="<?php echo $project_id; ?>">
    
    <div class="filter-group">
        <label for="section">Section</label>
        <select name="section" id="section" class="form-select form-select-sm">
            <?php foreach (TRAIL_SECTIONS as $key => $label): ?>
                <option value="<?php echo htmlspecialchars($key); ?>" <?php echo $selected_section === $key ? 'selected' : ''; ?>>
                    <?php echo htmlspecialchars($label); ?>
                </option>
            <?php endforeach; ?>
        </select>
    </div>
    
    <div class="filter-group">
        <label for="minute">Minute</label>
        <select name="minute" id="minute" class="form-select form-select-sm">
            <?php for ($i = 0; $i <= 14; $i++): ?>
                <option value="<?php echo $i; ?>" <?php echo $selected_minute === $i ? 'selected' : ''; ?>>
                    <?php echo $i; ?><?php echo $i === 0 ? ' (entry)' : ($i === 14 ? ' (15m prior)' : ''); ?>
                </option>
            <?php endfor; ?>
        </select>
    </div>
    
    <div class="filter-group">
        <label for="status">Status</label>
        <select name="status" id="status" class="form-select form-select-sm">
            <?php foreach (STATUS_OPTIONS as $key => $label): ?>
                <option value="<?php echo htmlspecialchars($key); ?>" <?php echo $selected_status === $key ? 'selected' : ''; ?>>
                    <?php echo htmlspecialchars($label); ?>
                </option>
            <?php endforeach; ?>
        </select>
    </div>
    
    <div class="filter-group">
        <label for="analyse_mode">Analyse Mode</label>
        <select name="analyse_mode" id="analyse_mode" class="form-select form-select-sm">
            <option value="all" <?php echo $analyse_mode === 'all' ? 'selected' : ''; ?>>All trades</option>
            <option value="passed" <?php echo $analyse_mode === 'passed' ? 'selected' : ''; ?>>Only passed</option>
        </select>
    </div>
    
    <div class="filter-group">
        <label for="hours">Time Range</label>
        <select name="hours" id="hours" class="form-select form-select-sm">
            <?php foreach (HOURS_OPTIONS as $hours_value => $hours_label): ?>
                <option value="<?php echo $hours_value; ?>" <?php echo $selected_hours === $hours_value ? 'selected' : ''; ?>>
                    <?php echo htmlspecialchars($hours_label); ?>
                </option>
            <?php endforeach; ?>
        </select>
    </div>
    
    <button type="submit" class="btn btn-primary btn-sm">Apply Filters</button>
</form>

<!-- Records Info -->
<div class="records-info">
    <div>
        <strong><?php echo number_format($total_trades); ?></strong> trades in summary
        <?php if ($analyse_passed_only && !empty($active_filters)): ?>
            <span class="text-success">(passed only)</span>
        <?php elseif (!empty($active_filters)): ?>
            <span class="text-warning">(all trades)</span>
        <?php endif; ?>
        &nbsp;|&nbsp;
        <span class="section-badge"><?php echo htmlspecialchars(TRAIL_SECTIONS[$selected_section]); ?></span>
        &nbsp;
        <span class="minute-badge">Minute <?php echo $selected_minute; ?></span>
    </div>
    <div>
        <?php echo count($section_fields); ?> fields in section
    </div>
</div>

<!-- Filter Rules Section -->
<div class="filter-rules-section">
    <div class="filter-rules-header">
        <h5>
            <i class="ri-filter-3-line text-primary"></i>
            Project Filters
        </h5>
        <?php if (!empty($active_filters)): ?>
            <span class="active-filters-count"><?php echo count($active_filters); ?> active</span>
        <?php endif; ?>
    </div>
    
    <p class="text-muted fs-12 mb-3">
        Add filters based on <strong><?php echo htmlspecialchars(TRAIL_SECTIONS[$selected_section]); ?></strong> fields.
    </p>
    
    <!-- Add New Filter Form -->
    <form method="POST" action="" class="add-filter-form">
        <input type="hidden" name="filter_action" value="add_filter">
        
        <div class="filter-input-group">
            <label for="filter_field">Field</label>
            <select name="filter_field" id="filter_field" class="form-select form-select-sm" required onchange="updateFilterInputs()">
                <option value="" data-type="NUMERIC">-- Select --</option>
                <?php foreach ($section_fields as $field): 
                    $field_type = $field_types[$field] ?? 'NUMERIC';
                    $is_boolean = ($field_type === 'BOOLEAN');
                    $type_indicator = $is_boolean ? '🔘 ' : '';
                ?>
                    <option value="<?php echo htmlspecialchars($field); ?>" 
                            data-type="<?php echo $is_boolean ? 'BOOLEAN' : 'NUMERIC'; ?>">
                        <?php echo $type_indicator . htmlspecialchars(str_replace('_', ' ', $field)); ?>
                    </option>
                <?php endforeach; ?>
            </select>
        </div>
        
        <!-- Numeric filter inputs -->
        <div class="filter-input-group" id="numeric_filter_inputs">
            <label for="filter_from">From (min)</label>
            <input type="number" step="any" name="filter_from" id="filter_from" class="form-control form-control-sm" placeholder="Min">
        </div>
        
        <div class="filter-input-group" id="numeric_filter_to">
            <label for="filter_to">To (max)</label>
            <input type="number" step="any" name="filter_to" id="filter_to" class="form-control form-control-sm" placeholder="Max">
        </div>
        
        <!-- Boolean filter input -->
        <div class="filter-input-group" id="boolean_filter_inputs" style="display: none;">
            <label for="filter_bool_value">Value</label>
            <select name="filter_bool_value" id="filter_bool_value" class="form-select form-select-sm" style="min-width: 100px;">
                <option value="1">TRUE ✓</option>
                <option value="0">FALSE ✗</option>
            </select>
        </div>
        
        <div class="filter-input-group">
            <label for="filter_name">Name</label>
            <input type="text" name="filter_name" id="filter_name" class="form-control form-control-sm" placeholder="Optional">
        </div>
        
        <div class="filter-input-group" id="include_null_group">
            <label>&nbsp;</label>
            <div class="form-check">
                <input type="checkbox" class="form-check-input" name="include_null" id="include_null" value="1">
                <label class="form-check-label fs-12" for="include_null">+NULL</label>
            </div>
        </div>
        
        <div class="filter-input-group">
            <label>&nbsp;</label>
            <div class="form-check">
                <input type="checkbox" class="form-check-input" name="exclude_mode" id="exclude_mode" value="1">
                <label class="form-check-label fs-12 text-danger" for="exclude_mode">Exclude</label>
            </div>
        </div>
        
        <button type="submit" class="btn btn-success btn-sm">+ Add</button>
    </form>
    
    <!-- Existing Filter Rules -->
    <div class="existing-filters">
        <?php if (empty($all_filters)): ?>
            <p class="no-filters-msg">No filter rules yet. Add one above.</p>
        <?php else: ?>
            <?php foreach ($all_filters as $filter): ?>
                <div class="filter-rule-item <?php echo $filter['is_active'] ? '' : 'inactive'; ?>">
                    <div class="filter-rule-info">
                        <span class="filter-rule-field"><?php echo htmlspecialchars(str_replace('_', ' ', $filter['field_name'])); ?></span>
                        <?php if (!empty($filter['field_column'])): ?>
                            <span class="badge bg-primary-transparent text-primary fs-10">
                                <?php echo htmlspecialchars($filter['field_column']); ?>
                            </span>
                        <?php endif; ?>
                        <span class="filter-rule-range">
                            <?php
                            // Check if this is a boolean filter
                            $is_bool_filter = ($filter['from_value'] !== null && $filter['to_value'] !== null 
                                && $filter['from_value'] == $filter['to_value'] 
                                && ((float)$filter['from_value'] == 0 || (float)$filter['from_value'] == 1));
                            
                            if ($is_bool_filter) {
                                $bool_display = ((float)$filter['from_value'] == 1) ? '= TRUE ✓' : '= FALSE ✗';
                                $color = ((float)$filter['from_value'] == 1) ? 'success' : 'danger';
                                echo '<span class="text-' . $color . ' fw-semibold">' . $bool_display . '</span>';
                            } else {
                                $range_parts = [];
                                if ($filter['from_value'] !== null) {
                                    $range_parts[] = 'from ' . format_cell_value($filter['from_value']);
                                }
                                if ($filter['to_value'] !== null) {
                                    $range_parts[] = 'to ' . format_cell_value($filter['to_value']);
                                }
                                echo implode(' ', $range_parts);
                            }
                            ?>
                        </span>
                        <?php if (!empty($filter['exclude_mode'])): ?>
                            <span class="badge bg-danger-transparent text-danger fs-10">🚫 EXCLUDE</span>
                        <?php endif; ?>
                        <?php if (!empty($filter['include_null'])): ?>
                            <span class="badge bg-success-transparent text-success fs-10">+NULL</span>
                        <?php elseif (empty($filter['exclude_mode'])): ?>
                            <span class="badge bg-warning-transparent text-warning fs-10">−NULL</span>
                        <?php endif; ?>
                    </div>
                    <div class="filter-rule-actions">
                        <form method="POST" action="" style="display: inline;">
                            <input type="hidden" name="filter_action" value="toggle_filter">
                            <input type="hidden" name="filter_id" value="<?php echo $filter['id']; ?>">
                            <button type="submit" class="btn btn-sm" title="<?php echo $filter['is_active'] ? 'Disable' : 'Enable'; ?>">
                                <?php echo $filter['is_active'] ? '⏸️' : '▶️'; ?>
                            </button>
                        </form>
                        <form method="POST" action="" style="display: inline;" onsubmit="return confirm('Delete this filter?');">
                            <input type="hidden" name="filter_action" value="delete_filter">
                            <input type="hidden" name="filter_id" value="<?php echo $filter['id']; ?>">
                            <button type="submit" class="btn btn-sm text-danger" title="Delete">🗑️</button>
                        </form>
                    </div>
                </div>
            <?php endforeach; ?>
        <?php endif; ?>
    </div>
</div>

<?php if (!empty($active_filters)): ?>
    <?php if ($analyse_passed_only): ?>
    <div class="filter-applied-notice">
        <span>🔍</span>
        <span>Summary shows only trades passing all <?php echo count($active_filters); ?> filter(s) | Distribution below shows all trades with filter impact</span>
    </div>
    <?php else: ?>
    <div class="filter-applied-notice" style="background-color: rgba(var(--warning-rgb), 0.1); border-color: rgb(var(--warning-rgb)); color: rgb(var(--warning-rgb));">
        <span>📊</span>
        <span>Summary shows ALL trades | <?php echo count($active_filters); ?> filter(s) applied in Distribution below</span>
    </div>
    <?php endif; ?>
<?php endif; ?>

<!-- Stats Cards -->
<?php if (!empty($dist_totals)): ?>
<div class="stats-cards">
    <div class="stat-card">
        <div class="stat-label">Total Trades</div>
        <div class="stat-value"><?php echo number_format($dist_totals['base'] ?? 0); ?></div>
    </div>
    <div class="stat-card positive">
        <div class="stat-label">Passed Filters</div>
        <div class="stat-value"><?php echo number_format($dist_totals['filtered'] ?? 0); ?></div>
    </div>
    <div class="stat-card removed">
        <div class="stat-label">Removed</div>
        <div class="stat-value"><?php echo number_format($dist_totals['removed'] ?? 0); ?></div>
    </div>
    <div class="stat-card <?php echo (($dist_response['gains']['filtered_avg'] ?? 0) >= 0) ? 'positive' : 'negative'; ?>">
        <div class="stat-label">Avg Gain (Filtered)</div>
        <div class="stat-value">
            <?php echo number_format(($dist_response['gains']['filtered_avg'] ?? 0), 2); ?>
            <span class="stat-suffix">%</span>
        </div>
    </div>
</div>
<?php endif; ?>

<!-- Field Averages by Potential Gains Range -->
<?php if (!empty($field_stats) && !empty($gain_ranges)): ?>
<div class="summary-section">
    <div class="card custom-card">
        <div class="card-header d-flex justify-content-between align-items-center">
            <h6 class="mb-0">
                <i class="ri-bar-chart-grouped-line me-1 text-primary"></i>
                Field Averages by Potential Gains Range
                <?php if ($analyse_passed_only && !empty($active_filters)): ?>
                    <span class="badge bg-success-transparent text-success ms-2">PASSED ONLY</span>
                <?php elseif (!empty($active_filters)): ?>
                    <span class="badge bg-warning-transparent text-warning ms-2">ALL TRADES</span>
                <?php endif; ?>
            </h6>
            <div class="copy-ai-container">
                <button type="button" class="copy-ai-btn" onclick="copyTableForAI()">
                    <span>🤖</span> Copy for AI
                </button>
                <span class="copy-feedback" id="copyFeedback">✓ Copied!</span>
            </div>
        </div>
        <div class="card-body p-0">
            <div class="summary-table-container">
                <table class="summary-table" id="fieldAveragesTable">
                    <thead>
                        <tr>
                            <th>Field</th>
                            <?php foreach ($gain_ranges as $range): ?>
                                <th>
                                    <?php echo htmlspecialchars($range['label']); ?>
                                    <?php 
                                    $range_id = $range['id'];
                                    $first_field = array_key_first($field_stats);
                                    $range_count = $field_stats[$first_field]['ranges'][$range_id]['count'] ?? 0;
                                    ?>
                                    <br><span class="range-count">(<?php echo number_format($range_count); ?>)</span>
                                </th>
                            <?php endforeach; ?>
                        </tr>
                    </thead>
                    <tbody>
                        <?php foreach ($section_fields as $field): 
                            $stats = $field_stats[$field] ?? [];
                            $field_type = $stats['type'] ?? 'NUMERIC';
                        ?>
                        <tr>
                            <td>
                                <?php if ($field_type === 'BOOLEAN'): ?>
                                    <span title="Boolean field - shows % TRUE">🔘</span>
                                <?php endif; ?>
                                <?php echo htmlspecialchars(str_replace('_', ' ', $field)); ?>
                            </td>
                            <?php foreach ($gain_ranges as $range): 
                                $range_id = $range['id'];
                                $avg = $stats['ranges'][$range_id]['avg'] ?? null;
                                $display_val = format_cell_value($avg);
                                $class = get_profit_class($avg);
                            ?>
                            <td class="<?php echo $class; ?>">
                                <?php echo $display_val; ?>
                                <?php if ($field_type === 'BOOLEAN' && $avg !== null): ?>
                                    <span class="text-muted">%</span>
                                <?php endif; ?>
                            </td>
                            <?php endforeach; ?>
                        </tr>
                        <?php endforeach; ?>
                    </tbody>
                </table>
            </div>
        </div>
    </div>
</div>
<?php endif; ?>

<!-- Distribution Section -->
<?php if (!empty($distribution)): ?>
<div class="distribution-section">
    <div class="card custom-card">
        <div class="card-header">
            <h6 class="mb-0">
                <i class="ri-pie-chart-line me-1 text-info"></i>
                Gain Distribution (Filter Impact)
            </h6>
        </div>
        <div class="card-body p-0">
            <table class="distribution-table">
                <thead>
                    <tr>
                        <th>Gain Range</th>
                        <th>Base Count</th>
                        <th>After Filters</th>
                        <th>Removed</th>
                        <th style="width: 200px;">Distribution</th>
                    </tr>
                </thead>
                <tbody>
                    <?php 
                    $max_base = max(array_column($distribution, 'base_count'));
                    foreach ($distribution as $dist): 
                        $pct = $max_base > 0 ? ($dist['base_count'] / $max_base * 100) : 0;
                    ?>
                    <tr>
                        <td><strong><?php echo htmlspecialchars($dist['label']); ?></strong></td>
                        <td><?php echo number_format($dist['base_count']); ?></td>
                        <td class="text-success"><?php echo number_format($dist['filtered_count']); ?></td>
                        <td class="text-danger"><?php echo number_format($dist['removed']); ?></td>
                        <td>
                            <div class="distribution-bar">
                                <div class="distribution-bar-fill" style="width: <?php echo $pct; ?>%;"></div>
                                <span class="distribution-pct"><?php echo number_format($pct, 1); ?>%</span>
                            </div>
                        </td>
                    </tr>
                    <?php endforeach; ?>
                </tbody>
            </table>
        </div>
    </div>
</div>
<?php endif; ?>

<?php endif; // end if $use_duckdb && $project ?>

<script>
function updateFilterInputs() {
    const select = document.getElementById('filter_field');
    const selectedOption = select.options[select.selectedIndex];
    const fieldType = selectedOption.getAttribute('data-type');
    
    const numericInputs = document.getElementById('numeric_filter_inputs');
    const numericTo = document.getElementById('numeric_filter_to');
    const booleanInputs = document.getElementById('boolean_filter_inputs');
    const includeNullGroup = document.getElementById('include_null_group');
    
    if (fieldType === 'BOOLEAN') {
        numericInputs.style.display = 'none';
        numericTo.style.display = 'none';
        booleanInputs.style.display = 'block';
        includeNullGroup.style.display = 'none';
        
        document.getElementById('filter_from').value = '';
        document.getElementById('filter_to').value = '';
    } else {
        numericInputs.style.display = 'block';
        numericTo.style.display = 'block';
        booleanInputs.style.display = 'none';
        includeNullGroup.style.display = 'block';
    }
}

function copyTableForAI() {
    const table = document.getElementById('fieldAveragesTable');
    if (!table) return;
    
    let text = "PATTERN ANALYSIS DATA\n";
    text += "=====================\n\n";
    text += "Section: <?php echo htmlspecialchars(TRAIL_SECTIONS[$selected_section]); ?>\n";
    text += "Minute: <?php echo $selected_minute; ?>\n";
    text += "Total Trades: <?php echo $total_trades; ?>\n";
    text += "Hours: <?php echo $selected_hours; ?>\n\n";
    text += "FIELD AVERAGES BY GAIN RANGE:\n";
    text += "-----------------------------\n\n";
    
    // Get headers
    const headers = [];
    const headerCells = table.querySelectorAll('thead th');
    headerCells.forEach(cell => {
        let headerText = cell.textContent.replace(/\s+/g, ' ').trim();
        headerText = headerText.split('(')[0].trim(); // Remove count
        headers.push(headerText);
    });
    text += headers.join(' | ') + "\n";
    text += headers.map(() => '---').join(' | ') + "\n";
    
    // Get rows
    const rows = table.querySelectorAll('tbody tr');
    rows.forEach(row => {
        const cells = row.querySelectorAll('td');
        const rowData = [];
        cells.forEach(cell => {
            let cellText = cell.textContent.replace(/\s+/g, ' ').trim();
            cellText = cellText.replace('🔘', '').trim();
            rowData.push(cellText);
        });
        text += rowData.join(' | ') + "\n";
    });
    
    text += "\n\nPLEASE ANALYZE:\n";
    text += "1. Which fields show significant differences between high-gain (1%+, 2%+) and low/negative gain ranges?\n";
    text += "2. Suggest filter rules (field, min/max values) that could improve win rate.\n";
    text += "3. Identify any patterns that might be predictive of successful trades.\n";
    
    navigator.clipboard.writeText(text).then(() => {
        const feedback = document.getElementById('copyFeedback');
        feedback.classList.add('show');
        setTimeout(() => feedback.classList.remove('show'), 2000);
    });
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', function() {
    updateFilterInputs();
});
</script>

<?php
$content = ob_get_clean();
$scripts = '';

// Include the base layout
include __DIR__ . '/../../layouts/base.php';
?>

