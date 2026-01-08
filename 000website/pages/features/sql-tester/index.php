<?php
/**
 * SQL Tester - Query Master2's In-Memory DuckDB
 * 
 * Provides a web interface to:
 * - Execute read-only SQL queries against master2's DuckDB
 * - Browse database schema (all tables and columns)
 * - View query results in a formatted table
 */

// --- DuckDB API Client ---
require_once __DIR__ . '/../../../includes/config.php';
require_once __DIR__ . '/../../../includes/DuckDBClient.php';
$duckdb = new DuckDBClient(DUCKDB_API_URL);
$use_duckdb = $duckdb->isAvailable();

// --- Base URL for template ---
$baseUrl = '';

$error_message = '';
$success_message = '';
$query_results = null;
$schema_data = null;
$query_sql = '';
$execution_time = 0;

// Handle SQL query submission
if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['sql_query'])) {
    $query_sql = trim($_POST['sql_query']);
    
    if (empty($query_sql)) {
        $error_message = "Please enter a SQL query.";
    } elseif (!$use_duckdb) {
        $error_message = "DuckDB API is not available. Please start master2: python scheduler/master2.py";
    } else {
        // Execute query and measure time
        $start_time = microtime(true);
        $response = $duckdb->executeSQL($query_sql);
        $execution_time = round((microtime(true) - $start_time) * 1000, 2); // Convert to ms
        
        if ($response && isset($response['success']) && $response['success']) {
            $query_results = $response;
            $success_message = "Query executed successfully in {$execution_time}ms";
        } else {
            $error_message = $response['detail'] ?? "Query execution failed";
        }
    }
}

// Load schema data on page load
if ($use_duckdb) {
    $schema_response = $duckdb->getSchema();
    if ($schema_response && isset($schema_response['success']) && $schema_response['success']) {
        $schema_data = $schema_response['schema'];
    }
}

// Example queries
$example_queries = [
    'Recent Prices' => 'SELECT * FROM prices ORDER BY ts DESC LIMIT 10',
    'Active Cycles' => 'SELECT * FROM cycle_tracker WHERE cycle_end_time IS NULL ORDER BY created_at DESC',
    'Recent Trades' => 'SELECT * FROM follow_the_goat_buyins ORDER BY created_at DESC LIMIT 20',
    'Top Wallets' => 'SELECT wallet_address, trade_count, trade_success_percentage FROM wallet_profiles ORDER BY trade_count DESC LIMIT 10',
    'Current SOL Price' => "SELECT price, ts FROM prices WHERE token = 'SOL' ORDER BY ts DESC LIMIT 1",
    'Order Book Stats' => 'SELECT * FROM order_book_features ORDER BY timestamp DESC LIMIT 10',
    'Active Plays' => 'SELECT id, name, is_active FROM follow_the_goat_plays WHERE is_active = 1',
];

// --- Page Styles ---
ob_start();
?>
<style>
    .sql-tester-container {
        width: 100%;
    }
    
    .query-section {
        margin-bottom: 2rem;
    }
    
    .sql-editor-container {
        position: relative;
    }
    
    .sql-editor {
        font-family: 'SF Mono', 'Monaco', 'Consolas', monospace;
        background: var(--custom-white);
        border: 1px solid var(--default-border);
        border-radius: 0.5rem;
        padding: 1rem;
        min-height: 200px;
        max-height: 400px;
        width: 100%;
        color: var(--default-text-color);
        font-size: 0.9rem;
        line-height: 1.5;
        resize: vertical;
    }
    
    .sql-editor:focus {
        outline: none;
        border-color: rgb(var(--primary-rgb));
        box-shadow: 0 0 0 3px rgba(var(--primary-rgb), 0.1);
    }
    
    .query-actions {
        display: flex;
        gap: 0.75rem;
        align-items: center;
        margin-top: 1rem;
        flex-wrap: wrap;
    }
    
    .example-queries {
        display: flex;
        gap: 0.5rem;
        align-items: center;
    }
    
    .example-queries label {
        font-size: 0.85rem;
        color: var(--text-muted);
        white-space: nowrap;
    }
    
    .example-queries select {
        background: var(--custom-white);
        border: 1px solid var(--default-border);
        border-radius: 4px;
        padding: 0.4rem 0.8rem;
        font-size: 0.85rem;
        color: var(--default-text-color);
        cursor: pointer;
        min-width: 200px;
    }
    
    .results-section {
        margin-bottom: 2rem;
    }
    
    .results-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: 1rem;
        flex-wrap: wrap;
        gap: 1rem;
    }
    
    .results-info {
        display: flex;
        gap: 1rem;
        align-items: center;
    }
    
    .results-table-container {
        max-height: 600px;
        overflow: auto;
        border: 1px solid var(--default-border);
        border-radius: 0.5rem;
    }
    
    .results-table {
        font-size: 0.85rem;
        width: 100%;
        margin: 0;
    }
    
    .results-table th {
        position: sticky;
        top: 0;
        background: var(--custom-white);
        font-family: 'SF Mono', 'Monaco', 'Consolas', monospace;
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        color: var(--text-muted);
        font-weight: 600;
        border-bottom: 2px solid var(--default-border);
        padding: 0.75rem 0.5rem;
        white-space: nowrap;
        z-index: 10;
    }
    
    .results-table td {
        vertical-align: middle;
        padding: 0.75rem 0.5rem;
        border-bottom: 1px solid var(--default-border);
        font-family: 'SF Mono', 'Monaco', 'Consolas', monospace;
        font-size: 0.8rem;
    }
    
    .results-table tbody tr:hover {
        background: rgba(var(--primary-rgb), 0.05);
    }
    
    .schema-section {
        margin-top: 2rem;
    }
    
    .schema-search {
        margin-bottom: 1rem;
    }
    
    .schema-search input {
        background: var(--custom-white);
        border: 1px solid var(--default-border);
        border-radius: 4px;
        padding: 0.5rem 1rem;
        font-size: 0.9rem;
        color: var(--default-text-color);
        width: 100%;
        max-width: 400px;
    }
    
    .schema-table-card {
        background: var(--custom-white);
        border: 1px solid var(--default-border);
        border-left: 3px solid rgb(var(--primary-rgb));
        border-radius: 0.5rem;
        margin-bottom: 1rem;
        overflow: hidden;
    }
    
    .schema-table-header {
        padding: 1rem;
        cursor: pointer;
        display: flex;
        align-items: center;
        justify-content: space-between;
        transition: background 0.2s;
    }
    
    .schema-table-header:hover {
        background: rgba(var(--primary-rgb), 0.05);
    }
    
    .schema-table-name {
        font-family: 'SF Mono', 'Monaco', 'Consolas', monospace;
        font-size: 1rem;
        font-weight: 600;
        color: var(--default-text-color);
        display: flex;
        align-items: center;
        gap: 0.75rem;
    }
    
    .schema-table-body {
        padding: 0 1rem 1rem 1rem;
        display: none;
    }
    
    .schema-table-body.show {
        display: block;
    }
    
    .columns-list {
        list-style: none;
        padding: 0;
        margin: 0;
    }
    
    .column-item {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 0.5rem;
        border-bottom: 1px solid rgba(var(--default-border-rgb), 0.3);
    }
    
    .column-item:last-child {
        border-bottom: none;
    }
    
    .column-name {
        font-family: 'SF Mono', 'Monaco', 'Consolas', monospace;
        color: rgb(var(--primary-rgb));
        font-weight: 600;
        font-size: 0.9rem;
    }
    
    .column-type {
        font-family: 'SF Mono', 'Monaco', 'Consolas', monospace;
        color: var(--text-muted);
        font-size: 0.8rem;
    }
    
    .copy-table-btn {
        padding: 0.25rem 0.5rem;
        font-size: 0.75rem;
        border-radius: 4px;
    }
    
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
    
    .expand-icon {
        transition: transform 0.2s;
    }
    
    .expand-icon.rotated {
        transform: rotate(90deg);
    }
</style>
<?php
$styles = ob_get_clean();

// --- Page Content ---
ob_start();
?>

<!-- API Status Badge -->
<div class="api-status-badge" style="background: <?php echo $use_duckdb ? 'rgb(var(--success-rgb))' : 'rgb(var(--danger-rgb))'; ?>; color: white;">
    <?php echo $use_duckdb ? 'API Connected' : 'API Disconnected'; ?>
</div>

<div class="sql-tester-container">
    
    <!-- Page Header -->
    <div class="d-flex align-items-center justify-content-between page-header-breadcrumb flex-wrap gap-2 mb-3">
        <div>
            <nav>
                <ol class="breadcrumb mb-1">
                    <li class="breadcrumb-item"><a href="<?php echo $baseUrl; ?>/">Dashboard</a></li>
                    <li class="breadcrumb-item"><a href="#">Features</a></li>
                    <li class="breadcrumb-item active" aria-current="page">SQL Tester</li>
                </ol>
            </nav>
            <h1 class="page-title mb-0">SQL Tester</h1>
        </div>
    </div>
    
    <!-- Messages -->
    <?php if ($error_message): ?>
    <div class="alert alert-danger alert-dismissible fade show" role="alert">
        <i class="ri-error-warning-line me-2"></i><?php echo htmlspecialchars($error_message); ?>
        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    </div>
    <?php endif; ?>
    
    <?php if ($success_message): ?>
    <div class="alert alert-success alert-dismissible fade show" role="alert">
        <i class="ri-checkbox-circle-line me-2"></i><?php echo htmlspecialchars($success_message); ?>
        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    </div>
    <?php endif; ?>
    
    <!-- Query Section -->
    <div class="card custom-card query-section">
        <div class="card-header">
            <div class="card-title">
                <i class="ri-code-line me-2"></i>SQL Query Editor
            </div>
            <div class="ms-auto">
                <span class="badge bg-info-transparent">Read-only (SELECT)</span>
            </div>
        </div>
        <div class="card-body">
            <form method="POST" action="">
                <div class="sql-editor-container">
                    <textarea 
                        name="sql_query" 
                        id="sqlEditor" 
                        class="sql-editor" 
                        placeholder="Enter your SQL query here... (SELECT statements only, max 1000 rows)"><?php echo htmlspecialchars($query_sql); ?></textarea>
                </div>
                
                <div class="query-actions">
                    <button type="submit" class="btn btn-primary">
                        <i class="ri-play-line me-2"></i>Run Query
                    </button>
                    
                    <button type="button" class="btn btn-secondary" onclick="document.getElementById('sqlEditor').value = ''; return false;">
                        <i class="ri-delete-bin-line me-2"></i>Clear
                    </button>
                    
                    <div class="example-queries">
                        <label for="exampleSelect">Examples:</label>
                        <select id="exampleSelect" class="form-select" onchange="loadExample(this.value)">
                            <option value="">-- Select an example --</option>
                            <?php foreach ($example_queries as $name => $sql): ?>
                            <option value="<?php echo htmlspecialchars($sql); ?>"><?php echo htmlspecialchars($name); ?></option>
                            <?php endforeach; ?>
                        </select>
                    </div>
                </div>
            </form>
        </div>
    </div>
    
    <!-- Results Section -->
    <?php if ($query_results !== null && isset($query_results['rows'])): ?>
    <div class="card custom-card results-section">
        <div class="card-header">
            <div class="card-title">
                <i class="ri-table-line me-2"></i>Query Results
            </div>
            <div class="ms-auto">
                <span class="badge bg-success-transparent"><?php echo number_format($query_results['count']); ?> row<?php echo $query_results['count'] !== 1 ? 's' : ''; ?></span>
            </div>
        </div>
        <div class="card-body p-0">
            <?php if ($query_results['count'] > 0): ?>
            <div class="results-table-container">
                <table class="table results-table mb-0">
                    <thead>
                        <tr>
                            <?php foreach ($query_results['columns'] as $column): ?>
                            <th><?php echo htmlspecialchars($column); ?></th>
                            <?php endforeach; ?>
                        </tr>
                    </thead>
                    <tbody>
                        <?php foreach ($query_results['rows'] as $row): ?>
                        <tr>
                            <?php foreach ($row as $value): ?>
                            <td><?php echo $value === null ? '<span class="text-muted">NULL</span>' : htmlspecialchars($value); ?></td>
                            <?php endforeach; ?>
                        </tr>
                        <?php endforeach; ?>
                    </tbody>
                </table>
            </div>
            <?php else: ?>
            <div class="p-4 text-center text-muted">
                <i class="ri-inbox-line" style="font-size: 2rem;"></i>
                <p class="mb-0 mt-2">No results found</p>
            </div>
            <?php endif; ?>
        </div>
    </div>
    <?php endif; ?>
    
    <!-- Schema Browser Section -->
    <div class="card custom-card schema-section">
        <div class="card-header">
            <div class="card-title">
                <i class="ri-database-2-line me-2"></i>Database Schema
            </div>
            <div class="ms-auto">
                <?php if ($schema_data): ?>
                <span class="badge bg-info-transparent"><?php echo count($schema_data); ?> table<?php echo count($schema_data) !== 1 ? 's' : ''; ?></span>
                <?php endif; ?>
            </div>
        </div>
        <div class="card-body">
            <?php if (!$use_duckdb): ?>
            <div class="text-center text-muted p-4">
                <i class="ri-error-warning-line" style="font-size: 2rem;"></i>
                <p class="mb-0 mt-2">DuckDB API is not available</p>
                <p class="mb-0 mt-1">Please start master2: <code>python scheduler/master2.py</code></p>
            </div>
            <?php elseif ($schema_data): ?>
            <div class="schema-search mb-3">
                <input 
                    type="text" 
                    id="schemaSearch" 
                    class="form-control" 
                    placeholder="Search tables and columns..."
                    onkeyup="filterSchema(this.value)">
            </div>
            
            <div id="schemaContainer">
                <?php foreach ($schema_data as $table_name => $table_info): ?>
                <div class="schema-table-card" data-table-name="<?php echo htmlspecialchars($table_name); ?>">
                    <div class="schema-table-header" onclick="toggleTable(this)">
                        <div class="schema-table-name">
                            <i class="ri-arrow-right-s-line expand-icon"></i>
                            <span><?php echo htmlspecialchars($table_name); ?></span>
                            <span class="badge bg-secondary-transparent"><?php echo number_format($table_info['row_count']); ?> rows</span>
                            <span class="badge bg-primary-transparent"><?php echo count($table_info['columns']); ?> columns</span>
                        </div>
                        <button 
                            type="button" 
                            class="btn btn-sm btn-outline-primary copy-table-btn" 
                            onclick="copyTableName('<?php echo htmlspecialchars($table_name); ?>'); event.stopPropagation();">
                            <i class="ri-file-copy-line"></i> Copy
                        </button>
                    </div>
                    <div class="schema-table-body">
                        <ul class="columns-list">
                            <?php foreach ($table_info['columns'] as $column): ?>
                            <li class="column-item">
                                <span class="column-name"><?php echo htmlspecialchars($column['name']); ?></span>
                                <span class="column-type"><?php echo htmlspecialchars($column['type']); ?></span>
                            </li>
                            <?php endforeach; ?>
                        </ul>
                    </div>
                </div>
                <?php endforeach; ?>
            </div>
            <?php else: ?>
            <div class="text-center text-muted p-4">
                <i class="ri-loader-4-line" style="font-size: 2rem;"></i>
                <p class="mb-0 mt-2">Loading schema...</p>
            </div>
            <?php endif; ?>
        </div>
    </div>
    
</div>

<script>
    // Load example query
    function loadExample(sql) {
        if (sql) {
            document.getElementById('sqlEditor').value = sql;
        }
    }
    
    // Toggle table expansion
    function toggleTable(header) {
        const body = header.nextElementSibling;
        const icon = header.querySelector('.expand-icon');
        
        body.classList.toggle('show');
        icon.classList.toggle('rotated');
    }
    
    // Copy table name to clipboard and SQL editor
    function copyTableName(tableName) {
        // Copy to clipboard
        navigator.clipboard.writeText(tableName).then(() => {
            // Also insert into SQL editor
            const editor = document.getElementById('sqlEditor');
            const currentValue = editor.value.trim();
            
            // If editor is empty or has just whitespace, start fresh
            if (!currentValue) {
                editor.value = `SELECT * FROM ${tableName} LIMIT 10`;
            } else {
                // Otherwise append
                editor.value = currentValue + `\n${tableName}`;
            }
            
            // Show feedback
            const btn = event.target.closest('button');
            const originalHTML = btn.innerHTML;
            btn.innerHTML = '<i class="ri-check-line"></i> Copied';
            btn.classList.add('btn-success');
            btn.classList.remove('btn-outline-primary');
            
            setTimeout(() => {
                btn.innerHTML = originalHTML;
                btn.classList.remove('btn-success');
                btn.classList.add('btn-outline-primary');
            }, 1500);
        });
    }
    
    // Filter schema tables
    function filterSchema(searchTerm) {
        searchTerm = searchTerm.toLowerCase();
        const tables = document.querySelectorAll('.schema-table-card');
        
        tables.forEach(table => {
            const tableName = table.dataset.tableName.toLowerCase();
            const columns = Array.from(table.querySelectorAll('.column-name'))
                .map(el => el.textContent.toLowerCase());
            
            const matches = tableName.includes(searchTerm) || 
                          columns.some(col => col.includes(searchTerm));
            
            table.style.display = matches ? 'block' : 'none';
        });
    }
    
    // Auto-expand first table on load
    document.addEventListener('DOMContentLoaded', function() {
        const firstTable = document.querySelector('.schema-table-header');
        if (firstTable) {
            toggleTable(firstTable);
        }
    });
</script>

<?php
$content = ob_get_clean();
$scripts = '';

// Include the base layout
include __DIR__ . '/../../layouts/base.php';
?>
