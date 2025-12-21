<?php
/**
 * Goats (Plays) Page - Follow The Goat Trading Plays
 * Ported from chart/plays/index.php to v2 template system
 */

// --- Load Configuration from .env ---
require_once __DIR__ . '/../../chart/config.php';

// --- Base URL for v2 template ---
$rootFolder = basename($_SERVER['DOCUMENT_ROOT']);
$baseUrl = (isset($_SERVER['HTTPS']) && $_SERVER['HTTPS'] === 'on' ? 'https://' : 'http://') . $_SERVER['HTTP_HOST'] . dirname(dirname($_SERVER['SCRIPT_NAME']));

// --- Data Fetching ---
$dsn = "mysql:host=$db_host;dbname=$db_name;charset=$db_charset";
$options = [
    PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
    PDO::ATTR_EMULATE_PREPARES   => false,
];

$plays_data = [];
$error_message = '';
$success_message = '';

// Check for success/error messages from save/update operation
if (isset($_GET['success'])) {
    if ($_GET['success'] == '2') {
        $success_message = 'Play updated successfully!';
    } else {
        $success_message = 'Play created successfully!';
    }
}
if (isset($_GET['error'])) {
    $error_message = htmlspecialchars($_GET['error']);
}

try {
    $pdo = new PDO($dsn, $db_user, $db_pass, $options);

    // Fetch all plays
    $stmt = $pdo->prepare("SELECT id, created_at, name, description, sorting, short_play, tricker_on_perp, timing_conditions, play_log FROM solcatcher.follow_the_goat_plays ORDER BY sorting ASC, id DESC");
    $stmt->execute();
    $plays_data = $stmt->fetchAll();

} catch (\PDOException $e) {
    $error_message = "Database connection failed: " . $e->getMessage();
}

// --- Page Styles ---
ob_start();
?>
<style>
    .plays-grid {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
        gap: 1.25rem;
    }
    
    .play-card {
        background: var(--custom-white);
        border: 1px solid var(--default-border);
        border-radius: 0.5rem;
        overflow: hidden;
        transition: all 0.2s ease;
    }
    
    .play-card:hover {
        border-color: rgb(var(--primary-rgb));
        box-shadow: 0 4px 15px rgba(var(--primary-rgb), 0.15);
    }
    
    .play-card-header {
        padding: 1rem 1.25rem;
        border-bottom: 1px solid var(--default-border);
        background: rgba(var(--primary-rgb), 0.03);
    }
    
    .play-card-title {
        font-size: 1.1rem;
        font-weight: 600;
        color: var(--default-text-color);
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    
    .play-card-id {
        font-size: 0.75rem;
        color: var(--text-muted);
        margin-top: 0.25rem;
    }
    
    .play-card-body {
        padding: 1.25rem;
    }
    
    .play-card-description {
        color: var(--text-muted);
        font-size: 0.9rem;
        margin-bottom: 1rem;
        line-height: 1.5;
    }
    
    .play-card-description-text {
        max-height: 60px;
        overflow: hidden;
    }
    
    .play-log-summary {
        margin-top: 0.75rem;
        padding-top: 0.75rem;
        border-top: 1px dashed var(--default-border);
        display: flex;
        gap: 1.5rem;
    }
    
    .play-log-item {
        display: flex;
        flex-direction: column;
        gap: 0.15rem;
    }
    
    .play-log-label {
        font-size: 0.7rem;
        color: var(--text-muted);
        text-transform: uppercase;
    }
    
    .play-log-value {
        font-size: 0.9rem;
        font-weight: 600;
        color: var(--default-text-color);
    }
    
    .play-performance {
        background: rgba(var(--light-rgb), 0.5);
        border-radius: 0.375rem;
        padding: 1rem;
        margin-top: 1rem;
    }
    
    .play-performance-title {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 0.75rem;
        font-size: 0.85rem;
        font-weight: 600;
        color: var(--default-text-color);
    }
    
    .play-performance-updated {
        font-size: 0.7rem;
        color: var(--text-muted);
        font-weight: 400;
    }
    
    .play-performance-grid {
        display: grid;
        grid-template-columns: repeat(3, 1fr);
        gap: 0.75rem;
    }
    
    .play-performance-item {
        text-align: center;
    }
    
    .play-performance-value {
        font-size: 1.1rem;
        font-weight: 700;
    }
    
    .play-performance-value.positive { color: rgb(var(--success-rgb)); }
    .play-performance-value.negative { color: rgb(var(--danger-rgb)); }
    .play-performance-value.neutral { color: var(--text-muted); }
    
    .play-performance-label {
        font-size: 0.7rem;
        color: var(--text-muted);
        text-transform: uppercase;
        margin-top: 0.25rem;
    }
    
    .play-card-footer {
        padding: 1rem 1.25rem;
        border-top: 1px solid var(--default-border);
        display: flex;
        gap: 0.75rem;
        justify-content: flex-end;
    }
    
    .page-header-actions {
        display: flex;
        align-items: center;
        gap: 0.75rem;
        flex-wrap: wrap;
    }
    
    .form-container {
        background: var(--custom-white);
        border: 1px solid var(--default-border);
        border-radius: 0.5rem;
        padding: 1.5rem;
        margin-bottom: 1.5rem;
    }
    
    .form-container.hidden {
        display: none;
    }
    
    .form-container h3 {
        margin-bottom: 1.25rem;
        color: var(--default-text-color);
    }
    
    .form-actions {
        display: flex;
        gap: 0.75rem;
        margin-top: 1.5rem;
    }
    
    .tolerance-rules-container {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 1.5rem;
        margin-top: 1rem;
    }
    
    .tolerance-section h4 {
        font-size: 0.9rem;
        margin-bottom: 0.75rem;
        color: var(--default-text-color);
    }
    
    .tolerance-rule, .tolerance-rule-single {
        display: flex;
        gap: 0.75rem;
        align-items: flex-end;
        margin-bottom: 0.75rem;
        flex-wrap: wrap;
    }
    
    .tolerance-rule-label {
        font-size: 0.75rem;
        color: var(--text-muted);
        display: block;
        margin-bottom: 0.25rem;
    }
    
    .form-control-inline {
        width: 100px;
        padding: 0.375rem 0.5rem;
        font-size: 0.85rem;
        border: 1px solid var(--default-border);
        border-radius: 0.25rem;
        background: var(--custom-white);
        color: var(--default-text-color);
    }
    
    .pip-help-text {
        background: rgba(var(--info-rgb), 0.1);
        border-left: 4px solid rgb(var(--info-rgb));
        padding: 0.75rem;
        border-radius: 0.25rem;
        font-size: 0.85rem;
        color: var(--default-text-color);
    }
    
    .pip-conversion {
        font-size: 0.7rem;
        color: var(--text-muted);
        display: block;
        margin-top: 0.25rem;
    }
    
    @media (max-width: 768px) {
        .tolerance-rules-container {
            grid-template-columns: 1fr;
        }
        .plays-grid {
            grid-template-columns: 1fr;
        }
    }
</style>
<?php
$styles = ob_get_clean();

// --- Page Content ---
ob_start();
?>

<!-- Page Header -->
<div class="d-flex align-items-center justify-content-between page-header-breadcrumb flex-wrap gap-2">
    <div>
        <nav>
            <ol class="breadcrumb mb-1">
                <li class="breadcrumb-item"><a href="<?php echo $baseUrl; ?>/">Dashboard</a></li>
                <li class="breadcrumb-item active" aria-current="page">Goats</li>
            </ol>
        </nav>
        <h1 class="page-title fw-medium fs-18 mb-0">Follow The Goat Plays</h1>
    </div>
    <div class="page-header-actions">
        <select id="timeInterval" class="form-select" style="width: auto; min-width: 140px;" onchange="changeTimeInterval()">
            <option value="24">24 Hours</option>
            <option value="12">12 Hours</option>
            <option value="6">6 Hours</option>
            <option value="2">2 Hours</option>
            <option value="all" selected>All Time</option>
        </select>
        <button class="btn btn-danger" onclick="cleanupNoGos()" title="Delete no_go trades older than 24 hours">
            <i class="ri-delete-bin-line me-1"></i>Cleanup No-Gos
        </button>
        <button class="btn btn-primary" onclick="toggleCreateForm()">
            <i class="ri-add-line me-1"></i>Create New Play
        </button>
    </div>
</div>

<!-- Messages -->
<?php if ($success_message): ?>
<div class="alert alert-success alert-dismissible fade show" role="alert">
    <?php echo $success_message; ?>
    <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
</div>
<?php endif; ?>

<?php if ($error_message): ?>
<div class="alert alert-danger alert-dismissible fade show" role="alert">
    <?php echo $error_message; ?>
    <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
</div>
<?php endif; ?>

<!-- Create Play Form -->
<div class="form-container hidden" id="createPlayForm">
    <h3>Create New Play</h3>
    <form action="/chart/plays/save.php" method="POST" onsubmit="return validateForm()">
        <div class="mb-3">
            <label class="form-label" for="name">Name *</label>
            <input type="text" class="form-control" id="name" name="name" maxlength="60" required>
        </div>

        <div class="mb-3">
            <label class="form-label" for="description">Description *</label>
            <textarea class="form-control" id="description" name="description" maxlength="500" rows="3" required></textarea>
        </div>

        <div class="mb-3">
            <label class="form-label" for="find_wallets_sql">Find Wallets SQL Query *</label>
            <textarea class="form-control" id="find_wallets_sql" name="find_wallets_sql" rows="8" required placeholder="Enter SQL query to find wallets..."></textarea>
            <div class="form-text">This query will be validated before saving. <strong>Mandatory field:</strong> query must return <code>wallet_address</code></div>
        </div>

        <div class="mb-3">
            <label class="form-label">Sell Logic Tolerance Rules *</label>
            <div class="pip-help-text mb-3">
                <strong class="text-info">Understanding PIPs:</strong><br>
                PIPs make it easy to enter small decimal values. <strong>1 PIP = 0.0001 = 0.01%</strong><br>
                <strong>Examples:</strong> 10 PIPs = 0.001 (0.1%) | 100 PIPs = 0.01 (1%) | 1000 PIPs = 0.1 (10%) | 4000 PIPs = 0.4 (40%)<br>
                <strong>Formula:</strong> Decimal Value = PIPs / 10,000 | Percentage = PIPs / 100
            </div>
            
            <div class="tolerance-rules-container">
                <!-- Decreases Section -->
                <div class="tolerance-section">
                    <h4>Decreases</h4>
                    <div id="decreases-container">
                        <div class="tolerance-rule-single">
                            <div>
                                <label class="tolerance-rule-label">Tolerance (PIPs)</label>
                                <input type="number" step="1" class="form-control-inline pip-input" name="decrease_tolerance[]" value="10" required>
                                <small class="pip-conversion"></small>
                                <input type="hidden" name="decrease_range_from[]" value="-999999">
                                <input type="hidden" name="decrease_range_to[]" value="0">
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Increases Section -->
                <div class="tolerance-section">
                    <h4>Increases</h4>
                    <div id="increases-container">
                        <div class="tolerance-rule">
                            <div>
                                <label class="tolerance-rule-label">Range From (PIPs)</label>
                                <input type="number" step="1" class="form-control-inline pip-input" name="increase_range_from[]" value="0" required>
                                <small class="pip-conversion"></small>
                            </div>
                            <div>
                                <label class="tolerance-rule-label">Range To (PIPs)</label>
                                <input type="number" step="1" class="form-control-inline pip-input" name="increase_range_to[]" value="5000" required>
                                <small class="pip-conversion"></small>
                            </div>
                            <div>
                                <label class="tolerance-rule-label">Tolerance (PIPs)</label>
                                <input type="number" step="1" class="form-control-inline pip-input" name="increase_tolerance[]" value="10" required>
                                <small class="pip-conversion"></small>
                            </div>
                            <button type="button" class="btn btn-sm btn-danger" onclick="removeRule(this)">Remove</button>
                        </div>
                    </div>
                    <button type="button" class="btn btn-sm btn-success mt-2" onclick="addIncreaseRule()">+ Add Increase Rule</button>
                </div>
            </div>
        </div>

        <div class="mb-3">
            <label class="form-label" for="max_buys_per_cycle">Max Buys Per Cycle *</label>
            <input type="number" class="form-control" id="max_buys_per_cycle" name="max_buys_per_cycle" value="5" min="1" required style="max-width: 150px;">
        </div>

        <div class="form-actions">
            <button type="submit" class="btn btn-primary">Create Play</button>
            <button type="button" class="btn btn-secondary" onclick="toggleCreateForm()">Cancel</button>
        </div>
    </form>
</div>

<!-- Stats Card -->
<div class="card custom-card mb-3">
    <div class="card-body">
        <div class="d-flex align-items-center justify-content-between">
            <div>
                <span class="text-muted fs-12">Total Plays</span>
                <h4 class="mb-0 fw-semibold"><?php echo count($plays_data); ?></h4>
            </div>
            <div class="avatar avatar-lg bg-primary-transparent">
                <i class="ri-gamepad-line fs-24 text-primary"></i>
            </div>
        </div>
    </div>
</div>

<!-- Plays Grid -->
<?php if (!empty($plays_data)): ?>
<div class="plays-grid">
    <?php foreach ($plays_data as $play): ?>
    <div class="play-card" data-play-id="<?php echo $play['id']; ?>" data-short-play="<?php echo $play['short_play'] ?? 0; ?>">
        <div class="play-card-header">
            <div class="play-card-title">
                <?php echo htmlspecialchars($play['name']); ?>
            </div>
            <div class="play-card-id">Play ID: #<?php echo htmlspecialchars($play['id']); ?></div>
        </div>
        
        <!-- Badges Section -->
        <div class="d-flex flex-wrap gap-1 px-3 pt-3 justify-content-center">
            <?php 
                // Parse trigger mode and timing conditions
                $trigger_mode = 'any';
                if (!empty($play['tricker_on_perp'])) {
                    $trigger_data = json_decode($play['tricker_on_perp'], true);
                    $trigger_mode = $trigger_data['mode'] ?? 'any';
                }
                
                $timing_enabled = false;
                $timing_display = '';
                if (!empty($play['timing_conditions'])) {
                    $timing_data = json_decode($play['timing_conditions'], true);
                    if (!empty($timing_data['enabled'])) {
                        $timing_enabled = true;
                        $direction = $timing_data['price_direction'] === 'increase' ? '↑' : '↓';
                        $time_window = $timing_data['time_window_seconds'];
                        $threshold_decimal = $timing_data['price_change_threshold'];
                        $threshold_percent = $threshold_decimal * 100;
                        
                        if ($time_window < 60) {
                            $time_str = $time_window . 's';
                        } elseif ($time_window < 3600) {
                            $time_str = round($time_window / 60, 1) . 'm';
                        } else {
                            $time_str = round($time_window / 3600, 2) . 'h';
                        }
                        $timing_display = $direction . ' ' . number_format($threshold_percent, 2) . '% / ' . $time_str;
                    }
                }
            ?>
            
            <!-- Short/Long Badge -->
            <?php if (!empty($play['short_play'])): ?>
                <span class="badge bg-danger">SHORT</span>
            <?php else: ?>
                <span class="badge bg-success">LONG</span>
            <?php endif; ?>
            
            <!-- Trigger Mode Badge -->
            <?php if ($trigger_mode === 'short_only'): ?>
                <span class="badge bg-purple">SHORT WALLETS</span>
            <?php elseif ($trigger_mode === 'long_only'): ?>
                <span class="badge bg-success-transparent text-success">LONG WALLETS</span>
            <?php else: ?>
                <span class="badge bg-secondary-transparent text-muted">ANY WALLET</span>
            <?php endif; ?>
            
            <!-- Timing Badge -->
            <?php if ($timing_enabled): ?>
                <span class="badge bg-warning-transparent text-warning"><?php echo $timing_display; ?></span>
            <?php else: ?>
                <span class="badge bg-secondary-transparent text-muted">NO TIMING</span>
            <?php endif; ?>
        </div>
        
        <div class="play-card-body">
            <?php
                $wallets_found = null;
                $total_wallets = null;
                $filtered_wallets = null;
                $query_duration_seconds = null;

                if (!empty($play['play_log'])) {
                    $decoded = json_decode($play['play_log'], true);
                    if (json_last_error() === JSON_ERROR_NONE && is_array($decoded)) {
                        $cache_used = isset($decoded['cache']['used']) && $decoded['cache']['used'];
                        
                        if ($cache_used && isset($decoded['cache']['wallet_count'])) {
                            $total_wallets = (int) $decoded['cache']['wallet_count'];
                        } elseif (isset($decoded['wallets']['initial_count'])) {
                            $total_wallets = (int) $decoded['wallets']['initial_count'];
                        } elseif (isset($decoded['query']['row_count'])) {
                            $total_wallets = (int) $decoded['query']['row_count'];
                        }
                        
                        if (isset($decoded['bundle']['applied']) && $decoded['bundle']['applied']) {
                            if (isset($decoded['bundle']['filtered_count'])) {
                                $filtered_wallets = (int) $decoded['bundle']['filtered_count'];
                            }
                        } elseif (isset($decoded['wallets']['filtered_count'])) {
                            $filtered_wallets = (int) $decoded['wallets']['filtered_count'];
                        }
                        
                        $wallets_found = $filtered_wallets ?? $total_wallets;

                        if (isset($decoded['duration_ms'])) {
                            $query_duration_seconds = max(0, (float) $decoded['duration_ms']) / 1000;
                        } elseif (isset($decoded['query']['duration_ms'])) {
                            $query_duration_seconds = max(0, (float) $decoded['query']['duration_ms']) / 1000;
                        }
                    }
                }
            ?>
            <div class="play-card-description">
                <div class="play-card-description-text">
                    <?php echo nl2br(htmlspecialchars($play['description'])); ?>
                </div>
                <?php if ($wallets_found !== null || $query_duration_seconds !== null): ?>
                <div class="play-log-summary">
                    <?php if ($wallets_found !== null): ?>
                    <div class="play-log-item">
                        <span class="play-log-label">Wallets <?php echo $filtered_wallets !== null && $total_wallets !== null && $filtered_wallets !== $total_wallets ? 'Active' : 'Found'; ?></span>
                        <span class="play-log-value">
                            <?php if ($filtered_wallets !== null && $total_wallets !== null && $filtered_wallets !== $total_wallets): ?>
                                <?php echo number_format($filtered_wallets); ?> <span class="text-muted opacity-50">/ <?php echo number_format($total_wallets); ?></span>
                            <?php else: ?>
                                <?php echo number_format($wallets_found); ?>
                            <?php endif; ?>
                        </span>
                    </div>
                    <?php endif; ?>
                    <?php if ($query_duration_seconds !== null): ?>
                    <div class="play-log-item">
                        <span class="play-log-label">Query Runtime</span>
                        <span class="play-log-value"><?php echo number_format($query_duration_seconds, 2); ?>s</span>
                    </div>
                    <?php endif; ?>
                </div>
                <?php endif; ?>
            </div>
            
            <!-- Performance Metrics -->
            <div class="play-performance">
                <div class="play-performance-title">
                    <span id="perf-title-<?php echo $play['id']; ?>">All Time Performance</span>
                    <span class="play-performance-updated" id="updated-<?php echo $play['id']; ?>">--</span>
                </div>
                <div class="play-performance-grid" id="performance-<?php echo $play['id']; ?>">
                    <div class="play-performance-item">
                        <div class="play-performance-value neutral" id="perf-sum-<?php echo $play['id']; ?>">--</div>
                        <div class="play-performance-label">Total P/L</div>
                    </div>
                    <div class="play-performance-item">
                        <div class="play-performance-value neutral" id="perf-winloss-<?php echo $play['id']; ?>">
                            <span class="win-count">--</span> / <span class="loss-count">--</span>
                        </div>
                        <div class="play-performance-label">Win / Loss</div>
                        <div id="perf-active-<?php echo $play['id']; ?>" style="margin-top: 4px; font-size: 0.7rem; font-weight: 600;">
                            <span class="active-display text-muted">--</span>
                        </div>
                    </div>
                    <div class="play-performance-item">
                        <div class="play-performance-value neutral" id="perf-live-<?php echo $play['id']; ?>">--</div>
                        <div class="play-performance-label">No Gos</div>
                    </div>
                </div>
            </div>
        </div>
        
        <div class="play-card-footer">
            <button class="btn btn-sm btn-outline-secondary" onclick="viewDetails(event, <?php echo $play['id']; ?>)">
                <i class="ri-eye-line me-1"></i>View Trades
            </button>
            <button class="btn btn-sm btn-outline-success" onclick="duplicatePlay(event, <?php echo $play['id']; ?>)">
                <i class="ri-file-copy-line me-1"></i>Duplicate
            </button>
        </div>
    </div>
    <?php endforeach; ?>
</div>
<?php else: ?>
<div class="alert alert-info">
    <h5 class="alert-heading">No Plays Found</h5>
    <p class="mb-0">Create your first play to get started!</p>
</div>
<?php endif; ?>

<script>
    function toggleCreateForm() {
        const createForm = document.getElementById('createPlayForm');
        createForm.classList.toggle('hidden');
    }

    function addIncreaseRule() {
        const container = document.getElementById('increases-container');
        const ruleHtml = `
            <div class="tolerance-rule">
                <div>
                    <label class="tolerance-rule-label">Range From (PIPs)</label>
                    <input type="number" step="1" class="form-control-inline pip-input" name="increase_range_from[]" required oninput="updatePipConversion(this)">
                    <small class="pip-conversion"></small>
                </div>
                <div>
                    <label class="tolerance-rule-label">Range To (PIPs)</label>
                    <input type="number" step="1" class="form-control-inline pip-input" name="increase_range_to[]" required oninput="updatePipConversion(this)">
                    <small class="pip-conversion"></small>
                </div>
                <div>
                    <label class="tolerance-rule-label">Tolerance (PIPs)</label>
                    <input type="number" step="1" class="form-control-inline pip-input" name="increase_tolerance[]" required oninput="updatePipConversion(this)">
                    <small class="pip-conversion"></small>
                </div>
                <button type="button" class="btn btn-sm btn-danger" onclick="removeRule(this)">Remove</button>
            </div>
        `;
        container.insertAdjacentHTML('beforeend', ruleHtml);
    }

    function removeRule(button) {
        button.closest('.tolerance-rule').remove();
    }

    function updatePipConversion(input) {
        const pips = parseFloat(input.value) || 0;
        const decimal = pips / 10000;
        const percentage = pips / 100;
        const conversionText = input.parentElement.querySelector('.pip-conversion');
        if (conversionText) {
            conversionText.textContent = '= ' + decimal.toFixed(4) + ' (' + percentage.toFixed(2) + '%)';
        }
    }

    document.addEventListener('DOMContentLoaded', function() {
        document.querySelectorAll('.pip-input').forEach(input => {
            updatePipConversion(input);
            input.addEventListener('input', function() {
                updatePipConversion(this);
            });
        });
    });

    function convertPipsToDecimals(form) {
        form.querySelectorAll('.pip-input').forEach(input => {
            const pips = parseFloat(input.value) || 0;
            const decimal = pips / 10000;
            input.value = decimal;
        });
        return true;
    }

    function validateForm() {
        const sql = document.getElementById('find_wallets_sql').value.trim();
        if (!sql) {
            alert('Please enter a SQL query.');
            return false;
        }
        const form = document.querySelector('#createPlayForm form');
        convertPipsToDecimals(form);
        return true;
    }

    function viewDetails(event, playId) {
        event.stopPropagation();
        window.location.href = '/v2/goats/unique/?id=' + playId;
    }

    function changeTimeInterval() {
        const dropdown = document.getElementById('timeInterval');
        dropdown.style.opacity = '0.6';
        dropdown.style.pointerEvents = 'none';
        
        loadPlayMetrics().finally(() => {
            dropdown.style.opacity = '1';
            dropdown.style.pointerEvents = 'auto';
        });
    }

    async function loadPlayMetrics() {
        const playIds = <?php echo json_encode(array_column($plays_data, 'id')); ?>;
        const timeInterval = document.getElementById('timeInterval')?.value || '24';
        
        for (const playId of playIds) {
            const titleElement = document.getElementById('perf-title-' + playId);
            if (titleElement) {
                const titleText = timeInterval === 'all' ? 'All Time Performance' : timeInterval + 'h Performance';
                titleElement.textContent = titleText;
            }
        }
        
        try {
            const response = await fetch('/chart/plays/get_all_performance.php?hours=' + timeInterval);
            const result = await response.json();
            
            if (!result.success) {
                console.error('Error loading performance data:', result.error);
                return;
            }
            
            const now = new Date();
            const timeString = now.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
            
            for (const playId of playIds) {
                const data = result.plays[playId];
                if (!data) continue;
                
                const playCard = document.querySelector(`[data-play-id="${playId}"]`);
                const isShortPlay = playCard && playCard.getAttribute('data-short-play') == '1';
                
                document.getElementById('updated-' + playId).textContent = timeString;
                
                const sumPL = data.total_profit_loss;
                const sumElement = document.getElementById('perf-sum-' + playId);
                if (sumPL !== null && sumPL !== undefined) {
                    const arrow = sumPL > 0 ? '↑ ' : sumPL < 0 ? '↓ ' : '';
                    const sign = sumPL >= 0 ? '+' : '';
                    sumElement.textContent = arrow + sign + sumPL.toFixed(2) + '%';
                    
                    let colorClass;
                    if (isShortPlay) {
                        colorClass = sumPL > 0 ? 'negative' : sumPL < 0 ? 'positive' : 'neutral';
                    } else {
                        colorClass = sumPL > 0 ? 'positive' : sumPL < 0 ? 'negative' : 'neutral';
                    }
                    sumElement.className = 'play-performance-value ' + colorClass;
                } else {
                    sumElement.textContent = '--';
                    sumElement.className = 'play-performance-value neutral';
                }
                
                const winLossElement = document.getElementById('perf-winloss-' + playId);
                const winning = data.winning_trades || 0;
                const losing = data.losing_trades || 0;
                const winSpan = winLossElement.querySelector('.win-count');
                const lossSpan = winLossElement.querySelector('.loss-count');
                
                if (winSpan && lossSpan) {
                    winSpan.textContent = winning;
                    winSpan.style.color = winning > 0 ? 'rgb(var(--success-rgb))' : '';
                    lossSpan.textContent = losing;
                    lossSpan.style.color = losing > 0 ? 'rgb(var(--danger-rgb))' : '';
                }
                
                const activeElement = document.getElementById('perf-active-' + playId);
                if (activeElement) {
                    const activeTrades = data.active_trades || 0;
                    const avgProfit = data.active_avg_profit;
                    const activeDisplaySpan = activeElement.querySelector('.active-display');
                    
                    if (activeDisplaySpan) {
                        if (activeTrades > 0 && avgProfit !== null && avgProfit !== undefined) {
                            const profitFormatted = Math.abs(avgProfit).toFixed(1) + '%';
                            activeDisplaySpan.textContent = activeTrades + '/' + profitFormatted;
                            activeDisplaySpan.style.color = avgProfit > 0 ? 'rgb(var(--success-rgb))' : 'rgb(var(--danger-rgb))';
                        } else if (activeTrades > 0) {
                            activeDisplaySpan.textContent = activeTrades + '/0%';
                        } else {
                            activeDisplaySpan.textContent = '0';
                        }
                    }
                }
                
                const liveElement = document.getElementById('perf-live-' + playId);
                liveElement.textContent = data.total_no_gos || 0;
                liveElement.className = 'play-performance-value neutral';
            }
        } catch (error) {
            console.error('Error loading performance data:', error);
        }
    }

    if (<?php echo count($plays_data); ?> > 0) {
        loadPlayMetrics();
    }

    async function cleanupNoGos() {
        if (!confirm('This will delete all no_go trades older than 24 hours. Continue?')) {
            return;
        }
        
        const button = event.target.closest('button');
        button.disabled = true;
        const originalText = button.innerHTML;
        button.innerHTML = '<i class="ri-loader-4-line me-1"></i>Cleaning...';
        
        try {
            const response = await fetch('/chart/plays/cleanup_no_gos.php');
            const data = await response.json();
            
            if (data.success) {
                alert(data.message);
                loadPlayMetrics();
            } else {
                alert('Error cleaning up no-gos: ' + data.error);
            }
        } catch (error) {
            console.error('Error cleaning up no-gos:', error);
            alert('Error cleaning up no-gos. Please try again.');
        } finally {
            button.disabled = false;
            button.innerHTML = originalText;
        }
    }

    async function duplicatePlay(event, playId) {
        event.stopPropagation();
        
        const newName = prompt('Enter a name for the duplicated play:');
        if (!newName || newName.trim() === '') {
            return;
        }
        
        try {
            const response = await fetch('/chart/plays/duplicate.php', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded',
                },
                body: 'id=' + playId + '&new_name=' + encodeURIComponent(newName.trim())
            });
            const data = await response.json();
            
            if (data.success) {
                alert('Play duplicated successfully! New Play ID: ' + data.new_id);
                window.location.reload();
            } else {
                alert('Error duplicating play: ' + data.error);
            }
        } catch (error) {
            console.error('Error duplicating play:', error);
            alert('Error duplicating play. Please try again.');
        }
    }
</script>

<?php
$content = ob_get_clean();
$scripts = '';

// Include the base layout
include __DIR__ . '/../pages/layouts/base.php';
?>

