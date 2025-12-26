<?php
/**
 * Goats (Plays) Page - Follow The Goat Trading Plays
 * Migrated to use DuckDBClient API
 */

// --- Load DuckDB Client ---
require_once __DIR__ . '/../includes/DuckDBClient.php';

// --- Base URL for v2 template ---
$rootFolder = basename($_SERVER['DOCUMENT_ROOT']);
$baseUrl = (isset($_SERVER['HTTPS']) && $_SERVER['HTTPS'] === 'on' ? 'https://' : 'http://') . $_SERVER['HTTP_HOST'] . dirname(dirname($_SERVER['SCRIPT_NAME']));

// --- Initialize API Client ---
$client = new DuckDBClient();

$plays_data = [];
$error_message = '';
$success_message = '';
$api_available = $client->isAvailable();

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

// --- Fetch plays from API ---
if ($api_available) {
    $result = $client->getPlays();
    if ($result && isset($result['plays'])) {
        $plays_data = $result['plays'];
    } else {
        $error_message = 'Failed to load plays from API';
    }
} else {
    $error_message = 'API server is not available. Please ensure master.py is running.';
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
    
    .api-status {
        display: inline-flex;
        align-items: center;
        gap: 0.5rem;
        padding: 0.25rem 0.75rem;
        border-radius: 0.25rem;
        font-size: 0.75rem;
        font-weight: 500;
    }
    
    .api-status.online {
        background: rgba(var(--success-rgb), 0.1);
        color: rgb(var(--success-rgb));
    }
    
    .api-status.offline {
        background: rgba(var(--danger-rgb), 0.1);
        color: rgb(var(--danger-rgb));
    }
    
    .api-status-dot {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        background: currentColor;
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
        <span class="api-status <?php echo $api_available ? 'online' : 'offline'; ?>">
            <span class="api-status-dot"></span>
            API <?php echo $api_available ? 'Online' : 'Offline'; ?>
        </span>
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
    <form id="createForm" onsubmit="return handleCreatePlay(event)">
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
            <button type="submit" class="btn btn-primary" id="createPlayBtn">Create Play</button>
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
                    $trigger_data = is_string($play['tricker_on_perp']) 
                        ? json_decode($play['tricker_on_perp'], true) 
                        : $play['tricker_on_perp'];
                    $trigger_mode = $trigger_data['mode'] ?? 'any';
                }
                
                $timing_enabled = false;
                $timing_display = '';
                if (!empty($play['timing_conditions'])) {
                    $timing_data = is_string($play['timing_conditions']) 
                        ? json_decode($play['timing_conditions'], true) 
                        : $play['timing_conditions'];
                    if (!empty($timing_data['enabled'])) {
                        $timing_enabled = true;
                        $direction = ($timing_data['price_direction'] ?? 'increase') === 'increase' ? '↑' : '↓';
                        $time_window = $timing_data['time_window_seconds'] ?? 0;
                        $threshold_decimal = $timing_data['price_change_threshold'] ?? 0;
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
            <div class="play-card-description">
                <div class="play-card-description-text">
                    <?php echo nl2br(htmlspecialchars($play['description'] ?? '')); ?>
                </div>
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
    // API Base URL - uses PHP proxy to reach Flask API on server
    const API_BASE = '<?php echo dirname($_SERVER["SCRIPT_NAME"]); ?>/../api/proxy.php';
    
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

    function buildSellLogic(form) {
        const decreases = [];
        const increases = [];
        
        // Process decreases
        const decreaseTolerance = form.querySelectorAll('[name="decrease_tolerance[]"]');
        const decreaseFrom = form.querySelectorAll('[name="decrease_range_from[]"]');
        const decreaseTo = form.querySelectorAll('[name="decrease_range_to[]"]');
        
        for (let i = 0; i < decreaseTolerance.length; i++) {
            const pips = parseFloat(decreaseTolerance[i].value) || 0;
            decreases.push({
                range_from: parseFloat(decreaseFrom[i]?.value || -999999) / 10000,
                range_to: parseFloat(decreaseTo[i]?.value || 0) / 10000,
                tolerance: pips / 10000
            });
        }
        
        // Process increases
        const increaseTolerance = form.querySelectorAll('[name="increase_tolerance[]"]');
        const increaseFrom = form.querySelectorAll('[name="increase_range_from[]"]');
        const increaseTo = form.querySelectorAll('[name="increase_range_to[]"]');
        
        for (let i = 0; i < increaseTolerance.length; i++) {
            increases.push({
                range_from: parseFloat(increaseFrom[i].value) / 10000,
                range_to: parseFloat(increaseTo[i].value) / 10000,
                tolerance: parseFloat(increaseTolerance[i].value) / 10000
            });
        }
        
        return {
            tolerance_rules: {
                decreases: decreases,
                increases: increases
            }
        };
    }

    async function handleCreatePlay(event) {
        event.preventDefault();
        
        const form = event.target;
        const submitBtn = document.getElementById('createPlayBtn');
        submitBtn.disabled = true;
        submitBtn.innerHTML = '<i class="ri-loader-4-line me-1"></i>Creating...';
        
        try {
            const data = {
                name: form.querySelector('[name="name"]').value,
                description: form.querySelector('[name="description"]').value,
                find_wallets_sql: form.querySelector('[name="find_wallets_sql"]').value,
                sell_logic: buildSellLogic(form),
                max_buys_per_cycle: parseInt(form.querySelector('[name="max_buys_per_cycle"]').value)
            };
            
            const response = await fetch(API_BASE + '/plays', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(data)
            });
            
            const result = await response.json();
            
            if (result.success) {
                window.location.href = '?success=1';
            } else {
                alert('Error creating play: ' + (result.error || 'Unknown error'));
            }
        } catch (error) {
            console.error('Error creating play:', error);
            alert('Error creating play. Please try again.');
        } finally {
            submitBtn.disabled = false;
            submitBtn.innerHTML = 'Create Play';
        }
        
        return false;
    }

    function viewDetails(event, playId) {
        event.stopPropagation();
        window.location.href = 'unique/?id=' + playId;
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
        const timeInterval = document.getElementById('timeInterval')?.value || 'all';
        
        for (const playId of playIds) {
            const titleElement = document.getElementById('perf-title-' + playId);
            if (titleElement) {
                const titleText = timeInterval === 'all' ? 'All Time Performance' : timeInterval + 'h Performance';
                titleElement.textContent = titleText;
            }
        }
        
        try {
            const response = await fetch(API_BASE + '/plays/performance?hours=' + timeInterval);
            const result = await response.json();
            
            if (!result.success) {
                console.error('Error loading performance data:', result.error);
                return;
            }
            
            const now = new Date();
            const timeString = now.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
            
            for (const playId of playIds) {
                // JSON keys are strings, convert playId to string for lookup
                const data = result.plays[String(playId)];
                if (!data) {
                    // No data means no trades - show zeros
                    document.getElementById('updated-' + playId).textContent = timeString;
                    const sumElement = document.getElementById('perf-sum-' + playId);
                    if (sumElement) {
                        sumElement.textContent = '+0.00%';
                        sumElement.className = 'play-performance-value neutral';
                    }
                    const winLossElement = document.getElementById('perf-winloss-' + playId);
                    if (winLossElement) {
                        const winSpan = winLossElement.querySelector('.win-count');
                        const lossSpan = winLossElement.querySelector('.loss-count');
                        if (winSpan) winSpan.textContent = '0';
                        if (lossSpan) lossSpan.textContent = '0';
                    }
                    const activeElement = document.getElementById('perf-active-' + playId);
                    if (activeElement) {
                        const activeDisplaySpan = activeElement.querySelector('.active-display');
                        if (activeDisplaySpan) activeDisplaySpan.textContent = '0';
                    }
                    const noGoElement = document.getElementById('perf-nogo-' + playId);
                    if (noGoElement) {
                        const noGoSpan = noGoElement.querySelector('.nogo-count');
                        if (noGoSpan) noGoSpan.textContent = '0';
                    }
                    continue;
                }
                
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
            const response = await fetch(API_BASE + '/buyins/cleanup_no_gos', {
                method: 'DELETE'
            });
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
            const response = await fetch(API_BASE + '/plays/' + playId + '/duplicate', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ new_name: newName.trim() })
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
