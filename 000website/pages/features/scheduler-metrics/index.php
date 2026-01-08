<?php
/**
 * Scheduler Metrics Dashboard
 * 
 * Monitors job execution times and highlights slow-running jobs.
 * Shows average execution duration vs expected interval for each job.
 */

// --- Database API Client ---
require_once __DIR__ . '/../../../includes/config.php';
require_once __DIR__ . '/../../../includes/DatabaseClient.php';
$db = new DatabaseClient(DATABASE_API_URL);
$api_available = $db->isAvailable();

// --- Base URL for template ---
$baseUrl = '';

$error_message = '';
$metrics_data = [];
$scheduler_status = null;

// Parameters - support minutes for short intervals
$minutes = isset($_GET['minutes']) ? (int)$_GET['minutes'] : 60;  // Default 60 minutes (1 hour)
$refresh = isset($_GET['refresh']) ? (int)$_GET['refresh'] : 30;  // Default 30s (API can be slow on Windows)

// Validate parameters
$minutes = max(5, min(1440, $minutes));  // 5 min to 24 hours
$hours = $minutes / 60;  // Convert to hours for API call
$refresh = max(10, min(120, $refresh));  // Min 10s refresh to avoid overloading

// Fetch data
if ($api_available) {
    // Get job execution metrics (pass hours as float for sub-hour intervals)
    $metrics_response = $db->getJobMetrics((float)$hours);
    if ($metrics_response && isset($metrics_response['jobs'])) {
        $metrics_data = $metrics_response['jobs'];
    }
    
    // Get scheduler status for uptime info
    $scheduler_status = $db->getSchedulerStatus();
} else {
    $error_message = "Website API is not available. Please start the API: python scheduler/website_api.py";
}

// Calculate summary stats
$total_jobs = count($metrics_data);
$slow_jobs = 0;
$healthy_jobs = 0;
$total_executions = 0;
$total_errors = 0;

foreach ($metrics_data as $job) {
    if ($job['is_slow']) {
        $slow_jobs++;
    } else {
        $healthy_jobs++;
    }
    $total_executions += $job['execution_count'] ?? 0;
    $total_errors += $job['error_count'] ?? 0;
}

// Sort jobs: slow jobs first, then by avg duration desc
uasort($metrics_data, function($a, $b) {
    if ($a['is_slow'] !== $b['is_slow']) {
        return $b['is_slow'] - $a['is_slow'];
    }
    return ($b['avg_duration_ms'] ?? 0) - ($a['avg_duration_ms'] ?? 0);
});

// --- Page Styles ---
ob_start();
?>
<style>
    .metrics-container {
        width: 100%;
    }
    
    .metrics-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        flex-wrap: wrap;
        gap: 1rem;
        margin-bottom: 1.5rem;
    }
    
    .metrics-title {
        display: flex;
        align-items: center;
        gap: 0.75rem;
    }
    
    .metrics-title h1 {
        font-size: 1.5rem;
        font-weight: 600;
        margin: 0;
    }
    
    .live-badge {
        display: inline-flex;
        align-items: center;
        gap: 0.4rem;
        padding: 0.3rem 0.7rem;
        background: rgba(var(--success-rgb), 0.15);
        color: rgb(var(--success-rgb));
        border-radius: 20px;
        font-size: 0.7rem;
        font-weight: 600;
        text-transform: uppercase;
    }
    
    .live-badge .pulse {
        width: 8px;
        height: 8px;
        background: rgb(var(--success-rgb));
        border-radius: 50%;
        animation: pulse 1.5s infinite;
    }
    
    @keyframes pulse {
        0%, 100% { opacity: 1; transform: scale(1); }
        50% { opacity: 0.5; transform: scale(1.2); }
    }
    
    .filter-bar {
        display: flex;
        align-items: center;
        gap: 1rem;
        flex-wrap: wrap;
    }
    
    .filter-group {
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    
    .filter-group label {
        font-size: 0.75rem;
        color: var(--text-muted);
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    .filter-group select {
        background: var(--custom-white);
        border: 1px solid var(--default-border);
        border-radius: 4px;
        padding: 0.35rem 0.6rem;
        font-size: 0.85rem;
        color: var(--default-text-color);
        cursor: pointer;
    }
    
    .filter-group select option {
        background: var(--custom-white);
        color: var(--default-text-color);
    }
    
    .stats-grid {
        display: grid;
        grid-template-columns: repeat(5, 1fr);
        gap: 1rem;
        margin-bottom: 1.5rem;
    }
    
    @media (max-width: 992px) {
        .stats-grid {
            grid-template-columns: repeat(3, 1fr);
        }
    }
    
    @media (max-width: 576px) {
        .stats-grid {
            grid-template-columns: repeat(2, 1fr);
        }
    }
    
    .stat-card {
        background: var(--custom-white);
        border: 1px solid var(--default-border);
        border-radius: 0.5rem;
        padding: 1rem;
        text-align: center;
    }
    
    .stat-card .label {
        font-size: 0.65rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        color: var(--text-muted);
        margin-bottom: 0.3rem;
    }
    
    .stat-card .value {
        font-size: 1.4rem;
        font-weight: 700;
        color: var(--default-text-color);
    }
    
    .stat-card .value.success {
        color: rgb(var(--success-rgb));
    }
    
    .stat-card .value.warning {
        color: rgb(var(--warning-rgb));
    }
    
    .stat-card .value.danger {
        color: rgb(var(--danger-rgb));
    }
    
    .jobs-table {
        font-size: 0.85rem;
        width: 100%;
    }
    
    .jobs-table th {
        font-size: 0.7rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        color: var(--text-muted);
        font-weight: 600;
        border-bottom: 2px solid var(--default-border);
        padding: 0.75rem 0.5rem;
        white-space: nowrap;
    }
    
    .jobs-table td {
        vertical-align: middle;
        padding: 0.75rem 0.5rem;
        border-bottom: 1px solid var(--default-border);
    }
    
    .jobs-table tbody tr {
        transition: all 0.15s ease;
    }
    
    .jobs-table tbody tr:hover {
        background: rgba(var(--primary-rgb), 0.05) !important;
    }
    
    .jobs-table tbody tr.slow-job {
        background: rgba(var(--danger-rgb), 0.08);
        border-left: 3px solid rgb(var(--danger-rgb));
    }
    
    .jobs-table tbody tr.warning-job {
        background: rgba(var(--warning-rgb), 0.08);
        border-left: 3px solid rgb(var(--warning-rgb));
    }
    
    .jobs-table tbody tr.healthy-job {
        border-left: 3px solid rgb(var(--success-rgb));
    }
    
    .job-name {
        font-family: 'SF Mono', 'Monaco', 'Consolas', monospace;
        font-size: 0.85rem;
        font-weight: 600;
    }
    
    .duration-value {
        font-family: 'SF Mono', 'Monaco', 'Consolas', monospace;
        font-size: 0.85rem;
    }
    
    .duration-value.fast {
        color: rgb(var(--success-rgb));
    }
    
    .duration-value.normal {
        color: var(--default-text-color);
    }
    
    .duration-value.slow {
        color: rgb(var(--warning-rgb));
    }
    
    .duration-value.critical {
        color: rgb(var(--danger-rgb));
    }
    
    .status-badge {
        display: inline-block;
        padding: 0.2rem 0.5rem;
        border-radius: 4px;
        font-size: 0.65rem;
        font-weight: 600;
        text-transform: uppercase;
    }
    
    .status-badge.healthy {
        background: rgba(var(--success-rgb), 0.15);
        color: rgb(var(--success-rgb));
    }
    
    .status-badge.warning {
        background: rgba(var(--warning-rgb), 0.15);
        color: rgb(var(--warning-rgb));
    }
    
    .status-badge.slow {
        background: rgba(var(--danger-rgb), 0.15);
        color: rgb(var(--danger-rgb));
    }
    
    .progress-bar-container {
        width: 100%;
        height: 6px;
        background: rgba(255,255,255,0.1);
        border-radius: 3px;
        overflow: hidden;
    }
    
    .progress-bar-fill {
        height: 100%;
        border-radius: 3px;
        transition: width 0.3s ease;
    }
    
    .progress-bar-fill.healthy {
        background: rgb(var(--success-rgb));
    }
    
    .progress-bar-fill.warning {
        background: rgb(var(--warning-rgb));
    }
    
    .progress-bar-fill.critical {
        background: rgb(var(--danger-rgb));
    }
    
    .sparkline-container {
        width: 120px;
        height: 30px;
        display: flex;
        align-items: flex-end;
        gap: 1px;
    }
    
    .sparkline-bar {
        flex: 1;
        min-width: 2px;
        background: rgba(var(--primary-rgb), 0.6);
        border-radius: 1px 1px 0 0;
        transition: height 0.2s ease;
    }
    
    .sparkline-bar.error {
        background: rgba(var(--danger-rgb), 0.8);
    }
    
    .countdown-bar {
        height: 3px;
        background: rgba(var(--primary-rgb), 0.2);
        border-radius: 2px;
        margin-top: 1rem;
        overflow: hidden;
    }
    
    .countdown-bar .progress {
        height: 100%;
        background: rgb(var(--primary-rgb));
        width: 100%;
        animation: countdown <?php echo $refresh; ?>s linear infinite;
    }
    
    @keyframes countdown {
        from { width: 100%; }
        to { width: 0%; }
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
    
    .empty-state {
        text-align: center;
        padding: 4rem 2rem;
        border: 1px dashed var(--default-border);
        border-radius: 0.5rem;
    }
    
    .scheduler-uptime {
        font-size: 0.75rem;
        color: rgba(255,255,255,0.6);
    }
</style>
<?php
$styles = ob_get_clean();

// --- Page Content ---
ob_start();
?>

<!-- API Status Badge -->
<div class="api-status-badge" style="background: <?php echo $api_available ? 'rgb(var(--success-rgb))' : 'rgb(var(--danger-rgb))'; ?>; color: white;">
    <?php echo $api_available ? 'API Connected' : 'API Disconnected'; ?>
</div>

<div class="metrics-container">
    
    <!-- Page Header -->
    <div class="d-flex align-items-center justify-content-between page-header-breadcrumb flex-wrap gap-2 mb-3">
        <div>
            <nav>
                <ol class="breadcrumb mb-1">
                    <li class="breadcrumb-item"><a href="<?php echo $baseUrl; ?>/">Dashboard</a></li>
                    <li class="breadcrumb-item"><a href="#">Features</a></li>
                    <li class="breadcrumb-item active" aria-current="page">Scheduler Metrics</li>
                </ol>
            </nav>
        </div>
    </div>
    
    <!-- Metrics Header -->
    <div class="metrics-header">
        <div class="metrics-title">
            <h1>Scheduler Metrics</h1>
            <span class="live-badge">
                <span class="pulse"></span>
                Live
            </span>
            <span class="scheduler-uptime" id="schedulerUptime">Uptime: -</span>
        </div>
        
        <form method="GET" action="" class="filter-bar">
            <div class="filter-group">
                <label for="minutes">History</label>
                <select name="minutes" id="minutes" onchange="this.form.submit()">
                    <option value="5" <?php echo $minutes === 5 ? 'selected' : ''; ?>>5 min</option>
                    <option value="15" <?php echo $minutes === 15 ? 'selected' : ''; ?>>15 min</option>
                    <option value="30" <?php echo $minutes === 30 ? 'selected' : ''; ?>>30 min</option>
                    <option value="60" <?php echo $minutes === 60 ? 'selected' : ''; ?>>1 hour</option>
                    <option value="120" <?php echo $minutes === 120 ? 'selected' : ''; ?>>2 hours</option>
                    <option value="360" <?php echo $minutes === 360 ? 'selected' : ''; ?>>6 hours</option>
                    <option value="720" <?php echo $minutes === 720 ? 'selected' : ''; ?>>12 hours</option>
                    <option value="1440" <?php echo $minutes === 1440 ? 'selected' : ''; ?>>24 hours</option>
                </select>
            </div>
            
            <div class="filter-group">
                <label for="refresh">Refresh</label>
                <select name="refresh" id="refresh" onchange="this.form.submit()">
                    <option value="10" <?php echo $refresh === 10 ? 'selected' : ''; ?>>10s</option>
                    <option value="30" <?php echo $refresh === 30 ? 'selected' : ''; ?>>30s</option>
                    <option value="60" <?php echo $refresh === 60 ? 'selected' : ''; ?>>60s</option>
                    <option value="120" <?php echo $refresh === 120 ? 'selected' : ''; ?>>2min</option>
                </select>
            </div>
        </form>
    </div>
    
    <!-- Messages -->
    <?php if ($error_message): ?>
    <div class="alert alert-danger alert-dismissible fade show" role="alert">
        <i class="ri-error-warning-line me-2"></i><?php echo htmlspecialchars($error_message); ?>
        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    </div>
    <?php endif; ?>
    
    <!-- Stats Grid -->
    <div class="stats-grid">
        <div class="stat-card">
            <div class="label">Total Jobs</div>
            <div class="value"><?php echo number_format($total_jobs); ?></div>
        </div>
        <div class="stat-card">
            <div class="label">Healthy</div>
            <div class="value success"><?php echo number_format($healthy_jobs); ?></div>
        </div>
        <div class="stat-card">
            <div class="label">Slow Jobs</div>
            <div class="value <?php echo $slow_jobs > 0 ? 'danger' : 'success'; ?>"><?php echo number_format($slow_jobs); ?></div>
        </div>
        <div class="stat-card">
            <div class="label">Total Executions</div>
            <div class="value"><?php echo number_format($total_executions); ?></div>
        </div>
        <div class="stat-card">
            <div class="label">Errors</div>
            <div class="value <?php echo $total_errors > 0 ? 'warning' : 'success'; ?>"><?php echo number_format($total_errors); ?></div>
        </div>
    </div>
    
    <!-- Countdown bar -->
    <div class="countdown-bar">
        <div class="progress"></div>
    </div>
    
    <!-- Jobs Table -->
    <?php if (empty($metrics_data)): ?>
    <div class="empty-state mt-3">
        <div class="mb-3">
            <i class="ri-timer-line text-muted" style="font-size: 3rem;"></i>
        </div>
        <h4 class="text-muted">No metrics data available</h4>
        <p class="text-muted mb-0">Job execution metrics will appear here once the scheduler starts running jobs.</p>
        <p class="text-muted mb-0 mt-2">Start the scheduler: <code>python scheduler/master.py</code></p>
    </div>
    <?php else: ?>
    <div class="card custom-card mt-3">
        <div class="card-header">
            <div class="card-title">
                <i class="ri-dashboard-3-line me-2"></i>Job Execution Metrics
            </div>
            <div class="ms-auto">
                <span class="badge bg-info-transparent">Last <?php 
                    if ($minutes < 60) {
                        echo $minutes . ' min';
                    } else {
                        $h = $minutes / 60;
                        echo $h . ' hour' . ($h > 1 ? 's' : '');
                    }
                ?></span>
            </div>
        </div>
        <div class="card-body p-0">
            <div class="table-responsive">
                <table class="table jobs-table mb-0">
                    <thead>
                        <tr>
                            <th>Job ID</th>
                            <th>Status</th>
                            <th>Avg Duration</th>
                            <th>Max Duration</th>
                            <th>Expected Interval</th>
                            <th>Usage</th>
                            <th>Executions</th>
                            <th>Errors</th>
                            <th>Recent Activity</th>
                        </tr>
                    </thead>
                    <tbody>
                        <?php foreach ($metrics_data as $job_id => $job): 
                            $avg_ms = $job['avg_duration_ms'] ?? 0;
                            $max_ms = $job['max_duration_ms'] ?? 0;
                            $expected_ms = $job['expected_interval_ms'] ?? 60000;
                            $is_slow = $job['is_slow'] ?? false;
                            
                            // Calculate usage percentage
                            $usage_pct = ($expected_ms > 0) ? min(100, ($avg_ms / $expected_ms) * 100) : 0;
                            
                            // Determine status
                            $status_class = 'healthy';
                            $row_class = 'healthy-job';
                            if ($usage_pct > 100) {
                                $status_class = 'slow';
                                $row_class = 'slow-job';
                            } elseif ($usage_pct > 80) {
                                $status_class = 'warning';
                                $row_class = 'warning-job';
                            }
                            
                            // Format durations
                            $avg_display = $avg_ms < 1000 ? round($avg_ms, 1) . ' ms' : round($avg_ms / 1000, 2) . ' s';
                            $max_display = $max_ms < 1000 ? round($max_ms, 1) . ' ms' : round($max_ms / 1000, 2) . ' s';
                            
                            // Format expected interval
                            if ($expected_ms < 1000) {
                                $expected_display = $expected_ms . ' ms';
                            } elseif ($expected_ms < 60000) {
                                $expected_display = ($expected_ms / 1000) . ' s';
                            } elseif ($expected_ms < 3600000) {
                                $expected_display = ($expected_ms / 60000) . ' min';
                            } else {
                                $expected_display = ($expected_ms / 3600000) . ' hr';
                            }
                            
                            // Get recent executions for sparkline
                            $recent = $job['recent_executions'] ?? [];
                        ?>
                        <tr class="<?php echo $row_class; ?>">
                            <td>
                                <span class="job-name"><?php echo htmlspecialchars($job_id); ?></span>
                            </td>
                            <td>
                                <span class="status-badge <?php echo $status_class; ?>">
                                    <?php echo $status_class === 'slow' ? 'SLOW' : ($status_class === 'warning' ? 'WARNING' : 'OK'); ?>
                                </span>
                            </td>
                            <td>
                                <span class="duration-value <?php echo $status_class === 'slow' ? 'critical' : ($status_class === 'warning' ? 'slow' : 'fast'); ?>">
                                    <?php echo $avg_display; ?>
                                </span>
                            </td>
                            <td>
                                <span class="duration-value normal">
                                    <?php echo $max_display; ?>
                                </span>
                            </td>
                            <td>
                                <span class="text-muted"><?php echo $expected_display; ?></span>
                            </td>
                            <td style="width: 120px;">
                                <div class="d-flex align-items-center gap-2">
                                    <div class="progress-bar-container">
                                        <div class="progress-bar-fill <?php echo $status_class === 'slow' ? 'critical' : ($status_class === 'warning' ? 'warning' : 'healthy'); ?>" 
                                             style="width: <?php echo min(100, $usage_pct); ?>%"></div>
                                    </div>
                                    <span class="text-muted" style="font-size: 0.7rem; min-width: 35px;">
                                        <?php echo round($usage_pct); ?>%
                                    </span>
                                </div>
                            </td>
                            <td>
                                <span class="badge bg-secondary-transparent">
                                    <?php echo number_format($job['execution_count'] ?? 0); ?>
                                </span>
                            </td>
                            <td>
                                <?php if (($job['error_count'] ?? 0) > 0): ?>
                                <span class="badge bg-danger-transparent">
                                    <?php echo number_format($job['error_count']); ?>
                                </span>
                                <?php else: ?>
                                <span class="text-muted">0</span>
                                <?php endif; ?>
                            </td>
                            <td>
                                <?php if (!empty($recent)): ?>
                                <div class="sparkline-container" title="Recent <?php echo count($recent); ?> executions">
                                    <?php 
                                    // Take last 20 for sparkline
                                    $sparkline_data = array_slice($recent, 0, 20);
                                    $max_duration = max(array_column($sparkline_data, 'duration_ms') ?: [1]);
                                    foreach ($sparkline_data as $exec): 
                                        $height_pct = ($exec['duration_ms'] / $max_duration) * 100;
                                        $is_error = ($exec['status'] ?? '') === 'error';
                                    ?>
                                    <div class="sparkline-bar <?php echo $is_error ? 'error' : ''; ?>" 
                                         style="height: <?php echo max(10, $height_pct); ?>%"
                                         title="<?php echo round($exec['duration_ms'], 1); ?> ms"></div>
                                    <?php endforeach; ?>
                                </div>
                                <?php else: ?>
                                <span class="text-muted">-</span>
                                <?php endif; ?>
                            </td>
                        </tr>
                        <?php endforeach; ?>
                    </tbody>
                </table>
            </div>
        </div>
    </div>
    <?php endif; ?>
    
</div>

<script>
    // Auto-refresh
    const REFRESH_SECONDS = <?php echo $refresh; ?>;
    
    setTimeout(function() {
        location.reload();
    }, REFRESH_SECONDS * 1000);
    
    // Update scheduler uptime
    <?php 
    // Validate scheduler_started is a non-empty string that looks like a date
    $valid_scheduler_started = false;
    $scheduler_started_value = '';
    if ($scheduler_status && isset($scheduler_status['scheduler_started']) && !empty($scheduler_status['scheduler_started'])) {
        $scheduler_started_value = $scheduler_status['scheduler_started'];
        // Basic validation: must contain digits and common date separators
        if (preg_match('/\d{4}[-\/]\d{2}[-\/]\d{2}/', $scheduler_started_value)) {
            $valid_scheduler_started = true;
        }
    }
    ?>
    <?php if ($valid_scheduler_started): ?>
    const schedulerStarted = '<?php echo htmlspecialchars($scheduler_started_value, ENT_QUOTES, 'UTF-8'); ?>';
    
    function updateUptime() {
        const uptimeEl = document.getElementById('schedulerUptime');
        if (!uptimeEl || !schedulerStarted) {
            if (uptimeEl) uptimeEl.textContent = 'Uptime: -';
            return;
        }
        
        // Parse scheduler start time as UTC
        let startedUTC;
        try {
            if (schedulerStarted.includes('T')) {
                startedUTC = schedulerStarted.endsWith('Z') 
                    ? new Date(schedulerStarted).getTime()
                    : new Date(schedulerStarted + 'Z').getTime();
            } else {
                startedUTC = new Date(schedulerStarted.replace(' ', 'T') + 'Z').getTime();
            }
            
            // Validate the parsed date
            if (isNaN(startedUTC)) {
                uptimeEl.textContent = 'Uptime: -';
                return;
            }
        } catch (e) {
            uptimeEl.textContent = 'Uptime: -';
            return;
        }
        
        // Get current UTC time
        const now = new Date();
        const nowUTC = Date.UTC(
            now.getUTCFullYear(),
            now.getUTCMonth(),
            now.getUTCDate(),
            now.getUTCHours(),
            now.getUTCMinutes(),
            now.getUTCSeconds()
        );
        
        const uptimeSeconds = Math.floor((nowUTC - startedUTC) / 1000);
        
        // Validate uptime is reasonable (not negative or NaN)
        if (isNaN(uptimeSeconds) || uptimeSeconds < 0) {
            uptimeEl.textContent = 'Uptime: -';
            return;
        }
        
        let uptimeText = 'Uptime: ';
        if (uptimeSeconds < 60) {
            uptimeText += uptimeSeconds + 's';
        } else if (uptimeSeconds < 3600) {
            uptimeText += Math.floor(uptimeSeconds / 60) + 'm ' + (uptimeSeconds % 60) + 's';
        } else if (uptimeSeconds < 86400) {
            const hours = Math.floor(uptimeSeconds / 3600);
            const mins = Math.floor((uptimeSeconds % 3600) / 60);
            uptimeText += hours + 'h ' + mins + 'm';
        } else {
            const days = Math.floor(uptimeSeconds / 86400);
            const hours = Math.floor((uptimeSeconds % 86400) / 3600);
            uptimeText += days + 'd ' + hours + 'h';
        }
        uptimeEl.textContent = uptimeText;
    }
    
    // Update immediately and every second
    updateUptime();
    setInterval(updateUptime, 1000);
    <?php else: ?>
    // Scheduler start time not available - show placeholder
    (function() {
        const uptimeEl = document.getElementById('schedulerUptime');
        if (uptimeEl) uptimeEl.textContent = 'Uptime: -';
    })();
    <?php endif; ?>
</script>

<?php
$content = ob_get_clean();
$scripts = '';

// Include the base layout
include __DIR__ . '/../../layouts/base.php';
?>

