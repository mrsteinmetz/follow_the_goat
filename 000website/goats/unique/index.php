<?php
/**
 * Unique Play Details - Follow The Goat Trading Play
 * Ported from chart/plays/unique/index.php to v2 template system
 */

// --- Load Configuration from .env ---
$timing = [];
$timing['script_start'] = microtime(true);

require_once __DIR__ . '/../../../chart/config.php';
$timing['config_loaded'] = microtime(true);

// Get play ID from query parameter
$play_id = (int)($_GET['id'] ?? 0);

if ($play_id <= 0) {
    header('Location: ../index.php?error=' . urlencode('Invalid play ID'));
    exit;
}

$is_restricted_play = ($play_id === 46);

// --- Base URL for v2 template ---
$rootFolder = basename($_SERVER['DOCUMENT_ROOT']);
$baseUrl = (isset($_SERVER['HTTPS']) && $_SERVER['HTTPS'] === 'on' ? 'https://' : 'http://') . $_SERVER['HTTP_HOST'] . dirname(dirname(dirname($_SERVER['SCRIPT_NAME'])));

// --- Data Fetching ---
$dsn = "mysql:host=$db_host;dbname=$db_name;charset=$db_charset";
$options = [
    PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
    PDO::ATTR_EMULATE_PREPARES   => false,
];

$play = null;
$trades = [];
$archived_trades = [];
$live_total_count = 0;
$archive_total_count = 0;
$error_message = '';
$success_message = '';
$chart_data = [
    'prices' => [],
    'trade_markers' => []
];

// Check for success/error messages
if (isset($_GET['success'])) {
    $success_message = 'Play updated successfully!';
}
if (isset($_GET['error'])) {
    $error_message = htmlspecialchars($_GET['error']);
}

try {
    $timing['before_db_connect'] = microtime(true);
    $pdo = new PDO($dsn, $db_user, $db_pass, $options);
    $timing['after_db_connect'] = microtime(true);

    // Fetch play details (including project_id)
    $timing['before_play_query'] = microtime(true);
    $stmt = $pdo->prepare("SELECT id, name, description, sorting, sell_logic, short_play, tricker_on_perp, timing_conditions, bundle_trades, project_id FROM solcatcher.follow_the_goat_plays WHERE id = :id");
    $stmt->execute(['id' => $play_id]);
    $play = $stmt->fetch();
    $timing['after_play_query'] = microtime(true);
    
    // Fetch all pattern config projects for the dropdown
    $pattern_projects = [];
    try {
        $projectStmt = $pdo->query("
            SELECT p.id, p.name, p.description, 
                   (SELECT COUNT(*) FROM pattern_config_filters f WHERE f.project_id = p.id AND f.is_active = 1) as filter_count
            FROM pattern_config_projects p
            ORDER BY p.name ASC
        ");
        $pattern_projects = $projectStmt->fetchAll();
    } catch (\PDOException $e) {
        $pattern_projects = [];
    }
    
    if (!$play) {
        header('Location: ../index.php?error=' . urlencode('Play not found'));
        exit;
    }

    // Handle AJAX Load More Request
    if (isset($_GET['ajax_load_more'])) {
        $offset = (int)($_GET['offset'] ?? 0);
        $table_type = $_GET['table'] ?? 'archive';
        
        if ($table_type === 'live') {
            $ajaxStmt = $pdo->prepare("
                SELECT 
                    id,
                    wallet_address,
                    tolerance,
                    block_timestamp,
                    price as org_price_entry,
                    followed_at,
                    our_entry_price,
                    our_exit_price,
                    our_exit_timestamp,
                    our_profit_loss,
                    our_status,
                    price_movements,
                    higest_price_reached,
                    current_price
                FROM solcatcher.follow_the_goat_buyins
                WHERE play_id = :play_id
                AND our_status != 'no_go'
                ORDER BY block_timestamp DESC
                LIMIT 100 OFFSET :offset
            ");
        } else {
            $ajaxStmt = $pdo->prepare("
                SELECT 
                    id,
                    wallet_address,
                    tolerance,
                    block_timestamp,
                    price as org_price_entry,
                    followed_at,
                    our_entry_price,
                    our_exit_price,
                    our_exit_timestamp,
                    our_profit_loss,
                    our_status,
                    price_movements,
                    higest_price_reached,
                    current_price,
                    potential_gains
                FROM solcatcher.follow_the_goat_buyins_archive
                WHERE play_id = :play_id
                ORDER BY followed_at DESC
                LIMIT 100 OFFSET :offset
            ");
        }
        $ajaxStmt->execute(['play_id' => $play_id, 'offset' => $offset]);
        $more_trades = $ajaxStmt->fetchAll();
        
        if (empty($more_trades)) {
            echo '';
            exit;
        }

        $status_badge_map = [
            'pending' => ['label' => 'active', 'class' => 'bg-info-transparent'],
            'sold' => ['label' => 'completed', 'class' => 'bg-success-transparent'],
            'cancelled' => ['label' => 'cancelled', 'class' => 'bg-danger-transparent'],
        ];
        
        foreach ($more_trades as $trade) {
            $status_key = strtolower($trade['our_status'] ?? '');
            if (isset($status_badge_map[$status_key])) {
                $status_badge = $status_badge_map[$status_key];
            } else {
                $safe_key = $status_key !== '' ? preg_replace('/[^a-z0-9_-]+/i', '', $status_key) : 'unknown';
                $status_badge = [
                    'label' => $status_key !== '' ? $status_key : 'unknown',
                    'class' => 'bg-secondary-transparent'
                ];
            }
            
            $profit_class = 'text-muted';
            if ($trade['our_profit_loss'] !== null) {
                if (!empty($play['short_play'])) {
                    $profit_class = $trade['our_profit_loss'] > 0 ? 'text-danger' : ($trade['our_profit_loss'] < 0 ? 'text-success' : 'text-muted');
                } else {
                    $profit_class = $trade['our_profit_loss'] > 0 ? 'text-success' : ($trade['our_profit_loss'] < 0 ? 'text-danger' : 'text-muted');
                }
            }

            $source_type = $table_type === 'live' ? 'live' : 'archive';
            echo '<tr onclick="viewTradeDetail(' . $trade['id'] . ', ' . $play_id . ', \'' . $source_type . '\')" style="cursor: pointer;" data-status="' . htmlspecialchars($status_key) . '">';
            echo '<td><a href="https://solscan.io/token/' . urlencode($trade['wallet_address']) . '" target="_blank" rel="noopener" class="text-primary" title="' . htmlspecialchars($trade['wallet_address']) . '" onclick="event.stopPropagation();"><code>' . substr(htmlspecialchars($trade['wallet_address']), 0, 12) . '...</code></a></td>';
            echo '<td class="text-center">' . ($trade['followed_at'] ? date('M d, H:i', strtotime($trade['followed_at'])) : '--') . '</td>';
            echo '<td class="text-center">' . ($trade['our_exit_timestamp'] ? date('M d, H:i', strtotime($trade['our_exit_timestamp'])) : '--') . '</td>';
            echo '<td class="text-center">' . ($trade['our_entry_price'] ? '$' . number_format($trade['our_entry_price'], 6) : '<span class="text-muted">--</span>') . '</td>';
            echo '<td class="text-center">' . ($trade['our_exit_price'] ? '$' . number_format($trade['our_exit_price'], 6) : '<span class="text-muted">--</span>') . '</td>';
            echo '<td class="text-center">' . ($trade['our_profit_loss'] !== null ? '<span class="fw-semibold ' . $profit_class . '">' . ($trade['our_profit_loss'] > 0 ? '+' : '') . number_format($trade['our_profit_loss'], 2) . '%</span>' : '<span class="text-muted">--</span>') . '</td>';
            
            // Add potential column only for archive
            if ($table_type !== 'live') {
                $potential_display = '--';
                $potential_class = 'text-muted';
                if (isset($trade['potential_gains']) && $trade['potential_gains'] !== null) {
                    $potential_val = (float)$trade['potential_gains'];
                    $potential_display = ($potential_val > 0 ? '+' : '') . number_format($potential_val, 2) . '%';
                    if (!empty($play['short_play'])) {
                        $potential_class = $potential_val > 0 ? 'text-danger' : ($potential_val < 0 ? 'text-success' : 'text-muted');
                    } else {
                        $potential_class = $potential_val > 0 ? 'text-success' : ($potential_val < 0 ? 'text-danger' : 'text-muted');
                    }
                }
                echo '<td class="text-center"><span class="fw-semibold ' . $potential_class . '">' . $potential_display . '</span></td>';
            }
            
            echo '<td class="text-center"><span class="badge ' . htmlspecialchars($status_badge['class']) . '">' . htmlspecialchars($status_badge['label']) . '</span></td>';
            echo '<td class="text-center"><button class="btn btn-sm btn-icon btn-primary-light" onclick="viewTradeDetail(' . $trade['id'] . ', ' . $play_id . ', \'' . $source_type . '\'); event.stopPropagation();" title="View Details"><i class="ri-eye-line"></i></button></td>';
            echo '</tr>';
        }
        exit;
    }

    // Fetch trades for this play (limited to 100)
    $timing['before_trades_query'] = microtime(true);
    $stmt = $pdo->prepare("
        SELECT 
            id,
            wallet_address,
            tolerance,
            block_timestamp,
            price as org_price_entry,
            followed_at,
            our_entry_price,
            our_exit_price,
            our_exit_timestamp,
            our_profit_loss,
            our_status,
            price_movements,
            higest_price_reached,
            current_price
        FROM solcatcher.follow_the_goat_buyins
        WHERE play_id = :play_id
        AND our_status != 'no_go'
        ORDER BY block_timestamp DESC
        LIMIT 100
    ");
    $stmt->execute(['play_id' => $play_id]);
    $trades = $stmt->fetchAll();
    
    // Get total count of live trades for "Load More" button
    $liveCountStmt = $pdo->prepare("SELECT COUNT(*) as total FROM solcatcher.follow_the_goat_buyins WHERE play_id = :play_id AND our_status != 'no_go'");
    $liveCountStmt->execute(['play_id' => $play_id]);
    $live_total_count = (int)$liveCountStmt->fetch()['total'];
    $timing['after_trades_query'] = microtime(true);
    $timing['trades_count'] = count($trades);

    // Fetch most recent completed trades from archive (limited to 100)
    $timing['before_archived_trades_query'] = microtime(true);
    $archivedStmt = $pdo->prepare("
        SELECT 
            id,
            wallet_address,
            tolerance,
            block_timestamp,
            price as org_price_entry,
            followed_at,
            our_entry_price,
            our_exit_price,
            our_exit_timestamp,
            our_profit_loss,
            our_status,
            price_movements,
            higest_price_reached,
            current_price,
            potential_gains
        FROM solcatcher.follow_the_goat_buyins_archive
        WHERE play_id = :play_id
        ORDER BY followed_at DESC
        LIMIT 100
    ");
    $archivedStmt->execute(['play_id' => $play_id]);
    $archived_trades = $archivedStmt->fetchAll();
    $timing['after_archived_trades_query'] = microtime(true);
    $timing['archived_trades_count'] = count($archived_trades);
    
    // Get total count of archived trades for "Load More" button
    $archiveCountStmt = $pdo->prepare("SELECT COUNT(*) as total FROM solcatcher.follow_the_goat_buyins_archive WHERE play_id = :play_id");
    $archiveCountStmt->execute(['play_id' => $play_id]);
    $archive_total_count = (int)$archiveCountStmt->fetch()['total'];

    // Fetch no_go trades for chart visualization
    $timing['before_nogo_trades_query'] = microtime(true);
    $nogoStmt = $pdo->prepare("
        SELECT 
            id,
            wallet_address,
            tolerance,
            block_timestamp,
            price as org_price_entry,
            followed_at,
            our_entry_price,
            our_exit_price,
            our_exit_timestamp,
            our_profit_loss,
            our_status,
            price_movements,
            higest_price_reached,
            current_price
        FROM solcatcher.follow_the_goat_buyins
        WHERE play_id = :play_id
        AND our_status = 'no_go'
        ORDER BY block_timestamp DESC
        LIMIT 50
    ");
    $nogoStmt->execute(['play_id' => $play_id]);
    $nogo_trades = $nogoStmt->fetchAll();
    $timing['after_nogo_trades_query'] = microtime(true);
    $timing['nogo_trades_count'] = count($nogo_trades);

    // Fetch date range for chart (limited to last 24 hours for performance)
    // Skip chart data for play_id = 46 (too many trades every 30s causes slow load)
    if (!$is_restricted_play && (!empty($trades) || !empty($archived_trades) || !empty($nogo_trades))) {
        $timing['before_date_range_query'] = microtime(true);
        
        // Limit chart to last 24 hours
        $end_time = new DateTime();
        $start_time = new DateTime();
        $start_time->sub(new DateInterval('PT24H'));
        
        // Add 10 minute buffer
        $start_time->sub(new DateInterval('PT10M'));
        $end_time->add(new DateInterval('PT10M'));
        
        $timing['after_date_range_query'] = microtime(true);
        
        // Chart always shows last 24 hours
        if (true) {
            
            $time_diff_seconds = $end_time->getTimestamp() - $start_time->getTimestamp();
            $target_points = 1500;
            $sample_interval_seconds = max(1, floor($time_diff_seconds / $target_points));
            
            $timing['before_price_query'] = microtime(true);
            $price_stmt = $pdo->prepare("
                SELECT 
                    AVG(value) as value,
                    FROM_UNIXTIME(time_bucket) as created_at
                FROM (
                    SELECT 
                        value,
                        FLOOR(UNIX_TIMESTAMP(created_at) / :interval1) * :interval2 as time_bucket
                    FROM price_points_archive 
                    WHERE coin_id = 5 
                    AND created_at BETWEEN :start_date AND :end_date
                ) as bucketed_data
                GROUP BY time_bucket
                ORDER BY time_bucket ASC
            ");
            $price_stmt->execute([
                'start_date' => $start_time->format('Y-m-d H:i:s'),
                'end_date' => $end_time->format('Y-m-d H:i:s'),
                'interval1' => $sample_interval_seconds,
                'interval2' => $sample_interval_seconds
            ]);
            $timing['after_price_query_exec'] = microtime(true);
            
            $price_count = 0;
            while ($row = $price_stmt->fetch()) {
                $chart_data['prices'][] = ['x' => $row['created_at'], 'y' => (float)$row['value']];
                $price_count++;
            }
            $timing['after_price_fetch'] = microtime(true);
            $timing['price_points_count'] = $price_count;
            $timing['sample_interval_seconds'] = $sample_interval_seconds;
            
            $timing['before_chart_trades_query'] = microtime(true);
            
            $chartArchivedStmt = $pdo->prepare("
                SELECT 
                    id, wallet_address, followed_at, our_entry_price, our_exit_price,
                    our_exit_timestamp, our_profit_loss, our_status
                FROM solcatcher.follow_the_goat_buyins_archive
                WHERE play_id = :play_id
                AND followed_at BETWEEN :start_date AND :end_date
                ORDER BY followed_at ASC
            ");
            $chartArchivedStmt->execute([
                'play_id' => $play_id,
                'start_date' => $start_time->format('Y-m-d H:i:s'),
                'end_date' => $end_time->format('Y-m-d H:i:s')
            ]);
            $chart_archived_trades = $chartArchivedStmt->fetchAll();
            
            $timing['after_chart_trades_query'] = microtime(true);
            $timing['chart_trades_count'] = count($chart_archived_trades);
            
            $timing['before_marker_processing'] = microtime(true);
            $chart_data['trades'] = [];
            
            $processTrade = function($trade) use (&$chart_data, $play) {
                if ($trade['followed_at']) {
                    $status = strtolower($trade['our_status']);
                    $color = '#10b981';
                    
                    if ($status === 'no_go') {
                        $color = '#f59e0b';
                    } elseif ($trade['our_profit_loss'] !== null) {
                        if (!empty($play['short_play'])) {
                            $color = $trade['our_profit_loss'] > 0 ? '#ef4444' : '#10b981';
                        } else {
                            $color = $trade['our_profit_loss'] > 0 ? '#10b981' : '#ef4444';
                        }
                    } elseif ($status === 'cancelled') {
                        $color = '#ef4444';
                    }
                    
                    $chart_data['trades'][] = [
                        'id' => $trade['id'],
                        'entry_time' => $trade['followed_at'],
                        'exit_time' => $trade['our_exit_timestamp'],
                        'entry_price' => $trade['our_entry_price'],
                        'exit_price' => $trade['our_exit_price'],
                        'status' => $trade['our_status'],
                        'profit_loss' => $trade['our_profit_loss'],
                        'color' => $color
                    ];
                    
                    if ($status !== 'no_go') {
                        $chart_data['trade_markers'][] = $trade['followed_at'];
                    }
                }
            };
            
            foreach ($chart_archived_trades as $trade) {
                $processTrade($trade);
            }
            
            $timing['after_marker_processing'] = microtime(true);
        }
    }
    
    $timing['before_json_processing'] = microtime(true);

} catch (\PDOException $e) {
    $error_message = "Database error: " . $e->getMessage();
}

// Parse sell_logic JSON
$tolerance_rules = ['increases' => [], 'decreases' => []];
if ($play && !empty($play['sell_logic'])) {
    $sell_logic = json_decode($play['sell_logic'], true);
    if ($sell_logic && isset($sell_logic['tolerance_rules'])) {
        $tolerance_rules = $sell_logic['tolerance_rules'];
    }
}

$json_chart_data = json_encode($chart_data);
$timing['after_json_processing'] = microtime(true);
$timing['page_ready'] = microtime(true);

// Calculate timing differences
$timing_report = [
    'Config Load' => ($timing['config_loaded'] - $timing['script_start']) * 1000
];

if (isset($timing['after_db_connect'], $timing['before_db_connect'])) {
    $timing_report['DB Connection'] = ($timing['after_db_connect'] - $timing['before_db_connect']) * 1000;
}

if (isset($timing['after_play_query'], $timing['before_play_query'])) {
    $timing_report['Play Query'] = ($timing['after_play_query'] - $timing['before_play_query']) * 1000;
}

if (isset($timing['after_trades_query'], $timing['before_trades_query'])) {
    $timing_report['Trades Query'] = ($timing['after_trades_query'] - $timing['before_trades_query']) * 1000;
}

if (isset($timing['trades_count'])) {
    $timing_report['Trades Count'] = $timing['trades_count'];
}

if (isset($timing['after_archived_trades_query'], $timing['before_archived_trades_query'])) {
    $timing_report['Archived Trades Query'] = ($timing['after_archived_trades_query'] - $timing['before_archived_trades_query']) * 1000;
}

if (isset($timing['archived_trades_count'])) {
    $timing_report['Archived Trades Count'] = $timing['archived_trades_count'];
}

if (isset($timing['after_nogo_trades_query'], $timing['before_nogo_trades_query'])) {
    $timing_report['NoGo Trades Query'] = ($timing['after_nogo_trades_query'] - $timing['before_nogo_trades_query']) * 1000;
}

if (isset($timing['nogo_trades_count'])) {
    $timing_report['NoGo Trades Count'] = $timing['nogo_trades_count'];
}

if (isset($timing['before_date_range_query'])) {
    $timing_report['Date Range Query'] = ($timing['after_date_range_query'] - $timing['before_date_range_query']) * 1000;
}

if (isset($timing['after_price_query_exec'], $timing['before_price_query'])) {
    $timing_report['Price Query Execute'] = ($timing['after_price_query_exec'] - $timing['before_price_query']) * 1000;
}

if (isset($timing['after_price_fetch'], $timing['after_price_query_exec'])) {
    $timing_report['Price Data Fetch'] = ($timing['after_price_fetch'] - $timing['after_price_query_exec']) * 1000;
}

if (isset($timing['price_points_count'])) {
    $timing_report['Price Points Count'] = $timing['price_points_count'];
}

if (isset($timing['sample_interval_seconds'])) {
    $timing_report['Sample Interval (seconds)'] = $timing['sample_interval_seconds'];
}

if (isset($timing['after_price_fetch'], $timing['before_price_query'])) {
    $timing_report['Total Price Processing'] = ($timing['after_price_fetch'] - $timing['before_price_query']) * 1000;
}

if (isset($timing['after_marker_processing'], $timing['before_marker_processing'])) {
    $timing_report['Marker Processing'] = ($timing['after_marker_processing'] - $timing['before_marker_processing']) * 1000;
}

if (isset($timing['after_json_processing'], $timing['before_json_processing'])) {
    $timing_report['JSON Processing'] = ($timing['after_json_processing'] - $timing['before_json_processing']) * 1000;
}

$timing_report['Total Page Time'] = ($timing['page_ready'] - $timing['script_start']) * 1000;

$json_timing_report = json_encode($timing_report);

// Calculate statistics
$statsStmt = $pdo->prepare("
    SELECT 
        COUNT(id) AS total_trades,
        SUM(potential_gains) as potential_gains,
        SUM(our_profit_loss) total_gain,
        SUM(CASE WHEN our_status = 'no_go' THEN 1 ELSE 0 END) AS no_go_count,
        SUM(CASE WHEN our_status = 'sold' THEN 1 ELSE 0 END) AS sold_count
    FROM solcatcher.follow_the_goat_buyins_archive 
    WHERE play_id = :play_id
");
$statsStmt->execute(['play_id' => $play_id]);
$stats = $statsStmt->fetch();

$activeStmt = $pdo->prepare("
    SELECT COUNT(id) AS active_count
    FROM solcatcher.follow_the_goat_buyins
    WHERE play_id = :play_id
    AND our_status = 'pending'
");
$activeStmt->execute(['play_id' => $play_id]);
$active_result = $activeStmt->fetch();

$total_trades = (int)($stats['total_trades'] ?? 0);
$active_trades = (int)($active_result['active_count'] ?? 0);
$completed_trades = (int)($stats['sold_count'] ?? 0);
$no_go_count = (int)($stats['no_go_count'] ?? 0);
$total_profit_loss = (float)($stats['total_gain'] ?? 0);
$total_potential_gains = (float)($stats['potential_gains'] ?? 0);

$status_badge_map = [
    'pending' => ['label' => 'active', 'class' => 'bg-info-transparent'],
    'sold' => ['label' => 'completed', 'class' => 'bg-success-transparent'],
    'cancelled' => ['label' => 'cancelled', 'class' => 'bg-danger-transparent'],
];

// --- Page Styles ---
ob_start();
?>
<style>
    .stats-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
        gap: 1rem;
    }
    
    .stat-card {
        background: var(--custom-white);
        padding: 1.25rem;
        border-radius: 0.5rem;
        border: 1px solid var(--default-border);
    }
    
    .stat-label {
        font-size: 0.75rem;
        color: var(--text-muted);
        margin-bottom: 0.5rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    .stat-value {
        font-size: 1.5rem;
        font-weight: 700;
        color: var(--default-text-color);
    }
    
    .stat-value.positive { color: rgb(var(--success-rgb)); }
    .stat-value.negative { color: rgb(var(--danger-rgb)); }
    
    .tolerance-grid {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 1.5rem;
    }
    
    @media (max-width: 768px) {
        .tolerance-grid {
            grid-template-columns: 1fr;
        }
    }
    
    .tolerance-card {
        background: var(--custom-white);
        border: 1px solid var(--default-border);
        border-radius: 0.5rem;
        overflow: hidden;
    }
    
    .tolerance-header {
        padding: 1rem 1.25rem;
        border-bottom: 2px solid rgb(var(--primary-rgb));
        background: rgba(var(--primary-rgb), 0.05);
    }
    
    .tolerance-header h3 {
        font-size: 1rem;
        font-weight: 600;
        margin: 0;
        color: var(--default-text-color);
    }
    
    .tolerance-header .subtitle {
        font-size: 0.8rem;
        color: var(--text-muted);
        margin-top: 0.25rem;
    }
    
    #tradeChart {
        min-height: 500px;
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
    }
    
    .hour-header-row td {
        background: rgba(var(--primary-rgb), 0.15) !important;
        color: rgb(var(--primary-rgb));
        font-weight: 600;
        font-size: 0.9rem;
    }
</style>
<?php
$styles = ob_get_clean();

// --- Page Content ---
ob_start();
?>

<!-- Page Header -->
<div class="d-flex align-items-center justify-content-between page-header-breadcrumb flex-wrap gap-2 mb-3">
    <div>
        <nav>
            <ol class="breadcrumb mb-1">
                <li class="breadcrumb-item"><a href="<?php echo $baseUrl; ?>/">Dashboard</a></li>
                <li class="breadcrumb-item"><a href="<?php echo $baseUrl; ?>/goats/">Goats</a></li>
                <li class="breadcrumb-item active" aria-current="page"><?php echo htmlspecialchars($play['name'] ?? 'Unknown'); ?></li>
            </ol>
        </nav>
        <h1 class="page-title fw-medium fs-18 mb-0"><?php echo htmlspecialchars($play['name']); ?></h1>
    </div>
    <div class="d-flex gap-2 align-items-center">
        <div class="d-flex align-items-center gap-2">
            <label for="playSorting" class="text-muted fs-12">Priority:</label>
            <select id="playSorting" class="form-select form-select-sm" style="width: auto;" onchange="updatePlaySorting(<?php echo $play_id; ?>, this.value)" <?php echo $is_restricted_play ? 'disabled title="Editing disabled for this play"' : ''; ?>>
                <?php for ($i = 1; $i <= 10; $i++): ?>
                    <option value="<?php echo $i; ?>" <?php echo ($play['sorting'] == $i) ? 'selected' : ''; ?>>
                        <?php echo $i; ?>
                    </option>
                <?php endfor; ?>
            </select>
        </div>
        <button id="editPlayBtn" class="btn btn-primary" onclick="toggleEditForm()" <?php echo $is_restricted_play ? 'disabled title="Editing disabled for this play"' : ''; ?>>
            <i class="ri-edit-line me-1"></i>Edit Play
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

<?php if ($is_restricted_play): ?>
<div class="alert alert-warning" role="alert">
    <i class="ri-error-warning-line me-1"></i> Editing and deleting are disabled for this play.
</div>
<?php endif; ?>

<!-- Play Info & Badges -->
<div class="card custom-card mb-3">
    <div class="card-body">
        <p class="text-muted mb-3"><?php echo htmlspecialchars($play['description']); ?></p>
        
        <!-- Badges Section -->
        <div class="d-flex flex-wrap gap-2 justify-content-center">
            <?php 
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
                <span class="badge bg-danger fs-12 fw-semibold">SHORT</span>
            <?php else: ?>
                <span class="badge bg-success fs-12 fw-semibold">LONG</span>
            <?php endif; ?>
            
            <!-- Trigger Mode Badge -->
            <?php if ($trigger_mode === 'short_only'): ?>
                <span class="badge bg-purple-transparent text-purple">SHORT WALLETS ONLY</span>
            <?php elseif ($trigger_mode === 'long_only'): ?>
                <span class="badge bg-success-transparent text-success">LONG WALLETS ONLY</span>
            <?php else: ?>
                <span class="badge bg-secondary-transparent text-muted">ANY WALLET</span>
            <?php endif; ?>
            
            <!-- Timing Badge -->
            <?php if ($timing_enabled): ?>
                <span class="badge bg-warning-transparent text-warning">⏱️ TIMING <?php echo $timing_display; ?></span>
            <?php else: ?>
                <span class="badge bg-secondary-transparent text-muted">⏱️ NO TIMING</span>
            <?php endif; ?>
        </div>
    </div>
</div>

<!-- Edit Play Form -->
<div class="form-container hidden" id="editPlayForm">
    <h3>Edit Play</h3>
    <form action="update_play.php" method="POST" onsubmit="return validateEditForm()">
        <input type="hidden" id="edit_play_id" name="play_id" value="<?php echo $play_id; ?>">
        
        <div class="mb-3">
            <label class="form-label" for="edit_name">Name *</label>
            <input type="text" class="form-control" id="edit_name" name="name" maxlength="60" required>
        </div>

        <div class="mb-3">
            <label class="form-label" for="edit_description">Description *</label>
            <textarea class="form-control" id="edit_description" name="description" maxlength="500" rows="3" required></textarea>
        </div>

        <div class="mb-3">
            <label class="form-label" for="edit_find_wallets_sql">Find Wallets SQL Query *</label>
            <textarea class="form-control" id="edit_find_wallets_sql" name="find_wallets_sql" rows="8" required placeholder="Enter SQL query to find wallets..."></textarea>
            <div class="form-text">This query will be validated before saving. <strong>Mandatory field:</strong> query must return <code>wallet_address</code></div>
        </div>

        <div class="mb-3">
            <label class="form-label" for="edit_project_ids">Pattern Config Projects</label>
            <select class="form-select" id="edit_project_ids" name="project_ids[]" multiple size="5" style="min-height: 120px;">
                <?php foreach ($pattern_projects as $project): ?>
                <option value="<?php echo $project['id']; ?>">
                    <?php echo htmlspecialchars($project['name']); ?> (<?php echo $project['filter_count']; ?> filters)
                </option>
                <?php endforeach; ?>
            </select>
            <div class="form-text">Hold Ctrl (Cmd on Mac) to select multiple projects for trade validation. Leave empty for no project filter.</div>
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
                <div class="tolerance-section">
                    <h4>Decreases</h4>
                    <div id="edit-decreases-container"></div>
                </div>

                <div class="tolerance-section">
                    <h4>Increases</h4>
                    <div id="edit-increases-container"></div>
                    <button type="button" class="btn btn-sm btn-success mt-2" onclick="addEditIncreaseRule()">+ Add Increase Rule</button>
                </div>
            </div>
        </div>

        <div class="mb-3">
            <label class="form-label" for="edit_max_buys_per_cycle">Max Buys Per Cycle *</label>
            <input type="number" class="form-control" id="edit_max_buys_per_cycle" name="max_buys_per_cycle" value="5" min="1" required style="max-width: 150px;">
        </div>

        <div class="mb-3">
            <div class="form-check">
                <input type="checkbox" class="form-check-input" id="edit_short_play" name="short_play" value="1">
                <label class="form-check-label" for="edit_short_play">Play is using SHORT play</label>
            </div>
            <div class="form-text">Enable this if the play profits from price decreases (short positions)</div>
        </div>

        <div class="mb-3">
            <label class="form-label" for="edit_trigger_on_perp">Trigger On Perpetual Type *</label>
            <select class="form-select" id="edit_trigger_on_perp" name="trigger_on_perp" required>
                <option value="any">Trigger on any wallet</option>
                <option value="short_only">Trigger only on wallets running SHORT plays</option>
                <option value="long_only">Trigger only on wallets running LONG plays</option>
            </select>
            <div class="form-text">Control which wallet types trigger this play based on their perpetual position</div>
        </div>

        <!-- Bundle Trades Section -->
        <div class="card custom-card mb-3">
            <div class="card-body">
                <div class="form-check mb-3">
                    <input type="checkbox" class="form-check-input" id="enable_bundle_trades" name="bundle_enabled" value="1">
                    <label class="form-check-label fw-semibold" for="enable_bundle_trades">Enable Bundle Trades</label>
                </div>
                <div id="bundle_trades_settings" style="display: none;">
                    <div class="row g-3">
                        <div class="col-md-6">
                            <label class="form-label">Number of Trades:</label>
                            <input type="number" class="form-control" id="bundle_num_trades" name="bundle_num_trades" min="1" step="1" placeholder="e.g., 3">
                        </div>
                        <div class="col-md-6">
                            <label class="form-label">Within Seconds:</label>
                            <input type="number" class="form-control" id="bundle_seconds" name="bundle_seconds" min="1" step="1" placeholder="e.g., 15">
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Cache Found Wallets Section -->
        <div class="card custom-card mb-3">
            <div class="card-body">
                <div class="form-check mb-3">
                    <input type="checkbox" class="form-check-input" id="enable_cashe_wallets" name="cashe_enabled" value="1">
                    <label class="form-check-label fw-semibold" for="enable_cashe_wallets">Cache Found Wallets</label>
                </div>
                <div id="cashe_wallets_settings" style="display: none;">
                    <div>
                        <label class="form-label">Cache Duration (seconds):</label>
                        <input type="number" class="form-control" id="cashe_seconds" name="cashe_seconds" min="1" step="1" placeholder="e.g., 300" style="max-width: 200px;">
                        <div class="form-text" id="cashe_time_display">How long to cache wallet results</div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Pattern Validator Settings Section -->
        <div class="card custom-card mb-3">
            <div class="card-body">
                <h6 class="fw-semibold mb-3">Pattern Validator Settings</h6>
                <div class="form-check mb-3">
                    <input type="checkbox" class="form-check-input" id="edit_pattern_validator_enable" name="pattern_validator_enable" value="1">
                    <label class="form-check-label fw-semibold" for="edit_pattern_validator_enable">Enable Pattern Validator</label>
                    <div class="form-text">When enabled, trades will be validated against pattern rules before execution</div>
                </div>
                <div class="form-check mb-3">
                    <input type="checkbox" class="form-check-input" id="edit_pattern_update_by_ai" name="pattern_update_by_ai" value="1">
                    <label class="form-check-label fw-semibold" for="edit_pattern_update_by_ai">AI Auto-Update Pattern Config</label>
                    <div class="form-text">Let AI automatically select the best performing filter projects</div>
                </div>
                <div id="ai_update_notice" class="alert alert-info mb-0" style="display: none;">
                    <i class="ri-robot-line me-2"></i>
                    <strong>AI Management Enabled:</strong> Projects will be automatically selected based on the best performing filters. The project selector above has been disabled.
                </div>
            </div>
        </div>

        <div class="form-actions">
            <button type="submit" class="btn btn-primary" <?php echo $is_restricted_play ? 'disabled' : ''; ?>>Update Play</button>
            <button type="button" class="btn btn-secondary" onclick="toggleEditForm()">Cancel</button>
            <button type="button" class="btn btn-danger" onclick="deletePlayFromEdit()" id="deletePlayBtn" <?php echo $is_restricted_play ? 'disabled' : ''; ?>>Delete Play</button>
        </div>
    </form>
</div>

<!-- Statistics Grid -->
<div class="stats-grid mb-3">
    <div class="stat-card">
        <div class="stat-label">Total Trades</div>
        <div class="stat-value"><?php echo $total_trades; ?></div>
    </div>
    <div class="stat-card">
        <div class="stat-label">Active Trades</div>
        <div class="stat-value"><?php echo $active_trades; ?></div>
    </div>
    <div class="stat-card">
        <div class="stat-label">Completed Trades</div>
        <div class="stat-value"><?php echo $completed_trades; ?></div>
    </div>
    <div class="stat-card">
        <div class="stat-label">No Go Trades</div>
        <div class="stat-value" style="color: rgb(var(--warning-rgb));"><?php echo $no_go_count; ?></div>
    </div>
    <div class="stat-card">
        <div class="stat-label">Sum Profit/Loss</div>
        <div class="stat-value <?php 
            if (!empty($play['short_play'])) {
                echo $total_profit_loss > 0 ? 'negative' : ($total_profit_loss < 0 ? 'positive' : '');
            } else {
                echo $total_profit_loss > 0 ? 'positive' : ($total_profit_loss < 0 ? 'negative' : '');
            }
        ?>">
            <?php echo $total_profit_loss > 0 ? '+' : ''; ?><?php echo number_format($total_profit_loss, 2); ?>%
        </div>
    </div>
    <div class="stat-card">
        <div class="stat-label">Total Potential</div>
        <div class="stat-value <?php 
            if (!empty($play['short_play'])) {
                echo $total_potential_gains > 0 ? 'negative' : ($total_potential_gains < 0 ? 'positive' : '');
            } else {
                echo $total_potential_gains > 0 ? 'positive' : ($total_potential_gains < 0 ? 'negative' : '');
            }
        ?>">
            <?php echo $total_potential_gains > 0 ? '+' : ''; ?><?php echo number_format($total_potential_gains, 2); ?>%
        </div>
    </div>
</div>

<!-- Price Chart (Last 24 Hours) -->
<?php if ($is_restricted_play): ?>
<div class="card custom-card mb-3">
    <div class="card-header">
        <div class="card-title">Price Chart</div>
    </div>
    <div class="card-body text-center py-5">
        <i class="ri-timer-line fs-48 text-warning mb-3 d-block"></i>
        <h5 class="text-warning">Chart Disabled for Performance</h5>
        <p class="text-muted mb-0">This play has trades every 30 seconds. The chart is disabled to improve page load speed.</p>
    </div>
</div>
<?php elseif (!empty($chart_data['prices'])): ?>
<div class="card custom-card mb-3">
    <div class="card-header">
        <div class="card-title">Price Chart <small class="text-muted fw-normal">(Last 24 Hours)</small></div>
        <div class="ms-auto d-flex gap-2">
            <span class="badge bg-primary-transparent"><?php echo count($chart_data['prices']); ?> price points</span>
            <span class="badge bg-secondary-transparent"><?php echo count($chart_data['trades']); ?> trades in range</span>
        </div>
    </div>
    <div class="card-body">
        <div class="d-flex gap-3 mb-3 fs-13">
            <span class="text-success"><span style="display: inline-block; width: 20px; height: 3px; background: rgb(var(--success-rgb)); vertical-align: middle; margin-right: 5px;"></span> Profitable</span>
            <span class="text-danger"><span style="display: inline-block; width: 20px; height: 3px; background: rgb(var(--danger-rgb)); vertical-align: middle; margin-right: 5px;"></span> Losing</span>
            <span class="text-warning"><span style="display: inline-block; width: 20px; height: 3px; background: rgb(var(--warning-rgb)); vertical-align: middle; margin-right: 5px;"></span> No Go</span>
        </div>
        <div id="tradeChart"></div>
    </div>
</div>
<?php else: ?>
<div class="card custom-card mb-3">
    <div class="card-header">
        <div class="card-title">Price Chart</div>
    </div>
    <div class="card-body text-center py-5">
        <i class="ri-line-chart-line fs-48 text-muted mb-3 d-block"></i>
        <p class="text-muted mb-0">No price data available for this play's trades.</p>
    </div>
</div>
<?php endif; ?>

<!-- Tolerance Rules Section -->
<div class="tolerance-grid mb-3">
    <!-- Increases Table -->
    <div class="tolerance-card">
        <div class="tolerance-header">
            <h3>Tolerance Above Entry Price</h3>
            <div class="subtitle">Increase</div>
        </div>
        <div class="table-responsive">
            <table class="table table-bordered mb-0">
                <thead>
                    <tr>
                        <th class="text-center">Range</th>
                        <th class="text-center">Tolerance</th>
                    </tr>
                </thead>
                <tbody>
                    <?php if (!empty($tolerance_rules['increases'])): ?>
                        <?php foreach ($tolerance_rules['increases'] as $rule): ?>
                            <tr>
                                <td class="text-center">
                                    <span class="text-success fw-semibold">
                                        <?php echo number_format($rule['range'][0] * 100, 2); ?>% to <?php echo number_format($rule['range'][1] * 100, 2); ?>%
                                    </span>
                                    <br>
                                    <small class="text-muted">
                                        (<?php echo number_format($rule['range'][0], 4); ?> to <?php echo number_format($rule['range'][1], 4); ?>)
                                    </small>
                                </td>
                                <td class="text-center">
                                    <?php echo number_format($rule['tolerance'] * 100, 3); ?>%
                                    <br>
                                    <small class="text-muted">(<?php echo number_format($rule['tolerance'], 4); ?>)</small>
                                </td>
                            </tr>
                        <?php endforeach; ?>
                    <?php else: ?>
                        <tr>
                            <td colspan="2" class="text-center text-muted">No rules defined</td>
                        </tr>
                    <?php endif; ?>
                </tbody>
            </table>
        </div>
    </div>

    <!-- Decreases Table -->
    <div class="tolerance-card">
        <div class="tolerance-header">
            <h3>Tolerance Below Entry Price</h3>
            <div class="subtitle">Decrease</div>
        </div>
        <div class="p-4 text-center">
            <?php if (!empty($tolerance_rules['decreases'])): ?>
                <?php $rule = $tolerance_rules['decreases'][0]; ?>
                <div class="fs-2 fw-bold text-danger mb-2">
                    <?php echo number_format($rule['tolerance'] * 100, 3); ?>%
                </div>
                <div class="text-muted">
                    (<?php echo number_format($rule['tolerance'], 4); ?>)
                </div>
            <?php else: ?>
                <div class="text-muted">No tolerance defined</div>
            <?php endif; ?>
        </div>
    </div>
</div>

<!-- Live Trades -->
<div class="card custom-card mb-3">
    <div class="card-header">
        <div class="card-title">Live Trades</div>
        <div class="ms-auto d-flex gap-2 align-items-center">
            <span class="badge bg-info-transparent">Showing <?php echo count($trades); ?> of <?php echo $live_total_count; ?></span>
        </div>
    </div>
    <div class="card-body">
        <?php if (empty($trades)): ?>
        <div class="text-center py-5">
            <i class="ri-bar-chart-line fs-48 text-muted mb-3 d-block"></i>
            <h5 class="text-muted">No Live Trades</h5>
            <p class="text-muted mb-0">This play doesn't have any live trades right now.</p>
        </div>
        <?php else: ?>
        <div class="table-responsive">
            <table class="table table-bordered text-nowrap">
                <thead>
                    <tr>
                        <th>Wallet Address</th>
                        <th class="text-center">Entered</th>
                        <th class="text-center">Exited</th>
                        <th class="text-center">Entry Price</th>
                        <th class="text-center">Exit Price</th>
                        <th class="text-center">Profit/Loss</th>
                        <th class="text-center">Status</th>
                        <th class="text-center">Details</th>
                    </tr>
                </thead>
                <tbody id="live-trades-body">
                    <?php 
                    $current_hour = null;
                    foreach ($trades as $trade): 
                        $current_price = $trade['current_price'] ?? null;
                        $display_exit_price = $trade['our_exit_price'] ?? $current_price;
                        
                        if ($trade['followed_at']) {
                            $trade_hour = date('Y-m-d H:00:00', strtotime($trade['followed_at']));
                            
                            if ($current_hour !== $trade_hour) {
                                $current_hour = $trade_hour;
                                $hour_display = date('l, F j, Y - g:00 A', strtotime($trade['followed_at']));
                                ?>
                                <tr class="hour-header-row">
                                    <td colspan="8">📅 <?php echo $hour_display; ?></td>
                                </tr>
                                <?php
                            }
                        }
                        
                        $status_key = strtolower($trade['our_status'] ?? '');
                        if (isset($status_badge_map[$status_key])) {
                            $status_badge = $status_badge_map[$status_key];
                        } else {
                            $safe_key = $status_key !== '' ? preg_replace('/[^a-z0-9_-]+/i', '', $status_key) : 'unknown';
                            $status_badge = [
                                'label' => $status_key !== '' ? $status_key : 'unknown',
                                'class' => 'bg-secondary-transparent'
                            ];
                        }
                    ?>
                    <tr onclick="viewTradeDetail(<?php echo $trade['id']; ?>, <?php echo $play_id; ?>, 'live')" style="cursor: pointer;">
                        <td>
                            <a href="https://solscan.io/token/<?php echo urlencode($trade['wallet_address']); ?>" target="_blank" rel="noopener" class="text-primary" title="<?php echo htmlspecialchars($trade['wallet_address']); ?>" onclick="event.stopPropagation();">
                                <code><?php echo substr(htmlspecialchars($trade['wallet_address']), 0, 12); ?>...</code>
                            </a>
                        </td>
                        <td class="text-center">
                            <?php echo $trade['followed_at'] ? date('M d, H:i', strtotime($trade['followed_at'])) : '--'; ?>
                        </td>
                        <td class="text-center">
                            <?php echo $trade['our_exit_timestamp'] ? date('M d, H:i', strtotime($trade['our_exit_timestamp'])) : '--'; ?>
                        </td>
                        <td class="text-center">
                            <?php if ($trade['our_entry_price']): ?>
                                $<?php echo number_format($trade['our_entry_price'], 6); ?>
                            <?php else: ?>
                                <span class="text-muted">--</span>
                            <?php endif; ?>
                        </td>
                        <td class="text-center">
                            <?php if ($display_exit_price): ?>
                                $<?php echo number_format($display_exit_price, 6); ?>
                                <?php if (!$trade['our_exit_price'] && $current_price): ?>
                                    <small class="text-warning">(current)</small>
                                <?php endif; ?>
                            <?php else: ?>
                                <span class="text-muted">--</span>
                            <?php endif; ?>
                        </td>
                        <td class="text-center">
                            <?php if ($trade['our_profit_loss'] !== null): ?>
                                <span class="fw-semibold <?php 
                                    if (!empty($play['short_play'])) {
                                        echo $trade['our_profit_loss'] > 0 ? 'text-danger' : ($trade['our_profit_loss'] < 0 ? 'text-success' : 'text-muted');
                                    } else {
                                        echo $trade['our_profit_loss'] > 0 ? 'text-success' : ($trade['our_profit_loss'] < 0 ? 'text-danger' : 'text-muted');
                                    }
                                ?>">
                                    <?php echo $trade['our_profit_loss'] > 0 ? '+' : ''; ?><?php echo number_format($trade['our_profit_loss'], 2); ?>%
                                </span>
                            <?php elseif ($trade['our_status'] === 'pending' && $trade['current_price'] && $trade['our_entry_price']): ?>
                                <?php 
                                    $pending_gain_loss = (($trade['current_price'] - $trade['our_entry_price']) / $trade['our_entry_price']) * 100;
                                    if (!empty($play['short_play'])) {
                                        $pending_class = $pending_gain_loss > 0 ? 'text-danger' : ($pending_gain_loss < 0 ? 'text-success' : 'text-muted');
                                    } else {
                                        $pending_class = 'text-warning';
                                    }
                                ?>
                                <span class="fw-semibold <?php echo $pending_class; ?>">
                                    <?php echo $pending_gain_loss > 0 ? '+' : ''; ?><?php echo number_format($pending_gain_loss, 2); ?>%
                                </span>
                            <?php else: ?>
                                <span class="text-muted">--</span>
                            <?php endif; ?>
                        </td>
                        <td class="text-center">
                            <span class="badge <?php echo htmlspecialchars($status_badge['class']); ?>">
                                <?php echo htmlspecialchars($status_badge['label']); ?>
                            </span>
                        </td>
                        <td class="text-center">
                            <button class="btn btn-sm btn-icon btn-primary-light" onclick="viewTradeDetail(<?php echo $trade['id']; ?>, <?php echo $play_id; ?>, 'live'); event.stopPropagation();" title="View Details">
                                <i class="ri-eye-line"></i>
                            </button>
                        </td>
                    </tr>
                    <?php endforeach; ?>
                </tbody>
            </table>
        </div>
        <?php endif; ?>
    </div>
    <?php if (!empty($trades) && $live_total_count > 100): ?>
    <div class="card-footer text-center">
        <button id="loadMoreLiveBtn" class="btn btn-outline-secondary" onclick="loadMoreTrades(<?php echo $play_id; ?>, 'live')">
            Load 100 More (<?php echo $live_total_count - 100; ?> remaining)
        </button>
    </div>
    <?php endif; ?>
</div>

<!-- Completed Trades -->
<div class="card custom-card mb-3">
    <div class="card-header">
        <div class="card-title">Recent Completed Trades</div>
        <div class="ms-auto d-flex gap-2 align-items-center">
            <div class="form-check form-switch me-3">
                <input class="form-check-input" type="checkbox" id="hideNoGoTrades" onchange="toggleNoGoTrades(this.checked)">
                <label class="form-check-label fs-12" for="hideNoGoTrades">Hide no_go trades</label>
            </div>
            <span class="badge bg-success-transparent" id="archiveCountBadge">Showing <?php echo count($archived_trades); ?> of <?php echo $archive_total_count; ?></span>
        </div>
    </div>
    <div class="card-body">
        <?php if (empty($archived_trades)): ?>
        <div class="text-center py-5">
            <i class="ri-check-double-line fs-48 text-muted mb-3 d-block"></i>
            <h5 class="text-muted">No Archived Trades</h5>
            <p class="text-muted mb-0">No completed trades have been archived yet.</p>
        </div>
        <?php else: ?>
        <div class="table-responsive">
            <table class="table table-bordered text-nowrap">
                <thead>
                    <tr>
                        <th>Wallet Address</th>
                        <th class="text-center">Entered</th>
                        <th class="text-center">Exited</th>
                        <th class="text-center">Entry Price</th>
                        <th class="text-center">Exit Price</th>
                        <th class="text-center">Profit/Loss</th>
                        <th class="text-center">Potential</th>
                        <th class="text-center">Status</th>
                        <th class="text-center">Details</th>
                    </tr>
                </thead>
                <tbody id="archived-trades-body">
                    <?php foreach ($archived_trades as $trade): 
                        $status_key = strtolower($trade['our_status'] ?? '');
                        if (isset($status_badge_map[$status_key])) {
                            $status_badge = $status_badge_map[$status_key];
                        } else {
                            $safe_key = $status_key !== '' ? preg_replace('/[^a-z0-9_-]+/i', '', $status_key) : 'unknown';
                            $status_badge = [
                                'label' => $status_key !== '' ? $status_key : 'unknown',
                                'class' => 'bg-secondary-transparent'
                            ];
                        }
                    ?>
                    <tr onclick="viewTradeDetail(<?php echo $trade['id']; ?>, <?php echo $play_id; ?>, 'archive')" style="cursor: pointer;" data-status="<?php echo htmlspecialchars($status_key); ?>">
                        <td>
                            <a href="https://solscan.io/token/<?php echo urlencode($trade['wallet_address']); ?>" target="_blank" rel="noopener" class="text-primary" title="<?php echo htmlspecialchars($trade['wallet_address']); ?>" onclick="event.stopPropagation();">
                                <code><?php echo substr(htmlspecialchars($trade['wallet_address']), 0, 12); ?>...</code>
                            </a>
                        </td>
                        <td class="text-center">
                            <?php echo $trade['followed_at'] ? date('M d, H:i', strtotime($trade['followed_at'])) : '--'; ?>
                        </td>
                        <td class="text-center">
                            <?php echo $trade['our_exit_timestamp'] ? date('M d, H:i', strtotime($trade['our_exit_timestamp'])) : '--'; ?>
                        </td>
                        <td class="text-center">
                            <?php if ($trade['our_entry_price']): ?>
                                $<?php echo number_format($trade['our_entry_price'], 6); ?>
                            <?php else: ?>
                                <span class="text-muted">--</span>
                            <?php endif; ?>
                        </td>
                        <td class="text-center">
                            <?php if ($trade['our_exit_price']): ?>
                                $<?php echo number_format($trade['our_exit_price'], 6); ?>
                            <?php else: ?>
                                <span class="text-muted">--</span>
                            <?php endif; ?>
                        </td>
                        <td class="text-center">
                            <?php if ($trade['our_profit_loss'] !== null): ?>
                                <span class="fw-semibold <?php 
                                    if (!empty($play['short_play'])) {
                                        echo $trade['our_profit_loss'] > 0 ? 'text-danger' : ($trade['our_profit_loss'] < 0 ? 'text-success' : 'text-muted');
                                    } else {
                                        echo $trade['our_profit_loss'] > 0 ? 'text-success' : ($trade['our_profit_loss'] < 0 ? 'text-danger' : 'text-muted');
                                    }
                                ?>">
                                    <?php echo $trade['our_profit_loss'] > 0 ? '+' : ''; ?><?php echo number_format($trade['our_profit_loss'], 2); ?>%
                                </span>
                            <?php else: ?>
                                <span class="text-muted">--</span>
                            <?php endif; ?>
                        </td>
                        <td class="text-center">
                            <?php 
                            $potential_display = '--';
                            $potential_class = 'text-muted';
                            if (isset($trade['potential_gains']) && $trade['potential_gains'] !== null) {
                                $potential_val = (float)$trade['potential_gains'];
                                $potential_display = ($potential_val > 0 ? '+' : '') . number_format($potential_val, 2) . '%';
                                
                                if (!empty($play['short_play'])) {
                                    $potential_class = $potential_val > 0 ? 'text-danger' : ($potential_val < 0 ? 'text-success' : 'text-muted');
                                } else {
                                    $potential_class = $potential_val > 0 ? 'text-success' : ($potential_val < 0 ? 'text-danger' : 'text-muted');
                                }
                            }
                            ?>
                            <span class="fw-semibold <?php echo $potential_class; ?>">
                                <?php echo $potential_display; ?>
                            </span>
                        </td>
                        <td class="text-center">
                            <span class="badge <?php echo htmlspecialchars($status_badge['class']); ?>">
                                <?php echo htmlspecialchars($status_badge['label']); ?>
                            </span>
                        </td>
                        <td class="text-center">
                            <button class="btn btn-sm btn-icon btn-primary-light" onclick="viewTradeDetail(<?php echo $trade['id']; ?>, <?php echo $play_id; ?>, 'archive'); event.stopPropagation();" title="View Details">
                                <i class="ri-eye-line"></i>
                            </button>
                        </td>
                    </tr>
                    <?php endforeach; ?>
                </tbody>
            </table>
        </div>
        <?php endif; ?>
    </div>
    <?php if (!empty($archived_trades) && $archive_total_count > 100): ?>
    <div class="card-footer text-center">
        <button id="loadMoreArchiveBtn" class="btn btn-outline-secondary" onclick="loadMoreTrades(<?php echo $play_id; ?>, 'archive')">
            Load 100 More (<?php echo $archive_total_count - 100; ?> remaining)
        </button>
    </div>
    <?php endif; ?>
</div>

<?php
$content = ob_get_clean();

// --- Page Scripts ---
ob_start();
?>
<!-- Apex Charts JS -->
<script src="<?php echo $baseUrl; ?>/assets/libs/apexcharts/apexcharts.min.js"></script>

<script>
    // Inject PHP data into JavaScript
    window.isRestrictedPlay = <?php echo $is_restricted_play ? 'true' : 'false'; ?>;
    window.chartData = <?php echo $json_chart_data; ?>;
    window.playId = <?php echo $play_id; ?>;

    // Performance Timing Report
    (function() {
        const timingData = <?php echo $json_timing_report; ?>;
        console.log('%c⏱️ PAGE PERFORMANCE TIMING REPORT', 'color: rgb(var(--primary-rgb)); font-size: 14px; font-weight: bold;');
        console.table(timingData);
    })();

    // Initialize Chart
    document.addEventListener('DOMContentLoaded', function() {
        const chartData = window.chartData;
        
        if (!chartData || !chartData.prices || chartData.prices.length === 0) {
            return;
        }
        
        // Format data for ApexCharts
        const priceData = chartData.prices.map(item => ({
            x: new Date(item.x + ' UTC').getTime(),
            y: item.y
        }));
        
        // Build annotations for trade entries/exits
        const annotations = { xaxis: [] };
        const trades = chartData.trades || [];
        
        trades.forEach(function(trade, index) {
            const entryTime = new Date(trade.entry_time + ' UTC').getTime();
            const exitTime = trade.exit_time ? new Date(trade.exit_time + ' UTC').getTime() : null;
            
            if (isNaN(entryTime)) return;
            
            // Entry marker
            annotations.xaxis.push({
                x: entryTime,
                borderColor: trade.color,
                strokeDashArray: 3,
                label: {
                    borderColor: trade.color,
                    style: {
                        color: '#fff',
                        background: trade.color,
                        fontSize: '10px'
                    },
                    text: 'Entry'
                }
            });
            
            // Exit marker and range
            if (exitTime && !isNaN(exitTime) && exitTime > entryTime) {
                annotations.xaxis.push({
                    x: exitTime,
                    borderColor: trade.color,
                    strokeDashArray: 3,
                    label: {
                        borderColor: trade.color,
                        style: {
                            color: '#fff',
                            background: trade.color,
                            fontSize: '10px'
                        },
                        text: 'Exit'
                    }
                });
                
                // Range highlight
                annotations.xaxis.push({
                    x: entryTime,
                    x2: exitTime,
                    fillColor: trade.color,
                    opacity: 0.1,
                    label: {
                        text: trade.profit_loss !== null ? (trade.profit_loss > 0 ? '+' : '') + parseFloat(trade.profit_loss).toFixed(2) + '%' : ''
                    }
                });
            }
        });
        
        var options = {
            series: [{
                name: 'SOL Price',
                data: priceData
            }],
            chart: {
                type: 'line',
                height: 500,
                background: 'transparent',
                toolbar: {
                    show: true,
                    tools: {
                        download: true,
                        selection: true,
                        zoom: true,
                        zoomin: true,
                        zoomout: true,
                        pan: true,
                        reset: true
                    }
                },
                animations: {
                    enabled: false
                }
            },
            stroke: {
                curve: 'smooth',
                width: 2
            },
            colors: ['rgb(var(--primary-rgb))'],
            grid: {
                borderColor: 'rgba(255,255,255,0.1)',
                strokeDashArray: 3
            },
            xaxis: {
                type: 'datetime',
                labels: {
                    datetimeUTC: false,
                    style: {
                        colors: 'var(--text-muted)',
                        fontSize: '11px'
                    }
                }
            },
            yaxis: {
                labels: {
                    style: {
                        colors: 'var(--text-muted)',
                        fontSize: '11px'
                    },
                    formatter: function(val) {
                        return '$' + val.toFixed(4);
                    }
                }
            },
            tooltip: {
                theme: 'dark',
                x: {
                    format: 'MMM dd, HH:mm:ss'
                }
            },
            annotations: annotations
        };

        var chart = new ApexCharts(document.querySelector("#tradeChart"), options);
        chart.render();
    });
    
    // Update play sorting
    async function updatePlaySorting(playId, sorting) {
        if (window.isRestrictedPlay) {
            alert('Priority changes are disabled for this play.');
            return;
        }
        try {
            const response = await fetch('/chart/plays/update_sorting.php', {
                method: 'POST',
                headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                body: `play_id=${playId}&sorting=${sorting}`
            });
            
            const data = await response.json();
            
            if (data.success) {
                const select = document.getElementById('playSorting');
                const originalBg = select.style.backgroundColor;
                select.style.backgroundColor = 'rgba(var(--success-rgb), 0.3)';
                setTimeout(() => { select.style.backgroundColor = originalBg; }, 500);
            } else {
                alert('Error updating priority: ' + data.error);
            }
        } catch (error) {
            console.error('Error updating sorting:', error);
            alert('Error updating priority. Please try again.');
        }
    }
    
    // Play editing functions
    function toggleEditForm() {
        if (window.isRestrictedPlay) {
            alert('Editing is disabled for this play.');
            return;
        }
        const editForm = document.getElementById('editPlayForm');
        const isHidden = editForm.classList.contains('hidden');
        
        if (isHidden) {
            loadPlayForEdit();
            editForm.classList.remove('hidden');
            editForm.scrollIntoView({ behavior: 'smooth', block: 'start' });
        } else {
            editForm.classList.add('hidden');
        }
    }

    async function loadPlayForEdit() {
        if (window.isRestrictedPlay) return;
        const playId = <?php echo $play_id; ?>;
        
        try {
            const response = await fetch('/chart/plays/get_play_for_edit.php?id=' + playId);
            const data = await response.json();
            
            if (data.success) {
                document.getElementById('edit_play_id').value = data.id;
                document.getElementById('edit_name').value = data.name;
                document.getElementById('edit_description').value = data.description;
                document.getElementById('edit_find_wallets_sql').value = data.find_wallets_sql.query;
                document.getElementById('edit_max_buys_per_cycle').value = data.max_buys_per_cycle;
                
                // Handle multi-select for project_ids
                const projectSelect = document.getElementById('edit_project_ids');
                if (projectSelect) {
                    let projectIds = data.project_ids || [];
                    
                    console.log('Raw project_ids from API:', data.project_ids, 'Type:', typeof data.project_ids);
                    
                    // Handle case where project_ids might be a string (e.g., "[1,2]" or "1,2")
                    if (typeof projectIds === 'string') {
                        try {
                            projectIds = JSON.parse(projectIds);
                            console.log('Parsed from JSON string:', projectIds);
                        } catch (e) {
                            // Fallback: try comma-separated
                            projectIds = projectIds.split(',').map(id => parseInt(id.trim())).filter(id => !isNaN(id));
                            console.log('Parsed from comma-separated:', projectIds);
                        }
                    }
                    
                    // Ensure all IDs are integers for comparison
                    const projectIdNumbers = Array.isArray(projectIds) 
                        ? projectIds.map(id => parseInt(id)).filter(id => !isNaN(id))
                        : [];
                    
                    console.log('Pre-selecting project_ids:', projectIdNumbers);
                    console.log('Available options:', Array.from(projectSelect.options).map(o => ({value: o.value, text: o.text})));
                    
                    // Clear all selections first, then set the correct ones
                    Array.from(projectSelect.options).forEach(opt => {
                        const optValue = parseInt(opt.value);
                        const shouldSelect = projectIdNumbers.includes(optValue);
                        opt.selected = shouldSelect;
                        if (shouldSelect) {
                            console.log(`Selected option: ${opt.value} - ${opt.text}`);
                        }
                    });
                } else {
                    console.error('Project select element not found!');
                }
                
                document.getElementById('edit_short_play').checked = data.short_play == 1;
                
                const triggerMode = data.trigger_on_perp?.mode || 'any';
                document.getElementById('edit_trigger_on_perp').value = triggerMode;
                
                if (data.bundle_trades && data.bundle_trades.enabled) {
                    document.getElementById('enable_bundle_trades').checked = true;
                    document.getElementById('bundle_trades_settings').style.display = 'block';
                    document.getElementById('bundle_num_trades').value = data.bundle_trades.num_trades || '';
                    document.getElementById('bundle_seconds').value = data.bundle_trades.seconds || '';
                } else {
                    document.getElementById('enable_bundle_trades').checked = false;
                    document.getElementById('bundle_trades_settings').style.display = 'none';
                }
                
                if (data.cashe_wallets && data.cashe_wallets.enabled) {
                    document.getElementById('enable_cashe_wallets').checked = true;
                    document.getElementById('cashe_wallets_settings').style.display = 'block';
                    document.getElementById('cashe_seconds').value = data.cashe_wallets.seconds || '';
                    document.getElementById('cashe_seconds').dispatchEvent(new Event('input'));
                } else {
                    document.getElementById('enable_cashe_wallets').checked = false;
                    document.getElementById('cashe_wallets_settings').style.display = 'none';
                }
                
                // Handle pattern validator settings
                const patternValidatorEnabled = data.pattern_validator_enable == 1;
                const patternUpdateByAi = data.pattern_update_by_ai == 1;
                
                document.getElementById('edit_pattern_validator_enable').checked = patternValidatorEnabled;
                document.getElementById('edit_pattern_update_by_ai').checked = patternUpdateByAi;
                
                // Apply AI update state to UI elements (reuse projectSelect from above)
                const validatorEnableCheckbox = document.getElementById('edit_pattern_validator_enable');
                const aiNotice = document.getElementById('ai_update_notice');
                
                if (patternUpdateByAi) {
                    validatorEnableCheckbox.disabled = true;
                    if (projectSelect) {
                        projectSelect.disabled = true;
                        projectSelect.style.opacity = '0.5';
                        projectSelect.style.cursor = 'not-allowed';
                    }
                    aiNotice.style.display = 'block';
                } else {
                    validatorEnableCheckbox.disabled = false;
                    if (projectSelect) {
                        projectSelect.disabled = false;
                        projectSelect.style.opacity = '1';
                        projectSelect.style.cursor = '';
                    }
                    aiNotice.style.display = 'none';
                }
                
                document.getElementById('edit-decreases-container').innerHTML = '';
                document.getElementById('edit-increases-container').innerHTML = '';
                
                if (data.sell_logic.tolerance_rules.decreases && data.sell_logic.tolerance_rules.decreases.length > 0) {
                    const rule = data.sell_logic.tolerance_rules.decreases[0];
                    addEditDecreaseRule(rule.range[0], rule.range[1], rule.tolerance);
                }
                
                if (data.sell_logic.tolerance_rules.increases) {
                    data.sell_logic.tolerance_rules.increases.forEach(rule => {
                        addEditIncreaseRule(rule.range[0], rule.range[1], rule.tolerance);
                    });
                }
            } else {
                alert('Error loading play data: ' + data.error);
            }
        } catch (error) {
            console.error('Error loading play for edit:', error);
            alert('Error loading play data');
        }
    }

    function addEditDecreaseRule(rangeFrom = null, rangeTo = null, tolerance = null) {
        const container = document.getElementById('edit-decreases-container');
        const tolerancePips = tolerance !== null ? Math.round(tolerance * 10000) : '';
        const ruleHtml = `
            <div class="tolerance-rule-single">
                <div>
                    <label class="tolerance-rule-label">Tolerance (PIPs)</label>
                    <input type="number" step="1" class="form-control-inline pip-input" name="decrease_tolerance[]" value="${tolerancePips}" required oninput="updatePipConversion(this)">
                    <small class="pip-conversion">${tolerance !== null ? '= ' + tolerance.toFixed(4) + ' (' + (tolerance * 100).toFixed(2) + '%)' : ''}</small>
                    <input type="hidden" name="decrease_range_from[]" value="-999999">
                    <input type="hidden" name="decrease_range_to[]" value="0">
                </div>
            </div>
        `;
        container.innerHTML = ruleHtml;
    }

    function addEditIncreaseRule(rangeFrom = null, rangeTo = null, tolerance = null) {
        const container = document.getElementById('edit-increases-container');
        const rangeFromPips = rangeFrom !== null ? Math.round(rangeFrom * 10000) : '';
        const rangeToPips = rangeTo !== null ? Math.round(rangeTo * 10000) : '';
        const tolerancePips = tolerance !== null ? Math.round(tolerance * 10000) : '';
        const ruleHtml = `
            <div class="tolerance-rule">
                <div>
                    <label class="tolerance-rule-label">Range From (PIPs)</label>
                    <input type="number" step="1" class="form-control-inline pip-input" name="increase_range_from[]" value="${rangeFromPips}" required oninput="updatePipConversion(this)">
                    <small class="pip-conversion">${rangeFrom !== null ? '= ' + rangeFrom + ' (' + (rangeFrom * 100).toFixed(2) + '%)' : ''}</small>
                </div>
                <div>
                    <label class="tolerance-rule-label">Range To (PIPs)</label>
                    <input type="number" step="1" class="form-control-inline pip-input" name="increase_range_to[]" value="${rangeToPips}" required oninput="updatePipConversion(this)">
                    <small class="pip-conversion">${rangeTo !== null ? '= ' + rangeTo + ' (' + (rangeTo * 100).toFixed(2) + '%)' : ''}</small>
                </div>
                <div>
                    <label class="tolerance-rule-label">Tolerance (PIPs)</label>
                    <input type="number" step="1" class="form-control-inline pip-input" name="increase_tolerance[]" value="${tolerancePips}" required oninput="updatePipConversion(this)">
                    <small class="pip-conversion">${tolerance !== null ? '= ' + tolerance + ' (' + (tolerance * 100).toFixed(2) + '%)' : ''}</small>
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

    function convertPipsToDecimals(form) {
        form.querySelectorAll('.pip-input').forEach(input => {
            const pips = parseFloat(input.value) || 0;
            const decimal = pips / 10000;
            input.value = decimal;
        });
        return true;
    }

    function validateEditForm() {
        if (window.isRestrictedPlay) {
            alert('Editing is disabled for this play.');
            return false;
        }
        const sql = document.getElementById('edit_find_wallets_sql').value.trim();
        if (!sql) {
            alert('Please enter a SQL query.');
            return false;
        }
        const form = document.querySelector('#editPlayForm form');
        convertPipsToDecimals(form);
        return true;
    }

    async function deletePlayFromEdit() {
        if (window.isRestrictedPlay) {
            alert('Deleting is disabled for this play.');
            return;
        }
        const playId = document.getElementById('edit_play_id').value;
        const playName = document.getElementById('edit_name').value;
        
        if (!confirm(`Are you sure you want to delete the play "${playName}"?\n\nThis action cannot be undone.`)) {
            return;
        }
        
        try {
            const response = await fetch('/chart/plays/delete.php?id=' + playId, { method: 'POST' });
            const data = await response.json();
            
            if (data.success) {
                alert('Play deleted successfully!');
                window.location.href = '/v2/goats/';
            } else {
                alert('Error deleting play: ' + data.error);
            }
        } catch (error) {
            console.error('Error deleting play:', error);
            alert('Error deleting play. Please try again.');
        }
    }

    // Initialize form handlers
    document.addEventListener('DOMContentLoaded', function() {
        document.querySelectorAll('.pip-input').forEach(input => {
            updatePipConversion(input);
            input.addEventListener('input', function() {
                updatePipConversion(this);
            });
        });
        
        document.getElementById('enable_bundle_trades').addEventListener('change', function(e) {
            document.getElementById('bundle_trades_settings').style.display = e.target.checked ? 'block' : 'none';
        });
        
        document.getElementById('enable_cashe_wallets').addEventListener('change', function(e) {
            document.getElementById('cashe_wallets_settings').style.display = e.target.checked ? 'block' : 'none';
        });
        
        document.getElementById('cashe_seconds').addEventListener('input', function(e) {
            const seconds = parseInt(e.target.value) || 0;
            const display = document.getElementById('cashe_time_display');
            
            if (seconds < 60) {
                display.textContent = `= ${seconds} second${seconds !== 1 ? 's' : ''}`;
            } else if (seconds < 3600) {
                const minutes = (seconds / 60).toFixed(1);
                display.textContent = `= ${minutes} minute${minutes !== '1.0' ? 's' : ''}`;
            } else {
                const hours = (seconds / 3600).toFixed(2);
                display.textContent = `= ${hours} hour${hours !== '1.00' ? 's' : ''}`;
            }
        });
        
        // Pattern Validator AI Update checkbox handler
        document.getElementById('edit_pattern_update_by_ai').addEventListener('change', function(e) {
            const aiEnabled = e.target.checked;
            const validatorEnableCheckbox = document.getElementById('edit_pattern_validator_enable');
            const projectSelect = document.getElementById('edit_project_ids');
            const aiNotice = document.getElementById('ai_update_notice');
            
            if (aiEnabled) {
                // Auto-enable pattern validator when AI update is enabled
                validatorEnableCheckbox.checked = true;
                validatorEnableCheckbox.disabled = true;
                
                // Disable and grey out project selector
                projectSelect.disabled = true;
                projectSelect.style.opacity = '0.5';
                projectSelect.style.cursor = 'not-allowed';
                
                // Show AI notice
                aiNotice.style.display = 'block';
            } else {
                // Re-enable pattern validator checkbox
                validatorEnableCheckbox.disabled = false;
                
                // Re-enable project selector
                projectSelect.disabled = false;
                projectSelect.style.opacity = '1';
                projectSelect.style.cursor = '';
                
                // Hide AI notice
                aiNotice.style.display = 'none';
            }
        });
    });
    
    function viewTradeDetail(tradeId, playId, source = 'live') {
        const params = new URLSearchParams({
            id: tradeId,
            play_id: playId,
            return_url: window.location.pathname + window.location.search
        });
        if (source === 'archive') {
            params.set('source', 'archive');
        }
        window.location.href = `trade/?${params.toString()}`;
    }
    
    // Toggle no_go trades visibility
    function toggleNoGoTrades(hide) {
        const tbody = document.getElementById('archived-trades-body');
        if (!tbody) return;
        
        const rows = tbody.querySelectorAll('tr[data-status]');
        let visibleCount = 0;
        let noGoCount = 0;
        let totalCount = rows.length;
        
        rows.forEach(row => {
            if (row.dataset.status === 'no_go') {
                noGoCount++;
                if (hide) {
                    row.style.display = 'none';
                } else {
                    row.style.display = '';
                    visibleCount++;
                }
            } else {
                row.style.display = '';
                visibleCount++;
            }
        });
        
        // Update the badge count
        const badge = document.getElementById('archiveCountBadge');
        if (badge) {
            if (hide && noGoCount > 0) {
                badge.innerHTML = `<span class="text-success">${visibleCount} shown</span> <span class="text-warning">(${noGoCount} no_go hidden)</span>`;
                badge.className = 'badge bg-light';
            } else {
                badge.innerHTML = `Showing ${totalCount} of <?php echo $archive_total_count; ?>`;
                badge.className = 'badge bg-success-transparent';
            }
        }
        
        // Save preference to localStorage
        localStorage.setItem('hideNoGoTrades_<?php echo $play_id; ?>', hide ? '1' : '0');
    }
    
    // Initialize toggle state from localStorage
    document.addEventListener('DOMContentLoaded', function() {
        const savedPref = localStorage.getItem('hideNoGoTrades_<?php echo $play_id; ?>');
        if (savedPref === '1') {
            const checkbox = document.getElementById('hideNoGoTrades');
            if (checkbox) {
                checkbox.checked = true;
                toggleNoGoTrades(true);
            }
        }
    });
    
    // Track offsets for each table
    let offsets = { live: 100, archive: 100 };
    let isLoading = { live: false, archive: false };

    async function loadMoreTrades(playId, tableType = 'archive') {
        if (isLoading[tableType]) return;
        
        const btnId = tableType === 'live' ? 'loadMoreLiveBtn' : 'loadMoreArchiveBtn';
        const tbodyId = tableType === 'live' ? 'live-trades-body' : 'archived-trades-body';
        const btn = document.getElementById(btnId);
        
        if (!btn) return;
        
        const originalText = btn.innerText;
        
        isLoading[tableType] = true;
        btn.innerText = 'Loading...';
        btn.disabled = true;
        
        try {
            const response = await fetch(`index.php?id=${playId}&ajax_load_more=1&offset=${offsets[tableType]}&table=${tableType}`);
            const html = await response.text();
            
            if (html.trim()) {
                const tbody = document.getElementById(tbodyId);
                tbody.insertAdjacentHTML('beforeend', html);
                offsets[tableType] += 100;
                
                // Apply no_go filter to newly loaded archive rows if filter is enabled
                if (tableType === 'archive') {
                    const hideNoGo = document.getElementById('hideNoGoTrades')?.checked;
                    if (hideNoGo) {
                        toggleNoGoTrades(true);
                    }
                }
                
                // Update button text with remaining count
                const loadedCount = offsets[tableType];
                const totalCount = tableType === 'live' ? <?php echo $live_total_count ?? 0; ?> : <?php echo $archive_total_count ?? 0; ?>;
                const remaining = totalCount - loadedCount;
                
                if (remaining > 0) {
                    btn.innerText = `Load 100 More (${remaining} remaining)`;
                } else {
                    btn.style.display = 'none';
                }
            } else {
                btn.style.display = 'none';
            }
        } catch (error) {
            console.error('Error loading more trades:', error);
            alert('Error loading more trades');
            btn.innerText = originalText;
        } finally {
            isLoading[tableType] = false;
            btn.disabled = false;
        }
    }
</script>
<?php
$scripts = ob_get_clean();

// Include the base layout
include __DIR__ . '/../../pages/layouts/base.php';
?>

