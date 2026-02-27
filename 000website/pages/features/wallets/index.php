<?php
/**
 * Paper Wallets - List Page
 * Shows all wallets with balance, P/L summary, and links to trade detail.
 */

require_once __DIR__ . '/../../../includes/DatabaseClient.php';
require_once __DIR__ . '/../../../includes/config.php';
$db = new DatabaseClient(DATABASE_API_URL);
$api_available = $db->isAvailable();

$baseUrl = '';

$wallets = [];
$error_message = '';

if ($api_available) {
    $response = $db->getWallets();
    if ($response && isset($response['wallets'])) {
        $wallets = $response['wallets'];
    } else {
        $error_message = 'Failed to load wallets from API.';
    }
} else {
    $error_message = 'API server is not available.';
}

// --- Styles ---
ob_start();
?>
<style>
    .wallet-grid {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
        gap: 1.25rem;
    }

    .wallet-card {
        background: var(--custom-white);
        border: 1px solid var(--default-border);
        border-radius: 0.5rem;
        overflow: hidden;
        transition: all 0.2s ease;
        cursor: pointer;
        text-decoration: none;
        display: block;
        color: inherit;
    }

    .wallet-card:hover {
        border-color: rgb(var(--primary-rgb));
        box-shadow: 0 4px 20px rgba(var(--primary-rgb), 0.15);
        transform: translateY(-2px);
        color: inherit;
        text-decoration: none;
    }

    .wallet-card-header {
        padding: 1.1rem 1.25rem 0.9rem;
        border-bottom: 1px solid var(--default-border);
        background: rgba(var(--primary-rgb), 0.03);
        display: flex;
        align-items: center;
        justify-content: space-between;
    }

    .wallet-card-name {
        font-size: 1.05rem;
        font-weight: 600;
        color: var(--default-text-color);
    }

    .wallet-type-badge {
        font-size: 0.65rem;
        font-weight: 600;
        padding: 0.2rem 0.55rem;
        border-radius: 99px;
        text-transform: uppercase;
        letter-spacing: 0.4px;
    }

    .wallet-card-body {
        padding: 1.25rem;
    }

    .balance-row {
        display: flex;
        align-items: baseline;
        gap: 0.5rem;
        margin-bottom: 1.1rem;
    }

    .balance-value {
        font-size: 1.8rem;
        font-weight: 700;
        color: var(--default-text-color);
        font-family: 'SF Mono', 'Monaco', 'Consolas', monospace;
    }

    .balance-initial {
        font-size: 0.8rem;
        color: var(--text-muted);
    }

    .balance-bar-wrap {
        background: var(--default-border);
        border-radius: 99px;
        height: 5px;
        margin-bottom: 1.1rem;
        overflow: hidden;
    }

    .balance-bar {
        height: 100%;
        border-radius: 99px;
        transition: width 0.4s ease;
    }

    .stats-grid {
        display: grid;
        grid-template-columns: repeat(4, 1fr);
        gap: 0.75rem;
        margin-top: 0.25rem;
    }

    .stat-box {
        text-align: center;
        background: rgba(var(--light-rgb), 0.4);
        border-radius: 0.35rem;
        padding: 0.6rem 0.25rem;
    }

    .stat-box-value {
        font-size: 1rem;
        font-weight: 700;
        color: var(--default-text-color);
        display: block;
        line-height: 1.2;
    }

    .stat-box-label {
        font-size: 0.62rem;
        color: var(--text-muted);
        text-transform: uppercase;
        letter-spacing: 0.4px;
        margin-top: 0.2rem;
        display: block;
    }

    .pl-row {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 0.75rem 0;
        border-top: 1px dashed var(--default-border);
        margin-top: 0.9rem;
    }

    .pl-label {
        font-size: 0.75rem;
        color: var(--text-muted);
        font-weight: 500;
    }

    .pl-value {
        font-size: 1rem;
        font-weight: 700;
        font-family: 'SF Mono', 'Monaco', 'Consolas', monospace;
    }

    .text-positive { color: rgb(var(--success-rgb)); }
    .text-negative { color: rgb(var(--danger-rgb)); }
    .text-neutral  { color: var(--text-muted); }

    .empty-state {
        text-align: center;
        padding: 4rem 2rem;
        border: 1px dashed var(--default-border);
        border-radius: 0.5rem;
    }
</style>
<?php
$styles = ob_get_clean();

// --- Page Content ---
ob_start();
?>

<!-- Page Header -->
<div class="d-flex align-items-center justify-content-between page-header-breadcrumb flex-wrap gap-2 mb-4">
    <div>
        <nav>
            <ol class="breadcrumb mb-1">
                <li class="breadcrumb-item"><a href="/">Dashboard</a></li>
                <li class="breadcrumb-item"><a href="#">Features</a></li>
                <li class="breadcrumb-item active" aria-current="page">Wallets</li>
            </ol>
        </nav>
        <h1 class="page-title fw-medium fs-18 mb-0">Paper Wallets</h1>
    </div>
    <div class="d-flex align-items-center gap-2">
        <span class="badge <?php echo $api_available ? 'bg-success-transparent text-success' : 'bg-danger-transparent text-danger'; ?> fs-12 px-3 py-2">
            <span class="me-1"><?php echo $api_available ? '●' : '○'; ?></span>
            API <?php echo $api_available ? 'Online' : 'Offline'; ?>
        </span>
    </div>
</div>

<?php if ($error_message): ?>
<div class="alert alert-danger alert-dismissible fade show">
    <i class="ri-error-warning-line me-2"></i><?php echo htmlspecialchars($error_message); ?>
    <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
</div>
<?php endif; ?>

<?php if (empty($wallets)): ?>
<div class="empty-state">
    <i class="ri-wallet-3-line text-muted mb-3 d-block" style="font-size: 3rem;"></i>
    <h4 class="text-muted">No wallets found</h4>
    <p class="text-muted mb-0">Start the wallet executor to create and seed the test wallet.</p>
    <code class="d-block mt-2 text-muted" style="font-size: 0.8rem;">python3 scheduler/run_component.py --component wallet_executor</code>
</div>
<?php else: ?>
<div class="wallet-grid">
<?php foreach ($wallets as $w):
    $balance      = floatval($w['balance']);
    $initial      = floatval($w['initial_balance']);
    $pl_usdc      = floatval($w['total_pl_usdc']);
    $pl_pct       = floatval($w['total_pl_pct']);
    $pl_sign      = $pl_usdc >= 0 ? '+' : '';
    $pl_class     = $pl_usdc > 0 ? 'text-positive' : ($pl_usdc < 0 ? 'text-negative' : 'text-neutral');
    $wins         = intval($w['winning_trades']);
    $losses       = intval($w['losing_trades']);
    $open         = intval($w['open_trades']);
    $closed       = intval($w['closed_trades']);
    $fee_pct      = floatval($w['fee_rate']) * 100;
    $invest_pct   = floatval($w['invest_pct'] ?? 0.20) * 100;

    // Balance bar — ratio of current balance to initial (capped 0–150%)
    $bar_ratio    = $initial > 0 ? min($balance / $initial, 1.5) : 0;
    $bar_width    = round($bar_ratio / 1.5 * 100);
    $bar_color    = $balance >= $initial ? 'rgb(var(--success-rgb))' : 'rgb(var(--danger-rgb))';
?>
<a href="/pages/features/wallets/detail.php?id=<?php echo intval($w['id']); ?>" class="wallet-card">
    <div class="wallet-card-header">
        <div class="wallet-card-name">
            <i class="ri-wallet-3-line me-2 text-primary"></i><?php echo htmlspecialchars($w['name']); ?>
        </div>
        <?php if ($w['is_test']): ?>
        <span class="wallet-type-badge bg-info-transparent text-info">Test</span>
        <?php else: ?>
        <span class="wallet-type-badge bg-success-transparent text-success">Live</span>
        <?php endif; ?>
    </div>

    <div class="wallet-card-body">

        <!-- Current balance -->
        <div class="balance-row">
            <span class="balance-value">$<?php echo number_format($balance, 2); ?></span>
            <span class="balance-initial">/ $<?php echo number_format($initial, 2); ?> initial</span>
        </div>

        <!-- Balance bar -->
        <div class="balance-bar-wrap">
            <div class="balance-bar" style="width: <?php echo $bar_width; ?>%; background: <?php echo $bar_color; ?>;"></div>
        </div>

        <!-- Stats grid -->
        <div class="stats-grid">
            <div class="stat-box">
                <span class="stat-box-value" style="color: rgb(var(--success-rgb));"><?php echo $wins; ?></span>
                <span class="stat-box-label">Wins</span>
            </div>
            <div class="stat-box">
                <span class="stat-box-value" style="color: rgb(var(--danger-rgb));"><?php echo $losses; ?></span>
                <span class="stat-box-label">Losses</span>
            </div>
            <div class="stat-box">
                <span class="stat-box-value" style="color: rgb(var(--warning-rgb));"><?php echo $open; ?></span>
                <span class="stat-box-label">Open</span>
            </div>
            <div class="stat-box">
                <span class="stat-box-value"><?php echo $closed; ?></span>
                <span class="stat-box-label">Closed</span>
            </div>
        </div>

        <!-- P/L row -->
        <div class="pl-row">
            <span class="pl-label">Total P/L &nbsp;·&nbsp; <?php echo $invest_pct; ?>% per trade &nbsp;·&nbsp; <?php echo $fee_pct; ?>% fee</span>
            <span class="pl-value <?php echo $pl_class; ?>">
                <?php echo $pl_sign; ?>$<?php echo number_format($pl_usdc, 2); ?>
                <span style="font-size: 0.8rem; font-weight: 500;">(<?php echo $pl_sign . number_format($pl_pct, 2); ?>%)</span>
            </span>
        </div>

    </div>
</a>
<?php endforeach; ?>
</div>
<?php endif; ?>

<script>
    // Auto-refresh every 30 seconds
    setTimeout(() => location.reload(), 30000);
</script>

<?php
$content = ob_get_clean();
$scripts = '';
include __DIR__ . '/../../layouts/base.php';
?>
