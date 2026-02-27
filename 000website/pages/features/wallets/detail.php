<?php
/**
 * Paper Wallet - Detail Page
 * Shows wallet summary and a full table of all trades with fees and P/L.
 */

require_once __DIR__ . '/../../../includes/DatabaseClient.php';
require_once __DIR__ . '/../../../includes/config.php';
$db = new DatabaseClient(DATABASE_API_URL);
$api_available = $db->isAvailable();

$baseUrl = '';

// Helper — must be declared outside any loop to avoid "Cannot redeclare" fatal errors
function rel_time(?string $ts): string {
    if (!$ts) return '-';
    $diff = time() - strtotime($ts);
    if ($diff < 60)    return $diff . 's ago';
    if ($diff < 3600)  return floor($diff / 60)   . 'm ago';
    if ($diff < 86400) return floor($diff / 3600)  . 'h ago';
    return date('M j, H:i', strtotime($ts));
}

$wallet_id = isset($_GET['id']) ? intval($_GET['id']) : 0;
$status_filter = $_GET['status'] ?? '';
$limit = isset($_GET['limit']) ? intval($_GET['limit']) : 200;

$wallet = null;
$trades = [];
$error_message = '';

if (!$wallet_id) {
    $error_message = 'No wallet ID specified.';
} elseif ($api_available) {
    // Load wallet info
    $wallets_resp = $db->getWallets();
    if ($wallets_resp && isset($wallets_resp['wallets'])) {
        foreach ($wallets_resp['wallets'] as $w) {
            if (intval($w['id']) === $wallet_id) {
                $wallet = $w;
                break;
            }
        }
    }
    if (!$wallet) {
        $error_message = 'Wallet not found.';
    } else {
        $trades_resp = $db->getWalletTrades(
            $wallet_id,
            $status_filter !== '' ? $status_filter : null,
            $limit
        );
        if ($trades_resp && isset($trades_resp['trades'])) {
            $trades = $trades_resp['trades'];
        }
    }
} else {
    $error_message = 'API server is not available.';
}

// --- Styles ---
ob_start();
?>
<style>
    .wallet-summary {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(160px, 1fr));
        gap: 1rem;
        margin-bottom: 1.5rem;
    }

    .summary-card {
        background: var(--custom-white);
        border: 1px solid var(--default-border);
        border-radius: 0.5rem;
        padding: 1rem 1.1rem;
        text-align: center;
    }

    .summary-value {
        font-size: 1.4rem;
        font-weight: 700;
        font-family: 'SF Mono', 'Monaco', 'Consolas', monospace;
        color: var(--default-text-color);
        display: block;
        line-height: 1.2;
    }

    .summary-label {
        font-size: 0.65rem;
        color: var(--text-muted);
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-top: 0.35rem;
        display: block;
    }

    .text-positive { color: rgb(var(--success-rgb)) !important; }
    .text-negative { color: rgb(var(--danger-rgb)) !important; }
    .text-neutral  { color: var(--text-muted) !important; }
    .text-warning-c { color: rgb(var(--warning-rgb)) !important; }

    .trades-table {
        font-size: 0.82rem;
    }

    .trades-table th {
        font-size: 0.7rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        color: var(--text-muted);
        font-weight: 600;
        border-bottom: 2px solid var(--default-border);
        white-space: nowrap;
    }

    .trades-table td {
        vertical-align: middle;
        padding: 0.65rem 0.5rem;
        white-space: nowrap;
    }

    .trades-table tbody tr {
        transition: background 0.12s ease;
    }

    .trades-table tbody tr:hover {
        background: rgba(var(--primary-rgb), 0.06) !important;
    }

    .status-badge {
        padding: 0.2rem 0.55rem;
        border-radius: 4px;
        font-size: 0.68rem;
        font-weight: 600;
        text-transform: uppercase;
        white-space: nowrap;
    }

    .status-open      { background: rgba(var(--warning-rgb), 0.15); color: rgb(var(--warning-rgb)); }
    .status-closed    { background: rgba(var(--success-rgb), 0.15); color: rgb(var(--success-rgb)); }
    .status-cancelled { background: rgba(var(--secondary-rgb), 0.15); color: var(--text-muted); }
    .status-missed    { background: rgba(var(--secondary-rgb), 0.1); color: var(--text-muted); }

    .mono {
        font-family: 'SF Mono', 'Monaco', 'Consolas', monospace;
    }

    .fee-chip {
        display: inline-block;
        background: rgba(var(--danger-rgb), 0.1);
        color: rgb(var(--danger-rgb));
        border-radius: 3px;
        padding: 0.1rem 0.4rem;
        font-size: 0.7rem;
        font-weight: 600;
    }

    .filter-row {
        background: var(--custom-white);
        border: 1px solid var(--default-border);
        border-radius: 0.5rem;
        padding: 0.85rem 1rem;
        margin-bottom: 1.25rem;
        display: flex;
        align-items: center;
        gap: 1rem;
        flex-wrap: wrap;
    }

    .pl-cell-positive { color: rgb(var(--success-rgb)); font-weight: 700; }
    .pl-cell-negative { color: rgb(var(--danger-rgb)); font-weight: 700; }
    .pl-cell-zero     { color: var(--text-muted); }

    .empty-state {
        text-align: center;
        padding: 3rem 2rem;
        border: 1px dashed var(--default-border);
        border-radius: 0.5rem;
    }

    .buyin-link {
        color: rgb(var(--primary-rgb));
        text-decoration: none;
        font-weight: 600;
    }
    .buyin-link:hover { text-decoration: underline; }

    .back-link {
        display: inline-flex;
        align-items: center;
        gap: 0.35rem;
        color: var(--text-muted);
        font-size: 0.85rem;
        text-decoration: none;
        margin-bottom: 0.5rem;
    }
    .back-link:hover { color: rgb(var(--primary-rgb)); }
</style>
<?php
$styles = ob_get_clean();

// --- Compute trade stats ---
$total_fees    = 0;
$total_pl      = 0;
$count_open    = 0;
$count_closed  = 0;
$count_cancelled = 0;
$count_missed  = 0;
$wins = 0;
$losses = 0;

foreach ($trades as $t) {
    $total_fees += floatval($t['buy_fee_usdc'] ?? 0) + floatval($t['sell_fee_usdc'] ?? 0);
    if ($t['status'] === 'closed') {
        $total_pl += floatval($t['profit_loss_usdc'] ?? 0);
        $count_closed++;
        if (floatval($t['profit_loss_usdc'] ?? 0) > 0) $wins++;
        else $losses++;
    } elseif ($t['status'] === 'open') {
        $count_open++;
    } elseif ($t['status'] === 'cancelled') {
        $count_cancelled++;
    } elseif ($t['status'] === 'missed') {
        $count_missed++;
    }
}

// --- Page Content ---
ob_start();
?>

<!-- Back link -->
<a href="/pages/features/wallets/" class="back-link">
    <i class="ri-arrow-left-line"></i> Back to Wallets
</a>

<!-- Page Header -->
<div class="d-flex align-items-center justify-content-between page-header-breadcrumb flex-wrap gap-2 mb-3">
    <div>
        <nav>
            <ol class="breadcrumb mb-1">
                <li class="breadcrumb-item"><a href="/">Dashboard</a></li>
                <li class="breadcrumb-item"><a href="/pages/features/wallets/">Wallets</a></li>
                <li class="breadcrumb-item active"><?php echo $wallet ? htmlspecialchars($wallet['name']) : 'Wallet'; ?></li>
            </ol>
        </nav>
        <h1 class="page-title fw-medium fs-18 mb-0">
            <i class="ri-wallet-3-line me-2 text-primary"></i>
            <?php echo $wallet ? htmlspecialchars($wallet['name']) : 'Wallet Detail'; ?>
            <?php if ($wallet && $wallet['is_test']): ?>
            <span class="badge bg-info-transparent text-info ms-2" style="font-size:0.65rem;">Test</span>
            <?php endif; ?>
        </h1>
    </div>
</div>

<?php if ($error_message): ?>
<div class="alert alert-danger"><i class="ri-error-warning-line me-2"></i><?php echo htmlspecialchars($error_message); ?></div>
<?php endif; ?>

<?php if ($wallet): ?>

<?php
$bal        = floatval($wallet['balance']);
$init       = floatval($wallet['initial_balance']);
$w_pl_usdc  = floatval($wallet['total_pl_usdc']);
$w_pl_pct   = floatval($wallet['total_pl_pct']);
$w_pl_sign  = $w_pl_usdc >= 0 ? '+' : '';
$w_pl_class = $w_pl_usdc > 0 ? 'text-positive' : ($w_pl_usdc < 0 ? 'text-negative' : 'text-neutral');
$invest_pct = floatval($wallet['invest_pct'] ?? 0.20) * 100;
$fee_pct    = floatval($wallet['fee_rate']) * 100;
?>

<!-- Wallet Summary Cards -->
<div class="wallet-summary">
    <div class="summary-card">
        <span class="summary-value mono">$<?php echo number_format($bal, 2); ?></span>
        <span class="summary-label">Current Balance</span>
    </div>
    <div class="summary-card">
        <span class="summary-value mono">$<?php echo number_format($init, 2); ?></span>
        <span class="summary-label">Initial Balance</span>
    </div>
    <div class="summary-card">
        <span class="summary-value mono <?php echo $w_pl_class; ?>"><?php echo $w_pl_sign; ?>$<?php echo number_format($w_pl_usdc, 2); ?></span>
        <span class="summary-label">Total P/L (USDC)</span>
    </div>
    <div class="summary-card">
        <span class="summary-value mono <?php echo $w_pl_class; ?>"><?php echo $w_pl_sign . number_format($w_pl_pct, 2); ?>%</span>
        <span class="summary-label">Total P/L (%)</span>
    </div>
    <div class="summary-card">
        <span class="summary-value mono" style="color: rgb(var(--danger-rgb));">$<?php echo number_format($total_fees, 4); ?></span>
        <span class="summary-label">Total Fees Paid</span>
    </div>
    <div class="summary-card">
        <span class="summary-value" style="color: rgb(var(--success-rgb));"><?php echo $wins; ?> <span style="color:var(--text-muted);font-size:1rem;">/ <?php echo $losses; ?></span></span>
        <span class="summary-label">Win / Loss</span>
    </div>
    <div class="summary-card">
        <span class="summary-value" style="color: rgb(var(--warning-rgb));"><?php echo $count_open; ?></span>
        <span class="summary-label">Open Trades</span>
    </div>
    <div class="summary-card">
        <span class="summary-value text-muted"><?php echo $invest_pct; ?>% / <?php echo $fee_pct; ?>%</span>
        <span class="summary-label">Invest / Fee Rate</span>
    </div>
</div>

<!-- Filters -->
<div class="filter-row">
    <form method="GET" action="" class="d-flex align-items-center gap-2 flex-wrap" style="width:100%">
        <input type="hidden" name="id" value="<?php echo $wallet_id; ?>">
        <label class="mb-0 text-muted" style="font-size:0.85rem;">Filter:</label>
        <select name="status" class="form-select form-select-sm" style="width:auto;">
            <option value="">All Statuses</option>
            <option value="open"      <?php echo $status_filter === 'open'      ? 'selected' : ''; ?>>Open</option>
            <option value="closed"    <?php echo $status_filter === 'closed'    ? 'selected' : ''; ?>>Closed</option>
            <option value="cancelled" <?php echo $status_filter === 'cancelled' ? 'selected' : ''; ?>>Cancelled</option>
            <option value="missed"    <?php echo $status_filter === 'missed'    ? 'selected' : ''; ?>>Missed</option>
        </select>
        <select name="limit" class="form-select form-select-sm" style="width:auto;">
            <option value="50"  <?php echo $limit === 50  ? 'selected' : ''; ?>>50</option>
            <option value="100" <?php echo $limit === 100 ? 'selected' : ''; ?>>100</option>
            <option value="200" <?php echo $limit === 200 ? 'selected' : ''; ?>>200</option>
            <option value="500" <?php echo $limit === 500 ? 'selected' : ''; ?>>500</option>
        </select>
        <button type="submit" class="btn btn-primary btn-sm">
            <i class="ri-filter-line me-1"></i>Apply
        </button>
        <a href="?id=<?php echo $wallet_id; ?>" class="btn btn-outline-secondary btn-sm">
            <i class="ri-refresh-line me-1"></i>Reset
        </a>
        <span class="ms-auto text-muted" style="font-size:0.8rem;">
            <?php echo count($trades); ?> trades shown &nbsp;·&nbsp;
            <?php echo $count_open; ?> open &nbsp;·&nbsp;
            <?php echo $count_closed; ?> closed &nbsp;·&nbsp;
            <?php echo $count_cancelled; ?> cancelled &nbsp;·&nbsp;
            <?php echo $count_missed; ?> missed
        </span>
    </form>
</div>

<!-- Trades Table -->
<?php if (empty($trades)): ?>
<div class="empty-state">
    <i class="ri-exchange-line text-muted d-block mb-3" style="font-size:3rem;"></i>
    <h4 class="text-muted">No trades yet</h4>
    <p class="text-muted mb-0">Trades appear as pump signals are detected and executed.</p>
</div>
<?php else: ?>
<div class="card custom-card">
    <div class="card-body p-0">
        <div class="table-responsive">
            <table class="table trades-table mb-0">
                <thead>
                    <tr>
                        <th>#</th>
                        <th>Buyin</th>
                        <th>Play</th>
                        <th>Status</th>
                        <th>Entry Price</th>
                        <th>Exit Price</th>
                        <th>Position (USDC)</th>
                        <th>SOL Bought</th>
                        <th>Buy Fee</th>
                        <th>Sell Fee</th>
                        <th>Total Fees</th>
                        <th>P/L (USDC)</th>
                        <th>P/L (%)</th>
                        <th>Opened</th>
                        <th>Closed</th>
                    </tr>
                </thead>
                <tbody>
                <?php foreach ($trades as $t):
                    $status       = $t['status'] ?? 'unknown';
                    $pl_usdc      = $t['profit_loss_usdc'] !== null ? floatval($t['profit_loss_usdc']) : null;
                    $pl_pct       = $t['profit_loss_pct']  !== null ? floatval($t['profit_loss_pct'])  : null;
                    $buy_fee      = floatval($t['buy_fee_usdc']  ?? 0);
                    $sell_fee     = floatval($t['sell_fee_usdc'] ?? 0);
                    $total_fee    = $buy_fee + $sell_fee;
                    $entry_price  = $t['entry_price'] !== null ? floatval($t['entry_price']) : null;
                    $exit_price   = $t['exit_price']  !== null ? floatval($t['exit_price'])  : null;
                    $position     = $t['position_usdc'] !== null ? floatval($t['position_usdc']) : null;
                    $sol_amt      = $t['sol_amount'] !== null ? floatval($t['sol_amount']) : null;

                    if ($pl_usdc !== null) {
                        $pl_class = $pl_usdc > 0 ? 'pl-cell-positive' : ($pl_usdc < 0 ? 'pl-cell-negative' : 'pl-cell-zero');
                        $pl_sign  = $pl_usdc >= 0 ? '+' : '';
                    }
                    if ($pl_pct !== null) {
                        $pct_sign = $pl_pct >= 0 ? '+' : '';
                    }

                ?>
                <tr>
                    <td class="text-muted"><?php echo intval($t['id']); ?></td>
                    <td>
                        <?php if ($t['buyin_id'] && $t['position_usdc'] > 0): ?>
                        <a href="/goats/unique/trade/index.php?id=<?php echo intval($t['buyin_id']); ?>" class="buyin-link" title="View buyin trade detail">
                            #<?php echo intval($t['buyin_id']); ?>
                        </a>
                        <?php else: ?>
                        <span class="text-muted">#<?php echo intval($t['buyin_id']); ?></span>
                        <?php endif; ?>
                    </td>
                    <td>
                        <?php if ($t['play_id']): ?>
                        <span class="badge bg-primary-transparent">Play <?php echo intval($t['play_id']); ?></span>
                        <?php else: ?>
                        <span class="text-muted">-</span>
                        <?php endif; ?>
                    </td>
                    <td>
                        <span class="status-badge status-<?php echo htmlspecialchars($status); ?>">
                            <?php echo htmlspecialchars($status); ?>
                        </span>
                    </td>
                    <td class="mono">
                        <?php echo $entry_price !== null ? '$' . number_format($entry_price, 4) : '-'; ?>
                    </td>
                    <td class="mono">
                        <?php if ($exit_price !== null): ?>
                        $<?php echo number_format($exit_price, 4); ?>
                        <?php elseif ($status === 'open'): ?>
                        <span class="text-warning-c" style="font-size:0.72rem;">pending…</span>
                        <?php else: ?>
                        <span class="text-muted">-</span>
                        <?php endif; ?>
                    </td>
                    <td class="mono">
                        <?php echo $position !== null && $position > 0 ? '$' . number_format($position, 2) : '-'; ?>
                    </td>
                    <td class="mono">
                        <?php echo $sol_amt !== null && $sol_amt > 0 ? number_format($sol_amt, 6) . ' SOL' : '-'; ?>
                    </td>
                    <td>
                        <?php if ($buy_fee > 0): ?>
                        <span class="fee-chip">$<?php echo number_format($buy_fee, 4); ?></span>
                        <?php else: ?>
                        <span class="text-muted">-</span>
                        <?php endif; ?>
                    </td>
                    <td>
                        <?php if ($sell_fee > 0): ?>
                        <span class="fee-chip">$<?php echo number_format($sell_fee, 4); ?></span>
                        <?php else: ?>
                        <span class="text-muted">-</span>
                        <?php endif; ?>
                    </td>
                    <td>
                        <?php if ($total_fee > 0): ?>
                        <span class="fee-chip">$<?php echo number_format($total_fee, 4); ?></span>
                        <?php else: ?>
                        <span class="text-muted">-</span>
                        <?php endif; ?>
                    </td>
                    <td class="mono">
                        <?php if ($pl_usdc !== null): ?>
                        <span class="<?php echo $pl_class; ?>"><?php echo $pl_sign; ?>$<?php echo number_format($pl_usdc, 4); ?></span>
                        <?php elseif ($status === 'open'): ?>
                        <span class="text-warning-c" style="font-size:0.72rem;">open</span>
                        <?php else: ?>
                        <span class="text-muted">-</span>
                        <?php endif; ?>
                    </td>
                    <td class="mono">
                        <?php if ($pl_pct !== null): ?>
                        <span class="<?php echo $pl_class; ?>"><?php echo $pct_sign; ?><?php echo number_format($pl_pct, 2); ?>%</span>
                        <?php else: ?>
                        <span class="text-muted">-</span>
                        <?php endif; ?>
                    </td>
                    <td style="font-size:0.75rem; color:var(--text-muted);">
                        <?php echo rel_time($t['created_at']); ?>
                    </td>
                    <td style="font-size:0.75rem; color:var(--text-muted);">
                        <?php echo rel_time($t['closed_at']); ?>
                    </td>
                </tr>
                <?php endforeach; ?>
                </tbody>
            </table>
        </div>
    </div>
</div>
<?php endif; // trades ?>

<?php endif; // wallet ?>

<script>
    // Auto-refresh every 15 seconds when there are open trades
    const hasOpen = <?php echo $count_open > 0 ? 'true' : 'false'; ?>;
    if (hasOpen) {
        setTimeout(() => location.reload(), 15000);
    }
</script>

<?php
$content = ob_get_clean();
$scripts = '';
include __DIR__ . '/../../layouts/base.php';
?>
