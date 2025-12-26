<?php
/**
 * Pattern Config Builder Page - Project Management
 * Migrated from: 000old_code/solana_node/v2/pattern-builder/index.php
 * 
 * Uses DuckDB API for data operations (dual-writes to MySQL + DuckDB)
 */

// --- DuckDB API Client ---
require_once __DIR__ . '/../../../includes/DuckDBClient.php';
define('DUCKDB_API_URL', 'http://127.0.0.1:5050');
$duckdb = new DuckDBClient(DUCKDB_API_URL);
$use_duckdb = $duckdb->isAvailable();

// --- Base URL for template ---
$baseUrl = '';

$error_message = '';
$success_message = '';
$projects = [];

// Handle POST actions
if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    $action = $_POST['action'] ?? '';
    
    if ($action === 'create_project') {
        $project_name = trim($_POST['project_name'] ?? '');
        $project_description = trim($_POST['project_description'] ?? '');
        
        if (!empty($project_name)) {
            $result = $duckdb->createPatternProject($project_name, $project_description ?: null);
            if ($result && isset($result['success']) && $result['success']) {
                $success_message = "Project '{$project_name}' created successfully!";
            } else {
                $error_message = $result['error'] ?? 'Failed to create project.';
            }
        } else {
            $error_message = 'Project name is required.';
        }
    } elseif ($action === 'delete_project') {
        $project_id = (int) ($_POST['project_id'] ?? 0);
        if ($project_id > 0) {
            $result = $duckdb->deletePatternProject($project_id);
            if ($result && isset($result['success']) && $result['success']) {
                $success_message = 'Project deleted successfully.';
            } else {
                $error_message = $result['error'] ?? 'Failed to delete project.';
            }
        }
    }
    
    // Redirect to prevent form resubmission
    header('Location: ' . $_SERVER['PHP_SELF'] . ($success_message ? '?msg=success' : ($error_message ? '?msg=error&err=' . urlencode($error_message) : '')));
    exit;
}

// Check for messages from redirect
if (isset($_GET['msg'])) {
    if ($_GET['msg'] === 'success') {
        $success_message = 'Operation completed successfully.';
    } elseif ($_GET['msg'] === 'error') {
        $error_message = $_GET['err'] ?? 'An error occurred.';
    }
}

// Fetch all projects
if ($use_duckdb) {
    $response = $duckdb->getPatternProjects();
    if ($response && isset($response['projects'])) {
        $projects = $response['projects'];
    }
} else {
    $error_message = "DuckDB API is not available. Please start the scheduler: python scheduler/master.py";
}

// --- Page Styles ---
ob_start();
?>
<style>
    .projects-grid {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
        gap: 1.25rem;
    }
    
    .project-card {
        background: var(--custom-white);
        border: 1px solid var(--default-border);
        border-radius: 0.5rem;
        padding: 1.25rem;
        transition: all 0.2s ease;
        cursor: pointer;
    }
    
    .project-card:hover {
        border-color: rgb(var(--primary-rgb));
        box-shadow: 0 4px 15px rgba(var(--primary-rgb), 0.15);
        transform: translateY(-2px);
    }
    
    .project-card-header {
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        margin-bottom: 0.75rem;
    }
    
    .project-name {
        font-size: 1.1rem;
        font-weight: 600;
        color: var(--default-text-color);
        margin: 0;
    }
    
    .project-description {
        color: var(--text-muted);
        font-size: 0.9rem;
        margin-bottom: 1rem;
        line-height: 1.5;
        min-height: 1.5rem;
    }
    
    .project-meta {
        display: flex;
        flex-wrap: wrap;
        gap: 1rem;
        padding-top: 0.75rem;
        border-top: 1px solid var(--default-border);
    }
    
    .meta-item {
        display: flex;
        flex-direction: column;
        gap: 0.15rem;
    }
    
    .meta-label {
        font-size: 0.65rem;
        text-transform: uppercase;
        color: var(--text-muted);
        letter-spacing: 0.05em;
    }
    
    .meta-value {
        font-size: 0.85rem;
        font-weight: 600;
        color: var(--default-text-color);
    }
    
    .meta-value.filters { color: rgb(var(--primary-rgb)); }
    .meta-value.active { color: rgb(var(--success-rgb)); }
    
    .empty-state {
        text-align: center;
        padding: 3rem 2rem;
        border: 1px dashed var(--default-border);
        border-radius: 0.5rem;
    }
    
    .empty-state-icon {
        font-size: 3rem;
        margin-bottom: 1rem;
    }
    
    /* Delete Modal */
    .delete-modal {
        display: none;
        position: fixed;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        background: rgba(0, 0, 0, 0.5);
        z-index: 1050;
        justify-content: center;
        align-items: center;
    }
    
    .delete-modal.active {
        display: flex;
    }
    
    .delete-modal-content {
        background: var(--custom-white);
        border: 1px solid var(--default-border);
        border-radius: 0.5rem;
        padding: 1.5rem;
        max-width: 400px;
        width: 90%;
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
                <li class="breadcrumb-item"><a href="<?php echo $baseUrl; ?>/pages/features/patterns/">Features</a></li>
                <li class="breadcrumb-item active" aria-current="page">Patterns</li>
            </ol>
        </nav>
        <h1 class="page-title fw-medium fs-18 mb-0">Pattern Config Projects</h1>
    </div>
</div>

<!-- Messages -->
<?php if ($success_message): ?>
<div class="alert alert-success alert-dismissible fade show" role="alert">
    <i class="ri-checkbox-circle-line me-2"></i><?php echo htmlspecialchars($success_message); ?>
    <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
</div>
<?php endif; ?>

<?php if ($error_message): ?>
<div class="alert alert-danger alert-dismissible fade show" role="alert">
    <i class="ri-error-warning-line me-2"></i><?php echo htmlspecialchars($error_message); ?>
    <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
</div>
<?php endif; ?>

<!-- Create Project Card -->
<div class="card custom-card mb-3">
    <div class="card-header">
        <h6 class="mb-0"><i class="ri-add-circle-line me-1"></i>Create New Project</h6>
    </div>
    <div class="card-body">
        <form method="POST" action="" class="row g-3 align-items-end">
            <input type="hidden" name="action" value="create_project">
            
            <div class="col-md-4">
                <label for="project_name" class="form-label">Project Name *</label>
                <input type="text" class="form-control" name="project_name" id="project_name" placeholder="e.g., Whale Activity Research" required>
            </div>
            
            <div class="col-md-5">
                <label for="project_description" class="form-label">Description (optional)</label>
                <input type="text" class="form-control" name="project_description" id="project_description" placeholder="Brief description of what you're analyzing...">
            </div>
            
            <div class="col-md-3">
                <button type="submit" class="btn btn-success w-100" <?php echo !$use_duckdb ? 'disabled' : ''; ?>>
                    <i class="ri-rocket-line me-1"></i>Create Project
                </button>
            </div>
        </form>
    </div>
</div>

<!-- Stats Card -->
<div class="card custom-card mb-3">
    <div class="card-body">
        <div class="d-flex align-items-center justify-content-between">
            <div>
                <span class="text-muted fs-12">Total Projects</span>
                <h4 class="mb-0 fw-semibold"><?php echo count($projects); ?></h4>
            </div>
            <div class="avatar avatar-lg bg-primary-transparent">
                <i class="ri-folder-chart-line fs-24 text-primary"></i>
            </div>
        </div>
    </div>
</div>

<!-- Projects Grid -->
<?php if (empty($projects)): ?>
<div class="empty-state">
    <div class="empty-state-icon">
        <i class="ri-folder-open-line text-muted"></i>
    </div>
    <h4 class="text-muted">No projects yet</h4>
    <p class="text-muted mb-0">Create your first project above to start analyzing trading patterns.</p>
</div>
<?php else: ?>
<div class="projects-grid">
    <?php foreach ($projects as $project): ?>
        <div class="project-card" onclick="navigateToProject(<?php echo $project['id']; ?>, event)">
            <div class="project-card-header">
                <h4 class="project-name"><?php echo htmlspecialchars($project['name']); ?></h4>
                <button type="button" class="btn btn-sm btn-outline-danger" onclick="showDeleteModal(<?php echo $project['id']; ?>, '<?php echo htmlspecialchars(addslashes($project['name'])); ?>', event)" title="Delete">
                    <i class="ri-delete-bin-line"></i>
                </button>
            </div>
            <p class="project-description">
                <?php echo $project['description'] ? htmlspecialchars($project['description']) : '<em class="text-muted">No description</em>'; ?>
            </p>
            <div class="project-meta">
                <div class="meta-item">
                    <span class="meta-label">Filters</span>
                    <span class="meta-value filters"><?php echo (int) ($project['filter_count'] ?? 0); ?> total</span>
                </div>
                <div class="meta-item">
                    <span class="meta-label">Active</span>
                    <span class="meta-value active"><?php echo (int) ($project['active_filter_count'] ?? 0); ?> active</span>
                </div>
                <div class="meta-item">
                    <span class="meta-label">Created</span>
                    <span class="meta-value"><?php echo $project['created_at'] ? date('M j, Y', strtotime($project['created_at'])) : '-'; ?></span>
                </div>
                <div class="meta-item">
                    <span class="meta-label">Updated</span>
                    <span class="meta-value"><?php echo $project['updated_at'] ? date('M j, g:ia', strtotime($project['updated_at'])) : '-'; ?></span>
                </div>
            </div>
        </div>
    <?php endforeach; ?>
</div>
<?php endif; ?>

<!-- Delete Confirmation Modal -->
<div class="delete-modal" id="deleteModal">
    <div class="delete-modal-content">
        <h5 class="mb-3"><i class="ri-delete-bin-line text-danger me-2"></i>Delete Project</h5>
        <p class="text-muted">Are you sure you want to delete "<span id="deleteProjectName" class="fw-semibold"></span>"? This will also delete all associated filters.</p>
        <form method="POST" action="" id="deleteForm">
            <input type="hidden" name="action" value="delete_project">
            <input type="hidden" name="project_id" id="deleteProjectId">
            <div class="d-flex gap-2 justify-content-end">
                <button type="button" class="btn btn-secondary" onclick="hideDeleteModal()">Cancel</button>
                <button type="submit" class="btn btn-danger">Delete</button>
            </div>
        </form>
    </div>
</div>

<script>
    function navigateToProject(projectId, event) {
        if (event.target.closest('.btn-outline-danger')) return;
        // TODO: Navigate to project detail page when it's created
        // window.location.href = '/pages/features/patterns/project.php?id=' + projectId;
        alert('Project detail page coming soon! Project ID: ' + projectId);
    }

    function showDeleteModal(projectId, projectName, event) {
        event.stopPropagation();
        document.getElementById('deleteProjectId').value = projectId;
        document.getElementById('deleteProjectName').textContent = projectName;
        document.getElementById('deleteModal').classList.add('active');
    }

    function hideDeleteModal() {
        document.getElementById('deleteModal').classList.remove('active');
    }

    document.getElementById('deleteModal').addEventListener('click', function(e) {
        if (e.target === this) hideDeleteModal();
    });
    
    // Close modal on Escape key
    document.addEventListener('keydown', function(e) {
        if (e.key === 'Escape') hideDeleteModal();
    });
</script>

<?php
$content = ob_get_clean();
$scripts = '';

// Include the base layout
include __DIR__ . '/../../layouts/base.php';
?>

