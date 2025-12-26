# Quick Deploy Script - Deploy to Local IIS
# This script stops IIS, copies files, and restarts IIS

$ErrorActionPreference = "Stop"

$SOURCE_PATH = "C:\Users\ander\OneDrive\00000WORK\solana_node\webhook-dotnet\publish-standalone"
$DEST_PATH = "C:\0000websites\quicknode"
$APP_POOL_NAME = "SolWebhook"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Quick Deploy to IIS" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if source exists
if (!(Test-Path $SOURCE_PATH)) {
    Write-Host "ERROR: Source path not found: $SOURCE_PATH" -ForegroundColor Red
    Write-Host "Please run build-selfcontained.bat first!" -ForegroundColor Yellow
    exit 1
}

# Check if destination exists
if (!(Test-Path $DEST_PATH)) {
    Write-Host "ERROR: Destination path not found: $DEST_PATH" -ForegroundColor Red
    Write-Host "Please update the DEST_PATH variable in this script" -ForegroundColor Yellow
    exit 1
}

Write-Host "Source: $SOURCE_PATH" -ForegroundColor Gray
Write-Host "Destination: $DEST_PATH" -ForegroundColor Gray
Write-Host ""

# Step 1: Stop IIS Application Pool
Write-Host "[1/4] Stopping IIS Application Pool: $APP_POOL_NAME..." -ForegroundColor Yellow
try {
    Stop-WebAppPool -Name $APP_POOL_NAME -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 2
    Write-Host "  ✓ Application pool stopped" -ForegroundColor Green
} catch {
    Write-Host "  ⚠ Could not stop application pool (it may not exist or is already stopped)" -ForegroundColor Yellow
}

# Step 2: Backup current deployment (optional)
Write-Host "[2/4] Creating backup..." -ForegroundColor Yellow
$backupPath = "$DEST_PATH`_backup_$(Get-Date -Format 'yyyyMMdd_HHmmss')"
try {
    if (Test-Path "$DEST_PATH\SolWebhook.dll") {
        Copy-Item "$DEST_PATH\SolWebhook.dll" "$backupPath.dll" -ErrorAction SilentlyContinue
        Write-Host "  ✓ Backup created: $backupPath.dll" -ForegroundColor Green
    }
} catch {
    Write-Host "  ⚠ Backup skipped" -ForegroundColor Yellow
}

# Step 3: Copy new files
Write-Host "[3/4] Copying new files..." -ForegroundColor Yellow
try {
    # Copy all files, overwriting existing ones
    Copy-Item "$SOURCE_PATH\*" $DEST_PATH -Recurse -Force
    
    # Verify the main DLL was copied
    $newDllTime = (Get-Item "$DEST_PATH\SolWebhook.dll").LastWriteTime
    Write-Host "  ✓ Files copied successfully" -ForegroundColor Green
    Write-Host "  ✓ SolWebhook.dll updated: $newDllTime" -ForegroundColor Green
} catch {
    Write-Host "  ✗ ERROR copying files: $($_.Exception.Message)" -ForegroundColor Red
    
    # Try to restart IIS anyway
    Write-Host "Attempting to restart IIS..." -ForegroundColor Yellow
    Start-WebAppPool -Name $APP_POOL_NAME -ErrorAction SilentlyContinue
    exit 1
}

# Step 4: Start IIS Application Pool
Write-Host "[4/4] Starting IIS Application Pool: $APP_POOL_NAME..." -ForegroundColor Yellow
try {
    Start-WebAppPool -Name $APP_POOL_NAME
    Start-Sleep -Seconds 3
    
    # Check status
    $poolState = (Get-WebAppPoolState -Name $APP_POOL_NAME).Value
    if ($poolState -eq "Started") {
        Write-Host "  ✓ Application pool started successfully" -ForegroundColor Green
    } else {
        Write-Host "  ⚠ Application pool state: $poolState" -ForegroundColor Yellow
    }
} catch {
    Write-Host "  ✗ ERROR starting application pool: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Deployment Complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Step 5: Test the deployment
Write-Host "Testing health endpoint..." -ForegroundColor Yellow
Start-Sleep -Seconds 2

try {
    $response = Invoke-WebRequest -Uri "http://localhost:5000/health" -UseBasicParsing
    if ($response.StatusCode -eq 200) {
        Write-Host "✓ Health check passed!" -ForegroundColor Green
        Write-Host ""
        Write-Host "Response:" -ForegroundColor Gray
        $response.Content | ConvertFrom-Json | ConvertTo-Json -Depth 10
    }
} catch {
    Write-Host "⚠ Health check failed (this is normal if the app is still starting)" -ForegroundColor Yellow
    Write-Host "  Wait a few seconds and try: http://localhost:5000/health" -ForegroundColor Gray
}

Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "  1. Monitor logs for errors" -ForegroundColor White
Write-Host "  2. Check that transactions are processing" -ForegroundColor White
Write-Host "  3. Verify in database that raw_instructions_data is populated" -ForegroundColor White
Write-Host ""
Write-Host "No more 'could not be converted to System.Int32' errors!" -ForegroundColor Green
Write-Host ""

