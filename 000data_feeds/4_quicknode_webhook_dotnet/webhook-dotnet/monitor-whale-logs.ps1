# Monitor Whale Activity Endpoint Logs in Real-Time
# Run this after deploying to see logs as they come in

$logPath = "C:\inetpub\wwwroot\solwebhook\logs"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Whale Activity Endpoint Log Monitor" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if logs directory exists
if (-not (Test-Path $logPath)) {
    Write-Host "[ERROR] Logs directory not found: $logPath" -ForegroundColor Red
    Write-Host ""
    Write-Host "Make sure:" -ForegroundColor Yellow
    Write-Host "1. IIS webhook is deployed" -ForegroundColor Yellow
    Write-Host "2. Stdout logging is enabled in web.config" -ForegroundColor Yellow
    Write-Host "3. The logs directory exists" -ForegroundColor Yellow
    exit 1
}

# Find the most recent log file
$logFiles = Get-ChildItem "$logPath\stdout_*.log" -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending

if ($logFiles.Count -eq 0) {
    Write-Host "[WARNING] No log files found in $logPath" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Waiting for logs to be created..." -ForegroundColor Yellow
    Write-Host "Try sending a test request to the endpoint." -ForegroundColor Yellow
    Write-Host ""
    
    # Create a dummy file to watch
    $watchFile = "$logPath\stdout_*.log"
} else {
    $latestLog = $logFiles[0]
    Write-Host "[INFO] Monitoring: $($latestLog.Name)" -ForegroundColor Green
    Write-Host "[INFO] Last modified: $($latestLog.LastWriteTime)" -ForegroundColor Green
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host ""
    
    # Show last 20 lines first
    Write-Host "--- Last 20 lines ---" -ForegroundColor Yellow
    Get-Content $latestLog.FullName -Tail 20
    Write-Host ""
    Write-Host "--- Waiting for new entries (Ctrl+C to exit) ---" -ForegroundColor Yellow
    Write-Host ""
    
    $watchFile = $latestLog.FullName
}

# Monitor for new entries
try {
    Get-Content $watchFile -Wait -Tail 0 | ForEach-Object {
        $line = $_
        
        # Color code based on content
        if ($line -match "\[ERROR\]") {
            Write-Host $line -ForegroundColor Red
        }
        elseif ($line -match "\[WARNING\]") {
            Write-Host $line -ForegroundColor Yellow
        }
        elseif ($line -match "\[SUCCESS\]") {
            Write-Host $line -ForegroundColor Green
        }
        elseif ($line -match "WHALE WEBHOOK - Received request") {
            Write-Host $line -ForegroundColor Cyan
        }
        elseif ($line -match "SUMMARY:") {
            Write-Host $line -ForegroundColor Magenta
        }
        elseif ($line -match "============|========") {
            Write-Host $line -ForegroundColor Cyan
        }
        elseif ($line -match "ProcessWhaleMovements - (START|END)") {
            Write-Host $line -ForegroundColor Magenta
        }
        else {
            Write-Host $line
        }
    }
}
catch {
    Write-Host ""
    Write-Host "[ERROR] Monitoring stopped: $($_.Exception.Message)" -ForegroundColor Red
}

