Write-Host "Building Self-Contained .NET Webhook..." -ForegroundColor Cyan
Write-Host "This includes the runtime - no installation needed on server!" -ForegroundColor Yellow
Write-Host ""

# Check if dotnet is available
try {
    $dotnetVersion = dotnet --version
    Write-Host "Found .NET SDK: $dotnetVersion" -ForegroundColor Green
    Write-Host ""
} catch {
    Write-Host "ERROR: .NET SDK not found on this computer!" -ForegroundColor Red
    Write-Host ""
    Write-Host "Download .NET 6.0 SDK from:" -ForegroundColor Yellow
    Write-Host "https://dotnet.microsoft.com/download/dotnet/6.0" -ForegroundColor Cyan
    Write-Host ""
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host "Building for Windows x64 (self-contained)..." -ForegroundColor Cyan
dotnet publish -c Release -o publish-standalone --self-contained true -r win-x64

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "SUCCESS! Build complete!" -ForegroundColor Green
    Write-Host ""
    Write-Host "Files are in: publish-standalone\" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Next steps:" -ForegroundColor Cyan
    Write-Host "1. Copy ALL files from 'publish-standalone' to your IIS server"
    Write-Host "2. Replace web.config with web-selfcontained.config"
    Write-Host "3. Install ASP.NET Core Module (if not already installed)"
    Write-Host "4. Run 'iisreset' on the server"
    Write-Host ""
    Write-Host "See START_HERE.txt for detailed instructions" -ForegroundColor Yellow
    Write-Host ""
} else {
    Write-Host ""
    Write-Host "Build FAILED!" -ForegroundColor Red
    Write-Host "Check the error messages above" -ForegroundColor Yellow
    Write-Host ""
}

Read-Host "Press Enter to exit"


