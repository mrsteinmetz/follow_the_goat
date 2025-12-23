# Build and publish the webhook
Write-Host "Building .NET Webhook..." -ForegroundColor Cyan
dotnet publish -c Release -o publish

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "Build complete! Files are in the 'publish' folder." -ForegroundColor Green
    Write-Host ""
    Write-Host "Next steps:" -ForegroundColor Yellow
    Write-Host "1. Copy the 'publish' folder contents to your IIS server"
    Write-Host "2. Follow instructions in DEPLOY.txt"
    Write-Host ""
} else {
    Write-Host ""
    Write-Host "Build failed! Make sure .NET SDK is installed." -ForegroundColor Red
    Write-Host "Download from: https://dotnet.microsoft.com/download" -ForegroundColor Yellow
    Write-Host ""
}


