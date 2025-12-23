@echo off
echo Building Self-Contained .NET Webhook...
echo This includes the runtime, so you don't need to install .NET on the server!
echo.

echo Clearing NuGet cache...
dotnet nuget locals all --clear
echo.

echo Restoring packages...
dotnet restore
echo.

echo Building for Windows x64 (self-contained)...
dotnet publish -c Release -o publish-standalone --self-contained true -r win-x64
echo.

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ============================================
    echo SUCCESS! Build complete!
    echo ============================================
    echo.
    echo Files are in the 'publish-standalone' folder.
    echo.
    echo Next steps:
    echo 1. Copy ALL files from 'publish-standalone' to your IIS server
    echo 2. On server: Delete web.config, rename web-selfcontained.config to web.config
    echo 3. Make sure ASP.NET Core Module is installed on server
    echo 4. Run 'iisreset' on the server
    echo 5. Test: http://your-server/health
    echo.
    echo This version does NOT require .NET to be installed on the server!
    echo.
) else (
    echo.
    echo ============================================
    echo Build FAILED!
    echo ============================================
    echo.
    echo Possible issues:
    echo 1. .NET SDK not installed - Download: https://dotnet.microsoft.com/download/dotnet/8.0
    echo 2. NuGet connectivity issues - Check internet connection
    echo 3. Package restore failed - Try running 'dotnet restore' manually
    echo.
)
pause

