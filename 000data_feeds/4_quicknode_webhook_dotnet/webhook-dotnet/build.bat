@echo off
echo Building .NET Webhook...
echo.

dotnet --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: .NET SDK is not installed on this computer!
    echo.
    echo You need .NET SDK to build the project.
    echo Download from: https://dotnet.microsoft.com/download/dotnet/6.0
    echo.
    echo OR use build-selfcontained.bat if you have .NET SDK
    echo.
    pause
    exit /b 1
)

echo Detected .NET SDK: 
dotnet --version
echo.

echo Building standard deployment...
dotnet publish -c Release -o publish
echo.

if %ERRORLEVEL% EQU 0 (
    echo Build complete! Files are in the 'publish' folder.
    echo.
    echo IMPORTANT: Your server needs .NET 6.0 Hosting Bundle installed!
    echo If you get HTTP 500.31 error, see FIX_500_ERROR.txt
    echo.
    echo OR run build-selfcontained.bat for no-install-needed version
    echo.
) else (
    echo Build failed!
    echo.
)
pause

