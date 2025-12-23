.NET CORE WEBHOOK FOR SOLANA TRADES
====================================

QUICK START:
1. Build: build.bat
2. Test locally: dotnet run (then use test.bat in another terminal)
3. Deploy: Copy 'publish' folder contents to IIS server
4. Configure IIS (see DEPLOY.txt)

ENDPOINTS:
- POST / - Main webhook (for QuickNode)
- GET /health - Health check

ADVANTAGES OVER PHP:
- 200x faster server
- Native async processing
- Better memory management
- Compiled code (faster execution)
- Built-in connection pooling

FILES:
- Program.cs - Main application
- SolWebhook.csproj - Project file
- web.config - IIS configuration
- appsettings.json - Application settings

REQUIREMENTS:
- .NET 6.0+ SDK (for building)
- .NET 6.0+ Runtime or Hosting Bundle (for IIS)

BUILD LOCALLY:
dotnet build

RUN LOCALLY:
dotnet run

PUBLISH FOR IIS:
dotnet publish -c Release -o publish

