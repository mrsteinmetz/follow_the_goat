using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Timers;
using MySqlConnector;
using DuckDB.NET.Data;

// Database connection string
const string ConnectionString = "Server=116.202.51.115;Database=solcatcher;User ID=solcatcher;Password=jjJH!la9823JKJsdfjk76jH;Connection Timeout=5;Default Command Timeout=30;";

// =============================================================================
// DuckDB In-Memory Hot Storage (24hr retention)
// =============================================================================

// Global in-memory DuckDB connection (singleton pattern like Python TradingDataEngine)
var duckDbConnection = new DuckDBConnection("Data Source=:memory:");
duckDbConnection.Open();
Console.WriteLine("[DUCKDB] In-memory database connection opened");

// Initialize DuckDB tables
InitializeDuckDbTables(duckDbConnection);

// DuckDB connection lock for thread safety
var duckDbLock = new object();

// Background cleanup timer (every hour, removes data older than 24h)
var cleanupTimer = new System.Timers.Timer(TimeSpan.FromHours(1).TotalMilliseconds);
cleanupTimer.Elapsed += (sender, e) => CleanupOldDuckDbData(duckDbConnection, duckDbLock);
cleanupTimer.AutoReset = true;
cleanupTimer.Start();
Console.WriteLine("[DUCKDB] Background cleanup timer started (hourly, 24hr retention)");

void InitializeDuckDbTables(DuckDBConnection conn)
{
    Console.WriteLine("[DUCKDB] Initializing tables...");
    
    // Create sol_stablecoin_trades table (hot storage)
    const string createTradesTable = @"
        CREATE TABLE IF NOT EXISTS sol_stablecoin_trades (
            id BIGINT PRIMARY KEY,
            signature VARCHAR NOT NULL,
            wallet_address VARCHAR NOT NULL,
            direction VARCHAR NOT NULL,
            sol_amount DOUBLE NOT NULL,
            stablecoin VARCHAR NOT NULL,
            stablecoin_amount DOUBLE NOT NULL,
            price DOUBLE NOT NULL,
            block_height BIGINT NOT NULL,
            slot BIGINT NOT NULL,
            block_time BIGINT NOT NULL,
            trade_timestamp TIMESTAMP NOT NULL,
            has_perp_position BOOLEAN DEFAULT FALSE,
            perp_platform VARCHAR,
            perp_direction VARCHAR,
            perp_size DOUBLE,
            perp_leverage DOUBLE,
            perp_entry_price DOUBLE,
            raw_instructions_data VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )";
    
    // Create whale_movements table (hot storage)
    const string createWhaleTable = @"
        CREATE TABLE IF NOT EXISTS whale_movements (
            id BIGINT PRIMARY KEY,
            signature VARCHAR NOT NULL,
            wallet_address VARCHAR NOT NULL,
            whale_type VARCHAR NOT NULL,
            current_balance DOUBLE,
            sol_change DOUBLE,
            abs_change DOUBLE,
            percentage_moved DOUBLE,
            direction VARCHAR,
            action VARCHAR,
            movement_significance VARCHAR,
            previous_balance DOUBLE,
            fee_paid DOUBLE,
            block_time BIGINT,
            timestamp TIMESTAMP NOT NULL,
            received_at TIMESTAMP NOT NULL,
            slot BIGINT,
            has_perp_position BOOLEAN DEFAULT FALSE,
            perp_platform VARCHAR,
            perp_direction VARCHAR,
            perp_size DOUBLE,
            perp_leverage DOUBLE,
            perp_entry_price DOUBLE,
            raw_data_json VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )";
    
    using var cmd1 = conn.CreateCommand();
    cmd1.CommandText = createTradesTable;
    cmd1.ExecuteNonQuery();
    Console.WriteLine("[DUCKDB] Created table: sol_stablecoin_trades");
    
    using var cmd2 = conn.CreateCommand();
    cmd2.CommandText = createWhaleTable;
    cmd2.ExecuteNonQuery();
    Console.WriteLine("[DUCKDB] Created table: whale_movements");
    
    // Create indexes for fast queries
    var indexes = new[]
    {
        "CREATE INDEX IF NOT EXISTS idx_trades_signature ON sol_stablecoin_trades(signature)",
        "CREATE INDEX IF NOT EXISTS idx_trades_wallet ON sol_stablecoin_trades(wallet_address)",
        "CREATE INDEX IF NOT EXISTS idx_trades_created_at ON sol_stablecoin_trades(created_at)",
        "CREATE INDEX IF NOT EXISTS idx_trades_direction ON sol_stablecoin_trades(direction)",
        "CREATE INDEX IF NOT EXISTS idx_whale_signature ON whale_movements(signature)",
        "CREATE INDEX IF NOT EXISTS idx_whale_wallet ON whale_movements(wallet_address)",
        "CREATE INDEX IF NOT EXISTS idx_whale_created_at ON whale_movements(created_at)",
        "CREATE INDEX IF NOT EXISTS idx_whale_type ON whale_movements(whale_type)"
    };
    
    foreach (var indexSql in indexes)
    {
        using var idxCmd = conn.CreateCommand();
        idxCmd.CommandText = indexSql;
        idxCmd.ExecuteNonQuery();
    }
    Console.WriteLine("[DUCKDB] Created indexes");
    
    Console.WriteLine("[DUCKDB] Table initialization complete");
}

void CleanupOldDuckDbData(DuckDBConnection conn, object lockObj)
{
    try
    {
        Console.WriteLine("[DUCKDB] Starting cleanup of data older than 24 hours...");
        
        lock (lockObj)
        {
            // Delete trades older than 24 hours
            using var tradesCmd = conn.CreateCommand();
            tradesCmd.CommandText = "DELETE FROM sol_stablecoin_trades WHERE created_at < NOW() - INTERVAL 24 HOUR";
            var tradesDeleted = tradesCmd.ExecuteNonQuery();
            
            // Delete whale movements older than 24 hours
            using var whaleCmd = conn.CreateCommand();
            whaleCmd.CommandText = "DELETE FROM whale_movements WHERE created_at < NOW() - INTERVAL 24 HOUR";
            var whaleDeleted = whaleCmd.ExecuteNonQuery();
            
            Console.WriteLine($"[DUCKDB] Cleanup complete: {tradesDeleted} trades, {whaleDeleted} whale movements removed");
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[DUCKDB] [ERROR] Cleanup failed: {ex.Message}");
    }
}

// Auto-increment ID tracking for DuckDB (since it doesn't have AUTO_INCREMENT)
long _nextTradeId = 1;
long _nextWhaleId = 1;
var _idLock = new object();

long GetNextTradeId()
{
    lock (_idLock) { return _nextTradeId++; }
}

long GetNextWhaleId()
{
    lock (_idLock) { return _nextWhaleId++; }
}

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

TeeTextWriter? whaleConsoleWriter = null;

try
{
    var originalConsoleOut = Console.Out;
    var logsDirectory = Path.Combine(AppContext.BaseDirectory, "logs");
    Directory.CreateDirectory(logsDirectory);
    var logPath = Path.Combine(logsDirectory, $"whale-activity-{DateTime.UtcNow:yyyyMMdd}.log");
    var fileStream = new FileStream(logPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
    var fileWriter = new StreamWriter(fileStream) { AutoFlush = true };
    whaleConsoleWriter = new TeeTextWriter(originalConsoleOut, fileWriter);
    Console.SetOut(whaleConsoleWriter);
    Console.WriteLine($"[LOGGING] Whale activity log initialized at {logPath}");
}
catch (Exception logSetupEx)
{
    Console.WriteLine($"[LOGGING] Failed to initialize whale activity log file: {logSetupEx.Message}");
}

if (whaleConsoleWriter != null)
{
    void DisposeLogWriter()
    {
        whaleConsoleWriter.Dispose();
    }

    app.Lifetime.ApplicationStopping.Register(DisposeLogWriter);
    AppDomain.CurrentDomain.ProcessExit += (_, __) => DisposeLogWriter();
}

// Register DuckDB cleanup on shutdown
void DisposeDuckDb()
{
    try
    {
        cleanupTimer.Stop();
        cleanupTimer.Dispose();
        duckDbConnection.Close();
        duckDbConnection.Dispose();
        Console.WriteLine("[DUCKDB] Connection closed and cleaned up");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[DUCKDB] [ERROR] Cleanup on shutdown failed: {ex.Message}");
    }
}
app.Lifetime.ApplicationStopping.Register(DisposeDuckDb);
AppDomain.CurrentDomain.ProcessExit += (_, __) => DisposeDuckDb();

// Root endpoint - Welcome page
app.MapGet("/", () =>
{
    return Results.Content(@"
<!DOCTYPE html>
<html>
<head>
    <title>SOL Webhook</title>
    <style>
        body { font-family: Arial, sans-serif; max-width: 800px; margin: 50px auto; padding: 20px; }
        h1 { color: #333; }
        .status { background: #e8f5e9; padding: 15px; border-radius: 5px; margin: 20px 0; }
        .endpoint { background: #f5f5f5; padding: 10px; margin: 10px 0; border-left: 3px solid #2196F3; }
        code { background: #f5f5f5; padding: 2px 5px; border-radius: 3px; }
    </style>
</head>
<body>
    <h1>🚀 SOL Stablecoin Trades Webhook</h1>
    <div class='status'>
        <strong>✓ Webhook is running!</strong>
    </div>
    <h2>Available Endpoints:</h2>
    <div class='endpoint'>
        <strong>GET /health</strong><br>
        Check webhook health and recent activity<br>
        <a href='/health'>→ View Health Status</a>
    </div>
    <div class='endpoint'>
        <strong>POST /</strong><br>
        Main webhook endpoint (for QuickNode)<br>
        Accepts JSON trade data
    </div>
    <div class='endpoint'>
        <strong>POST /webhooks/whale-activity</strong><br>
        Whale wallet tracking endpoint<br>
        Accepts whale movement data from QuickNode stream
    </div>
    <div class='endpoint'>
        <strong>GET /api/whale-movements</strong><br>
        Get whale movements from DuckDB in-memory (24hr hot storage)<br>
        <a href='/api/whale-movements?limit=10'>→ View Latest 10 Whale Movements</a>
    </div>
    <div class='endpoint'>
        <strong>GET /api/trades</strong><br>
        Get trades from DuckDB in-memory (24hr hot storage)<br>
        <a href='/api/trades?limit=10'>→ View Latest 10 Trades</a>
    </div>
    <h2>Configuration:</h2>
    <ul>
        <li>Database: Dual-Write (DuckDB + MySQL)</li>
        <li>Hot Storage: DuckDB In-Memory (24hr retention)</li>
        <li>Master Storage: MySQL (full history)</li>
        <li>Response Time: ~5-10ms</li>
        <li>Processing: Asynchronous</li>
        <li>Platform: .NET 8.0 (Self-Contained)</li>
    </ul>
    <p><small>Running on IIS - " + DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss") + @" UTC</small></p>
</body>
</html>
", "text/html");
});

// Health check endpoint
app.MapGet("/health", async () =>
{
    try
    {
        // MySQL stats
        await using var conn = new MySqlConnection(ConnectionString);
        await conn.OpenAsync();
        
        var tradesCmd = new MySqlCommand(@"
            SELECT COUNT(*) as recent_count 
            FROM sol_stablecoin_trades 
            WHERE created_at >= DATE_SUB(NOW(), INTERVAL 5 MINUTE)", conn);
        
        var recentTrades = Convert.ToInt32(await tradesCmd.ExecuteScalarAsync());
        
        var whaleCmd = new MySqlCommand(@"
            SELECT COUNT(*) as recent_count 
            FROM whale_movements 
            WHERE created_at >= DATE_SUB(NOW(), INTERVAL 5 MINUTE)", conn);
        
        var recentWhaleMovements = 0;
        try
        {
            recentWhaleMovements = Convert.ToInt32(await whaleCmd.ExecuteScalarAsync());
        }
        catch
        {
            // Table might not exist yet, ignore
        }
        
        // DuckDB stats
        int duckDbTrades = 0;
        int duckDbWhale = 0;
        string duckDbStatus = "ok";
        try
        {
            lock (duckDbLock)
            {
                using var duckTradesCmd = duckDbConnection.CreateCommand();
                duckTradesCmd.CommandText = "SELECT COUNT(*) FROM sol_stablecoin_trades";
                duckDbTrades = Convert.ToInt32(duckTradesCmd.ExecuteScalar());
                
                using var duckWhaleCmd = duckDbConnection.CreateCommand();
                duckWhaleCmd.CommandText = "SELECT COUNT(*) FROM whale_movements";
                duckDbWhale = Convert.ToInt32(duckWhaleCmd.ExecuteScalar());
            }
        }
        catch (Exception duckEx)
        {
            duckDbStatus = $"error: {duckEx.Message}";
        }
        
        return Results.Ok(new
        {
            status = "healthy",
            timestamp = DateTime.UtcNow,
            mysql = new
            {
                recent_trades = recentTrades,
                recent_whale_movements = recentWhaleMovements
            },
            duckdb = new
            {
                status = duckDbStatus,
                trades_in_hot_storage = duckDbTrades,
                whale_movements_in_hot_storage = duckDbWhale,
                retention = "24 hours"
            }
        });
    }
    catch (Exception ex)
    {
        return Results.Ok(new
        {
            status = "error",
            message = ex.Message,
            timestamp = DateTime.UtcNow
        });
    }
});

// =============================================================================
// API Endpoints - Read from DuckDB In-Memory Hot Storage
// =============================================================================

// GET /api/whale-movements - Fetch whale movements from DuckDB
// Supports time-range filtering with start/end parameters (ISO 8601 format)
app.MapGet("/api/whale-movements", (int? limit, DateTime? start, DateTime? end) =>
{
    try
    {
        // When time-range is specified, allow up to 10000 records; otherwise cap at 500
        var maxLimit = (start.HasValue || end.HasValue) 
            ? Math.Min(limit ?? 10000, 10000) 
            : Math.Min(limit ?? 100, 500);
        var results = new List<object>();
        
        lock (duckDbLock)
        {
            using var cmd = duckDbConnection.CreateCommand();
            
            // Build WHERE clause for time filtering
            var whereClauses = new List<string>();
            if (start.HasValue)
                whereClauses.Add($"timestamp >= '{start.Value:yyyy-MM-dd HH:mm:ss}'");
            if (end.HasValue)
                whereClauses.Add($"timestamp <= '{end.Value:yyyy-MM-dd HH:mm:ss}'");
            
            var whereClause = whereClauses.Count > 0 
                ? "WHERE " + string.Join(" AND ", whereClauses) 
                : "";
            
            cmd.CommandText = $@"
                SELECT id, signature, wallet_address, whale_type, current_balance, 
                       sol_change, abs_change, percentage_moved, direction, action,
                       movement_significance, previous_balance, fee_paid, block_time,
                       timestamp, received_at, slot, has_perp_position, perp_platform,
                       perp_direction, perp_size, perp_leverage, perp_entry_price,
                       created_at
                FROM whale_movements 
                {whereClause}
                ORDER BY timestamp DESC 
                LIMIT {maxLimit}";
            
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                results.Add(new
                {
                    id = reader.GetInt64(0),
                    signature = reader.GetString(1),
                    wallet_address = reader.GetString(2),
                    whale_type = reader.GetString(3),
                    current_balance = reader.IsDBNull(4) ? 0 : reader.GetDouble(4),
                    sol_change = reader.IsDBNull(5) ? 0 : reader.GetDouble(5),
                    abs_change = reader.IsDBNull(6) ? 0 : reader.GetDouble(6),
                    percentage_moved = reader.IsDBNull(7) ? 0 : reader.GetDouble(7),
                    direction = reader.IsDBNull(8) ? "" : reader.GetString(8),
                    action = reader.IsDBNull(9) ? "" : reader.GetString(9),
                    movement_significance = reader.IsDBNull(10) ? "" : reader.GetString(10),
                    previous_balance = reader.IsDBNull(11) ? 0 : reader.GetDouble(11),
                    fee_paid = reader.IsDBNull(12) ? 0 : reader.GetDouble(12),
                    block_time = reader.IsDBNull(13) ? 0 : reader.GetInt64(13),
                    timestamp = reader.IsDBNull(14) ? DateTime.UtcNow : reader.GetDateTime(14),
                    received_at = reader.IsDBNull(15) ? DateTime.UtcNow : reader.GetDateTime(15),
                    slot = reader.IsDBNull(16) ? 0 : reader.GetInt64(16),
                    has_perp_position = reader.IsDBNull(17) ? false : reader.GetBoolean(17),
                    perp_platform = reader.IsDBNull(18) ? null : reader.GetString(18),
                    perp_direction = reader.IsDBNull(19) ? null : reader.GetString(19),
                    perp_size = reader.IsDBNull(20) ? (double?)null : reader.GetDouble(20),
                    perp_leverage = reader.IsDBNull(21) ? (double?)null : reader.GetDouble(21),
                    perp_entry_price = reader.IsDBNull(22) ? (double?)null : reader.GetDouble(22),
                    created_at = reader.IsDBNull(23) ? DateTime.UtcNow : reader.GetDateTime(23)
                });
            }
        }
        
        return Results.Ok(new
        {
            success = true,
            source = "duckdb_inmemory",
            count = results.Count,
            results = results,
            timestamp = DateTime.UtcNow
        });
    }
    catch (Exception ex)
    {
        return Results.Ok(new
        {
            success = false,
            source = "duckdb_inmemory",
            error = ex.Message,
            results = new List<object>(),
            timestamp = DateTime.UtcNow
        });
    }
});

// GET /api/trades - Fetch trades from DuckDB
// Supports:
//   - after_id: Incremental sync - only return trades with id > after_id (FAST, preferred)
//   - start/end: Time-range filtering with ISO 8601 format (legacy)
//   - limit: Max records to return
app.MapGet("/api/trades", (int? limit, long? after_id, DateTime? start, DateTime? end) =>
{
    try
    {
        // For incremental sync (after_id), allow up to 5000 records per batch
        // For time-range queries, allow up to 10000; otherwise cap at 500
        var maxLimit = after_id.HasValue
            ? Math.Min(limit ?? 5000, 5000)
            : (start.HasValue || end.HasValue) 
                ? Math.Min(limit ?? 10000, 10000) 
                : Math.Min(limit ?? 100, 500);
        
        var results = new List<object>();
        long maxId = 0;
        
        lock (duckDbLock)
        {
            using var cmd = duckDbConnection.CreateCommand();
            
            // Build WHERE clause - prefer after_id for incremental sync
            var whereClauses = new List<string>();
            if (after_id.HasValue)
                whereClauses.Add($"id > {after_id.Value}");
            if (start.HasValue)
                whereClauses.Add($"trade_timestamp >= '{start.Value:yyyy-MM-dd HH:mm:ss}'");
            if (end.HasValue)
                whereClauses.Add($"trade_timestamp <= '{end.Value:yyyy-MM-dd HH:mm:ss}'");
            
            var whereClause = whereClauses.Count > 0 
                ? "WHERE " + string.Join(" AND ", whereClauses) 
                : "";
            
            // For incremental sync, order by ID ASC to get oldest-first (proper ordering)
            // For time queries, order by timestamp DESC (most recent first)
            var orderBy = after_id.HasValue ? "ORDER BY id ASC" : "ORDER BY trade_timestamp DESC";
            
            cmd.CommandText = $@"
                SELECT id, signature, wallet_address, direction, sol_amount, stablecoin,
                       stablecoin_amount, price, block_height, slot, block_time, 
                       trade_timestamp, has_perp_position, perp_platform, perp_direction,
                       perp_size, perp_leverage, perp_entry_price, created_at
                FROM sol_stablecoin_trades 
                {whereClause}
                {orderBy}
                LIMIT {maxLimit}";
            
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var id = reader.GetInt64(0);
                if (id > maxId) maxId = id;
                
                results.Add(new
                {
                    id = id,
                    signature = reader.GetString(1),
                    wallet_address = reader.GetString(2),
                    direction = reader.GetString(3),
                    sol_amount = reader.GetDouble(4),
                    stablecoin = reader.GetString(5),
                    stablecoin_amount = reader.GetDouble(6),
                    price = reader.GetDouble(7),
                    block_height = reader.GetInt64(8),
                    slot = reader.GetInt64(9),
                    block_time = reader.GetInt64(10),
                    trade_timestamp = reader.GetDateTime(11),
                    has_perp_position = reader.IsDBNull(12) ? false : reader.GetBoolean(12),
                    perp_platform = reader.IsDBNull(13) ? null : reader.GetString(13),
                    perp_direction = reader.IsDBNull(14) ? null : reader.GetString(14),
                    perp_size = reader.IsDBNull(15) ? (double?)null : reader.GetDouble(15),
                    perp_leverage = reader.IsDBNull(16) ? (double?)null : reader.GetDouble(16),
                    perp_entry_price = reader.IsDBNull(17) ? (double?)null : reader.GetDouble(17),
                    created_at = reader.IsDBNull(18) ? DateTime.UtcNow : reader.GetDateTime(18)
                });
            }
        }
        
        return Results.Ok(new
        {
            success = true,
            source = "duckdb_inmemory",
            count = results.Count,
            max_id = maxId,  // Return max_id for incremental sync tracking
            results = results,
            timestamp = DateTime.UtcNow
        });
    }
    catch (Exception ex)
    {
        return Results.Ok(new
        {
            success = false,
            source = "duckdb_inmemory",
            error = ex.Message,
            max_id = 0,
            results = new List<object>(),
            timestamp = DateTime.UtcNow
        });
    }
});

// Main webhook endpoint
app.MapPost("/", async (HttpContext context) =>
{
    try
    {
        // Read and parse JSON
        var data = await JsonSerializer.DeserializeAsync<WebhookData>(context.Request.Body);
        
        // Handle PING
        if (data?.Message == "PING")
        {
            return Results.Ok(new
            {
                status = "success",
                message = "PONG",
                timestamp = DateTime.UtcNow
            });
        }
        
        var transactionCount = data?.MatchedTransactions?.Count ?? 0;
        
        // CRITICAL: Respond immediately to QuickNode
        var response = Results.Ok(new
        {
            status = "accepted",
            received = transactionCount,
            timestamp = DateTime.UtcNow
        });
        
        // Process asynchronously (fire and forget)
        if (transactionCount > 0)
        {
            _ = Task.Run(async () =>
            {
                try
                {
                    await ProcessTransactions(data!.MatchedTransactions!);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERROR] Processing failed: {ex.Message}");
                }
            });
        }
        
        return response;
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[ERROR] Webhook error: {ex.Message}");
        return Results.Ok(new
        {
            status = "error",
            message = ex.Message,
            timestamp = DateTime.UtcNow
        });
    }
});

// Whale activity webhook endpoint
app.MapPost("/webhooks/whale-activity", async (HttpContext context) =>
{
    var requestId = Guid.NewGuid().ToString("N").Substring(0, 8);
    Console.WriteLine($"[{requestId}] ============================================");
    Console.WriteLine($"[{requestId}] WHALE WEBHOOK - Received request at {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} UTC");
    
    try
    {
        // Read raw body first for debugging
        string rawBody;
        using (var reader = new StreamReader(context.Request.Body, leaveOpen: false))
        {
            rawBody = await reader.ReadToEndAsync();
        }
        
        Console.WriteLine($"[{requestId}] Raw payload length: {rawBody.Length} bytes");
        Console.WriteLine($"[{requestId}] Raw payload preview: {(rawBody.Length > 500 ? rawBody.Substring(0, 500) + "..." : rawBody)}");
        
        // Parse JSON
        WhaleWebhookData? data = null;
        try
        {
            data = JsonSerializer.Deserialize<WhaleWebhookData>(rawBody);
            Console.WriteLine($"[{requestId}] JSON deserialization successful");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[{requestId}] [ERROR] JSON deserialization failed: {ex.Message}");
            Console.WriteLine($"[{requestId}] [ERROR] Stack trace: {ex.StackTrace}");
            throw;
        }
        
        var movementCount = data?.WhaleMovements?.Count ?? 0;
        Console.WriteLine($"[{requestId}] Parsed {movementCount} whale movements from payload");
        
        if (data?.WhaleMovements != null)
        {
            foreach (var movement in data.WhaleMovements)
            {
                Console.WriteLine($"[{requestId}] Movement: sig={movement.Signature?.Substring(0, 8)}..., wallet={movement.WalletAddress?.Substring(0, 8)}..., type={movement.WhaleType}, change={movement.AbsChange}");
            }
        }
        
        // CRITICAL: Respond immediately to QuickNode
        var response = Results.Ok(new
        {
            status = "accepted",
            received = movementCount,
            timestamp = DateTime.UtcNow,
            requestId = requestId
        });
        
        // Process asynchronously (fire and forget)
        if (movementCount > 0)
        {
            Console.WriteLine($"[{requestId}] Starting async processing of {movementCount} movements");
            _ = Task.Run(async () =>
            {
                try
                {
                    await ProcessWhaleMovements(data!.WhaleMovements!, requestId);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[{requestId}] [ERROR] Whale processing failed: {ex.Message}");
                    Console.WriteLine($"[{requestId}] [ERROR] Stack trace: {ex.StackTrace}");
                }
            });
        }
        else
        {
            Console.WriteLine($"[{requestId}] No movements to process");
        }
        
        Console.WriteLine($"[{requestId}] Responding to QuickNode with status=accepted");
        return response;
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[{requestId}] [ERROR] Whale webhook error: {ex.Message}");
        Console.WriteLine($"[{requestId}] [ERROR] Stack trace: {ex.StackTrace}");
        return Results.Ok(new
        {
            status = "error",
            message = ex.Message,
            timestamp = DateTime.UtcNow,
            requestId = requestId
        });
    }
});

app.Run();

// Process transactions in background
async Task ProcessTransactions(List<TradeData> trades)
{
    var inserted = 0;
    var duplicates = 0;
    var errors = 0;
    var duckDbInserted = 0;
    var duckDbErrors = 0;
    
    await using var conn = new MySqlConnection(ConnectionString);
    await conn.OpenAsync();
    
    // Ensure tables exist
    await EnsureTablesExist(conn);
    
    const string insertSql = @"
        INSERT INTO sol_stablecoin_trades 
        (signature, wallet_address, direction, sol_amount, stablecoin, 
         stablecoin_amount, price, block_height, slot, block_time, trade_timestamp,
         has_perp_position, perp_platform, perp_direction, perp_size, perp_leverage, perp_entry_price, raw_instructions_data)
        VALUES 
        (@signature, @wallet_address, @direction, @sol_amount, @stablecoin,
         @stablecoin_amount, @price, @block_height, @slot, @block_time, @trade_timestamp,
         @has_perp_position, @perp_platform, @perp_direction, @perp_size, @perp_leverage, @perp_entry_price, @raw_instructions_data)";
    
    const string insertArchiveSql = @"
        INSERT INTO sol_stablecoin_trades_archive 
        (id, signature, wallet_address, direction, sol_amount, stablecoin, 
         stablecoin_amount, price, block_height, slot, block_time, trade_timestamp, archived_at,
         has_perp_position, perp_platform, perp_direction, perp_size, perp_leverage, perp_entry_price, raw_instructions_data)
        VALUES 
        (@id, @signature, @wallet_address, @direction, @sol_amount, @stablecoin,
         @stablecoin_amount, @price, @block_height, @slot, @block_time, @trade_timestamp, NOW(),
         @has_perp_position, @perp_platform, @perp_direction, @perp_size, @perp_leverage, @perp_entry_price, @raw_instructions_data)";
    
    foreach (var trade in trades)
    {
        try
        {
            if (string.IsNullOrEmpty(trade.Signature) || 
                string.IsNullOrEmpty(trade.WalletAddress) || 
                string.IsNullOrEmpty(trade.Direction))
            {
                errors++;
                continue;
            }
            
            var tradeTimestamp = DateTimeOffset.FromUnixTimeSeconds(trade.BlockTime).UtcDateTime;
            
            // Extract raw_instructions_data from top-level field
            string? rawInstructionsData = null;
            if (trade.RawInstructionsData != null && trade.RawInstructionsData.Count > 0)
            {
                try
                {
                    // Serialize the strongly-typed list back to JSON for database storage
                    rawInstructionsData = JsonSerializer.Serialize(trade.RawInstructionsData);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[WARNING] Failed to serialize raw_instructions_data: {ex.Message}");
                }
            }
            
            // =================================================================
            // DUAL-WRITE: DuckDB (hot storage) - fire-and-forget, non-blocking
            // =================================================================
            try
            {
                var duckDbId = GetNextTradeId();
                lock (duckDbLock)
                {
                    using var duckCmd = duckDbConnection.CreateCommand();
                    duckCmd.CommandText = @"
                        INSERT INTO sol_stablecoin_trades 
                        (id, signature, wallet_address, direction, sol_amount, stablecoin, 
                         stablecoin_amount, price, block_height, slot, block_time, trade_timestamp,
                         has_perp_position, perp_platform, perp_direction, perp_size, perp_leverage, 
                         perp_entry_price, raw_instructions_data, created_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)";
                    
                    duckCmd.Parameters.Add(new DuckDBParameter { Value = duckDbId });
                    duckCmd.Parameters.Add(new DuckDBParameter { Value = trade.Signature });
                    duckCmd.Parameters.Add(new DuckDBParameter { Value = trade.WalletAddress });
                    duckCmd.Parameters.Add(new DuckDBParameter { Value = trade.Direction.ToLower() });
                    duckCmd.Parameters.Add(new DuckDBParameter { Value = (double)trade.SolAmount });
                    duckCmd.Parameters.Add(new DuckDBParameter { Value = trade.Stablecoin });
                    duckCmd.Parameters.Add(new DuckDBParameter { Value = (double)trade.StablecoinAmount });
                    duckCmd.Parameters.Add(new DuckDBParameter { Value = (double)trade.Price });
                    duckCmd.Parameters.Add(new DuckDBParameter { Value = trade.BlockHeight });
                    duckCmd.Parameters.Add(new DuckDBParameter { Value = trade.Slot });
                    duckCmd.Parameters.Add(new DuckDBParameter { Value = trade.BlockTime });
                    duckCmd.Parameters.Add(new DuckDBParameter { Value = tradeTimestamp });
                    duckCmd.Parameters.Add(new DuckDBParameter { Value = trade.HasPerpPosition ?? false });
                    duckCmd.Parameters.Add(new DuckDBParameter { Value = trade.PerpPlatform ?? (object)DBNull.Value });
                    duckCmd.Parameters.Add(new DuckDBParameter { Value = trade.PerpDirection ?? (object)DBNull.Value });
                    duckCmd.Parameters.Add(new DuckDBParameter { Value = trade.PerpSize.HasValue ? (double)trade.PerpSize.Value : DBNull.Value });
                    duckCmd.Parameters.Add(new DuckDBParameter { Value = trade.PerpLeverage.HasValue ? (double)trade.PerpLeverage.Value : DBNull.Value });
                    duckCmd.Parameters.Add(new DuckDBParameter { Value = trade.PerpEntryPrice.HasValue ? (double)trade.PerpEntryPrice.Value : DBNull.Value });
                    duckCmd.Parameters.Add(new DuckDBParameter { Value = rawInstructionsData ?? (object)DBNull.Value });
                    duckCmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow });
                    
                    duckCmd.ExecuteNonQuery();
                }
                duckDbInserted++;
            }
            catch (Exception duckEx)
            {
                duckDbErrors++;
                Console.WriteLine($"[DUCKDB] [WARNING] Failed to insert trade to DuckDB (non-critical): {duckEx.Message}");
                // Continue to MySQL - DuckDB errors are non-critical
            }
            
            // =================================================================
            // MySQL (master storage) - existing logic unchanged
            // =================================================================
            long insertedId;
            await using (var cmd = new MySqlCommand(insertSql, conn))
            {
                cmd.Parameters.AddWithValue("@signature", trade.Signature);
                cmd.Parameters.AddWithValue("@wallet_address", trade.WalletAddress);
                cmd.Parameters.AddWithValue("@direction", trade.Direction.ToLower());
                cmd.Parameters.AddWithValue("@sol_amount", trade.SolAmount);
                cmd.Parameters.AddWithValue("@stablecoin", trade.Stablecoin);
                cmd.Parameters.AddWithValue("@stablecoin_amount", trade.StablecoinAmount);
                cmd.Parameters.AddWithValue("@price", trade.Price);
                cmd.Parameters.AddWithValue("@block_height", trade.BlockHeight);
                cmd.Parameters.AddWithValue("@slot", trade.Slot);
                cmd.Parameters.AddWithValue("@block_time", trade.BlockTime);
                cmd.Parameters.AddWithValue("@trade_timestamp", tradeTimestamp);
                cmd.Parameters.AddWithValue("@has_perp_position", trade.HasPerpPosition ?? false);
                cmd.Parameters.AddWithValue("@perp_platform", (object?)trade.PerpPlatform ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@perp_direction", (object?)trade.PerpDirection ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@perp_size", (object?)trade.PerpSize ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@perp_leverage", (object?)trade.PerpLeverage ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@perp_entry_price", (object?)trade.PerpEntryPrice ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@raw_instructions_data", (object?)rawInstructionsData ?? DBNull.Value);
                
                await cmd.ExecuteNonQueryAsync();
                insertedId = cmd.LastInsertedId;
            }
            
            // Insert into archive table
            try
            {
                await using var archiveCmd = new MySqlCommand(insertArchiveSql, conn);
                archiveCmd.Parameters.AddWithValue("@id", insertedId);
                archiveCmd.Parameters.AddWithValue("@signature", trade.Signature);
                archiveCmd.Parameters.AddWithValue("@wallet_address", trade.WalletAddress);
                archiveCmd.Parameters.AddWithValue("@direction", trade.Direction.ToLower());
                archiveCmd.Parameters.AddWithValue("@sol_amount", trade.SolAmount);
                archiveCmd.Parameters.AddWithValue("@stablecoin", trade.Stablecoin);
                archiveCmd.Parameters.AddWithValue("@stablecoin_amount", trade.StablecoinAmount);
                archiveCmd.Parameters.AddWithValue("@price", trade.Price);
                archiveCmd.Parameters.AddWithValue("@block_height", trade.BlockHeight);
                archiveCmd.Parameters.AddWithValue("@slot", trade.Slot);
                archiveCmd.Parameters.AddWithValue("@block_time", trade.BlockTime);
                archiveCmd.Parameters.AddWithValue("@trade_timestamp", tradeTimestamp);
                archiveCmd.Parameters.AddWithValue("@has_perp_position", trade.HasPerpPosition ?? false);
                archiveCmd.Parameters.AddWithValue("@perp_platform", (object?)trade.PerpPlatform ?? DBNull.Value);
                archiveCmd.Parameters.AddWithValue("@perp_direction", (object?)trade.PerpDirection ?? DBNull.Value);
                archiveCmd.Parameters.AddWithValue("@perp_size", (object?)trade.PerpSize ?? DBNull.Value);
                archiveCmd.Parameters.AddWithValue("@perp_leverage", (object?)trade.PerpLeverage ?? DBNull.Value);
                archiveCmd.Parameters.AddWithValue("@perp_entry_price", (object?)trade.PerpEntryPrice ?? DBNull.Value);
                archiveCmd.Parameters.AddWithValue("@raw_instructions_data", (object?)rawInstructionsData ?? DBNull.Value);
                
                await archiveCmd.ExecuteNonQueryAsync();
            }
            catch { /* Archive insert is non-critical */ }
            
            inserted++;
            var perpInfo = trade.HasPerpPosition == true 
                ? $" [PERP: {trade.PerpPlatform} {trade.PerpDirection} {trade.PerpSize} SOL]" 
                : "";
            Console.WriteLine($"[INFO] Inserted: {trade.Direction} {trade.SolAmount} SOL @ ${trade.Price}{perpInfo}");
        }
        catch (MySqlException ex) when (ex.Number == 1062) // Duplicate entry
        {
            duplicates++;
        }
        catch (Exception ex)
        {
            errors++;
            Console.WriteLine($"[ERROR] Failed to insert trade: {ex.Message}");
        }
    }
    
    Console.WriteLine($"[INFO] Processed {trades.Count} transactions: {inserted} MySQL inserted, {duplicates} duplicates, {errors} errors");
    Console.WriteLine($"[DUCKDB] Trades: {duckDbInserted} inserted, {duckDbErrors} errors");
}

async Task EnsureTablesExist(MySqlConnection conn)
{
    const string createMainTable = @"
        CREATE TABLE IF NOT EXISTS sol_stablecoin_trades (
            id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            signature VARCHAR(88) NOT NULL UNIQUE,
            wallet_address VARCHAR(44) NOT NULL,
            direction ENUM('buy', 'sell') NOT NULL,
            sol_amount DECIMAL(18, 9) NOT NULL,
            stablecoin VARCHAR(10) NOT NULL,
            stablecoin_amount DECIMAL(18, 2) NOT NULL,
            price DECIMAL(12, 2) NOT NULL,
            block_height BIGINT UNSIGNED NOT NULL,
            slot BIGINT UNSIGNED NOT NULL,
            block_time INT UNSIGNED NOT NULL,
            trade_timestamp DATETIME NOT NULL,
            raw_instructions_data TEXT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_wallet (wallet_address),
            INDEX idx_timestamp (trade_timestamp),
            INDEX idx_direction (direction),
            INDEX idx_stablecoin (stablecoin),
            INDEX idx_block_height (block_height),
            INDEX idx_created_at (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci";
    
    const string createArchiveTable = @"
        CREATE TABLE IF NOT EXISTS sol_stablecoin_trades_archive (
            id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            signature VARCHAR(88) NOT NULL,
            wallet_address VARCHAR(44) NOT NULL,
            direction ENUM('buy', 'sell') NOT NULL,
            sol_amount DECIMAL(18, 9) NOT NULL,
            stablecoin VARCHAR(10) NOT NULL,
            stablecoin_amount DECIMAL(18, 2) NOT NULL,
            price DECIMAL(12, 2) NOT NULL,
            block_height BIGINT UNSIGNED NOT NULL,
            slot BIGINT UNSIGNED NOT NULL,
            block_time INT UNSIGNED NOT NULL,
            trade_timestamp DATETIME NOT NULL,
            raw_instructions_data TEXT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_signature (signature),
            INDEX idx_wallet (wallet_address),
            INDEX idx_timestamp (trade_timestamp),
            INDEX idx_direction (direction),
            INDEX idx_stablecoin (stablecoin),
            INDEX idx_block_height (block_height),
            INDEX idx_created_at (created_at),
            INDEX idx_archived_at (archived_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci";
    
    await using (var cmd = new MySqlCommand(createMainTable, conn))
        await cmd.ExecuteNonQueryAsync();
    
    await using (var cmd = new MySqlCommand(createArchiveTable, conn))
        await cmd.ExecuteNonQueryAsync();
}

// Process whale movements in background
async Task ProcessWhaleMovements(List<WhaleMovementData> movements, string requestId)
{
    Console.WriteLine($"[{requestId}] ProcessWhaleMovements - START");
    Console.WriteLine($"[{requestId}] Processing {movements.Count} whale movements");
    
    var inserted = 0;
    var duplicates = 0;
    var errors = 0;
    var duckDbInserted = 0;
    var duckDbErrors = 0;
    
    try
    {
        Console.WriteLine($"[{requestId}] Opening database connection...");
        await using var conn = new MySqlConnection(ConnectionString);
        await conn.OpenAsync();
        Console.WriteLine($"[{requestId}] Database connection opened successfully");
        
        // Ensure whale movements table exists
        Console.WriteLine($"[{requestId}] Ensuring whale_movements table exists...");
        await EnsureWhaleTableExists(conn);
        Console.WriteLine($"[{requestId}] Table check complete");
        
        const string insertSql = @"
        INSERT INTO whale_movements 
        (signature, wallet_address, whale_type, current_balance, sol_change, 
         abs_change, percentage_moved, direction, action, movement_significance, 
         previous_balance, fee_paid, block_time, timestamp, received_at, slot,
         has_perp_position, perp_platform, perp_direction, perp_size, perp_leverage, perp_entry_price, raw_data_json)
        VALUES 
        (@signature, @wallet_address, @whale_type, @current_balance, @sol_change,
         @abs_change, @percentage_moved, @direction, @action, @movement_significance,
         @previous_balance, @fee_paid, @block_time, @timestamp, @received_at, @slot,
         @has_perp_position, @perp_platform, @perp_direction, @perp_size, @perp_leverage, @perp_entry_price, @raw_data_json)";
        
        for (int i = 0; i < movements.Count; i++)
        {
            var movement = movements[i];
            Console.WriteLine($"[{requestId}] ----------------------------------------");
            Console.WriteLine($"[{requestId}] Processing movement {i + 1}/{movements.Count}");
            
            try
            {
                // Validation logging
                Console.WriteLine($"[{requestId}] Validating movement data...");
                Console.WriteLine($"[{requestId}]   Signature: {movement.Signature ?? "NULL"}");
                Console.WriteLine($"[{requestId}]   WalletAddress: {movement.WalletAddress ?? "NULL"}");
                Console.WriteLine($"[{requestId}]   WhaleType: {movement.WhaleType ?? "NULL"}");
                
                if (string.IsNullOrEmpty(movement.Signature) || 
                    string.IsNullOrEmpty(movement.WalletAddress) || 
                    string.IsNullOrEmpty(movement.WhaleType))
                {
                    errors++;
                    Console.WriteLine($"[{requestId}] [ERROR] Validation failed - missing required fields");
                    Console.WriteLine($"[{requestId}]   Signature empty: {string.IsNullOrEmpty(movement.Signature)}");
                    Console.WriteLine($"[{requestId}]   WalletAddress empty: {string.IsNullOrEmpty(movement.WalletAddress)}");
                    Console.WriteLine($"[{requestId}]   WhaleType empty: {string.IsNullOrEmpty(movement.WhaleType)}");
                    continue;
                }
                
                Console.WriteLine($"[{requestId}] Validation passed");
                
                // Log all field values
                Console.WriteLine($"[{requestId}] Field values:");
                Console.WriteLine($"[{requestId}]   current_balance: {movement.CurrentBalance}");
                Console.WriteLine($"[{requestId}]   sol_change: {movement.SolChange}");
                Console.WriteLine($"[{requestId}]   abs_change: {movement.AbsChange}");
                Console.WriteLine($"[{requestId}]   percentage_moved: {movement.PercentageMoved}");
                Console.WriteLine($"[{requestId}]   direction: {movement.Direction ?? "NULL"}");
                Console.WriteLine($"[{requestId}]   action: {movement.Action ?? "NULL"}");
                Console.WriteLine($"[{requestId}]   movement_significance: {movement.MovementSignificance ?? "NULL"}");
                Console.WriteLine($"[{requestId}]   previous_balance: {movement.PreviousBalance}");
                Console.WriteLine($"[{requestId}]   fee_paid: {movement.FeePaid}");
                Console.WriteLine($"[{requestId}]   block_time: {movement.BlockTime}");
                Console.WriteLine($"[{requestId}]   timestamp: {movement.Timestamp}");
                Console.WriteLine($"[{requestId}]   received_at: {movement.ReceivedAt}");
                Console.WriteLine($"[{requestId}]   slot: {movement.Slot}");
                Console.WriteLine($"[{requestId}]   has_perp_position: {movement.HasPerpPosition}");
                Console.WriteLine($"[{requestId}]   perp_platform: {movement.PerpPlatform ?? "NULL"}");
                Console.WriteLine($"[{requestId}]   perp_direction: {movement.PerpDirection ?? "NULL"}");
                
                // Extract raw_data_json if present
                string? rawDataJson = null;
                if (movement.RawDataJson.HasValue && movement.RawDataJson.Value.ValueKind != JsonValueKind.Null)
                {
                    try
                    {
                        rawDataJson = movement.RawDataJson.Value.GetRawText();
                        Console.WriteLine($"[{requestId}] raw_data_json extracted: {rawDataJson.Length} bytes");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[{requestId}] [WARNING] Failed to serialize raw_data_json: {ex.Message}");
                    }
                }
                else
                {
                    Console.WriteLine($"[{requestId}] No raw_data_json present");
                }
                
                // Parse timestamps first (needed for both DuckDB and MySQL)
                DateTime timestamp;
                if (DateTime.TryParse(movement.Timestamp, out timestamp))
                {
                    Console.WriteLine($"[{requestId}] Parsed timestamp: {timestamp:yyyy-MM-dd HH:mm:ss}");
                }
                else
                {
                    timestamp = DateTime.UtcNow;
                    Console.WriteLine($"[{requestId}] [WARNING] Failed to parse timestamp '{movement.Timestamp}', using current time");
                }
                
                DateTime receivedAt;
                if (DateTime.TryParse(movement.ReceivedAt, out receivedAt))
                {
                    Console.WriteLine($"[{requestId}] Parsed received_at: {receivedAt:yyyy-MM-dd HH:mm:ss}");
                }
                else
                {
                    receivedAt = DateTime.UtcNow;
                    Console.WriteLine($"[{requestId}] [WARNING] Failed to parse received_at '{movement.ReceivedAt}', using current time");
                }
                
                // Normalize direction to in/out and fix abs_change if missing/zero
                var dirNormalized = (movement.Direction ?? "").ToLower();
                if (dirNormalized is "sending" or "sent" or "outbound") dirNormalized = "out";
                else if (dirNormalized is "receiving" or "received" or "inbound") dirNormalized = "in";
                var absChangeFixed = movement.AbsChange != 0 ? movement.AbsChange : Math.Abs(movement.SolChange);
                
                // =================================================================
                // DUAL-WRITE: DuckDB (hot storage) - fire-and-forget, non-blocking
                // =================================================================
                try
                {
                    var duckDbId = GetNextWhaleId();
                    lock (duckDbLock)
                    {
                        using var duckCmd = duckDbConnection.CreateCommand();
                        duckCmd.CommandText = @"
                            INSERT INTO whale_movements 
                            (id, signature, wallet_address, whale_type, current_balance, sol_change, 
                             abs_change, percentage_moved, direction, action, movement_significance,
                             previous_balance, fee_paid, block_time, timestamp, received_at, slot,
                             has_perp_position, perp_platform, perp_direction, perp_size, perp_leverage, 
                             perp_entry_price, raw_data_json, created_at)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25)";
                        
                        duckCmd.Parameters.Add(new DuckDBParameter { Value = duckDbId });
                        duckCmd.Parameters.Add(new DuckDBParameter { Value = movement.Signature });
                        duckCmd.Parameters.Add(new DuckDBParameter { Value = movement.WalletAddress });
                        duckCmd.Parameters.Add(new DuckDBParameter { Value = movement.WhaleType });
                        duckCmd.Parameters.Add(new DuckDBParameter { Value = (double)movement.CurrentBalance });
                        duckCmd.Parameters.Add(new DuckDBParameter { Value = (double)movement.SolChange });
                        duckCmd.Parameters.Add(new DuckDBParameter { Value = (double)absChangeFixed });
                        duckCmd.Parameters.Add(new DuckDBParameter { Value = (double)movement.PercentageMoved });
                        duckCmd.Parameters.Add(new DuckDBParameter { Value = dirNormalized });
                        duckCmd.Parameters.Add(new DuckDBParameter { Value = movement.Action ?? "" });
                        duckCmd.Parameters.Add(new DuckDBParameter { Value = movement.MovementSignificance ?? "" });
                        duckCmd.Parameters.Add(new DuckDBParameter { Value = movement.PreviousBalance.HasValue ? (double)movement.PreviousBalance.Value : 0.0 });
                        duckCmd.Parameters.Add(new DuckDBParameter { Value = movement.FeePaid.HasValue ? (double)movement.FeePaid.Value : 0.0 });
                        duckCmd.Parameters.Add(new DuckDBParameter { Value = movement.BlockTime ?? 0 });
                        duckCmd.Parameters.Add(new DuckDBParameter { Value = timestamp });
                        duckCmd.Parameters.Add(new DuckDBParameter { Value = receivedAt });
                        duckCmd.Parameters.Add(new DuckDBParameter { Value = movement.Slot ?? 0 });
                        duckCmd.Parameters.Add(new DuckDBParameter { Value = movement.HasPerpPosition ?? false });
                        duckCmd.Parameters.Add(new DuckDBParameter { Value = movement.PerpPlatform ?? (object)DBNull.Value });
                        duckCmd.Parameters.Add(new DuckDBParameter { Value = movement.PerpDirection ?? (object)DBNull.Value });
                        duckCmd.Parameters.Add(new DuckDBParameter { Value = movement.PerpSize.HasValue ? (double)movement.PerpSize.Value : DBNull.Value });
                        duckCmd.Parameters.Add(new DuckDBParameter { Value = movement.PerpLeverage.HasValue ? (double)movement.PerpLeverage.Value : DBNull.Value });
                        duckCmd.Parameters.Add(new DuckDBParameter { Value = movement.PerpEntryPrice.HasValue ? (double)movement.PerpEntryPrice.Value : DBNull.Value });
                        duckCmd.Parameters.Add(new DuckDBParameter { Value = rawDataJson ?? (object)DBNull.Value });
                        duckCmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow });
                        
                        duckCmd.ExecuteNonQuery();
                    }
                    duckDbInserted++;
                    Console.WriteLine($"[{requestId}] [DUCKDB] Whale movement inserted to hot storage");
                }
                catch (Exception duckEx)
                {
                    duckDbErrors++;
                    Console.WriteLine($"[{requestId}] [DUCKDB] [WARNING] Failed to insert to DuckDB (non-critical): {duckEx.Message}");
                    // Continue to MySQL - DuckDB errors are non-critical
                }
                
                // =================================================================
                // MySQL (master storage) - existing logic unchanged
                // =================================================================
                Console.WriteLine($"[{requestId}] Creating SQL command...");
                await using var cmd = new MySqlCommand(insertSql, conn);
                
                Console.WriteLine($"[{requestId}] Adding parameters...");
                cmd.Parameters.AddWithValue("@signature", movement.Signature);
                cmd.Parameters.AddWithValue("@wallet_address", movement.WalletAddress);
                cmd.Parameters.AddWithValue("@whale_type", movement.WhaleType);
                cmd.Parameters.AddWithValue("@current_balance", movement.CurrentBalance);
                cmd.Parameters.AddWithValue("@sol_change", movement.SolChange);
                cmd.Parameters.AddWithValue("@abs_change", absChangeFixed);
                cmd.Parameters.AddWithValue("@percentage_moved", movement.PercentageMoved);
                cmd.Parameters.AddWithValue("@direction", dirNormalized);
                cmd.Parameters.AddWithValue("@action", movement.Action ?? "");
                cmd.Parameters.AddWithValue("@movement_significance", movement.MovementSignificance ?? "");
                cmd.Parameters.AddWithValue("@previous_balance", movement.PreviousBalance ?? 0);
                cmd.Parameters.AddWithValue("@fee_paid", movement.FeePaid ?? 0);
                cmd.Parameters.AddWithValue("@block_time", movement.BlockTime ?? 0);
                cmd.Parameters.AddWithValue("@timestamp", timestamp);
                cmd.Parameters.AddWithValue("@received_at", receivedAt);
                cmd.Parameters.AddWithValue("@slot", movement.Slot ?? 0);
                cmd.Parameters.AddWithValue("@has_perp_position", movement.HasPerpPosition ?? false);
                cmd.Parameters.AddWithValue("@perp_platform", (object?)movement.PerpPlatform ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@perp_direction", (object?)movement.PerpDirection ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@perp_size", (object?)movement.PerpSize ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@perp_leverage", (object?)movement.PerpLeverage ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@perp_entry_price", (object?)movement.PerpEntryPrice ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@raw_data_json", (object?)rawDataJson ?? DBNull.Value);
                
                Console.WriteLine($"[{requestId}] All parameters added, executing SQL...");
                var rowsAffected = await cmd.ExecuteNonQueryAsync();
                Console.WriteLine($"[{requestId}] SQL executed successfully, rows affected: {rowsAffected}");
                
                inserted++;
                var perpInfo = movement.HasPerpPosition == true 
                    ? $" [PERP: {movement.PerpPlatform} {movement.PerpDirection} {movement.PerpSize} SOL]" 
                    : "";
                Console.WriteLine($"[{requestId}] [SUCCESS] Whale movement inserted: {movement.WhaleType} {movement.Direction} {movement.AbsChange} SOL ({movement.MovementSignificance}){perpInfo}");
            }
            catch (MySqlException ex) when (ex.Number == 1062) // Duplicate entry
            {
                duplicates++;
                Console.WriteLine($"[{requestId}] [DUPLICATE] Signature already exists: {movement.Signature}");
            }
            catch (MySqlException ex)
            {
                errors++;
                Console.WriteLine($"[{requestId}] [ERROR] MySQL error inserting whale movement:");
                Console.WriteLine($"[{requestId}] [ERROR]   Error Number: {ex.Number}");
                Console.WriteLine($"[{requestId}] [ERROR]   Error Message: {ex.Message}");
                Console.WriteLine($"[{requestId}] [ERROR]   SQL State: {ex.SqlState}");
                Console.WriteLine($"[{requestId}] [ERROR]   Stack trace: {ex.StackTrace}");
            }
            catch (Exception ex)
            {
                errors++;
                Console.WriteLine($"[{requestId}] [ERROR] Failed to insert whale movement:");
                Console.WriteLine($"[{requestId}] [ERROR]   Exception Type: {ex.GetType().Name}");
                Console.WriteLine($"[{requestId}] [ERROR]   Message: {ex.Message}");
                Console.WriteLine($"[{requestId}] [ERROR]   Stack trace: {ex.StackTrace}");
                if (ex.InnerException != null)
                {
                    Console.WriteLine($"[{requestId}] [ERROR]   Inner Exception: {ex.InnerException.Message}");
                }
            }
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[{requestId}] [ERROR] Fatal error in ProcessWhaleMovements:");
        Console.WriteLine($"[{requestId}] [ERROR]   Exception Type: {ex.GetType().Name}");
        Console.WriteLine($"[{requestId}] [ERROR]   Message: {ex.Message}");
        Console.WriteLine($"[{requestId}] [ERROR]   Stack trace: {ex.StackTrace}");
        if (ex.InnerException != null)
        {
            Console.WriteLine($"[{requestId}] [ERROR]   Inner Exception: {ex.InnerException.Message}");
        }
    }
    
    Console.WriteLine($"[{requestId}] ========================================");
    Console.WriteLine($"[{requestId}] SUMMARY: Processed {movements.Count} whale movements");
    Console.WriteLine($"[{requestId}]   ✓ MySQL Inserted: {inserted}");
    Console.WriteLine($"[{requestId}]   ⚠ Duplicates: {duplicates}");
    Console.WriteLine($"[{requestId}]   ✗ Errors: {errors}");
    Console.WriteLine($"[{requestId}]   🦆 DuckDB Inserted: {duckDbInserted}");
    Console.WriteLine($"[{requestId}]   🦆 DuckDB Errors: {duckDbErrors}");
    Console.WriteLine($"[{requestId}] ProcessWhaleMovements - END");
}

async Task EnsureWhaleTableExists(MySqlConnection conn)
{
    try
    {
        Console.WriteLine("[TABLE] Checking whale_movements table...");
        
        const string createWhaleTable = @"
        CREATE TABLE IF NOT EXISTS whale_movements (
            id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            signature VARCHAR(88) NOT NULL UNIQUE,
            wallet_address VARCHAR(44) NOT NULL,
            whale_type VARCHAR(20) NOT NULL,
            current_balance DECIMAL(18,2),
            sol_change DECIMAL(18,4),
            abs_change DECIMAL(18,4),
            percentage_moved DECIMAL(5,2),
            direction VARCHAR(20),
            action VARCHAR(20),
            movement_significance VARCHAR(20),
            previous_balance DECIMAL(18,2),
            fee_paid DECIMAL(10,6),
            block_time BIGINT,
            timestamp DATETIME NOT NULL,
            received_at DATETIME NOT NULL,
            slot BIGINT,
            has_perp_position BOOLEAN DEFAULT FALSE,
            perp_platform ENUM('drift', 'jupiter', 'mango', 'zeta') NULL,
            perp_direction ENUM('long', 'short') NULL,
            perp_size DECIMAL(18,9) NULL,
            perp_leverage DECIMAL(10,2) NULL,
            perp_entry_price DECIMAL(12,2) NULL,
            raw_data_json LONGTEXT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_timestamp (timestamp),
            INDEX idx_wallet_address (wallet_address),
            INDEX idx_whale_type (whale_type),
            INDEX idx_movement_significance (movement_significance),
            INDEX idx_direction (direction),
            INDEX idx_created_at (created_at),
            INDEX idx_has_perp (has_perp_position),
            INDEX idx_perp_platform (perp_platform),
            INDEX idx_perp_direction (perp_direction),
            INDEX idx_wallet_perp (wallet_address, has_perp_position, perp_direction)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci";
        
        await using var cmd = new MySqlCommand(createWhaleTable, conn);
        await cmd.ExecuteNonQueryAsync();
        
        Console.WriteLine("[TABLE] whale_movements table check complete");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[TABLE] [ERROR] Failed to ensure whale_movements table exists:");
        Console.WriteLine($"[TABLE] [ERROR]   Exception Type: {ex.GetType().Name}");
        Console.WriteLine($"[TABLE] [ERROR]   Message: {ex.Message}");
        Console.WriteLine($"[TABLE] [ERROR]   Stack trace: {ex.StackTrace}");
        throw;
    }
}

sealed class TeeTextWriter : TextWriter
{
    private readonly TextWriter _primaryWriter;
    private readonly TextWriter _secondaryWriter;
    private readonly object _lock = new();
    private bool _disposed;

    public TeeTextWriter(TextWriter primaryWriter, TextWriter secondaryWriter)
    {
        _primaryWriter = primaryWriter;
        _secondaryWriter = secondaryWriter;
    }

    public override Encoding Encoding => _primaryWriter.Encoding;

    public override void Write(char value)
    {
        lock (_lock)
        {
            _primaryWriter.Write(value);
            _secondaryWriter.Write(value);
        }
    }

    public override void Write(string? value)
    {
        lock (_lock)
        {
            _primaryWriter.Write(value);
            _secondaryWriter.Write(value);
        }
    }

    public override void WriteLine()
    {
        lock (_lock)
        {
            _primaryWriter.WriteLine();
            _secondaryWriter.WriteLine();
            _secondaryWriter.Flush();
        }
    }

    public override void WriteLine(string? value)
    {
        lock (_lock)
        {
            _primaryWriter.WriteLine(value);
            _secondaryWriter.WriteLine(value);
            _secondaryWriter.Flush();
        }
    }

    public override void Flush()
    {
        lock (_lock)
        {
            _primaryWriter.Flush();
            _secondaryWriter.Flush();
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (!disposing)
        {
            base.Dispose(disposing);
            return;
        }

        lock (_lock)
        {
            if (_disposed)
            {
                base.Dispose(disposing);
                return;
            }

            _secondaryWriter.Flush();
            _secondaryWriter.Dispose();
            _disposed = true;
        }

        base.Dispose(disposing);
    }
}

// Models
public record RawInstructionData(
    [property: JsonPropertyName("program_id")] string ProgramId,
    [property: JsonPropertyName("base58_data")] string? Base58Data,
    [property: JsonPropertyName("accounts")] JsonElement? Accounts
);

public record WebhookData(
    [property: JsonPropertyName("message")] string? Message,
    [property: JsonPropertyName("matchedTransactions")] List<TradeData>? MatchedTransactions
);

public record TradeData(
    [property: JsonPropertyName("signature")] string Signature,
    [property: JsonPropertyName("wallet_address")] string WalletAddress,
    [property: JsonPropertyName("direction")] string Direction,
    [property: JsonPropertyName("sol_amount")] decimal SolAmount,
    [property: JsonPropertyName("stablecoin")] string Stablecoin,
    [property: JsonPropertyName("stablecoin_amount")] decimal StablecoinAmount,
    [property: JsonPropertyName("price")] decimal Price,
    [property: JsonPropertyName("block_height")] long BlockHeight,
    [property: JsonPropertyName("slot")] long Slot,
    [property: JsonPropertyName("block_time")] long BlockTime,
    [property: JsonPropertyName("has_perp_position")] bool? HasPerpPosition,
    [property: JsonPropertyName("perp_platform")] string? PerpPlatform,
    [property: JsonPropertyName("perp_direction")] string? PerpDirection,
    [property: JsonPropertyName("perp_size")] decimal? PerpSize,
    [property: JsonPropertyName("perp_leverage")] decimal? PerpLeverage,
    [property: JsonPropertyName("perp_entry_price")] decimal? PerpEntryPrice,
    [property: JsonPropertyName("perp_debug_info")] JsonElement? PerpDebugInfo,
    [property: JsonPropertyName("raw_instructions_data")] List<RawInstructionData>? RawInstructionsData
);

// Whale tracking models
public record WhaleWebhookData(
    [property: JsonPropertyName("whaleMovements")] List<WhaleMovementData>? WhaleMovements,
    [property: JsonPropertyName("summary")] WhaleSummary? Summary,
    [property: JsonPropertyName("raw_data_json")] JsonElement? RawDataJson
);

public record WhaleMovementData(
    [property: JsonPropertyName("signature")] string Signature,
    [property: JsonPropertyName("wallet_address")] string WalletAddress,
    [property: JsonPropertyName("whale_type")] string WhaleType,
    [property: JsonPropertyName("current_balance")] decimal CurrentBalance,
    [property: JsonPropertyName("sol_change")] decimal SolChange,
    [property: JsonPropertyName("abs_change")] decimal AbsChange,
    [property: JsonPropertyName("percentage_moved")] decimal PercentageMoved,
    [property: JsonPropertyName("direction")] string? Direction,
    [property: JsonPropertyName("action")] string? Action,
    [property: JsonPropertyName("movement_significance")] string? MovementSignificance,
    [property: JsonPropertyName("previous_balance")] decimal? PreviousBalance,
    [property: JsonPropertyName("fee_paid")] decimal? FeePaid,
    [property: JsonPropertyName("block_time")] long? BlockTime,
    [property: JsonPropertyName("timestamp")] string? Timestamp,
    [property: JsonPropertyName("received_at")] string? ReceivedAt,
    [property: JsonPropertyName("slot")] long? Slot,
    [property: JsonPropertyName("has_perp_position")] bool? HasPerpPosition,
    [property: JsonPropertyName("perp_platform")] string? PerpPlatform,
    [property: JsonPropertyName("perp_direction")] string? PerpDirection,
    [property: JsonPropertyName("perp_size")] decimal? PerpSize,
    [property: JsonPropertyName("perp_leverage")] decimal? PerpLeverage,
    [property: JsonPropertyName("perp_entry_price")] decimal? PerpEntryPrice,
    [property: JsonPropertyName("raw_data_json")] JsonElement? RawDataJson
);

public record WhaleSummary(
    [property: JsonPropertyName("totalMovements")] int TotalMovements,
    [property: JsonPropertyName("totalVolume")] decimal TotalVolume,
    [property: JsonPropertyName("netFlow")] decimal NetFlow,
    [property: JsonPropertyName("receiving")] int Receiving,
    [property: JsonPropertyName("sending")] int Sending
);

