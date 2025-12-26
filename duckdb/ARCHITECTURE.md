# DuckDB Architecture - Central Reference

> **This is the single source of truth for all database schemas in this project.**  
> All features must reference and update this document when adding/modifying tables.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA FLOW ARCHITECTURE                           │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   ┌──────────────┐         ┌──────────────────┐         ┌────────────┐  │
│   │   Python     │         │   Central API    │         │    PHP     │  │
│   │   Services   │────────▶│  (Flask :5050)   │◀────────│  Frontend  │  │
│   └──────────────┘         └────────┬─────────┘         └────────────┘  │
│                                     │                                    │
│                          ┌──────────┴──────────┐                        │
│                          │    DUAL WRITE       │                        │
│                          └──────────┬──────────┘                        │
│                                     │                                    │
│              ┌──────────────────────┼──────────────────────┐            │
│              │                      │                      │            │
│              ▼                      │                      ▼            │
│   ┌──────────────────┐             │         ┌──────────────────┐      │
│   │     DuckDB       │             │         │      MySQL       │      │
│   │   (24hr Hot)     │◀────────────┘────────▶│  (Full History)  │      │
│   │  central.duckdb  │                       │    solcatcher    │      │
│   └──────────────────┘                       └──────────────────┘      │
│                                                                          │
│   • Fast reads for                            • Master storage           │
│     recent data                               • All historical data      │
│   • Auto-cleanup                              • Never deleted            │
│     after 24 hours                            • Source of truth          │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Database Files

| Database | Location | Purpose |
|----------|----------|---------|
| `central.duckdb` | `000data_feeds/` | Central DuckDB for all hot data (24hr) |
| `prices.duckdb` | `000data_feeds/1_jupiter_get_prices/` | Jupiter price data (24hr hot, no archive) |
| `solcatcher` | MySQL (116.202.51.115) | Master MySQL database (full history) |

---

## Hot/Cold Storage Pattern

Every time-series table follows this pattern:

```
┌─────────────────────────────────────────────────────────────────┐
│                        INCOMING DATA                             │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │   DUAL WRITE    │
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
              ▼              │              ▼
┌─────────────────────┐      │   ┌─────────────────────┐
│   DuckDB (HOT)      │      │   │   MySQL (MASTER)    │
│   Last 24 hours     │      │   │   Full history      │
│   Fast queries      │      │   │   Never deleted     │
│   Auto-cleanup      │      │   │   Source of truth   │
└─────────────────────┘      │   └─────────────────────┘
                             │
                    (hourly cleanup)
                             │
                             ▼
                    Data older than 24h
                    deleted from DuckDB
                    (kept in MySQL)
```

---

## Database: central.duckdb

### Table: follow_the_goat_plays (FULL DATA)
Play definitions - not time-based, keeps full data synced from MySQL.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Primary key |
| `created_at` | TIMESTAMP | When play was created |
| `name` | VARCHAR(60) | Play name |
| `description` | VARCHAR(500) | Play description |
| `find_wallets_sql` | JSON | SQL query to find wallets |
| `max_buys_per_cycle` | INTEGER | Max buys per cycle |
| `sell_logic` | JSON | Sell logic configuration |
| `sorting` | INTEGER | Sort order |
| `short_play` | INTEGER | 1 if short play |
| `tricker_on_perp` | JSON | Trigger on perpetual config |
| `timing_conditions` | JSON | Timing conditions |
| `bundle_trades` | JSON | Bundle trades config |
| `is_active` | INTEGER | 1 if active |
| `project_id` | INTEGER | Pattern config project ID |
| `project_ids` | JSON | Multiple project IDs |

### Table: follow_the_goat_buyins (24hr HOT)
Live trades - last 24 hours only.

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGINT | Primary key |
| `play_id` | INTEGER | Reference to play |
| `wallet_address` | VARCHAR(255) | Wallet address |
| `original_trade_id` | BIGINT | Original trade reference |
| `tolerance` | DOUBLE | Tolerance setting |
| `block_timestamp` | TIMESTAMP | Blockchain timestamp |
| `price` | DECIMAL(20,8) | Entry price |
| `followed_at` | TIMESTAMP | When we followed |
| `our_entry_price` | DECIMAL(20,8) | Our entry price |
| `our_exit_price` | DECIMAL(20,8) | Our exit price |
| `our_exit_timestamp` | TIMESTAMP | Exit time |
| `our_profit_loss` | DECIMAL(20,8) | P/L percentage |
| `our_status` | VARCHAR(20) | Status (pending, sold, no_go) |
| `current_price` | DECIMAL(20,8) | Current price |

**Indexes:**
- `idx_buyins_wallet` on `(wallet_address)`
- `idx_buyins_followed_at` on `(followed_at)`
- `idx_buyins_status` on `(our_status)`
- `idx_buyins_play_id` on `(play_id)`

### Table: follow_the_goat_buyins_price_checks (24hr HOT)
Price checks for active trades.

| Column | Type | Description |
|--------|------|-------------|
| `id` | UBIGINT | Primary key |
| `buyin_id` | UINTEGER | Reference to buyin |
| `checked_at` | TIMESTAMP | When checked |
| `current_price` | DECIMAL(20,8) | Current price |
| `entry_price` | DECIMAL(20,8) | Entry price |
| `highest_price` | DECIMAL(20,8) | Highest seen |
| `gain_from_entry` | DECIMAL(10,6) | Gain from entry |
| `drop_from_high` | DECIMAL(10,6) | Drop from high |
| `tolerance` | DECIMAL(10,6) | Current tolerance |
| `should_sell` | BOOLEAN | Should sell flag |

### Table: price_points (24hr HOT)
Price data points.

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGINT | Primary key |
| `ts_idx` | BIGINT | Timestamp index |
| `value` | DOUBLE | Price value |
| `created_at` | TIMESTAMP | When recorded |
| `coin_id` | INTEGER | Coin ID (5 = SOL) |

### Table: price_analysis (24hr HOT)
Price cycle analysis data - tracks price movements at multiple thresholds.
Populated by `000data_feeds/2_create_price_cycles/create_price_cycles.py`.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Primary key |
| `coin_id` | INTEGER | Coin ID (5 = SOL) |
| `price_point_id` | BIGINT | Reference to source price point (timestamp ms) |
| `sequence_start_id` | BIGINT | ID of price point that started this cycle (timestamp ms) |
| `sequence_start_price` | DECIMAL(20,8) | Price at cycle start |
| `current_price` | DECIMAL(20,8) | Current price |
| `percent_threshold` | DECIMAL(5,2) | Threshold (0.1, 0.2, 0.3, 0.4, 0.5) |
| `percent_increase` | DECIMAL(10,4) | Percent increase from cycle start |
| `highest_price_recorded` | DECIMAL(20,8) | Highest price in this cycle |
| `lowest_price_recorded` | DECIMAL(20,8) | Lowest price in this cycle |
| `procent_change_from_highest_price_recorded` | DECIMAL(10,4) | Drop from high |
| `percent_increase_from_lowest` | DECIMAL(10,4) | Rise from low |
| `price_cycle` | BIGINT | Reference to cycle_tracker.id |
| `created_at` | TIMESTAMP | When recorded |
| `processed_at` | TIMESTAMP | When processed |

**Indexes:**
- `idx_price_analysis_coin` on `(coin_id)`
- `idx_price_analysis_created_at` on `(created_at)`
- `idx_price_analysis_price_cycle` on `(price_cycle)`

### Table: cycle_tracker (24hr HOT)
Price cycle tracking - one record per cycle per threshold.
A new cycle starts when price drops from the highest by the threshold percentage.

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGINT | Primary key (unique cycle ID) |
| `coin_id` | INTEGER | Coin ID (5 = SOL) |
| `threshold` | DECIMAL(5,2) | Threshold percentage (0.1-0.5) |
| `cycle_start_time` | TIMESTAMP | When cycle started |
| `cycle_end_time` | TIMESTAMP | When cycle ended (NULL if active) |
| `sequence_start_id` | BIGINT | Price point ID at cycle start (timestamp ms) |
| `sequence_start_price` | DECIMAL(20,8) | Price at cycle start |
| `highest_price_reached` | DECIMAL(20,8) | Peak price in cycle |
| `lowest_price_reached` | DECIMAL(20,8) | Lowest price in cycle |
| `max_percent_increase` | DECIMAL(10,4) | Max growth from start |
| `max_percent_increase_from_lowest` | DECIMAL(10,4) | Max growth from low |
| `total_data_points` | INTEGER | Number of price points in cycle |
| `created_at` | TIMESTAMP | When record created |

**Indexes:**
- `idx_cycle_tracker_coin` on `(coin_id)`
- `idx_cycle_tracker_start` on `(cycle_start_time)`
- `idx_cycle_tracker_threshold` on `(threshold)`

### Table: wallet_profiles (24hr HOT - Special)
Wallet trading profiles - maps trades to completed price cycles.
Populated by `000data_feeds/5_create_profiles/create_profiles.py`.

**SPECIAL**: Both DuckDB AND MySQL only keep 24 hours of data (cleaned up hourly).

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGINT | Primary key |
| `wallet_address` | VARCHAR(255) | Wallet address |
| `threshold` | DECIMAL(5,2) | Cycle threshold (0.2-0.5) |
| `trade_id` | BIGINT | Reference to sol_stablecoin_trades.id |
| `trade_timestamp` | TIMESTAMP | When trade occurred |
| `price_cycle` | BIGINT | Reference to cycle_tracker.id |
| `price_cycle_start_time` | TIMESTAMP | When cycle started |
| `price_cycle_end_time` | TIMESTAMP | When cycle ended |
| `trade_entry_price_org` | DECIMAL(20,8) | Original trade price |
| `stablecoin_amount` | DOUBLE | Trade size in stablecoin |
| `trade_entry_price` | DECIMAL(20,8) | Entry price from price_points |
| `sequence_start_price` | DECIMAL(20,8) | Cycle start price |
| `highest_price_reached` | DECIMAL(20,8) | Cycle peak price |
| `lowest_price_reached` | DECIMAL(20,8) | Cycle low price |
| `long_short` | VARCHAR(10) | Perp direction (long/short/null) |
| `short` | TINYINT | 0=long, 1=short, 2=none |
| `created_at` | TIMESTAMP | When record created |

**Indexes:**
- `idx_wallet_profiles_wallet` on `(wallet_address)`
- `idx_wallet_profiles_threshold` on `(threshold)`
- `idx_wallet_profiles_trade_timestamp` on `(trade_timestamp)`
- `idx_wallet_profiles_price_cycle` on `(price_cycle)`
- `idx_wallet_profiles_short` on `(short)`

### Table: order_book_features (24hr HOT)
Binance order book features streamed via WebSocket.
Populated by `000data_feeds/3_binance_order_book_data/stream_binance_order_book_data.py`.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Primary key |
| `ts` | TIMESTAMP | When recorded |
| `venue` | VARCHAR(20) | Exchange venue (binance) |
| `quote_asset` | VARCHAR(10) | Quote asset (USDT) |
| `symbol` | VARCHAR(20) | Trading pair (SOLUSDT) |
| `best_bid` | DOUBLE | Best bid price |
| `best_ask` | DOUBLE | Best ask price |
| `mid_price` | DOUBLE | Mid price (bid+ask)/2 |
| `absolute_spread` | DOUBLE | Absolute spread (ask-bid) |
| `relative_spread_bps` | DOUBLE | Relative spread in basis points |
| `bid_depth_10` | DOUBLE | Total bid depth (top 10 levels) |
| `ask_depth_10` | DOUBLE | Total ask depth (top 10 levels) |
| `total_depth_10` | DOUBLE | Total depth (bid + ask) |
| `volume_imbalance` | DOUBLE | Volume imbalance (-1 to 1) |
| `bid_vwap_10` | DOUBLE | Bid VWAP (top 10 levels) |
| `ask_vwap_10` | DOUBLE | Ask VWAP (top 10 levels) |
| `bid_slope` | DOUBLE | Bid price-size slope |
| `ask_slope` | DOUBLE | Ask price-size slope |
| `microprice` | DOUBLE | Size-weighted mid price |
| `microprice_dev_bps` | DOUBLE | Microprice deviation from mid (bps) |
| `bid_depth_bps_5` | DOUBLE | Bid depth within 5 bps |
| `ask_depth_bps_5` | DOUBLE | Ask depth within 5 bps |
| `bid_depth_bps_10` | DOUBLE | Bid depth within 10 bps |
| `ask_depth_bps_10` | DOUBLE | Ask depth within 10 bps |
| `bid_depth_bps_25` | DOUBLE | Bid depth within 25 bps |
| `ask_depth_bps_25` | DOUBLE | Ask depth within 25 bps |
| `net_liquidity_change_1s` | DOUBLE | Net liquidity change over 1 second |
| `bids_json` | VARCHAR | JSON of top 20 bid levels |
| `asks_json` | VARCHAR | JSON of top 20 ask levels |
| `source` | VARCHAR(20) | Data source (WEBSOCKET) |

**Indexes:**
- `idx_order_book_features_ts` on `(ts)`
- `idx_order_book_features_symbol` on `(symbol)`

---

## Feature: Binance Order Book Stream

The Binance order book stream (`000data_feeds/3_binance_order_book_data/`) provides real-time order book data via WebSocket.

### How It Works

```
┌────────────────────────────────────────────────────────────────────────┐
│                     BINANCE ORDER BOOK STREAM                           │
├────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   Binance WebSocket    stream_binance_order_book_data.py               │
│   (depth20@100ms)  ──▶  (continuous stream)  ──▶  TradingDataEngine    │
│                               │                    (in-memory DuckDB)  │
│                               │                          │             │
│                               │                          ▼             │
│                               │                    Background Sync     │
│                               │                          │             │
│                               ▼                          ▼             │
│                            MySQL                      MySQL            │
│                     (order_book_features)      (full history)          │
│                                                                         │
└────────────────────────────────────────────────────────────────────────┘
```

### Features Calculated

| Feature | Description |
|---------|-------------|
| `mid_price` | (best_bid + best_ask) / 2 |
| `relative_spread_bps` | Spread in basis points |
| `volume_imbalance` | (bid_depth - ask_depth) / total_depth |
| `microprice` | Size-weighted mid price |
| `microprice_dev_bps` | Microprice deviation from mid |
| `bid/ask_depth_bps_X` | Depth within X basis points |
| `net_liquidity_change_1s` | Total depth change over 1 second |

### Usage

```python
# Started automatically by scheduler/master.py at startup
# To run standalone for testing:
python 000data_feeds/3_binance_order_book_data/stream_binance_order_book_data.py
```

---

## Feature: Price Cycles

The price cycles feature (`000data_feeds/2_create_price_cycles/`) tracks SOL price movements at multiple thresholds, detecting cycles based on price drops from highs.

### How It Works

```
┌────────────────────────────────────────────────────────────────────────┐
│                         PRICE CYCLE DETECTION                           │
├────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   prices.duckdb         create_price_cycles.py        central.duckdb   │
│   (price_points)  ────▶  (process every 5s)  ────▶  (price_analysis)   │
│                                    │                 (cycle_tracker)    │
│                                    │                                    │
│                                    ▼                                    │
│                                 MySQL                                   │
│                          (full history)                                │
│                                                                         │
└────────────────────────────────────────────────────────────────────────┘
```

### Thresholds Monitored

| Threshold | Cycle Reset Condition |
|-----------|----------------------|
| 0.1% | Price drops 0.1% from cycle high |
| 0.2% | Price drops 0.2% from cycle high |
| 0.3% | Price drops 0.3% from cycle high |
| 0.4% | Price drops 0.4% from cycle high |
| 0.5% | Price drops 0.5% from cycle high |

### Cycle Logic

1. **New Cycle**: Starts when no previous data exists, or when price drops below the cycle's highest recorded price by the threshold amount
2. **Cycle Continues**: As long as price stays within threshold of the cycle high
3. **Tracking**: Each cycle tracks highest/lowest price, percent changes, and total data points

### Usage

```python
# The scheduler runs this automatically every 5 seconds
# To run manually:
from create_price_cycles import process_price_cycles
processed = process_price_cycles()  # Returns number of price points processed

# For continuous testing:
python 000data_feeds/2_create_price_cycles/create_price_cycles.py --continuous
```

---

## Feature: Wallet Profiles

The wallet profiles feature (`000data_feeds/5_create_profiles/`) builds trading profiles by mapping wallet buy trades to completed price cycles.

### How It Works

```
┌────────────────────────────────────────────────────────────────────────┐
│                        WALLET PROFILE BUILDER                           │
├────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   sol_stablecoin_trades    create_profiles.py       wallet_profiles    │
│   (direction=buy)     ──▶  (process every 5s)  ──▶  (DuckDB 24hr)      │
│         │                        │                        │            │
│         │                        │                        │            │
│   cycle_tracker ─────────────────┘                        │            │
│   (cycle_end_time NOT NULL)                               ▼            │
│         │                                             MySQL            │
│   price_points ─────────────────────────────────▶  (also 24hr only)   │
│   (trade_entry_price)                                                  │
│                                                                         │
└────────────────────────────────────────────────────────────────────────┘
```

### Key Features

1. **All Thresholds**: Processes all thresholds (0.2%, 0.25%, 0.3%, 0.35%, 0.4%, 0.45%, 0.5%)
2. **Completed Cycles Only**: Only processes trades within completed cycles (cycle_end_time NOT NULL)
3. **Minimum Buys**: Wallets must have at least 10 buy trades to qualify
4. **24hr Retention in BOTH databases**: Unlike other tables, MySQL also only keeps 24 hours

### Data Joined

| Source Table | Data Used |
|--------------|-----------|
| `sol_stablecoin_trades` | Wallet address, trade timestamp, price, stablecoin amount, perp direction |
| `cycle_tracker` | Cycle ID, start/end times, sequence prices, high/low prices |
| `price_points` | Entry price (first price after trade timestamp) |

### Usage

```python
# The scheduler runs this automatically every 5 seconds
# To run manually:
from create_profiles import process_wallet_profiles
processed = process_wallet_profiles()  # Returns number of profiles processed

# For continuous testing:
python 000data_feeds/5_create_profiles/create_profiles.py --continuous
```

---

## API Endpoints

The Central API (`features/price_api/api.py`) provides these endpoints:

### Health & Status
- `GET /health` - Health check (DuckDB + MySQL status)
- `GET /stats` - Database statistics

### Plays
- `GET /plays` - Get all plays
- `GET /plays/<id>` - Get single play

### Buyins (Trades)
- `GET /buyins` - Get trades (params: play_id, status, hours, limit)
- `POST /buyins` - Create trade (dual-write)
- `PUT /buyins/<id>` - Update trade (dual-write)

### Price Checks
- `GET /price_checks` - Get price checks for a buyin
- `POST /price_checks` - Create price check (dual-write)

### Price Data
- `POST /price_points` - Get price points for charting (legacy)
- `GET /latest_prices` - Get latest prices (legacy)
- `GET /price_analysis` - Get price analysis
- `GET /cycle_tracker` - Get cycle data

### Admin
- `POST /admin/init_tables` - Initialize DuckDB tables
- `POST /admin/cleanup` - Clean up old DuckDB data
- `POST /admin/sync_from_mysql` - Sync from MySQL to DuckDB

### Generic Query
- `POST /query` - Flexible query endpoint

---

## Scheduler Jobs

The scheduler (`scheduler/master.py`) runs these jobs:

| Job | Interval | Description |
|-----|----------|-------------|
| `fetch_jupiter_prices` | 1 second | Fetch SOL/BTC/ETH prices from Jupiter API |
| `process_price_cycles` | 5 seconds | Process price data into cycle analysis |
| `process_wallet_profiles` | 5 seconds | Build wallet profiles from trades + cycles |
| `cleanup_jupiter_prices` | 1 hour | Clean up old data from prices.duckdb |
| `cleanup_duckdb_hot_tables` | 1 hour | Remove data older than 24h from DuckDB |
| `cleanup_wallet_profiles` | 1 hour | Clean up old profiles from BOTH DuckDB and MySQL |
| `sync_plays_from_mysql` | 5 minutes | Sync plays table from MySQL |

### Background Streams (Started Once at Startup)

| Stream | Description |
|--------|-------------|
| `binance_order_book_stream` | WebSocket stream of SOLUSDT order book from Binance (100ms updates) |

---

## Python Connection Patterns

### DuckDB (Hot Data - 24hr)
```python
from core.database import get_duckdb

with get_duckdb("central") as conn:
    result = conn.execute("SELECT * FROM follow_the_goat_buyins LIMIT 10").fetchall()
```

### MySQL (Historical Data)
```python
from core.database import get_mysql

with get_mysql() as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM follow_the_goat_buyins_archive LIMIT 10")
        result = cursor.fetchall()
```

### Dual Write (Both)
```python
from core.database import dual_write_insert

data = {
    'play_id': 1,
    'wallet_address': 'ABC123...',
    'our_status': 'pending'
}
duckdb_ok, mysql_ok = dual_write_insert('follow_the_goat_buyins', data)
```

### Smart Query (Auto-Routes)
```python
from core.database import smart_query
from datetime import datetime, timedelta

# Automatically uses DuckDB for recent data, MySQL for historical
results = smart_query(
    table='follow_the_goat_buyins',
    where={'play_id': 1},
    time_column='followed_at',
    start_time=datetime.now() - timedelta(hours=6),
    order_by='followed_at DESC',
    limit=100
)
```

---

## PHP Connection Pattern

```php
<?php
require_once 'includes/DuckDBClient.php';

$client = new DuckDBClient('http://127.0.0.1:5050');

// Get trades from last 24 hours (DuckDB)
$result = $client->getBuyins(playId: 1, hours: '24');

// Get historical trades (MySQL)
$result = $client->getBuyins(playId: 1, hours: 'all');

// Create trade (dual-write)
$client->createBuyin([
    'play_id' => 1,
    'wallet_address' => 'ABC123...',
]);
```

---

## Initial Setup

1. Initialize DuckDB tables:
```bash
python features/price_api/api.py --init
```

2. Sync data from MySQL:
```bash
python features/price_api/sync_from_mysql.py --init --hours 24
```

3. Start the API server:
```bash
python features/price_api/api.py
```

4. Start the scheduler:
```bash
python scheduler/master.py
```

---

## DuckDB CLI Access

```bash
# From project root
./duckdb/duckdb.exe 000data_feeds/central.duckdb

# Common commands
.tables              -- List all tables
.schema table_name   -- Show table schema
.mode markdown       -- Pretty output
.quit                -- Exit
```

---

## Migration from MySQL

When migrating code that uses MySQL directly:

1. **For reads**: Use the API or `get_duckdb()` for recent data, `get_mysql()` for historical
2. **For writes**: Use `dual_write_insert()` / `dual_write_update()` to write to both
3. **For PHP**: Use `DuckDBClient` class instead of direct MySQL PDO

### MySQL → DuckDB Type Mapping

| MySQL | DuckDB |
|-------|--------|
| `INT AUTO_INCREMENT` | `INTEGER PRIMARY KEY` |
| `BIGINT` | `BIGINT` |
| `DATETIME` | `TIMESTAMP` |
| `VARCHAR(n)` | `VARCHAR(n)` |
| `TEXT` | `VARCHAR` or `TEXT` |
| `DECIMAL(20,8)` | `DECIMAL(20,8)` |
| `JSON` | `JSON` |
| `TINYINT(1)` | `BOOLEAN` |
| `ENUM(...)` | `VARCHAR(...)` |

### Column Name Changes
| MySQL | DuckDB |
|-------|--------|
| `15_min_trail` | `fifteen_min_trail` |
