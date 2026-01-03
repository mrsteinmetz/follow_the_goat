# FastAPI Webhook (QuickNode Sink)

Endpoints (port 8001):
- `POST /webhook` – receive trade payloads, writes to `sol_stablecoin_trades`
- `POST /webhook/whale-activity/` – receive whale payloads, writes to `whale_movements`
- `GET /webhook/health` – health + counts
- `GET /webhook/api/trades` – incremental pull (after_id/start/end, limit)
- `GET /webhook/api/whale-movements` – whale data pull

QuickNode destination:
- **Set to:** `http://116.202.51.115:8001/webhook`
- (Optional whale) `http://116.202.51.115:8001/webhook/whale-activity/`

Storage:
- Direct write to TradingDataEngine (DuckDB in-memory hot storage, 24h)
- Existing scheduler cleanup handles archival to MySQL after 24h
