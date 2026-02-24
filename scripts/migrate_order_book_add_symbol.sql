-- Add symbol column to order_book_features if missing (e.g. older DBs created without it)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'order_book_features' AND column_name = 'symbol'
  ) THEN
    ALTER TABLE order_book_features ADD COLUMN symbol VARCHAR(20) DEFAULT 'SOLUSDT' NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_orderbook_symbol ON order_book_features(symbol);
    RAISE NOTICE 'Added column symbol to order_book_features';
  END IF;
END $$;
