# Optimized locking section for build_profiles_for_local_duckdb
# This replaces lines 706-784 in create_profiles.py

# Process all thresholds - use minimal locking per operation
for threshold in THRESHOLDS:
    try:
        # Get latest completed cycle end time (from DuckDB) - brief lock
        if lock:
            lock.acquire()
        try:
            latest_cycle_end = _get_latest_cycle_end(local_conn, threshold)
        finally:
            if lock:
                lock.release()
        
        if not latest_cycle_end:
            continue
        
        # Get last processed trade ID (from DuckDB state) - brief lock
        if lock:
            lock.acquire()
        try:
            last_trade_id = _get_last_id(threshold)
        finally:
            if lock:
                lock.release()
        
        # Build profiles using local connection - brief lock
        if lock:
            lock.acquire()
        try:
            profiles = build_profiles_batch_duckdb(
                threshold, last_trade_id, latest_cycle_end, 
                batch_size=BATCH_SIZE, conn=local_conn
            )
        finally:
            if lock:
                lock.release()
        
        if not profiles:
            continue
        
        # Insert profiles into local DuckDB - brief lock (PyArrow is FAST!)
        if lock:
            lock.acquire()
        try:
            inserted = _insert_profiles(local_conn, profiles)
        finally:
            if lock:
                lock.release()
        
        # DISABLED: Profiles should ONLY live in master2.py's local DuckDB
        # Website queries master2 (port 5052) directly, NOT master.py (port 5050)
        # Master.py should only have raw data ingestion (prices, trades, order book)
        # 
        # if data_client and profiles:
        #     try:
        #         data_client.insert_batch('wallet_profiles', api_profiles)
        #         logger.debug(f"Pushed {len(api_profiles)} profiles to Data Engine API")
        #     except Exception as api_err:
        #         logger.warning(f"Failed to push profiles to API (non-critical): {api_err}")
        
        # Update state (in DuckDB) - brief lock
        if profiles:
            max_id = max(p['trade_id'] for p in profiles)
            if lock:
                lock.acquire()
            try:
                _update_last_id(threshold, max_id)
                total_inserted += inserted
            finally:
                if lock:
                    lock.release()
            
            if inserted > 0:
                logger.info(f"Threshold {threshold}: created {inserted} profiles (trade_id up to {max_id})")
        
    except Exception as e:
        logger.error(f"Error processing threshold {threshold}: {e}")
        continue

if total_inserted > 0:
    logger.info(f"Inserted {total_inserted} profiles across all thresholds")

return total_inserted

