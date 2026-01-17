-- infrastructure/postgres/init_metrics.sql
-- ================================================================
-- E-COMMERCE STREAMING METRICS DATABASE SCHEMA
-- ================================================================

-- Drop existing tables (untuk development/testing)
DROP TABLE IF EXISTS payment_metrics CASCADE;
DROP TABLE IF EXISTS drop_off_analysis CASCADE;
DROP TABLE IF EXISTS real_time_funnel CASCADE;
DROP TABLE IF EXISTS gmv_metrics CASCADE;

-- ================================================================
-- 1. REAL-TIME FUNNEL TABLE
-- ================================================================
CREATE TABLE real_time_funnel (
    id SERIAL PRIMARY KEY,
    
    -- Time window
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    
    -- Funnel stages (counts)
    total_orders BIGINT NOT NULL DEFAULT 0,
    orders_with_items BIGINT NOT NULL DEFAULT 0,
    orders_with_payment BIGINT NOT NULL DEFAULT 0,
    
    -- Conversion rates (%)
    items_conversion_rate DECIMAL(5,2) DEFAULT 0.00,
    payment_conversion_rate DECIMAL(5,2) DEFAULT 0.00,
    
    -- Drop-offs
    dropped_after_order BIGINT DEFAULT 0,
    dropped_after_items BIGINT DEFAULT 0,
    
    -- Metadata
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Indexes for query performance
    CONSTRAINT unique_funnel_window UNIQUE(window_start, window_end)
);

CREATE INDEX idx_funnel_window_start ON real_time_funnel(window_start DESC);
CREATE INDEX idx_funnel_processed_at ON real_time_funnel(processed_at DESC);


-- ================================================================
-- 2. GMV METRICS TABLE
-- ================================================================
CREATE TABLE gmv_metrics (
    id SERIAL PRIMARY KEY,
    
    -- Time window
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    
    -- GMV metrics
    gmv DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    item_count BIGINT NOT NULL DEFAULT 0,
    unique_orders BIGINT NOT NULL DEFAULT 0,
    
    -- Price analytics
    avg_item_price DECIMAL(10,2) DEFAULT 0.00,
    max_item_price DECIMAL(10,2) DEFAULT 0.00,
    min_item_price DECIMAL(10,2) DEFAULT 0.00,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_gmv_window UNIQUE(window_start, window_end)
);

CREATE INDEX idx_gmv_window_start ON gmv_metrics(window_start DESC);
CREATE INDEX idx_gmv_value ON gmv_metrics(gmv DESC);


-- ================================================================
-- 3. DROP-OFF ANALYSIS TABLE
-- ================================================================
CREATE TABLE drop_off_analysis (
    id SERIAL PRIMARY KEY,
    
    -- Time window
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    
    -- Order status
    order_status VARCHAR(50) NOT NULL,
    
    -- Drop-off metrics
    dropped_orders BIGINT NOT NULL DEFAULT 0,
    unique_customers_affected BIGINT DEFAULT 0,
    drop_rate DECIMAL(10,2) DEFAULT 0.00,
    
    -- Alert system
    alert_triggered BOOLEAN DEFAULT FALSE,
    
    -- Sample data (for investigation)
    sample_order_ids TEXT[], -- Array of order IDs
    
    -- Metadata
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_dropoff_window_status 
        UNIQUE(window_start, window_end, order_status)
);

CREATE INDEX idx_dropoff_window_start ON drop_off_analysis(window_start DESC);
CREATE INDEX idx_dropoff_alert ON drop_off_analysis(alert_triggered) WHERE alert_triggered = TRUE;
CREATE INDEX idx_dropoff_status ON drop_off_analysis(order_status);


-- ================================================================
-- 4. PAYMENT METRICS TABLE
-- ================================================================
CREATE TABLE payment_metrics (
    id SERIAL PRIMARY KEY,
    
    -- Time window
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    
    -- Payment method
    payment_type VARCHAR(50) NOT NULL,
    
    -- Volume metrics
    transaction_count BIGINT NOT NULL DEFAULT 0,
    unique_orders BIGINT NOT NULL DEFAULT 0,
    total_payment_value DECIMAL(15,2) DEFAULT 0.00,
    avg_payment_value DECIMAL(10,2) DEFAULT 0.00,
    
    -- Installment analysis
    avg_installments DECIMAL(5,2) DEFAULT 0.00,
    max_installments INT DEFAULT 0,
    
    -- Success metrics
    successful_orders BIGINT DEFAULT 0,
    failed_orders BIGINT DEFAULT 0,
    success_rate DECIMAL(5,2) DEFAULT 0.00,
    
    -- Metadata
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_payment_window_type 
        UNIQUE(window_start, window_end, payment_type)
);

CREATE INDEX idx_payment_window_start ON payment_metrics(window_start DESC);
CREATE INDEX idx_payment_type ON payment_metrics(payment_type);
CREATE INDEX idx_payment_value ON payment_metrics(total_payment_value DESC);


-- ================================================================
-- VIEWS FOR EASY QUERYING
-- ================================================================

-- Latest Funnel Metrics (last hour)
CREATE OR REPLACE VIEW v_latest_funnel AS
SELECT 
    window_start,
    window_end,
    total_orders,
    orders_with_items,
    orders_with_payment,
    items_conversion_rate,
    payment_conversion_rate,
    dropped_after_order,
    dropped_after_items
FROM real_time_funnel
WHERE window_start >= NOW() - INTERVAL '1 hour'
ORDER BY window_start DESC;


-- Complete Funnel View (JOIN dengan GMV dan Payment)
-- Menggabungkan semua metrics berdasarkan window untuk analisis lengkap
CREATE OR REPLACE VIEW v_complete_funnel AS
SELECT 
    f.window_start,
    f.window_end,
    -- Funnel metrics
    f.total_orders,
    f.orders_with_items,
    f.orders_with_payment,
    f.items_conversion_rate,
    f.payment_conversion_rate,
    f.dropped_after_order,
    f.dropped_after_items,
    -- GMV metrics (dari tabel terpisah)
    COALESCE(g.gmv, 0) as total_gmv,
    COALESCE(g.item_count, 0) as total_items,
    -- Payment metrics (aggregate dari semua payment types)
    COALESCE(p.total_payment, 0) as total_payment,
    COALESCE(p.avg_payment, 0) as avg_payment_value,
    -- Metadata
    f.processed_at
FROM real_time_funnel f
LEFT JOIN gmv_metrics g 
    ON f.window_start = g.window_start 
    AND f.window_end = g.window_end
LEFT JOIN (
    SELECT 
        window_start,
        window_end,
        SUM(total_payment_value) as total_payment,
        AVG(avg_payment_value) as avg_payment
    FROM payment_metrics
    GROUP BY window_start, window_end
) p ON f.window_start = p.window_start 
    AND f.window_end = p.window_end
ORDER BY f.window_start DESC;


-- Payment Method Performance (last hour)
CREATE OR REPLACE VIEW v_payment_performance AS
SELECT 
    payment_type,
    SUM(transaction_count) as total_transactions,
    SUM(total_payment_value) as total_value,
    AVG(avg_payment_value) as avg_value,
    AVG(success_rate) as avg_success_rate
FROM payment_metrics
WHERE window_start >= NOW() - INTERVAL '1 hour'
GROUP BY payment_type
ORDER BY total_value DESC;


-- Active Alerts (drop-offs)
CREATE OR REPLACE VIEW v_active_alerts AS
SELECT 
    window_start,
    window_end,
    order_status,
    dropped_orders,
    unique_customers_affected,
    sample_order_ids,
    detected_at
FROM drop_off_analysis
WHERE alert_triggered = TRUE
  AND window_start >= NOW() - INTERVAL '1 hour'
ORDER BY dropped_orders DESC;


-- ================================================================
-- UTILITY FUNCTIONS
-- ================================================================

-- Function to clean old data (retention policy)
CREATE OR REPLACE FUNCTION cleanup_old_metrics(retention_days INT DEFAULT 30)
RETURNS TABLE(
    table_name TEXT,
    rows_deleted BIGINT
) AS $$
DECLARE
    deleted_count BIGINT;
BEGIN
    -- Clean real_time_funnel
    DELETE FROM real_time_funnel 
    WHERE window_start < NOW() - (retention_days || ' days')::INTERVAL;
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    table_name := 'real_time_funnel';
    rows_deleted := deleted_count;
    RETURN NEXT;
    
    -- Clean gmv_metrics
    DELETE FROM gmv_metrics 
    WHERE window_start < NOW() - (retention_days || ' days')::INTERVAL;
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    table_name := 'gmv_metrics';
    rows_deleted := deleted_count;
    RETURN NEXT;
    
    -- Clean drop_off_analysis
    DELETE FROM drop_off_analysis 
    WHERE window_start < NOW() - (retention_days || ' days')::INTERVAL;
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    table_name := 'drop_off_analysis';
    rows_deleted := deleted_count;
    RETURN NEXT;
    
    -- Clean payment_metrics
    DELETE FROM payment_metrics 
    WHERE window_start < NOW() - (retention_days || ' days')::INTERVAL;
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    table_name := 'payment_metrics';
    rows_deleted := deleted_count;
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;


-- ================================================================
-- GRANTS (adjust based on your user setup)
-- ================================================================
-- GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO your_spark_user;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO your_spark_user;


-- ================================================================
-- VERIFICATION QUERIES
-- ================================================================

-- Check table sizes
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
  AND tablename IN ('real_time_funnel', 'gmv_metrics', 'drop_off_analysis', 'payment_metrics')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;