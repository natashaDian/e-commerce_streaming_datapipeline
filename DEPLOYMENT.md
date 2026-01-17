# 🚀 Deployment Guide

Complete guide untuk deployment e-commerce streaming pipeline.

## 📋 Pre-Deployment Checklist

### System Requirements
- [ ] Docker 20.10+ installed
- [ ] Docker Compose 2.0+ installed
- [ ] Python 3.9+ installed
- [ ] Java 11+ installed (untuk Spark)
- [ ] Minimum 8GB RAM
- [ ] Minimum 20GB disk space

### Dataset Preparation
- [ ] Download dataset from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- [ ] Extract to `dataset/` directory
- [ ] Verify CSV files:
  - olist_orders_dataset.csv
  - olist_order_items_dataset.csv
  - olist_order_payments_dataset.csv
  - olist_customers_dataset.csv
  - olist_products_dataset.csv
  - olist_sellers_dataset.csv

## 🎯 Deployment Steps

### Step 1: Initial Setup

```bash
# Clone repository
git clone <your-repo-url>
cd ecommerce-streaming-pipeline

# Create .env file
cp .env.example .env
# Edit .env with your configurations

# Create vit env
python -m venv .venv

#windows
.venv\Scripts\activate

#macos/linux
source .venv/bin/activate

# Install Python dependencies
make setup
```

### Step 2: Start Infrastructure

```bash
# Start all services
make infra-up

# Wait for all services to be healthy (takes ~30 seconds)
# Verify status
make status

# Expected output:
# ✓ zookeeper     - healthy
# ✓ kafka         - healthy
# ✓ schema-registry - healthy
# ✓ postgres_db   - healthy
# ✓ prometheus    - healthy
# ✓ grafana       - healthy
```

### Step 3: Verify Services

```bash
# Test all connections
make test-all

# Should see:
# ✓ Producer initialized successfully
# ✓ Consumer connected successfully
# ✓ PostgreSQL connected successfully
```

### Step 4: Create Kafka Topics

```bash
# This is done automatically in infra-up
# But you can manually run:
make create-topics

# Verify topics created
make kafka-topics

# Expected output:
# olist.orders
# olist.order_items
# olist.payments
```

### Step 5: Run Pipeline

**Terminal 1 - Start Stream Processor:**
```bash
# Local mode (development, faster startup)
make consumer

# Cluster mode (production, muncul di Spark Master UI http://localhost:8080)
make consumer-cluster
```

**Terminal 2 - Start Event Generator:**
```bash
# Run 500 orders at 5x speed
make producer

# If successful, run full dataset
make producer-full
```

### Step 6: Monitor

```bash
# Check service status
make status

# View latest metrics
make db-latest

# Complete funnel view (JOIN with GMV & Payments)
make db-complete

# Access UIs:
# Kafka UI:     http://localhost:8090
# Grafana:      http://localhost:3000 (admin/admin)
# Spark UI:     http://localhost:4040 (local mode)
# Spark Master: http://localhost:8080 (cluster mode)
# Prometheus:   http://localhost:9090
```

## 🔒 Production Deployment

### Environment Configuration

**Update `.env` for production:**

```bash
# Environment
ENVIRONMENT=production

# Kafka (use internal URLs)
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
SCHEMA_REGISTRY_URL=http://schema-registry:8081

# Spark (increase resources)
SPARK_DRIVER_MEMORY=4g
SPARK_EXECUTOR_MEMORY=4g
SPARK_SHUFFLE_PARTITIONS=6

# PostgreSQL (use strong password)
POSTGRES_PASSWORD=<strong-password>
DB_POOL_SIZE=10
DB_MAX_OVERFLOW=20
```

### Scaling Configuration

**Kafka Topics - Increase Partitions:**
```bash
# Update infrastructure/kafka/topic_configs.json
{
  "partitions": 6,
  "replication_factor": 3
}

# Or manually:
docker exec kafka kafka-topics --alter \
    --bootstrap-server localhost:29092 \
    --topic olist.orders \
    --partitions 6
```

**Add More Consumers:**
```bash
# Consumer 1 (default checkpoint)
python -m src.consumers.stream_processor &

# Consumer 2 (separate checkpoint)
SPARK_CHECKPOINT_DIR=./checkpoint2 \
python -m src.consumers.stream_processor &
```

### Resource Limits (Docker Compose)

```yaml
services:
  kafka:
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 4G
        reservations:
          cpus: '1'
          memory: 2G
```

## 📊 Monitoring Setup

### Grafana Dashboards

1. Access Grafana: http://localhost:3000
2. Login: admin/admin
3. Navigate to Dashboards
4. Import dashboards from `infrastructure/grafana/dashboards/`

**Available Dashboards:**
- Real-Time Funnel Analysis
- GMV Trends and Forecasting
- Drop-off Detection
- Payment Method Analytics

### Alerts Configuration

**PostgreSQL Alerts:**
```sql
-- Create alert function
CREATE OR REPLACE FUNCTION check_drop_off_rate()
RETURNS trigger AS $$
BEGIN
  IF NEW.dropped_after_items > 100 THEN
    -- Send alert (implement notification logic)
    RAISE NOTICE 'High drop-off rate detected: %', NEW.dropped_after_items;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger
CREATE TRIGGER alert_high_dropoff
AFTER INSERT ON drop_off_analysis
FOR EACH ROW
EXECUTE FUNCTION check_drop_off_rate();
```

## 🐛 Troubleshooting

### Common Issues

**Issue 1: Kafka not connecting**
```bash
# Check Kafka logs
docker logs kafka

# Common fix: Wait longer for Kafka to be ready
sleep 30

# Or restart Kafka
docker restart kafka
```

**Issue 2: Checkpoint corruption**
```bash
# Clean checkpoints and restart
make clean-checkpoints
make consumer
```

**Issue 3: Out of memory**
```bash
# Increase Spark memory in .env
SPARK_DRIVER_MEMORY=4g
SPARK_EXECUTOR_MEMORY=4g

# Reduce shuffle partitions
SPARK_SHUFFLE_PARTITIONS=2
```

**Issue 4: PostgreSQL connection timeout**
```bash
# Increase connection pool
DB_POOL_SIZE=10
DB_MAX_OVERFLOW=20
DB_POOL_TIMEOUT=60
```

### Health Checks

```bash
# Check all services health
docker ps --format "table {{.Names}}\t{{.Status}}"

# Check Kafka topics
make kafka-topics

# Check PostgreSQL tables
make db-shell
\dt

# Check consumer lag
docker exec kafka kafka-consumer-groups \
    --bootstrap-server localhost:29092 \
    --describe \
    --group ecommerce-streaming-consumer
```

## 🔄 Maintenance

### Regular Tasks

**Daily:**
```bash
# Check logs for errors
tail -f logs/*.log

# Monitor consumer lag
make status
```

**Weekly:**
```bash
# Refresh materialized views
make db-shell
SELECT refresh_all_views();

# Cleanup old logs
make clean-logs
```

**Monthly:**
```bash
# Cleanup old data (keeps last 30 days)
make db-shell
SELECT cleanup_old_data();

# Backup PostgreSQL
docker exec postgres_db pg_dump -U ecommerce ecommerce_metrics > backup.sql
```

### Backup Strategy

**Automated Backup Script:**
```bash
#!/bin/bash
# backup.sh
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="backups"

# Create backup directory
mkdir -p $BACKUP_DIR

# Backup PostgreSQL
docker exec postgres_db pg_dump -U ecommerce ecommerce_metrics \
    > $BACKUP_DIR/postgres_$DATE.sql

# Backup checkpoints (optional)
tar -czf $BACKUP_DIR/checkpoint_$DATE.tar.gz checkpoint/

# Keep only last 7 days
find $BACKUP_DIR -name "*.sql" -mtime +7 -delete
find $BACKUP_DIR -name "*.tar.gz" -mtime +7 -delete

echo "Backup completed: $DATE"
```

## 📈 Performance Tuning

### Kafka Tuning

```properties
# In docker-compose.yml - kafka service
KAFKA_NUM_NETWORK_THREADS=8
KAFKA_NUM_IO_THREADS=8
KAFKA_SOCKET_SEND_BUFFER_BYTES=102400
KAFKA_SOCKET_RECEIVE_BUFFER_BYTES=102400
KAFKA_SOCKET_REQUEST_MAX_BYTES=104857600
```

### Spark Tuning

```python
# In config/spark_config.py
SPARK_CONFIGS = {
    # Increase parallelism
    "spark.sql.shuffle.partitions": "12",
    "spark.default.parallelism": "12",
    
    # Memory tuning
    "spark.memory.fraction": "0.8",
    "spark.memory.storageFraction": "0.3",
    
    # Network tuning
    "spark.network.timeout": "800s",
    "spark.executor.heartbeatInterval": "60s"
}
```

### PostgreSQL Tuning

```sql
-- In init.sql or manually
ALTER SYSTEM SET shared_buffers = '256MB';
ALTER SYSTEM SET effective_cache_size = '1GB';
ALTER SYSTEM SET maintenance_work_mem = '64MB';
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
ALTER SYSTEM SET wal_buffers = '16MB';
ALTER SYSTEM SET default_statistics_target = 100;
ALTER SYSTEM SET random_page_cost = 1.1;
ALTER SYSTEM SET effective_io_concurrency = 200;
ALTER SYSTEM SET work_mem = '4MB';
ALTER SYSTEM SET min_wal_size = '1GB';
ALTER SYSTEM SET max_wal_size = '4GB';

-- Restart PostgreSQL
SELECT pg_reload_conf();
```

## ✅ Deployment Checklist

Before going to production:

- [ ] All tests passing (`make test-all`)
- [ ] Monitoring dashboards configured
- [ ] Alerts setup
- [ ] Backup strategy in place
- [ ] Resource limits configured
- [ ] Scaling plan documented
- [ ] Disaster recovery plan ready
- [ ] Security review completed
- [ ] Performance benchmarks done
- [ ] Documentation updated

## 📞 Support

For issues or questions:
- Check [Troubleshooting](#-troubleshooting) section
- Review logs in `logs/` directory
- Check service status with `make status`
- Create GitHub issue with:
  - Error logs
  - Environment details
  - Steps to reproduce

---

**Happy Streaming! 🚀**