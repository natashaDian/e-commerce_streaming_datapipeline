# 🚀 E-Commerce Real-Time Streaming Pipeline

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/)
[![Spark](https://img.shields.io/badge/Spark-3.5.0-orange.svg)](https://spark.apache.org/)
[![Kafka](https://img.shields.io/badge/Kafka-7.4.0-black.svg)](https://kafka.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue.svg)](https://www.postgresql.org/)

Production-ready streaming data pipeline untuk real-time e-commerce analytics dengan fokus pada **Funnel Analysis**, **GMV Tracking**, dan **Drop-off Detection**.

## 📋 Table of Contents

- [Features](#-features)
- [Architecture](#-architecture)
- [Quick Start](#-quick-start)
- [Project Structure](#-project-structure)
- [Usage](#-usage)
- [Monitoring](#-monitoring)
- [Interview Q&A](#-critical-interview-questions)
- [Development](#-development)
- [Troubleshooting](#-troubleshooting)

## ✨ Features

### Real-Time Analytics
- ✅ **Real-time Funnel Analysis** - Track user journey: Order → Items → Payment
- ✅ **GMV Metrics** - Gross Merchandise Value tracking per minute/hour
- ✅ **Drop-off Detection** - Identify where users abandon transactions
- ✅ **Payment Analytics** - Payment method insights and patterns

### Production-Ready Architecture
- ✅ **Fault Tolerance** - Spark checkpointing + Kafka offset management
- ✅ **Exactly-Once Semantics** - Event deduplication via event_id
- ✅ **Late Data Handling** - Watermarking for out-of-order events
- ✅ **Scalability** - Partitioned topics + stateful processing
- ✅ **Monitoring** - Grafana dashboards + Prometheus metrics

### Code Quality
- ✅ **Modular Design** - Separated concerns (config, processors, sinks)
- ✅ **Schema Validation** - Avro schemas for data quality
- ✅ **Error Handling** - Comprehensive error handling and logging
- ✅ **Type Safety** - Type hints throughout codebase

## 🏗️ Architecture

```
┌──────────────┐
│ CSV Dataset  │ (Brazilian E-Commerce)
└──────┬───────┘
       │
       ▼
┌──────────────────────────────────────────────────────┐
│             Event Generator (Producer)                │
│  - Replay historical data as real-time events         │
│  - Configurable speed (1x, 10x, 100x)                │
│  - Schema validation (Avro)                           │
└──────────────┬───────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────┐
│              Apache Kafka (3 topics)                  │
│  - olist.orders       (Order events)                  │
│  - olist.order_items  (Item events)                   │
│  - olist.payments     (Payment events)                │
│                                                       │
│  Features: Partitioning, Retention, Compression       │
└──────────────┬───────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────┐
│         Spark Structured Streaming                    │
│  ┌──────────────────────────────────────────────┐    │
│  │ Stream Processors (Modular)                   │    │
│  │  - FunnelProcessor    (Conversion tracking)   │    │
│  │  - GMVProcessor       (Revenue metrics)       │    │
│  │  - DropOffProcessor   (Abandonment detection) │    │
│  │  - PaymentProcessor   (Payment analytics)     │    │
│  └──────────────────────────────────────────────┘    │
│                                                       │
│  Features: Deduplication, Watermarking, Joins         │
└──────────────┬───────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────┐
│              Sinks (Multiple outputs)                 │
│  - PostgreSQL (Persistent metrics)                    │
│  - Console    (Real-time monitoring)                  │
│  - Prometheus (System metrics)                        │
└──────────────┬───────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────┐
│            Grafana Dashboards                         │
│  - Real-time funnel visualization                     │
│  - GMV trends and forecasting                         │
│  - Drop-off analysis and alerts                       │
└───────────────────────────────────────────────────────┘
```

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.9+
- Java 11+ (for Spark)
- 8GB RAM minimum
- Dataset from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

### 1. Clone and Setup

```bash
# Clone repository
git clone <your-repo-url>
cd ecommerce-streaming-pipeline

python -m venv .venv

#windows
.venv\Scripts\activate

#macos/linux
source .venv/bin/activate

# Setup environment
make setup

# Download dataset and extract to dataset/ folder
# https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
```

### 2. Start Infrastructure

```bash
# Start all services (Kafka, PostgreSQL, Grafana, etc.)
make infra-up

# Verify services are running
make status
```

### 3. Run Pipeline

**Terminal 1 - Start Consumer:**
```bash
# Local mode (development)
make consumer

# Cluster mode (muncul di Spark UI http://localhost:8080)
make consumer-cluster
```

**Terminal 2 - Start Producer:**
```bash
# Test with 500 orders at 5x speed
make producer

# Or run full dataset
make producer-full
```

### 4. Monitor

```bash
# Check service status
make status

# View latest database metrics
make db-latest

# Monitoring URLs:
# Kafka UI:    http://localhost:8090
# Grafana:     http://localhost:3000 (admin/admin)
# Spark UI:    http://localhost:4040 (local mode)
# Spark Master: http://localhost:8080 (cluster mode)
```

## 📁 Project Structure

```
ecommerce-streaming-pipeline/
│
├── config/                      # Configuration layer
│   ├── kafka_config.py         # Kafka settings
│   ├── spark_config.py         # Spark settings
│   ├── database_config.py      # PostgreSQL settings
│   └── app_config.py           # Application settings
│
├── src/                         # Source code
│   ├── producers/              # Event producers
│   │   ├── base_producer.py   # Base producer class
│   │   └── event_generator.py # Main event generator
│   │
│   ├── consumers/              # Stream consumers
│   │   └── stream_processor.py # Main Spark processor
│   │
│   ├── processors/             # Business logic (modular)
│   │   ├── funnel_processor.py   # Conversion tracking
│   │   ├── gmv_processor.py      # Revenue metrics
│   │   ├── dropoff_processor.py  # Abandonment detection
│   │   └── payment_processor.py  # Payment analytics
│   │
│   ├── sinks/                  # Data sinks
│   │   └── metrics_sink.py    # PostgreSQL + Console (unified)
│   │
│   ├── schemas/                # Avro schemas
│   │   └── avro_schemas.py    # Schema definitions
│   │
│   └── utils/                  # Utilities
│       └── logger.py          # Logging configuration
│
├── infrastructure/             # Infrastructure as Code
│   ├── docker/
│   │   └── docker-compose.yml # All services
│   ├── kafka/
│   │   └── setup_topics.sh   # Topic creation
│   ├── postgres/
│   │   └── init.sql          # Database schema
│   └── grafana/
│       └── dashboards/       # Pre-built dashboards
│
├── scripts/                    # Operational scripts
│   └── setup_kafka_topics.sh  # Kafka setup
│
├── dataset/                    # Data files (not in git)
├── checkpoint/                 # Spark checkpoints
├── logs/                      # Application logs
│
├── Makefile                   # Automation commands
├── requirements.txt           # Python dependencies
└── README.md                  # This file
```

## 🎯 Usage

### Common Commands

```bash
# Setup & Infrastructure
make setup             # Install dependencies
make infra-up          # Start all services
make infra-down        # Stop all services
make status            # Check service status
make logs              # View service logs

# Pipeline
make producer          # Run event generator (500 events, 5x speed)
make producer-full     # Run full dataset
make consumer          # Run stream processor (local mode)
make consumer-cluster  # Run on Spark cluster (muncul di Spark UI)
make consumer-bg       # Run in background

# Database
make db-shell          # PostgreSQL shell
make db-latest         # Show latest metrics
make db-complete       # Complete funnel view (with GMV & payments)
make db-truncate       # Clear all tables

# Cleanup
make clean             # Clean checkpoints & logs
make reset             # Full reset

# Demo
make demo              # End-to-end pipeline demo
```

### Advanced Usage

**Custom Producer Settings:**
```bash
python -m src.producers.event_generator \
    --dataset-dir ./dataset \
    --speed 10 \
    --limit 1000
```

**Custom Consumer Settings:**
```bash
python -m src.consumers.stream_processor \
    --no-postgres \
    --no-console
```

## 📊 Monitoring

### Kafka UI (Port 8090)
- Topics overview
- Consumer groups
- Message inspection
- Partition details

### Grafana (Port 3000)
Default credentials: `admin/admin`

**Pre-built Dashboards:**
1. **Real-Time Funnel** - Conversion rates and drop-off visualization
2. **GMV Trends** - Revenue tracking and forecasting
3. **Drop-off Analysis** - Abandonment patterns
4. **Payment Insights** - Payment method distribution

### Spark UI (Port 4040)
- Streaming query progress
- Job execution details
- Stage metrics

### PostgreSQL Queries

```sql
-- Latest funnel metrics
SELECT * FROM real_time_funnel 
ORDER BY window_start DESC 
LIMIT 10;

-- Hourly GMV summary
SELECT * FROM hourly_funnel_summary 
ORDER BY hour DESC;

-- Drop-off by status
SELECT 
    order_status,
    SUM(dropped_orders) as total_dropped,
    AVG(dropped_orders) as avg_dropped
FROM drop_off_analysis
GROUP BY order_status;

-- Payment method distribution
SELECT 
    payment_type,
    SUM(payment_count) as total_payments,
    SUM(total_value) as total_revenue
FROM payment_metrics
GROUP BY payment_type
ORDER BY total_revenue DESC;
```

## 🔑 Critical Interview Questions

### 1. Apa yang terjadi kalau consumer mati?

**Jawaban:**
Spark Streaming menggunakan **checkpointing** untuk menyimpan state dan **Kafka offset management**. Saat consumer restart:

1. Read checkpoint terakhir
2. Resume dari last committed offset
3. Continue processing tanpa data loss

**Bukti dalam kode:**
```python
# config/spark_config.py
CHECKPOINT_DIR = "./checkpoint"

# Setiap query punya checkpoint sendiri
checkpoint_location = SparkConfig.get_checkpoint_location("funnel")
```

### 2. Apakah data hilang?

**TIDAK**, karena:

1. **Kafka Durability**
   - Data persisted di disk
   - Retention 7 hari (configurable)
   - Replication (untuk production)

2. **Spark Checkpointing**
   - State disimpan periodic
   - WAL (Write-Ahead Log)
   - Atomic commit

3. **Deduplication**
   ```python
   def _deduplicate(self, df: DataFrame) -> DataFrame:
       return df.dropDuplicates(["event_id"])
   ```

**Trade-off:** Bisa ada duplicates jika crash sebelum commit, tapi kita handle dengan deduplication.

### 3. Bisa replay event?

**YA, ada 3 cara:**

**Option 1: Reset Offset**
```bash
docker exec kafka kafka-consumer-groups \
    --bootstrap-server localhost:29092 \
    --group ecommerce-streaming-consumer \
    --reset-offsets --to-earliest \
    --all-topics --execute
```

**Option 2: Delete Checkpoint**
```bash
make clean-checkpoints
# Spark akan start dari "earliest"
```

**Option 3: New Consumer Group**
```python
# Ganti group_id di config
CONSUMER_CONFIG = {
    "group_id": "new-consumer-group-v2"
}
```

### 4. Apa dampaknya ke funnel?

**Real-time Funnel Metrics:**

```python
# FunnelProcessor menghitung:
- total_orders              # Stage 1
- orders_with_items         # Stage 2
- orders_with_payment       # Stage 3

# Conversion rates
items_conversion_rate = (orders_with_items / total_orders) × 100
payment_conversion_rate = (orders_with_payment / total_orders) × 100

# Drop-off detection
dropped_after_order = total_orders - orders_with_items
dropped_after_items = orders_with_items - orders_with_payment
```

**Dashboard menampilkan:**
- Real-time conversion rate
- Bottleneck identification
- Trend analysis

### 5. Bagaimana scaling Kafka topic?

**Horizontal Scaling:**

```bash
# Increase partitions
docker exec kafka kafka-topics --alter \
    --bootstrap-server localhost:29092 \
    --topic olist.orders \
    --partitions 6

# Add more consumers (parallel processing)
python -m src.consumers.stream_processor &  # Consumer 1
python -m src.consumers.stream_processor &  # Consumer 2
```

**Key Points:**
- Max parallelism = jumlah partitions
- Partition key: `order_id` (data locality)
- Auto rebalancing saat consumer join/leave

**Scaling Strategy:**
```
1 partition  + 1 consumer  = 1x throughput
3 partitions + 3 consumers = 3x throughput
6 partitions + 6 consumers = 6x throughput
```

## 🛠️ Development

### Setup Development Environment

```bash
# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt  # Testing tools

# Setup pre-commit hooks (optional)
pre-commit install
```

### Running Tests

```bash
# Unit tests
pytest tests/unit/

# Integration tests
pytest tests/integration/

# All tests
pytest tests/
```

### Code Quality

```bash
# Format code
black src/ config/

# Lint
pylint src/ config/

# Type checking
mypy src/ config/
```

## 🐛 Troubleshooting

### Kafka Connection Issues

```bash
# Check Kafka is running
docker ps | grep kafka

# Check Kafka logs
docker logs kafka

# Test connectivity
make test-producer
```

### Checkpoint Corruption

```bash
# Clean and restart
make clean-checkpoints
make consumer
```

### Out of Memory

```bash
# Increase Spark memory
export SPARK_DRIVER_MEMORY=4g
export SPARK_EXECUTOR_MEMORY=4g

# Or reduce shuffle partitions
# Edit config/spark_config.py:
SHUFFLE_PARTITIONS = 2
```

### PostgreSQL Connection Failed

```bash
# Check PostgreSQL is running
make status

# Test connection
make test-postgres

# Connect to shell
make db-shell
```

## 📚 References

- [Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)

**Built with ❤️ for demonstrating production-ready streaming architecture**