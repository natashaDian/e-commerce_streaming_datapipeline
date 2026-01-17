# ==================================================
# E-Commerce Streaming Pipeline - Makefile
# ==================================================

.PHONY: help setup infra-up infra-down status producer consumer db-latest clean

# Configuration
DOCKER_COMPOSE := docker-compose -f infrastructure/docker/docker-compose.yml
PYTHON := python3
KAFKA_CONTAINER := kafka
POSTGRES_CONTAINER := postgres_db
CHECKPOINT_DIR := checkpoint
LOGS_DIR := logs

# Load .env if exists
-include .env
export

# Colors
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m

# ==================================================
# Help
# ==================================================
help:
	@echo "$(BLUE)E-Commerce Streaming Pipeline$(NC)"
	@echo ""
	@echo "$(YELLOW)Quick Start:$(NC)"
	@echo "  $(GREEN)make setup$(NC)          - Install dependencies"
	@echo "  $(GREEN)make infra-up$(NC)       - Start all services"
	@echo "  $(GREEN)make producer$(NC)       - Run event generator"
	@echo "  $(GREEN)make consumer$(NC)       - Run stream processor"
	@echo ""
	@echo "$(YELLOW)Infrastructure:$(NC)"
	@echo "  $(GREEN)make infra-down$(NC)     - Stop all services"
	@echo "  $(GREEN)make status$(NC)         - Check service status"
	@echo "  $(GREEN)make logs$(NC)           - View service logs"
	@echo ""
	@echo "$(YELLOW)Database:$(NC)"
	@echo "  $(GREEN)make db-shell$(NC)       - PostgreSQL shell"
	@echo "  $(GREEN)make db-latest$(NC)      - Show latest metrics"
	@echo ""
	@echo "$(YELLOW)Cleanup:$(NC)"
	@echo "  $(GREEN)make clean$(NC)          - Clean checkpoints & logs"
	@echo "  $(GREEN)make reset$(NC)          - Full reset"
	@echo ""
	@echo "$(YELLOW)Monitoring:$(NC)"
	@echo "  Kafka UI  : http://localhost:8090"
	@echo "  Grafana   : http://localhost:3000 (admin/admin)"
	@echo "  Spark UI  : http://localhost:4040"

# ==================================================
# Setup
# ==================================================
setup:
	@echo "$(BLUE)Setting up project...$(NC)"
	@mkdir -p $(CHECKPOINT_DIR) $(LOGS_DIR)
	@pip install -r requirements.txt
	@echo "$(GREEN)✓ Setup complete$(NC)"

# ==================================================
# Infrastructure
# ==================================================
infra-up:
	@echo "$(BLUE)Starting infrastructure...$(NC)"
	@$(DOCKER_COMPOSE) up -d
	@echo "$(YELLOW)Waiting for services...$(NC)"
	@sleep 20
	@./scripts/setup_kafka_topics.sh || true
	@docker exec -i $(POSTGRES_CONTAINER) psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) < infrastructure/postgres/init_metrics.sql || true
	@echo "$(GREEN)✓ Infrastructure ready$(NC)"
	@$(MAKE) status

infra-down:
	@echo "$(BLUE)Stopping infrastructure...$(NC)"
	@$(DOCKER_COMPOSE) down
	@echo "$(GREEN)✓ Infrastructure stopped$(NC)"

status:
	@echo "$(BLUE)Service Status:$(NC)"
	@$(DOCKER_COMPOSE) ps
	@echo ""
	@echo -n "Kafka:      " && (docker exec $(KAFKA_CONTAINER) kafka-broker-api-versions --bootstrap-server localhost:29092 > /dev/null 2>&1 && echo "$(GREEN)✓ Running$(NC)" || echo "$(RED)✗ Down$(NC)")
	@echo -n "PostgreSQL: " && (docker exec $(POSTGRES_CONTAINER) pg_isready -U $(POSTGRES_USER) > /dev/null 2>&1 && echo "$(GREEN)✓ Running$(NC)" || echo "$(RED)✗ Down$(NC)")

logs:
	@$(DOCKER_COMPOSE) logs -f

# ==================================================
# Producer & Consumer
# ==================================================
producer:
	@echo "$(BLUE)Starting event generator...$(NC)"
	@KAFKA_BOOTSTRAP_SERVERS=localhost:9092 $(PYTHON) -m src.producers.event_generator --limit 500 --speed 5

producer-full:
	@echo "$(BLUE)Starting full event generator...$(NC)"
	@KAFKA_BOOTSTRAP_SERVERS=localhost:9092 $(PYTHON) -m src.producers.event_generator

consumer:
	@echo "$(BLUE)Starting stream processor (local mode)...$(NC)"
	@SPARK_MASTER=local[*] KAFKA_BOOTSTRAP_SERVERS=localhost:9092 $(PYTHON) -m src.consumers.stream_processor

consumer-cluster:
	@echo "$(BLUE)Starting stream processor (cluster mode)...$(NC)"
	@docker exec -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 -e POSTGRES_HOST=postgres_db spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.5.0,org.apache.spark:spark-avro_2.12:3.5.0 \
		--conf spark.driver.host=spark-master \
		--conf spark.driver.bindAddress=0.0.0.0 \
		/opt/spark/apps/run_spark_cluster.py

consumer-bg:
	@echo "$(BLUE)Starting stream processor (background)...$(NC)"
	@mkdir -p $(LOGS_DIR)
	@KAFKA_BOOTSTRAP_SERVERS=localhost:9092 $(PYTHON) -m src.consumers.stream_processor > $(LOGS_DIR)/consumer.log 2>&1 &
	@sleep 5
	@echo "$(GREEN)✓ Consumer running (logs: $(LOGS_DIR)/consumer.log)$(NC)"

# ==================================================
# Database
# ==================================================
db-shell:
	@docker exec -it $(POSTGRES_CONTAINER) psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)

db-latest:
	@echo "$(BLUE)Latest Metrics:$(NC)"
	@echo ""
	@echo "$(YELLOW)Funnel:$(NC)"
	@docker exec $(POSTGRES_CONTAINER) psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c \
		"SELECT window_start, total_orders, orders_with_items, orders_with_payment, items_conversion_rate, payment_conversion_rate FROM real_time_funnel ORDER BY window_start DESC LIMIT 3;"
	@echo "$(YELLOW)GMV:$(NC)"
	@docker exec $(POSTGRES_CONTAINER) psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c \
		"SELECT window_start, gmv, item_count, unique_orders FROM gmv_metrics ORDER BY window_start DESC LIMIT 3;"
	@echo "$(YELLOW)Drop-off:$(NC)"
	@docker exec $(POSTGRES_CONTAINER) psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c \
		"SELECT window_start, order_status, dropped_orders, alert_triggered FROM drop_off_analysis ORDER BY window_start DESC LIMIT 3;"
	@echo "$(YELLOW)Payments:$(NC)"
	@docker exec $(POSTGRES_CONTAINER) psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c \
		"SELECT window_start, payment_type, transaction_count, total_payment_value FROM payment_metrics ORDER BY window_start DESC LIMIT 3;"

db-complete:
	@echo "$(BLUE)Complete Funnel View (JOIN with GMV & Payments):$(NC)"
	@docker exec $(POSTGRES_CONTAINER) psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c \
		"SELECT * FROM v_complete_funnel ORDER BY window_start DESC LIMIT 5;"

db-truncate:
	@docker exec -i $(POSTGRES_CONTAINER) psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c \
		"TRUNCATE TABLE real_time_funnel, gmv_metrics, drop_off_analysis, payment_metrics RESTART IDENTITY;"
	@echo "$(GREEN)✓ Tables truncated$(NC)"

# ==================================================
# Kafka
# ==================================================
kafka-topics:
	@docker exec $(KAFKA_CONTAINER) kafka-topics --list --bootstrap-server localhost:29092

kafka-consume:
	@docker exec -it $(KAFKA_CONTAINER) kafka-console-consumer --bootstrap-server localhost:29092 --topic olist.orders --from-beginning

# ==================================================
# Cleanup
# ==================================================
clean:
	@echo "$(YELLOW)Cleaning checkpoints and logs...$(NC)"
	@rm -rf $(CHECKPOINT_DIR)/* $(LOGS_DIR)/*
	@mkdir -p $(CHECKPOINT_DIR) $(LOGS_DIR)
	@echo "$(GREEN)✓ Cleaned$(NC)"

reset: clean infra-down
	@$(DOCKER_COMPOSE) down -v
	@echo "$(GREEN)✓ Full reset complete$(NC)"

# ==================================================
# Demo (End-to-End)
# ==================================================
demo:
	@echo "$(BLUE)Running Pipeline Demo...$(NC)"
	@$(MAKE) infra-up
	@$(MAKE) clean
	@$(MAKE) db-truncate
	@$(MAKE) consumer-bg
	@$(MAKE) producer
	@sleep 30
	@$(MAKE) db-latest
	@echo "$(GREEN)✓ Demo complete$(NC)"
