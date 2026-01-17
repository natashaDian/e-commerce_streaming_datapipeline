#!/usr/bin/env python3
import sys
import os
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def print_header(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

def check_kafka():
    print_header("1. KAFKA CONNECTIVITY")
    try:
        from kafka import KafkaProducer, KafkaConsumer
        from kafka.admin import KafkaAdminClient
        
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        print("  ✅ Kafka Producer: Connected")
        producer.close()
        
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092')
        print("  ✅ Kafka Consumer: Connected")
        consumer.close()
        
        admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
        topics = admin.list_topics()
        expected_topics = ['olist.orders', 'olist.order_items', 'olist.payments']
        
        for topic in expected_topics:
            if topic in topics:
                print(f"  ✅ Topic '{topic}': Exists")
            else:
                print(f"  ❌ Topic '{topic}': Missing")
        
        admin.close()
        return True
    except Exception as e:
        print(f"  ❌ Kafka Error: {e}")
        return False

def check_postgres():
    print_header("2. POSTGRESQL CONNECTIVITY")
    try:
        import psycopg2
        from config.database_config import DatabaseConfig
        
        params = DatabaseConfig.get_connection_params()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        print(f"  ✅ PostgreSQL: Connected to {params['database']}")
        
        tables = ['real_time_funnel', 'gmv_metrics', 'drop_off_analysis', 'payment_metrics']
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            status = "✅" if count > 0 else "⚠️"
            print(f"  {status} Table '{table}': {count} rows")
        
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"  ❌ PostgreSQL Error: {e}")
        return False

def check_schema_registry():
    print_header("3. SCHEMA REGISTRY")
    try:
        import requests
        response = requests.get("http://localhost:8085/subjects", timeout=5)
        if response.status_code == 200:
            subjects = response.json()
            print(f"  ✅ Schema Registry: Connected (port 8085)")
            print(f"  ℹ️  Registered schemas: {len(subjects)}")
            for subj in subjects[:5]:
                print(f"      - {subj}")
            return True
        else:
            print(f"  ❌ Schema Registry: HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"  ⚠️ Schema Registry: Not available ({e})")
        return False

def check_prometheus():
    print_header("4. PROMETHEUS")
    try:
        import requests
        response = requests.get("http://localhost:9090/-/ready", timeout=5)
        if response.status_code == 200:
            print("  ✅ Prometheus: Ready")
            return True
        else:
            print(f"  ❌ Prometheus: HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"  ⚠️ Prometheus: Not available ({e})")
        return False

def check_grafana():
    print_header("5. GRAFANA")
    try:
        import requests
        response = requests.get("http://localhost:3000/api/health", timeout=5)
        if response.status_code == 200:
            print("  ✅ Grafana: Healthy")
            return True
        else:
            print(f"  ❌ Grafana: HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"  ⚠️ Grafana: Not available ({e})")
        return False

def check_metrics_data():
    print_header("6. METRICS VALIDATION")
    try:
        import psycopg2
        from config.database_config import DatabaseConfig
        
        params = DatabaseConfig.get_connection_params()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT 
                COUNT(*) as count,
                AVG(items_conversion_rate) as avg_items_conv,
                AVG(payment_conversion_rate) as avg_payment_conv
            FROM real_time_funnel
            WHERE window_start >= NOW() - INTERVAL '1 hour'
        """)
        row = cur.fetchone()
        if row[0] > 0:
            print(f"  ✅ Funnel Metrics (last hour):")
            print(f"      Windows: {row[0]}")
            print(f"      Avg Items Conversion: {row[1]:.2f}%" if row[1] else "      Avg Items Conversion: N/A")
            print(f"      Avg Payment Conversion: {row[2]:.2f}%" if row[2] else "      Avg Payment Conversion: N/A")
        else:
            print("  ⚠️ No funnel metrics in last hour")
        
        cur.execute("""
            SELECT 
                COUNT(*) as count,
                SUM(gmv) as total_gmv,
                AVG(gmv) as avg_gmv
            FROM gmv_metrics
            WHERE window_start >= NOW() - INTERVAL '1 hour'
        """)
        row = cur.fetchone()
        if row[0] > 0:
            print(f"  ✅ GMV Metrics (last hour):")
            print(f"      Windows: {row[0]}")
            print(f"      Total GMV: R$ {row[1]:,.2f}" if row[1] else "      Total GMV: N/A")
        else:
            print("  ⚠️ No GMV metrics in last hour")
        
        cur.execute("""
            SELECT COUNT(*) 
            FROM drop_off_analysis 
            WHERE alert_triggered = true 
            AND window_start >= NOW() - INTERVAL '1 hour'
        """)
        alert_count = cur.fetchone()[0]
        if alert_count > 0:
            print(f"  🚨 Active Alerts: {alert_count}")
        else:
            print(f"  ✅ No active alerts")
        
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"  ❌ Metrics Validation Error: {e}")
        return False

def main():
    print("\n" + "="*60)
    print("   E-COMMERCE STREAMING PIPELINE VALIDATION")
    print("   " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("="*60)
    
    results = {
        "Kafka": check_kafka(),
        "PostgreSQL": check_postgres(),
        "Schema Registry": check_schema_registry(),
        "Prometheus": check_prometheus(),
        "Grafana": check_grafana(),
        "Metrics": check_metrics_data(),
    }
    
    print_header("SUMMARY")
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for name, status in results.items():
        icon = "✅" if status else "❌"
        print(f"  {icon} {name}")
    
    print(f"\n  Result: {passed}/{total} checks passed")
    
    if passed == total:
        print("\n  🎉 All systems operational!")
        return 0
    else:
        print("\n  ⚠️ Some components need attention")
        return 1

if __name__ == "__main__":
    sys.exit(main())