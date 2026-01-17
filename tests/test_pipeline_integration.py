# tests/test_pipeline_integration.py
"""
Integration test untuk memastikan semua metrics masuk ke database
"""
import psycopg2
import time
from datetime import datetime, timedelta
from typing import Dict, List


class PipelineValidator:
    """Validator untuk streaming pipeline"""
    
    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.conn = None
        
    def connect(self):
        """Connect to PostgreSQL"""
        self.conn = psycopg2.connect(**self.db_config)
        print("✓ Connected to PostgreSQL")
    
    def close(self):
        """Close connection"""
        if self.conn:
            self.conn.close()
            print("✓ Connection closed")
    
    def check_table_exists(self, table_name: str) -> bool:
        """Check if table exists"""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                )
            """, (table_name,))
            return cur.fetchone()[0]
    
    def get_row_count(self, table_name: str, minutes: int = 60) -> int:
        """Get row count for recent data"""
        with self.conn.cursor() as cur:
            cur.execute(f"""
                SELECT COUNT(*) 
                FROM {table_name}
                WHERE window_start >= NOW() - INTERVAL '{minutes} minutes'
            """)
            return cur.fetchone()[0]
    
    def get_latest_window(self, table_name: str) -> dict:
        """Get latest window data"""
        with self.conn.cursor() as cur:
            cur.execute(f"""
                SELECT window_start, window_end
                FROM {table_name}
                ORDER BY window_start DESC
                LIMIT 1
            """)
            row = cur.fetchone()
            if row:
                return {
                    "window_start": row[0],
                    "window_end": row[1]
                }
            return None
    
    def validate_funnel_metrics(self) -> Dict:
        """Validate funnel metrics"""
        print("\n🔍 Validating Funnel Metrics...")
        
        results = {}
        
        with self.conn.cursor() as cur:
            # Check latest data
            cur.execute("""
                SELECT 
                    window_start,
                    total_orders,
                    orders_with_items,
                    orders_with_payment,
                    items_conversion_rate,
                    payment_conversion_rate,
                    total_gmv,
                    total_payment
                FROM real_time_funnel
                WHERE window_start >= NOW() - INTERVAL '1 hour'
                ORDER BY window_start DESC
                LIMIT 5
            """)
            
            rows = cur.fetchall()
            results['row_count'] = len(rows)
            
            if rows:
                latest = rows[0]
                print(f"  ✓ Latest window: {latest[0]}")
                print(f"  ✓ Total orders: {latest[1]}")
                print(f"  ✓ With items: {latest[2]} ({latest[4]}%)")
                print(f"  ✓ With payment: {latest[3]} ({latest[5]}%)")
                print(f"  ✓ GMV: ${latest[6]:,.2f}")
                
                # Validation checks
                results['valid'] = True
                if latest[1] == 0:
                    print("  ⚠️  WARNING: No orders in latest window")
                    results['valid'] = False
                    
                if latest[6] == 0:
                    print("  ⚠️  WARNING: GMV is zero")
                    results['valid'] = False
            else:
                print("  ❌ No data found in last hour")
                results['valid'] = False
        
        return results
    
    def validate_gmv_metrics(self) -> Dict:
        """Validate GMV metrics"""
        print("\n🔍 Validating GMV Metrics...")
        
        results = {}
        
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    window_start,
                    gmv,
                    item_count,
                    unique_orders,
                    avg_item_price
                FROM gmv_metrics
                WHERE window_start >= NOW() - INTERVAL '1 hour'
                ORDER BY window_start DESC
                LIMIT 5
            """)
            
            rows = cur.fetchall()
            results['row_count'] = len(rows)
            
            if rows:
                latest = rows[0]
                print(f"  ✓ Latest window: {latest[0]}")
                print(f"  ✓ GMV: ${latest[1]:,.2f}")
                print(f"  ✓ Items: {latest[2]}")
                print(f"  ✓ Orders: {latest[3]}")
                print(f"  ✓ Avg price: ${latest[4]:,.2f}")
                
                results['valid'] = latest[1] > 0
            else:
                print("  ❌ No data found")
                results['valid'] = False
        
        return results
    
    def validate_dropoff_metrics(self) -> Dict:
        """Validate drop-off metrics"""
        print("\n🔍 Validating Drop-off Analysis...")
        
        results = {}
        
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    window_start,
                    order_status,
                    dropped_orders,
                    unique_customers_affected,
                    alert_triggered
                FROM drop_off_analysis
                WHERE window_start >= NOW() - INTERVAL '1 hour'
                ORDER BY dropped_orders DESC
                LIMIT 5
            """)
            
            rows = cur.fetchall()
            results['row_count'] = len(rows)
            
            if rows:
                print(f"  ✓ Found {len(rows)} drop-off records")
                for row in rows:
                    alert = "🚨" if row[4] else ""
                    print(f"  {alert} {row[1]}: {row[2]} orders dropped")
                
                results['valid'] = True
                results['alerts'] = sum(1 for r in rows if r[4])
            else:
                print("  ⚠️  No drop-offs detected (could be good!)")
                results['valid'] = True  # This is OK
        
        return results
    
    def validate_payment_metrics(self) -> Dict:
        """Validate payment metrics"""
        print("\n🔍 Validating Payment Metrics...")
        
        results = {}
        
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    payment_type,
                    SUM(transaction_count) as transactions,
                    SUM(total_payment_value) as total_value,
                    AVG(success_rate) as avg_success_rate
                FROM payment_metrics
                WHERE window_start >= NOW() - INTERVAL '1 hour'
                GROUP BY payment_type
                ORDER BY total_value DESC
            """)
            
            rows = cur.fetchall()
            results['row_count'] = len(rows)
            
            if rows:
                print(f"  ✓ Payment methods analyzed:")
                for row in rows:
                    print(f"    - {row[0]}: {row[1]} txn, ${row[2]:,.2f}, {row[3]:.1f}% success")
                
                results['valid'] = True
            else:
                print("  ❌ No payment data found")
                results['valid'] = False
        
        return results
    
    def run_full_validation(self) -> bool:
        """Run complete validation suite"""
        print("="*70)
        print("🧪 PIPELINE VALIDATION TEST")
        print("="*70)
        
        all_valid = True
        
        # Check tables exist
        tables = ['real_time_funnel', 'gmv_metrics', 'drop_off_analysis', 'payment_metrics']
        print("\n📋 Checking tables...")
        for table in tables:
            exists = self.check_table_exists(table)
            status = "✓" if exists else "❌"
            print(f"  {status} {table}")
            if not exists:
                all_valid = False
        
        if not all_valid:
            print("\n❌ Some tables are missing! Run schema setup first.")
            return False
        
        # Validate each metric
        funnel = self.validate_funnel_metrics()
        gmv = self.validate_gmv_metrics()
        dropoff = self.validate_dropoff_metrics()
        payment = self.validate_payment_metrics()
        
        # Summary
        print("\n" + "="*70)
        print("📊 VALIDATION SUMMARY")
        print("="*70)
        
        metrics = [
            ("Funnel Metrics", funnel['valid']),
            ("GMV Metrics", gmv['valid']),
            ("Drop-off Analysis", dropoff['valid']),
            ("Payment Metrics", payment['valid'])
        ]
        
        for name, valid in metrics:
            status = "✓" if valid else "❌"
            print(f"  {status} {name}")
        
        all_valid = all(v for _, v in metrics)
        
        if all_valid:
            print("\n✅ ALL VALIDATIONS PASSED!")
        else:
            print("\n⚠️  SOME VALIDATIONS FAILED")
        
        print("="*70)
        
        return all_valid


def main():
    """Main test runner"""
    import os
    
    # Database config (adjust as needed)
    db_config = {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': os.getenv('POSTGRES_PORT', 5432),
        'database': os.getenv('POSTGRES_DB', 'ecommerce_streaming'),
        'user': os.getenv('POSTGRES_USER', 'spark'),
        'password': os.getenv('POSTGRES_PASSWORD', 'spark123')
    }
    
    validator = PipelineValidator(db_config)
    
    try:
        validator.connect()
        
        # Run validation
        success = validator.run_full_validation()
        
        # Exit code
        exit(0 if success else 1)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        exit(1)
    finally:
        validator.close()


if __name__ == "__main__":
    main()