import pandas as pd
import time
from datetime import datetime
from pathlib import Path
import logging

from src.producers.base_producer import BaseProducer
from config.kafka_config import KafkaConfig
from src.utils.logger import setup_logger


class EcommerceEventGenerator:
    
    def __init__(
        self,
        dataset_dir: str = "dataset",
        speed_multiplier: int = 1,
        enable_schema_validation: bool = True
    ):
        self.dataset_dir = Path(dataset_dir)
        self.speed_multiplier = speed_multiplier
        self.logger = setup_logger(self.__class__.__name__)
        
        self.producers = {
            "orders": BaseProducer(
                schema_name="order",
                validate_schema=enable_schema_validation
            ),
            "order_items": BaseProducer(
                schema_name="order_item",
                validate_schema=enable_schema_validation
            ),
            "payments": BaseProducer(
                schema_name="payment",
                validate_schema=enable_schema_validation
            )
        }
        
        self.topics = {
            "orders": KafkaConfig.get_topic_config("orders").name,
            "order_items": KafkaConfig.get_topic_config("order_items").name,
            "payments": KafkaConfig.get_topic_config("payments").name
        }
        
        self.logger.info(f"Event generator initialized with speed: {speed_multiplier}x")
    
    def load_datasets(self) -> None:
        self.logger.info("Loading datasets...")
        
        try:
            self.orders = pd.read_csv(
                self.dataset_dir / "olist_orders_dataset.csv"
            )
            self.order_items = pd.read_csv(
                self.dataset_dir / "olist_order_items_dataset.csv"
            )
            self.payments = pd.read_csv(
                self.dataset_dir / "olist_order_payments_dataset.csv"
            )
            
            self.customers = pd.read_csv(
                self.dataset_dir / "olist_customers_dataset.csv"
            )
            self.products = pd.read_csv(
                self.dataset_dir / "olist_products_dataset.csv"
            )
            self.sellers = pd.read_csv(
                self.dataset_dir / "olist_sellers_dataset.csv"
            )
            
            self.orders['order_purchase_timestamp'] = pd.to_datetime(
                self.orders['order_purchase_timestamp']
            )
            self.orders = self.orders.sort_values('order_purchase_timestamp')
            
            self.logger.info(f"Loaded {len(self.orders)} orders")
            self.logger.info(f"Loaded {len(self.order_items)} order items")
            self.logger.info(f"Loaded {len(self.payments)} payments")
            
        except FileNotFoundError as e:
            self.logger.error(f"Dataset file not found: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error loading datasets: {e}")
            raise
    
    def create_order_event(self, order_row: pd.Series) -> dict:
        return {
            'event_id': f"order_{order_row['order_id']}_{int(time.time()*1000)}",
            'event_type': 'order_created',
            'event_time': order_row['order_purchase_timestamp'].isoformat(),
            'order_id': order_row['order_id'],
            'customer_id': order_row['customer_id'],
            'order_status': order_row['order_status'],
            'order_approved_at': (
                str(order_row['order_approved_at']) 
                if pd.notna(order_row['order_approved_at']) 
                else None
            ),
            'estimated_delivery_date': (
                str(order_row['order_estimated_delivery_date'])
                if pd.notna(order_row['order_estimated_delivery_date']) 
                else None
            ),
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def create_order_item_events(self, order_id: str, order_time: datetime) -> list:
        items = self.order_items[self.order_items['order_id'] == order_id]
        events = []

        for _, item in items.iterrows():
            event = {
                'event_id': f"item_{order_id}_{item['order_item_id']}_{int(time.time()*1000)}",
                'event_type': 'item_added',
                'event_time': order_time.isoformat(),
                'order_id': order_id,
                'order_item_id': int(item['order_item_id']),
                'product_id': item['product_id'],
                'seller_id': item['seller_id'],
                'price': float(item['price']),
                'freight_value': float(item['freight_value']),
                'timestamp': datetime.utcnow().isoformat()
            }
            events.append(event)

        return events

    
    def create_payment_events(self, order_id: str, order_time: datetime) -> list:
        payments = self.payments[self.payments['order_id'] == order_id]
        events = []

        for _, payment in payments.iterrows():
            event = {
                'event_id': f"payment_{order_id}_{payment['payment_sequential']}_{int(time.time()*1000)}",
                'event_type': 'payment_processed',
                'event_time': order_time.isoformat(),
                'order_id': order_id,
                'payment_sequential': int(payment['payment_sequential']),
                'payment_type': payment['payment_type'],
                'payment_installments': int(payment['payment_installments']),
                'payment_value': float(payment['payment_value']),
                'timestamp': datetime.utcnow().isoformat()
            }
            events.append(event)

        return events
    
    def replay_events(self, limit: int = None, start_from: int = 0) -> dict:
        self.logger.info("Starting event replay...")
        
        orders_subset = self.orders.iloc[start_from:]
        if limit:
            orders_subset = orders_subset.head(limit)
        
        total_orders = len(orders_subset)
        
        stats = {
            "total_orders": total_orders,
            "orders_sent": 0,
            "items_sent": 0,
            "payments_sent": 0,
            "errors": 0,
            "start_time": time.time()
        }
        
        for idx, (_, order) in enumerate(orders_subset.iterrows(), 1):
            order_id = order['order_id']
            order_time = order['order_purchase_timestamp']

            try:
                order_event = self.create_order_event(order)
                if self.producers["orders"].send(
                    self.topics["orders"],
                    order_id,
                    order_event
                ):
                    stats["orders_sent"] += 1
                    self.logger.info(f"[{idx}/{total_orders}] Order: {order_id}")

                time.sleep(0.1 / self.speed_multiplier)

                item_events = self.create_order_item_events(order_id, order_time)
                for item_event in item_events:
                    if self.producers["order_items"].send(
                        self.topics["order_items"],
                        order_id,
                        item_event
                    ):
                        stats["items_sent"] += 1

                time.sleep(0.1 / self.speed_multiplier)

                payment_events = self.create_payment_events(order_id, order_time)
                for payment_event in payment_events:
                    if self.producers["payments"].send(
                        self.topics["payments"],
                        order_id,
                        payment_event
                    ):
                        stats["payments_sent"] += 1

                time.sleep(0.5 / self.speed_multiplier)

            except Exception as e:
                self.logger.error(f"Error processing order {order_id}: {e}")
                stats["errors"] += 1

                
                for producer in self.producers.values():
                    producer.flush()
                
                stats["end_time"] = time.time()
                stats["duration"] = stats["end_time"] - stats["start_time"]
                
                self._print_statistics(stats)
                return stats
    
    def _print_statistics(self, stats: dict) -> None:
        duration = stats["duration"]
        
        self.logger.info("=" * 60)
        self.logger.info("REPLAY STATISTICS")
        self.logger.info("=" * 60)
        self.logger.info(f"Total Orders Processed: {stats['total_orders']}")
        self.logger.info(f"Orders Sent: {stats['orders_sent']}")
        self.logger.info(f"Items Sent: {stats['items_sent']}")
        self.logger.info(f"Payments Sent: {stats['payments_sent']}")
        self.logger.info(f"Errors: {stats['errors']}")
        self.logger.info(f"Duration: {duration:.2f} seconds")
        self.logger.info(
            f"Throughput: {stats['orders_sent']/duration:.2f} orders/sec"
        )
        self.logger.info("=" * 60)
        
        for name, producer in self.producers.items():
            metrics = producer.get_metrics()
            self.logger.info(
                f"{name.upper()}: Sent={metrics['messages_sent']}, "
                f"Failed={metrics['messages_failed']}, "
                f"Success Rate={metrics['success_rate']:.2f}%"
            )
    
    def close(self) -> None:
        self.logger.info("Closing producers...")
        for producer in self.producers.values():
            producer.close()


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='E-commerce Event Generator'
    )
    parser.add_argument(
        '--dataset-dir',
        type=str,
        default='dataset',
        help='Directory containing CSV files'
    )
    parser.add_argument(
        '--speed',
        type=int,
        default=1,
        help='Speed multiplier (1=normal, 10=10x faster)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Limit number of orders to replay'
    )
    parser.add_argument(
        '--start-from',
        type=int,
        default=0,
        help='Start from order index'
    )
    parser.add_argument(
        '--no-validation',
        action='store_true',
        help='Disable schema validation'
    )
    
    args = parser.parse_args()
    
    generator = EcommerceEventGenerator(
        dataset_dir=args.dataset_dir,
        speed_multiplier=args.speed,
        enable_schema_validation=not args.no_validation
    )
    
    try:
        generator.load_datasets()
        generator.replay_events(
            limit=args.limit,
            start_from=args.start_from
        )
    except KeyboardInterrupt:
        print("\n\nReplay interrupted by user")
    except Exception as e:
        print(f"\nError during replay: {e}")
        raise
    finally:
        generator.close()


if __name__ == "__main__":
    main()