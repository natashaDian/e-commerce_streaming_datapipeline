from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

from config.kafka_config import KafkaConfig
from config.spark_config import SparkConfig
from config.database_config import DatabaseConfig
from src.processors.funnel_processor import FunnelProcessor
from src.processors.gmv_processor import GMVProcessor
from src.processors.dropoff_processor import DropOffProcessor
from src.processors.payment_processor import PaymentProcessor
from src.sinks.metrics_sink import MetricsSink
from src.utils.logger import setup_logger


class StreamProcessor:
    
    def __init__(
        self,
        enable_postgres: bool = True,
        enable_console: bool = True
    ):
        self.logger = setup_logger("StreamProcessor")
        self.enable_postgres = enable_postgres
        self.enable_console = enable_console
        
        self.spark = self._create_spark_session()
        
        self.kafka_config = KafkaConfig()
        self.spark_config = SparkConfig()
        self.db_config = DatabaseConfig()
        
        self.funnel_processor = FunnelProcessor(window_duration="1 minute", slide_duration="30 seconds")
        self.gmv_processor = GMVProcessor(
            window_duration="1 minute",
            slide_duration="30 seconds"
        )
        self.dropoff_processor = DropOffProcessor(window_duration="1 minute", slide_duration="30 seconds")
        self.payment_processor = PaymentProcessor(window_duration="1 minute", slide_duration="30 seconds")
        
        self.logger.info("Stream processor initialized")
    
    def _create_spark_session(self) -> SparkSession:
        builder = SparkSession.builder \
            .appName(SparkConfig.APP_NAME)
        
        for key, value in SparkConfig.get_spark_session_config().items():
            builder = builder.config(key, value)
        
        builder = builder.config("spark.jars.packages", SparkConfig.get_packages())
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        self.logger.info(f"Spark session created: {SparkConfig.APP_NAME}")
        return spark
    
    def _get_schemas(self) -> dict:
        return {
            "order": StructType([
                StructField("event_id", StringType(), False),
                StructField("event_type", StringType(), False),
                StructField("event_time", StringType(), False),
                StructField("order_id", StringType(), False),
                StructField("customer_id", StringType(), True),
                StructField("order_status", StringType(), True),
                StructField("order_approved_at", StringType(), True),
                StructField("estimated_delivery_date", StringType(), True),
                StructField("timestamp", StringType(), True)
            ]),
            "order_item": StructType([
                StructField("event_id", StringType(), False),
                StructField("event_type", StringType(), False),
                StructField("event_time", StringType(), False),
                StructField("order_id", StringType(), False),
                StructField("order_item_id", IntegerType(), True),
                StructField("product_id", StringType(), True),
                StructField("seller_id", StringType(), True),
                StructField("price", DoubleType(), True),
                StructField("freight_value", DoubleType(), True),
                StructField("timestamp", StringType(), True)
            ]),
            "payment": StructType([
                StructField("event_id", StringType(), False),
                StructField("event_type", StringType(), False),
                StructField("event_time", StringType(), False),
                StructField("order_id", StringType(), False),
                StructField("payment_sequential", IntegerType(), True),
                StructField("payment_type", StringType(), True),
                StructField("payment_installments", IntegerType(), True),
                StructField("payment_value", DoubleType(), True),
                StructField("timestamp", StringType(), True)
            ])
        }
    
    def _read_kafka_stream(self, topic_key: str, schema: StructType) -> DataFrame:
        topic_config = self.kafka_config.get_topic_config(topic_key)
        topic_name = topic_config.name
        
        self.logger.info(f"Reading stream from topic: {topic_name}")
        
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_config.BOOTSTRAP_SERVERS) \
            .option("subscribe", topic_name) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", "10000") \
            .load()
        
        parsed_df = df.select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("raw_value"),
            col("timestamp").alias("kafka_timestamp"),
            col("partition"),
            col("offset")
        ).withColumn(
            "parsed_data",
            from_json(col("raw_value"), schema)
        ).filter(
            col("parsed_data").isNotNull()
        )
        
        result_df = parsed_df.select(
            "kafka_key",
            "parsed_data.*",
            "kafka_timestamp",
            "partition",
            "offset"
        ).withColumn(
            "event_timestamp",
            col("kafka_timestamp")
        )
        
        self.logger.debug(f"Stream from {topic_name} configured (no watermark)")
        return result_df
    
    def _deduplicate(self, df: DataFrame) -> DataFrame:
        return df.dropDuplicates(["event_id"])
    
    def run(self):
        schemas = self._get_schemas()
        
        self.logger.info("Reading Kafka streams...")
        orders_df = self._deduplicate(
            self._read_kafka_stream("orders", schemas["order"])
        )
        items_df = self._deduplicate(
            self._read_kafka_stream("order_items", schemas["order_item"])
        )
        payments_df = self._deduplicate(
            self._read_kafka_stream("payments", schemas["payment"])
        )
        
        self.logger.info("Processing metrics...")
        
        funnel_metrics = self.funnel_processor.process(
            orders_df,
            items_df,
            payments_df
        )
        
        gmv_metrics = self.gmv_processor.process(items_df)
        
        dropoff_metrics = self.dropoff_processor.process(
            orders_df,
            payments_df
        )
        
        payment_metrics = self.payment_processor.process(
            orders_df,
            payments_df
        )
        
        queries = []
        
        funnel_sink = MetricsSink(
            "real_time_funnel",
            enable_console=self.enable_console,
            enable_postgres=self.enable_postgres
        )
        queries.append(
            funnel_sink.write_stream(
                funnel_metrics,
                self.spark_config.get_checkpoint_location("funnel"),
                output_mode="update",
                trigger_interval="30 seconds"
            )
        )
        
        gmv_sink = MetricsSink(
            "gmv_metrics",
            enable_console=self.enable_console,
            enable_postgres=self.enable_postgres
        )
        queries.append(
            gmv_sink.write_stream(
                gmv_metrics,
                self.spark_config.get_checkpoint_location("gmv"),
                output_mode="update",
                trigger_interval="30 seconds"
            )
        )
        
        dropoff_sink = MetricsSink(
            "drop_off_analysis",
            enable_console=self.enable_console,
            enable_postgres=self.enable_postgres
        )
        queries.append(
            dropoff_sink.write_stream(
                dropoff_metrics,
                self.spark_config.get_checkpoint_location("dropoff"),
                output_mode="update",
                trigger_interval="30 seconds"
            )
        )
        
        payment_sink = MetricsSink(
            "payment_metrics",
            enable_console=self.enable_console,
            enable_postgres=self.enable_postgres
        )
        queries.append(
            payment_sink.write_stream(
                payment_metrics,
                self.spark_config.get_checkpoint_location("payments"),
                output_mode="update",
                trigger_interval="30 seconds"
            )
        )
        
        self._print_summary(queries)
        
        try:
            for query in queries:
                query.awaitTermination()
        except KeyboardInterrupt:
            self.logger.info("Stopping queries gracefully...")
            for query in queries:
                query.stop()
    
    def _print_summary(self, queries: list):
        self.logger.info("=" * 70)
        self.logger.info("🚀 STREAMING PIPELINE STARTED")
        self.logger.info("=" * 70)
        self.logger.info(f"Total Queries: {len(queries)}")
        self.logger.info("")
        self.logger.info("Running Queries:")
        for query in queries:
            self.logger.info(f"  - {query.name}")
        self.logger.info("")
        self.logger.info("Monitoring:")
        self.logger.info(f"  - Kafka UI: http://localhost:8090")
        self.logger.info(f"  - Grafana: http://localhost:3000")
        self.logger.info(f"  - Spark UI: http://localhost:4040")
        self.logger.info("")
        self.logger.info("Features:")
        self.logger.info("  ✓ Real-time Funnel Analysis")
        self.logger.info("  ✓ GMV Tracking")
        self.logger.info("  ✓ Drop-off Detection")
        self.logger.info("  ✓ Payment Analytics")
        self.logger.info("")
        self.logger.info("Press Ctrl+C to stop")
        self.logger.info("=" * 70)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='E-commerce Stream Processor'
    )
    parser.add_argument(
        '--no-postgres',
        action='store_true',
        help='Disable PostgreSQL writes'
    )
    parser.add_argument(
        '--no-console',
        action='store_true',
        help='Disable console output'
    )
    
    args = parser.parse_args()
    
    processor = StreamProcessor(
        enable_postgres=not args.no_postgres,
        enable_console=not args.no_console
    )
    
    try:
        processor.run()
    except Exception as e:
        print(f"\nError: {e}")
        raise


if __name__ == "__main__":
    main()