from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType, StructField, LongType, DoubleType
from src.utils.logger import setup_logger


class FunnelProcessor:

    def __init__(self, window_duration: str = "1 minute", slide_duration: str = "30 seconds"):
        self.window_duration = window_duration
        self.slide_duration = slide_duration
        self.logger = setup_logger("FunnelProcessor")

    def process(
        self,
        orders_df: DataFrame,
        items_df: DataFrame,
        payments_df: DataFrame
    ) -> DataFrame:
        
        self.logger.info("Processing Real-Time Funnel...")

        result = orders_df \
            .dropDuplicates(["event_id"]) \
            .groupBy(window("event_timestamp", self.window_duration, self.slide_duration)).agg(
                count("*").alias("total_orders"),
                approx_count_distinct("order_id").alias("unique_orders"),
                
                sum(when(col("order_status") != "canceled", 1).otherwise(0)).alias("orders_with_items"),
                
                sum(when(
                    col("order_status").isin("delivered", "shipped", "invoiced", "processing"),
                    1
                ).otherwise(0)).alias("orders_with_payment"),
                
                sum(when(col("order_status") == "canceled", 1).otherwise(0)).alias("canceled_orders"),
                
                sum(when(
                    col("order_status").isin("created", "approved", "unavailable"),
                    1
                ).otherwise(0)).alias("pending_orders")
            ).select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("total_orders"),
                col("orders_with_items"),
                col("orders_with_payment"),
                
                when(col("total_orders") > 0,
                    round((col("orders_with_items") / col("total_orders")) * 100, 2)
                ).otherwise(0.0).alias("items_conversion_rate"),
                
                when(col("orders_with_items") > 0,
                    round((col("orders_with_payment") / col("orders_with_items")) * 100, 2)
                ).otherwise(0.0).alias("payment_conversion_rate"),
                
                col("canceled_orders").alias("dropped_after_order"),
                col("pending_orders").alias("dropped_after_items"),
                
                current_timestamp().alias("processed_at")
            )

        self.logger.info("Real-Time Funnel processing complete")
        return result