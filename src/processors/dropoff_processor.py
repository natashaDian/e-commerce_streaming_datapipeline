from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.utils.logger import setup_logger


class DropOffProcessor:
    def __init__(
        self, 
        window_duration: str = "1 minute",
        slide_duration: str = "30 seconds",
        alert_threshold: int = 5
    ):
        self.window_duration = window_duration
        self.slide_duration = slide_duration
        self.alert_threshold = alert_threshold
        self.logger = setup_logger("DropOffProcessor")
        
        self.dropoff_statuses = ["canceled", "unavailable", "created"]
        self.success_statuses = ["delivered", "shipped", "invoiced", "processing"]

    def process(
        self,
        orders_df: DataFrame,
        payments_df: DataFrame
    ) -> DataFrame:
        
        self.logger.info("Processing Drop-Off Detection...")

        status_counts = orders_df \
            .dropDuplicates(["event_id"]) \
            .withColumn(
                "is_dropoff",
                when(col("order_status").isin(self.dropoff_statuses), True).otherwise(False)
            ) \
            .withColumn(
                "is_success", 
                when(col("order_status").isin(self.success_statuses), True).otherwise(False)
            )

        result = status_counts \
            .groupBy(
                window("event_timestamp", self.window_duration, self.slide_duration),
                "order_status"
            ).agg(
                count("*").alias("status_count"),
                approx_count_distinct("customer_id").alias("unique_customers_affected"),
                first("is_dropoff").alias("is_dropoff_status"),
                collect_list(col("order_id")).alias("order_ids_list")
            ).select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("order_status"),
                
                when(col("is_dropoff_status"), col("status_count"))
                    .otherwise(lit(0)).alias("dropped_orders"),
                
                col("unique_customers_affected"),
                
                col("status_count").cast("double").alias("drop_rate"),
                
                when(
                    (col("is_dropoff_status")) & (col("status_count") > self.alert_threshold),
                    True
                ).otherwise(False).alias("alert_triggered"),
                
                slice(col("order_ids_list"), 1, 3).alias("sample_order_ids"),
                
                current_timestamp().alias("detected_at")
            )

        self.logger.info("Drop-Off Detection complete")
        return result