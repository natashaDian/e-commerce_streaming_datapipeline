from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.utils.logger import setup_logger


class PaymentProcessor:

    def __init__(self, window_duration: str = "1 minute", slide_duration: str = "30 seconds"):
        self.window_duration = window_duration
        self.slide_duration = slide_duration
        self.logger = setup_logger("PaymentProcessor")

    def process(
        self, 
        orders_df: DataFrame, 
        payments_df: DataFrame
    ) -> DataFrame:

        self.logger.info("Processing Payment Analytics...")

        enriched_payments = payments_df \
            .dropDuplicates(["event_id"]) \
            .withColumn(
                "is_successful",
                when(
                    (col("payment_value").isNotNull()) & (col("payment_value") > 0),
                    True
                ).otherwise(False)
            )

        result = enriched_payments \
            .groupBy(
                window("event_timestamp", self.window_duration, self.slide_duration),
                "payment_type"
            ).agg(
                count("*").alias("transaction_count"),
                approx_count_distinct("order_id").alias("unique_orders"),
                
                sum(coalesce(col("payment_value"), lit(0.0))).alias("total_payment_value"),
                avg(coalesce(col("payment_value"), lit(0.0))).alias("avg_payment_value_calc"),
                
                sum(when(col("is_successful"), 1).otherwise(0)).alias("successful_orders"),
                sum(when(~col("is_successful"), 1).otherwise(0)).alias("failed_orders"),
                
                avg(coalesce(col("payment_installments"), lit(1))).alias("avg_installments"),
                max(coalesce(col("payment_installments"), lit(1))).alias("max_installments")
            ).select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("payment_type"),
                col("transaction_count"),
                col("unique_orders"),
                round(col("total_payment_value"), 2).alias("total_payment_value"),
                round(col("avg_payment_value_calc"), 2).alias("avg_payment_value"),
                round(col("avg_installments"), 2).alias("avg_installments"),
                col("max_installments"),
                col("successful_orders"),
                col("failed_orders"),
                
                when(col("transaction_count") > 0,
                    round((col("successful_orders") / col("transaction_count")) * 100, 2)
                ).otherwise(0.0).alias("success_rate"),
                
                current_timestamp().alias("processed_at")
            )

        self.logger.info("Payment Analytics complete")
        return result