from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from src.utils.logger import setup_logger


class GMVProcessor:

    def __init__(
        self,
        window_duration: str = "1 minute",
        slide_duration: str = "30 seconds"
    ):
        self.window_duration = window_duration
        self.slide_duration = slide_duration
        self.logger = setup_logger("GMVProcessor")

    def process(self, items_df: DataFrame) -> DataFrame:

        self.logger.info("Processing GMV metrics...")

        gmv_metrics = items_df \
            .dropDuplicates(["event_id"]) \
            .withColumn(
                "total_value",
                col("price") + col("freight_value")
            ) \
            .groupBy(
                window(
                    "event_timestamp",
                    self.window_duration,
                    self.slide_duration
                )
            ) \
            .agg(
                sum("total_value").alias("gmv"),
                count("*").alias("item_count"),
                approx_count_distinct("order_id").alias("unique_orders"),
                avg("price").alias("avg_item_price"),
                max("price").alias("max_item_price"),
                min("price").alias("min_item_price")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                round(col("gmv"), 2).alias("gmv"),
                col("item_count"),
                col("unique_orders"),
                round(col("avg_item_price"), 2).alias("avg_item_price"),
                round(col("max_item_price"), 2).alias("max_item_price"),
                round(col("min_item_price"), 2).alias("min_item_price")
            )

        self.logger.info("GMV processing complete")
        return gmv_metrics