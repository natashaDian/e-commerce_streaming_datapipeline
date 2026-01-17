from pyspark.sql import DataFrame
from config.database_config import DatabaseConfig
from src.utils.logger import setup_logger


class MetricsSink:
    
    def __init__(self, table_name: str, enable_console: bool = True, enable_postgres: bool = True):
        self.table_name = table_name
        self.enable_console = enable_console
        self.enable_postgres = enable_postgres
        self.logger = setup_logger(f"Sink.{table_name}")
        
        self.db_config = DatabaseConfig()
        self.jdbc_url = self.db_config.get_jdbc_url()
        self.jdbc_props = self.db_config.JDBC_PROPERTIES.copy()
    
    def write_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        
        row_count = batch_df.count()
        
        if self.enable_console:
            self.logger.info(f"[{self.table_name}] Batch {batch_id}: {row_count} rows")
            batch_df.show(5, truncate=False)
        
        if self.enable_postgres:
            try:
                batch_df.write.jdbc(
                    url=self.jdbc_url,
                    table=self.table_name,
                    mode="append",
                    properties=self.jdbc_props
                )
                self.logger.info(f"✓ Written to {self.table_name}")
            except Exception as e:
                if "duplicate key" in str(e).lower():
                    self.logger.warning(f"⚠ Duplicate key (skipped)")
                else:
                    self.logger.error(f"❌ Write failed: {e}")
    
    def write_stream(self, streaming_df: DataFrame, checkpoint_location: str,
                     output_mode: str = "append", trigger_interval: str = "30 seconds"):
        return streaming_df.writeStream \
            .foreachBatch(self.write_batch) \
            .outputMode(output_mode) \
            .option("checkpointLocation", checkpoint_location) \
            .trigger(processingTime=trigger_interval) \
            .queryName(f"sink_{self.table_name}") \
            .start()