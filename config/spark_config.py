import os
from pathlib import Path
from typing import Dict


class SparkConfig:
    
    APP_NAME = "EcommerceStreamingPipeline"
    MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
    CHECKPOINT_DIR = str(Path("./checkpoint").absolute())
    
    DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "2g")
    EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "2g")
    
    SHUFFLE_PARTITIONS = 3
    
    KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    POSTGRES_DRIVER = "org.postgresql:postgresql:42.5.0"
    AVRO_PACKAGE = "org.apache.spark:spark-avro_2.12:3.5.0"
    
    SPARK_CONFIGS = {
        "spark.app.name": APP_NAME,
        "spark.master": MASTER,
        "spark.driver.memory": DRIVER_MEMORY,
        "spark.executor.memory": EXECUTOR_MEMORY,
        "spark.sql.shuffle.partitions": str(SHUFFLE_PARTITIONS),
        "spark.sql.streaming.checkpointLocation": CHECKPOINT_DIR,
        "spark.streaming.stopGracefullyOnShutdown": "true",
        "spark.sql.adaptive.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.ui.showConsoleProgress": "true",
        "spark.network.timeout": "800s"
    }
    
    @classmethod
    def get_checkpoint_location(cls, query_name: str) -> str:
        path = Path(cls.CHECKPOINT_DIR) / query_name
        path.mkdir(parents=True, exist_ok=True)
        return str(path.absolute())
    
    @classmethod
    def get_spark_session_config(cls) -> Dict[str, str]:
        return cls.SPARK_CONFIGS.copy()
    
    @classmethod
    def get_packages(cls) -> str:
        return ",".join([cls.KAFKA_PACKAGE, cls.POSTGRES_DRIVER, cls.AVRO_PACKAGE])