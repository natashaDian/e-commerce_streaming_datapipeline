import os
from dataclasses import dataclass
from typing import Dict, List


@dataclass
class KafkaTopicConfig:
    name: str
    partitions: int = 3
    replication_factor: int = 1


class KafkaConfig:
    
    BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    #topics yang dibutuhkan
    TOPICS = {
        "orders": KafkaTopicConfig(name="olist.orders"),
        "order_items": KafkaTopicConfig(name="olist.order_items"),
        "payments": KafkaTopicConfig(name="olist.payments")
    }
    
    #ekstensi konfigurasi producer dan consumer kafka
    PRODUCER_CONFIG = {
        "bootstrap_servers": BOOTSTRAP_SERVERS,
        "acks": "all",
        "retries": 3,
        "compression_type": "gzip",
        "linger_ms": 10,
        "batch_size": 16384
    }
    
    CONSUMER_CONFIG = {
        "bootstrap_servers": BOOTSTRAP_SERVERS,
        "group_id": os.getenv("KAFKA_CONSUMER_GROUP", "ecommerce-streaming-consumer"),
        "auto_offset_reset": "earliest",
        "enable_auto_commit": False
    }
    
    @classmethod
    def get_topic_config(cls, topic_key: str) -> KafkaTopicConfig:
        if topic_key not in cls.TOPICS:
            raise ValueError(f"Unknown topic: {topic_key}")
        return cls.TOPICS[topic_key]
    
    @classmethod
    def get_all_topic_names(cls) -> List[str]:
        return [t.name for t in cls.TOPICS.values()]
    
    @classmethod
    def get_producer_config(cls, **overrides) -> Dict:
        config = cls.PRODUCER_CONFIG.copy()
        config.update(overrides)
        return config
    
    @classmethod
    def get_consumer_config(cls, **overrides) -> Dict:
        config = cls.CONSUMER_CONFIG.copy()
        config.update(overrides)
        return config