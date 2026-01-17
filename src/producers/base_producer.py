import json
import logging
from typing import Dict, Any, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError

from config.kafka_config import KafkaConfig
from src.schemas.avro_schemas import AvroSchemaRegistry


class BaseProducer:
    
    def __init__(
        self,
        schema_name: Optional[str] = None,
        validate_schema: bool = True,
        **producer_overrides
    ):
        self.schema_name = schema_name
        self.validate_schema = validate_schema
        self.logger = logging.getLogger(self.__class__.__name__)
        
        producer_config = KafkaConfig.get_producer_config(**producer_overrides)
        
        producer_config['value_serializer'] = lambda v: json.dumps(v).encode('utf-8')
        producer_config['key_serializer'] = lambda k: k.encode('utf-8') if k else None
        
        try:
            self.producer = KafkaProducer(**producer_config)
            self.logger.info(f"Producer initialized: {producer_config['bootstrap_servers']}")
        except Exception as e:
            self.logger.error(f"Failed to initialize producer: {e}")
            raise
        
        self.messages_sent = 0
        self.messages_failed = 0
    
    def send(
        self,
        topic: str,
        key: str,
        value: Dict[str, Any],
        headers: Optional[list] = None
    ) -> bool:
        if self.validate_schema and self.schema_name:
            if not AvroSchemaRegistry.validate_schema(self.schema_name, value):
                self.logger.error(f"Schema validation failed for {self.schema_name}")
                self.messages_failed += 1
                return False
        
        try:
            future = self.producer.send(
                topic=topic,
                key=key,
                value=value,
                headers=headers
            )
            
            record_metadata = future.get(timeout=10)
            
            self.messages_sent += 1
            self.logger.debug(
                f"Message sent: topic={record_metadata.topic}, "
                f"partition={record_metadata.partition}, "
                f"offset={record_metadata.offset}"
            )
            return True
            
        except KafkaError as e:
            self.logger.error(f"Kafka error sending message: {e}")
            self.messages_failed += 1
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error sending message: {e}")
            self.messages_failed += 1
            return False
    
    def send_batch(
        self,
        topic: str,
        messages: list,
        key_extractor: callable = None
    ) -> tuple:
        success = 0
        failed = 0
        
        for msg in messages:
            key = key_extractor(msg) if key_extractor else None
            if self.send(topic, key, msg):
                success += 1
            else:
                failed += 1
        
        self.flush()
        return success, failed
    
    def flush(self):
        try:
            self.producer.flush()
            self.logger.debug("Producer flushed")
        except Exception as e:
            self.logger.error(f"Error flushing producer: {e}")
    
    def close(self):
        try:
            self.producer.close()
            self.logger.info(
                f"Producer closed. Sent: {self.messages_sent}, "
                f"Failed: {self.messages_failed}"
            )
        except Exception as e:
            self.logger.error(f"Error closing producer: {e}")
    
    def get_metrics(self) -> Dict[str, int]:
        return {
            "messages_sent": self.messages_sent,
            "messages_failed": self.messages_failed,
            "success_rate": (
                self.messages_sent / (self.messages_sent + self.messages_failed) * 100
                if (self.messages_sent + self.messages_failed) > 0
                else 0
            )
        }
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()