"""
Event producers module
Handles event generation and Kafka publishing
"""

from src.producers.base_producer import BaseProducer
from src.producers.event_generator import EcommerceEventGenerator

__all__ = [
    'BaseProducer',
    'EcommerceEventGenerator'
]
