ORDER_SCHEMA = {
    "type": "record",
    "name": "OrderEvent",
    "namespace": "com.ecommerce.events",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "event_type", "type": "string"},
        {"name": "event_time", "type": "string"},
        {"name": "order_id", "type": "string"},
        {"name": "customer_id", "type": ["null", "string"], "default": None},
        {"name": "order_status", "type": ["null", "string"], "default": None},
        {"name": "order_approved_at", "type": ["null", "string"], "default": None},
        {"name": "estimated_delivery_date", "type": ["null", "string"], "default": None},
        {"name": "timestamp", "type": "string"}
    ]
}

ORDER_ITEM_SCHEMA = {
    "type": "record",
    "name": "OrderItemEvent",
    "namespace": "com.ecommerce.events",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "event_type", "type": "string"},
        {"name": "event_time", "type": "string"},
        {"name": "order_id", "type": "string"},
        {"name": "order_item_id", "type": "int"},
        {"name": "product_id", "type": ["null", "string"], "default": None},
        {"name": "seller_id", "type": ["null", "string"], "default": None},
        {"name": "price", "type": "double"},
        {"name": "freight_value", "type": "double"},
        {"name": "timestamp", "type": "string"}
    ]
}

PAYMENT_SCHEMA = {
    "type": "record",
    "name": "PaymentEvent",
    "namespace": "com.ecommerce.events",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "event_type", "type": "string"},
        {"name": "event_time", "type": "string"},
        {"name": "order_id", "type": "string"},
        {"name": "payment_sequential", "type": "int"},
        {"name": "payment_type", "type": ["null", "string"], "default": None},
        {"name": "payment_installments", "type": "int"},
        {"name": "payment_value", "type": "double"},
        {"name": "timestamp", "type": "string"}
    ]
}

SCHEMAS = {
    "order": ORDER_SCHEMA,
    "order_item": ORDER_ITEM_SCHEMA,
    "payment": PAYMENT_SCHEMA
}


class AvroSchemaRegistry:
    
    @staticmethod
    def get_schema(schema_name: str) -> dict:
        if schema_name not in SCHEMAS:
            raise ValueError(f"Unknown schema: {schema_name}")
        return SCHEMAS[schema_name]
    
    @staticmethod
    def validate_schema(schema_name: str, data: dict) -> bool:
        schema = SCHEMAS.get(schema_name)
        if not schema:
            return False
        required = [f["name"] for f in schema["fields"] if "default" not in f]
        return all(field in data for field in required)