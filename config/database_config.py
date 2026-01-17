import os
from typing import Dict


class DatabaseConfig:
    
    #inisialisasi konfigurasi database dari environment variables
    DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
    DB_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
    DB_NAME = os.getenv("POSTGRES_DB", "ecommerce_metrics")
    DB_USER = os.getenv("POSTGRES_USER", "ecommerce")
    DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "ecommerce123")
    
    JDBC_PROPERTIES = {
        "user": DB_USER,
        "password": DB_PASSWORD,
        "driver": "org.postgresql.Driver",
        "stringtype": "unspecified"
    }
    
    @classmethod
    def get_jdbc_url(cls) -> str:
        return f"jdbc:postgresql://{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"
    
    @classmethod
    def get_connection_params(cls) -> Dict:
        return {
            "host": cls.DB_HOST,
            "port": cls.DB_PORT,
            "database": cls.DB_NAME,
            "user": cls.DB_USER,
            "password": cls.DB_PASSWORD
        }