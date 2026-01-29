from dataclasses import dataclass
from typing import Dict

@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    host: str
    port: int
    database: str
    user: str
    password: str
    
    def get_connection_string(self) -> str:
        """Returns PostgreSQL connection string"""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    def get_psycopg2_params(self) -> Dict:
        """Returns parameters for psycopg2.connect()"""
        return {
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'user': self.user,
            'password': self.password
        }

# Database configurations
# Note: In production, use Airflow Connections or environment variables
DB_CONFIGS = {
    'operational': DatabaseConfig(
        host='postgres-operational',  # Docker service name
        port=5432,
        database='operational',
        user='postgres',
        password='postgres'
    ),
    'mrr': DatabaseConfig(
        host='postgres-mrr',  # Docker service name
        port=5432,  # Internal port (container-to-container)
        database='mrr',
        user='postgres',
        password='postgres'
    ),
    'stg': DatabaseConfig(
        host='postgres-stg',  # Docker service name
        port=5432,
        database='stg',
        user='postgres',
        password='postgres'
    ),
    'dwh': DatabaseConfig(
        host='postgres-dwh',  # Docker service name
        port=5432,
        database='dwh',
        user='postgres',
        password='postgres'
    )
}

# ETL Configuration
ETL_CONFIG = {
    'default_batch_size': 1000,
    'enable_logging': True,
    'enable_high_water_mark': True,
    'enable_idempotency': True
}
