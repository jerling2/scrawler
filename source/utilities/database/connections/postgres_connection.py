from __future__ import annotations
from dataclasses import dataclass
import os
import psycopg2


@dataclass(frozen=True)
class PostgresConnectionConfig:
    """Configuration container for PostgreSQL database connection parameters.
    
    Attributes:
    - host (str): Database host address.
    - port (str): Database port number.
    - user (str): Database username for authentication.
    - password (str): Database password for authentication.
    - database (str): Name of the database to connect to.
    """
    host: str
    port: str
    user: str
    password: str
    database: str

    @classmethod
    def from_env(cls) -> PostgresConnectionConfig:
        return cls(
            host=os.environ['SCRAWLER_POSTGRES_HOST'],
            port=os.environ['SCRAWLER_POSTGRES_PORT'],
            user=os.environ['SCRAWLER_POSTGRES_USER'],
            password=os.environ['SCRAWLER_POSTGRES_PASS'],
            database=os.environ['SCRAWLER_POSTGRES_DATABASE'],
        )

    @property
    def dsn(self) -> str:
        """Constructs the PostgreSQL Data Source Name (DSN) connection string.
        
        Returns:
        - str: Formatted DSN string for psycopg2 connection.
        """
        return (
            f'postgresql://{self.user}:{self.password}'
            f'@{self.host}:{self.port}/{self.database}'
        )


class PostgresConnection:

    def __init__(self, config = PostgresConnectionConfig.from_env()):
        self.config = config
        self.client = None

    @property
    def is_closed(self) -> bool:
        return self.client is None or self.client.closed

    def connect(self):
        self.client = psycopg2.connect(self.config.dsn)

    def close(self):
        if self.is_closed:
            # Warning: Connection is already closed or was never established
            return
        self.client.close()
