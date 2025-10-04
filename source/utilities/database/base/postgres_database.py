"""
This module provides the PSQLDatabase class.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Any, Iterable
from functools import wraps
import re
import os
import psycopg2
from logging import Logger
from source.utilities.database.logging.logger import get_database_logger


logger = get_database_logger()


def log_errors(
    operation_name: str,
    logger_func: Logger=logger,
    reraise: bool = True,
    return_on_error: Optional[Any] = None
):
    """Wraps database operations with error logging and optional exception handling.
    
    Args:
    - operation_name (str): Name of the operation being logged.
    - logger_func (Logger): Logger instance to use. Defaults to module logger.
    - reraise (bool): If True, re-raises exceptions after logging. Defaults to True.
    - return_on_error (Optional[Any]): Value to return if exception occurs and reraise is False.
        Defaults to None.
    
    Returns:
    - callable: Decorated function with error logging capability.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                logger_func.error(
                    f"Error in '{operation_name}' for '{self.config.database}': "
                    f"{type(e).__name__}: {str(e).strip()}"
                )
                if reraise:
                    raise
            return return_on_error
        return wrapper
    return decorator


@dataclass(frozen=True)
class PSQLConfig:
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
    def from_env(cls) -> PSQLConfig:
        """Creates PSQLConfig instance from environment variables.
        
        Reads configuration from SCRAWLER_POSTGRES_* environment variables.
        
        Returns:
        - PSQLConfig: Configuration instance populated from environment.
        
        Raises:
        - ValueError: If any required environment variable is not defined.
        """
        host = os.getenv('SCRAWLER_POSTGRES_HOST', None)
        port = os.getenv('SCRAWLER_POSTGRES_PORT', None)
        user = os.getenv('SCRAWLER_POSTGRES_USER', None)
        password = os.getenv('SCRAWLER_POSTGRES_PASS', None)
        database = os.getenv('SCRAWLER_POSTGRES_DATABASE', None)
        if host is None:
            raise ValueError('SCRAWLER_POSTGRES_HOST is not defined')
        if port is None:
            raise ValueError('SCRAWLER_POSTGRES_PORT is not defined')
        if user is None:
            raise ValueError('SCRAWLER_POSTGRES_USER is not defined')
        if password is None:
            raise ValueError('SCRAWLER_POSTGRES_PASS is not defined')
        if database is None:
            raise ValueError('SCRAWLER_POSTGRES_DATABASE is not defined')
        return cls(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
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


@dataclass(frozen=True)
class SQLStatement:
    """Represents a parameterized SQL statement with validation.
    
    Attributes:
    - sql (str): SQL query string with optional parameter placeholders.
    - vals (Optional[tuple[Any]]): Tuple of values for parameterized query. Defaults to empty
        tuple.
    """
    sql: str
    vals: Optional[tuple[Any]] = ()

    def __post_init__(self):
        """Validates SQL statement format after initialization.
        
        Raises:
        - TypeError: If vals is not a tuple.
        - ValueError: If sql doesn't end with semicolon.
        """
        if not isinstance(self.vals, tuple):
            raise TypeError('vals must be a tuple')
        if not self.sql.endswith(";"):
            raise ValueError('SQL statement must end with semicolon')

    def __str__(self):
        """Returns formatted SQL statement with whitespace normalized.
        
        Returns:
        - str: SQL statement with parameters interpolated and whitespace compressed.
        """
        return re.sub(r'\s+', ' ', self.sql).strip() % self.vals


class PSQLDatabase:
    """Provides interface for PostgreSQL database operations using psycopg2.
    
    Supports connection management, query execution, transactions, and result
    fetching with built-in error logging.
    
    Attributes:
    - config (PSQLConfig): Database configuration parameters.
    - conn (psycopg2.connection): Active database connection or None if closed.
    """

    def __init__(self, config: PSQLConfig = PSQLConfig.from_env()):
        """Initializes PSQLDatabase instance with configuration.
        
        Args:
        - config (PSQLConfig): Database configuration. Defaults to config from environment
            variables.
        """
        self.config = config
        self.conn = None

    def __enter__(self):
        """Establishes database connection for context manager usage.
        
        Returns:
        - PSQLDatabase: Self reference for context manager.
        """
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, traceback):
        """Closes database connection when exiting context manager.
        
        Args:
        - exc_type: Exception type if raised in context.
        - exc_val: Exception value if raised in context.
        - traceback: Traceback object if exception raised in context.
        """
        _ = exc_type, exc_val, traceback
        self.close()

    @property
    def is_closed(self) -> bool:
        """Checks if database connection is closed or uninitialized.
        
        Returns:
        - bool: True if connection is None or closed, False otherwise.
        """
        return self.conn is None or self.conn.closed
            
    @log_errors(operation_name="connect")
    def connect(self):
        """Establishes connection to PostgreSQL database using configured DSN.
        
        Raises:
        - psycopg2.Error: If connection fails due to invalid credentials or network issues.
        """
        self.conn = psycopg2.connect(self.config.dsn)
        logger.info(f"Connected to '{self.config.database}' at {self.config.host}")

    @log_errors(operation_name="close")
    def close(self):
        """Closes active database connection if open.
        
        Logs warning if connection was already closed or never established.
        """
        if self.is_closed:
            logger.warning("Connection already closed or never established")
            return
        self.conn.close()
        logger.info(f"Closed connection to '{self.config.database}'")
    
    @log_errors(operation_name="health_check", reraise=False, return_on_error=False)
    def health_check(self) -> bool:
        """Verifies database connection is alive by executing simple query.
        
        Returns:
        - bool: True if connection is healthy, False if closed or query fails.
        """
        if self.is_closed:
            logger.warning("Health check failed: connection not established")
            return False
        with self.conn.cursor() as cur:
            cur.execute('SELECT 1')
        logger.info(f"Connection to '{self.config.database}' is healthy")
        return True
    
    @log_errors(operation_name="execute")
    def execute(self, statement: SQLStatement) -> None:
        """Executes single SQL statement and commits transaction.
        
        Args:
        - statement (SQLStatement): SQL statement to execute.
        
        Raises:
        - ConnectionError: If connection is closed.
        - psycopg2.Error: If statement execution fails.
        """
        if self.is_closed:
            raise ConnectionError("Database connection not established")
        with self.conn.cursor() as cur:
            cur.execute(statement.sql, statement.vals)
            self.conn.commit()
        logger.info(str(statement))

    @log_errors(operation_name="transaction")
    def transaction(self, statements: Iterable[SQLStatement]) -> None:
        """Executes multiple SQL statements as atomic transaction with rollback on failure.
        
        Args:
        - statements (Iterable[SQLStatement]): Collection of SQL statements to execute.
        
        Raises:
        - ConnectionError: If connection is closed.
        - psycopg2.Error: If any statement fails, triggering rollback.
        """
        if self.is_closed:
            raise ConnectionError("Database connection not established")
        try:
            with self.conn.cursor() as cur:
                for statement in statements:
                    cur.execute(statement.sql, statement.vals)
                self.conn.commit()
        except psycopg2.Error:
            self.conn.rollback()
            logger.warning("Transaction failed and rolled back")
            raise
        logger.info(f"Transaction({' '.join([str(s) for s in statements])})")

    @log_errors(operation_name="fetch_one")
    def fetch_one(self, statement: SQLStatement) -> tuple | None:
        """Executes query and returns first result row.
        
        Args:
        - statement (SQLStatement): SELECT query to execute.
        
        Returns:
        - tuple | None: First row of results as tuple, or None if no results.
        
        Raises:
        - ConnectionError: If connection is closed.
        - psycopg2.Error: If query execution fails.
        """
        if self.is_closed:
            raise ConnectionError("Database connection not established")
        with self.conn.cursor() as cur:
            cur.execute(statement.sql, statement.vals)
            result = cur.fetchone()
            logger.info(str(statement))
            return result
        
    @log_errors(operation_name="fetch_many")
    def fetch_many(self, statement: SQLStatement, size: int) -> list[tuple]:
        """Executes query and returns specified number of result rows.
        
        Args:
        - statement (SQLStatement): SELECT query to execute.
        - size (int): Maximum number of rows to fetch.
        
        Returns:
        - list[tuple]: List of result rows, each row as tuple.
        
        Raises:
        - ConnectionError: If connection is closed.
        - psycopg2.Error: If query execution fails.
        """
        if self.is_closed:
            raise ConnectionError("Database connection not established")
        with self.conn.cursor() as cur:
            cur.execute(statement.sql, statement.vals)
            results = cur.fetchmany(size)
            logger.info(str(statement))
            return results
        
    @log_errors(operation_name="fetch")
    def fetch(self, statement: SQLStatement) -> list[tuple]:
        """Executes query and returns all result rows.
        
        Args:
        - statement (SQLStatement): SELECT query to execute.
        
        Returns:
        - list[tuple]: List of all result rows, each row as tuple.
        
        Raises:
        - ConnectionError: If connection is closed.
        - psycopg2.Error: If query execution fails.
        """
        if self.is_closed:
            raise ConnectionError("Database connection not established")
        with self.conn.cursor() as cur:
            cur.execute(statement.sql, statement.vals)
            results = cur.fetchall()
            logger.info(str(statement))
            return results
