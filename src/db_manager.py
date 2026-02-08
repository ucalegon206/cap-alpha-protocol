import duckdb
import os
import logging
import pandas as pd
from typing import Optional, Any, Dict
from src.config_loader import get_db_path

logger = logging.getLogger(__name__)

class DBManager:
    """
    Technology-agnostic database manager.
    Currently supports DuckDB and MotherDuck.
    """
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or get_db_path()
        self.con = None
        self._initialize_connection()

    def _initialize_connection(self):
        """Initializes the database connection."""
        try:
            # Check if we are using MotherDuck
            if "md:" in self.db_path or os.getenv("MOTHERDUCK_TOKEN"):
                logger.info(f"Connecting to MotherDuck/DuckDB at {self.db_path}")
                self.con = duckdb.connect(self.db_path)
            else:
                logger.info(f"Connecting to local DuckDB at {self.db_path}")
                self.con = duckdb.connect(self.db_path)
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def execute(self, query: str, params: Optional[Dict[str, Any]] = None):
        """Executes a SQL query with explicit DataFrame registration for broad scope visibility."""
        try:
            if params:
                for name, value in params.items():
                    if isinstance(value, pd.DataFrame):
                        self.con.register(name, value)
                return self.con.execute(query, params)
            return self.con.execute(query)
        except Exception as e:
            logger.error(f"Query execution failed: {e}\nQuery: {query}")
            raise

    def fetch_df(self, query: str, params: Optional[Dict[str, Any]] = None):
        """Executes a query and returns a Pandas DataFrame."""
        try:
            if params:
                for name, value in params.items():
                    if isinstance(value, pd.DataFrame):
                        self.con.register(name, value)
                return self.con.execute(query, params).df()
            return self.con.execute(query).df()
        except Exception as e:
            logger.error(f"Failed to fetch DataFrame: {e}")
            raise

    def table_exists(self, table_name: str) -> bool:
        """Checks if a table exists in the database."""
        try:
            res = self.con.execute(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()
            return res[0] > 0
        except Exception:
            return False

    def close(self):
        """Closes the database connection."""
        if self.con:
            self.con.close()
            self.con = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
