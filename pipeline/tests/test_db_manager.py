
import pytest
import duckdb
import pandas as pd
import os
from src.db_manager import DBManager

@pytest.fixture
def db_path(tmp_path):
    """Create a temporary database path."""
    return str(tmp_path / "test_db_manager.duckdb")

@pytest.fixture
def db_manager(db_path):
    """Create a DBManager instance."""
    with DBManager(db_path) as db:
        yield db

def test_db_manager_initialization(db_path):
    """Test that DBManager initializes correctly."""
    db = DBManager(db_path)
    assert db.db_path == db_path
    assert db.con is not None
    db.close()
    assert db.con is None # Should be closed

def test_execute_query(db_manager):
    """Test executing a simple query."""
    db_manager.execute("CREATE TABLE test (id INTEGER, name VARCHAR)")
    db_manager.execute("INSERT INTO test VALUES (1, 'Alice')")
    result = db_manager.execute("SELECT * FROM test").fetchall()
    assert result == [(1, 'Alice')]

def test_fetch_df(db_manager):
    """Test fetching a DataFrame."""
    db_manager.execute("CREATE TABLE test_df (id INTEGER, value DOUBLE)")
    db_manager.execute("INSERT INTO test_df VALUES (1, 10.5), (2, 20.0)")
    
    df = db_manager.fetch_df("SELECT * FROM test_df ORDER BY id")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert df.iloc[0]['value'] == 10.5

def test_table_exists(db_manager):
    """Test checking if a table exists."""
    assert not db_manager.table_exists("non_existent_table")
    db_manager.execute("CREATE TABLE existing_table (id INTEGER)")
    assert db_manager.table_exists("existing_table")

def test_execute_with_params(db_manager):
    """Test executing with parameters."""
    db_manager.execute("CREATE TABLE test_params (id INTEGER, name VARCHAR)")
    db_manager.execute("INSERT INTO test_params VALUES (?, ?)", [1, 'Bob'])
    result = db_manager.execute("SELECT name FROM test_params WHERE id = ?", [1]).fetchone()
    assert result == ('Bob',)

def test_execute_with_dataframe_registration(db_manager):
    """Test executing a query that joins against a registered DataFrame."""
    # Create a DataFrame
    df = pd.DataFrame({'id': [1, 2], 'val': ['a', 'b']})
    
    # Register/use it in a query
    # DBManager.execute logic: "if params contains DF, register it"
    # Note: DBManager implementation might need kwargs or dict handling for registration
    # Inspecting source: params is Optional[Dict[str, Any]]
    
    # We pass the DF in params dict
    db_manager.execute("CREATE TABLE target (id INTEGER, name VARCHAR)")
    db_manager.execute("INSERT INTO target VALUES (1, 'One'), (2, 'Two')")
    
    result_df = db_manager.fetch_df("""
        SELECT t.name, d.val 
        FROM target t
        JOIN my_df d ON t.id = d.id
        ORDER BY t.id
    """, params={'my_df': df})
    
    assert len(result_df) == 2
    assert result_df.iloc[0]['val'] == 'a'

def test_context_manager(db_path):
    """Test using DBManager as a context manager."""
    with DBManager(db_path) as db:
        assert db.con is not None
        db.execute("CREATE TABLE test_ctx (id INTEGER)")
    
    # Connection should be closed/invalid after exit, 
    # but DBManager.close() sets self.con = None
    # We can't strictly check 'closed' state on None, but we can verify object state.
    # We cannot easily check the internal state of the closed connection if it's set to None.
    # But if we try to use it, it should fail or raise error if we retained a reference?
    # Actually, the instance `db` from the `as` block is available.
    assert db.con is None

def test_error_handling(db_manager):
    """Test that errors are raised for invalid queries."""
    with pytest.raises(Exception): # DuckDB raises duckdb.ParserException or similar, inheriting from Exception
        db_manager.execute("SELECT * FROM non_existent_table_random_xyz")
