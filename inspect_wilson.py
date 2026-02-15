import duckdb

# Connect to the database
try:
    con = duckdb.connect('nfl_dead_money.duckdb', read_only=True)
except:
    try:
        con = duckdb.connect('dbt/nfl_dead_money.duckdb', read_only=True)
    except Exception as e:
        print(f"Could not connect to database: {e}")
        exit(1)

# Query for Russell Wilson
query = """
SELECT * 
FROM contracts 
WHERE player_name LIKE '%Russell Wilson%'
"""

try:
    results = con.execute(query).fetchall()
    # Get column names
    cols = [desc[0] for desc in con.description]
    print(f"Columns: {cols}")
    for row in results:
        print(row)
except Exception as e:
    print(f"Error querying contracts: {e}")
    
    # Fallback: List tables to debug
    print("\nTables in database:")
    try:
        tables = con.execute("SHOW TABLES").fetchall()
        for t in tables:
            print(t)
    except:
        pass
