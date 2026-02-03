
import duckdb
import pandas as pd

DB_PATH = "data/nfl_data.db"

def diagnose():
    con = duckdb.connect(DB_PATH)
    
    print("--- Spotrac 2023 Stats ---")
    res = con.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(dead_cap_millions) as dead_cap_count,
            COUNT(age) as age_count,
            AVG(dead_cap_millions) as avg_dead_cap,
            AVG(age) as avg_age
        FROM silver_spotrac_contracts 
        WHERE year = 2023
    """).df()
    print(res)

    print("\n--- Example Dead Cap Values (2023) ---")
    res2 = con.execute("""
        SELECT player_name, dead_cap_millions, age 
        FROM silver_spotrac_contracts 
        WHERE year = 2023 AND dead_cap_millions > 0
        LIMIT 5
    """).df()
    print(res2)

    print("\n--- Why Double Dobbs? ---")
    res3 = con.execute("""
        SELECT player_name, team, year, COUNT(*) 
        FROM silver_spotrac_contracts 
        WHERE year = 2023 AND player_name LIKE '%Dobbs%'
        GROUP BY 1, 2, 3
    """).df()
    print(res3)

    con.close()

if __name__ == "__main__":
    diagnose()
