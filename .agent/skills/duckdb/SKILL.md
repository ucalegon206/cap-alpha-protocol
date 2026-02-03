---
name: DuckDB Standards
description: Best practices for DuckDB usage in the NFL Dead Money pipeline.
---

# DuckDB Standards

## 1. Connection Management
- Use short-lived connections for scripts (`duckdb.connect(DB_PATH)`).
- Always `con.close()` to release the lock file.

## 2. Upsert Pattern (Incremental Load)
When loading data for a specific period (e.g., Year 2024), use the **Delete-Insert** pattern to ensure idempotency.

```python
# Create table if not exists (Empty Limit 0 trick)
con.execute("CREATE TABLE IF NOT EXISTS my_table AS SELECT * FROM df LIMIT 0")

# Delete existing data for the partition
con.execute("DELETE FROM my_table WHERE year = 2024")

# Insert new data
con.execute("INSERT INTO my_table BY NAME SELECT * FROM df")
```

## 3. Performance
- Use `BY NAME` in inserts to handle column order changes gracefully.
- For large CSV loads, use `read_csv_auto` directly in SQL rather than Pandas when possible, UNLESS you need complex Python-side cleaning (like our `clean_doubled_name` logic).
