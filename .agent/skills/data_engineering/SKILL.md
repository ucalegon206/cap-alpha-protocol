---
name: Data Engineering
description: Principles for building idempotent, deterministic data pipelines in this project.
---

# Data Engineering Principles

## 1. Explicit Determinism (First Principle)
- **Rule**: Never blindly scan for only the "latest" file without context. 
- **Guidance**: Scripts must accept `year` and `week` parameters to target specific data partitions.
  - **BAD**: `glob("data/raw/*")` -> "Oh, I found 2024!"
  - **GOOD**: `ingest(year=2024, week=1)` -> "I am looking for `data/raw/2024/week_1_*`."

## 2. Idempotency via Partition Overwrite
- **Rule**: Pipeline tasks must be re-runnable without side effects (duplication).
- **Pattern**: Upsert / Replace Partition.
- **Implementation**:
  ```python
  # 1. Clear the specific partition
  con.execute("DELETE FROM table WHERE year = ?", [year])
  # 2. Insert the new state for that partition
  con.execute("INSERT INTO table SELECT * FROM df")
  ```
- **Constraint**: Do NOT drop and rebuild the entire table unless it is a full-history refresh (rare).

## 3. The Raw Layer is Immutable
- **Rule**: Once data is written to `data/raw/{year}/{week}_{ts}/`, it is never modified.
- **Correction**: If data is bad, scrape a NEW batch with a newer timestamp (e.g., `week_{ts+1}`) and re-run ingestion targeting that timestamp.
