# NFL Dead Money Pipeline - Visualization & Analysis Guide

## Overview

This document provides a comprehensive guide to the Airflow DAG visualization and the dead money analysis dashboard created in the pipeline.

---

## 📊 Generated Artifacts

### 1. **Pipeline DAG Architecture Diagram** 
📁 Location: `notebooks/outputs/pipeline_dag.png`

**Visual Components:**
- **Blue Nodes**: Data ingestion tasks (Spotrac snapshot, Player rankings backfill)
- **Orange Nodes**: Staging and validation (Stage raw data, Validate staging)
- **Green Nodes**: Transformation layer with dbt (dbt Seed references, dbt Staging, Normalization, dbt Marts analytics)
- **Red Nodes**: Quality checks and output (Quality validation, Merge dead money, Scrape rosters)

**Pipeline Flow:**
```
Data Sources
    ├── Spotrac Snapshot → Stage Raw Data ─→ Validate Staging ─→ dbt Seed
    │                                                             (Player Rankings)
    │                                                                 ↓
    └── Player Rankings → Historical Backfill ─────────────────→ dbt Staging
                                                                    ↓
                                                        Normalization (SQL)
                                                                    ↓
                                                          dbt Marts (Analytics)
                                                                    ↓
                                                        Quality Validation
                                                                    ↓
                                                        Merge Dead Money Output
                                                                    ↓
                                                        Scrape Rosters (Feed)
```

**Key Architectural Features:**
- **CeleryExecutor + Redis**: Distributed task execution with message queuing
- **DuckDB Backend**: Analytics-optimized data warehouse
- **dbt Orchestration**: Three-layer transformation (staging → processing → marts)
- **Retry Policy**: 2 retries with 5-minute delay for fault tolerance

---

## 📈 Dead Money Analysis Dashboards

### 2. **Yearly Dead Money Trends**
📁 Location: `notebooks/outputs/dead_money_by_year.html`

**Visualization Type:** Interactive line chart with dual metrics

**Key Metrics:**
- **Total Dead Money Per Year** (primary line): Sum of all dead cap hits across all players
- **Average Per Player** (secondary line): Mean dead cap hit per player

**Findings:**
- **Peak Year**: 2017 with $208.65M total dead money
- **Trend**: 43.7% decline from 2015 ($165.69M) to 2024 ($93.27M)
- **Average**: $133.84M per year across the 10-year period
- **Recent Stability**: 2023-2024 stabilized around $93-102M range

**Insights:**
- Early 2010s NFL had higher dead money obligations from major free agent contracts
- Recent years show improved salary cap management and contract structuring
- 2020 spike ($183.77M) likely due to pandemic-era restructurings

---

### 3. **Top 10 Players by Dead Money**
📁 Location: `notebooks/outputs/top_players_dead_money.html`

**Visualization Type:** Interactive horizontal bar chart

**Top 5 Players:**
1. **Aaron Miller**: $93.06M (3 years: TB, TEN, CLE)
2. **Calvin White**: $90.05M (4 years: SF, CLE, ARI, NO)
3. **Kirk Harris**: $87.85M (4 years: NYG, CLE, NE)
4. **Julio Clark**: $87.71M (4 years: SEA, ARI, DAL, GB)
5. **Larry Taylor**: $76.18M (4 years: CIN, DET, HOU, PIT)

**Analysis:**
- **Serial Offenders**: These players appear in multiple years across different teams
- **High Volatility**: Players like Aaron Miller average $31.02M per year when appearing
- **Contract Migration**: Pattern shows players being traded/released with cap hits remaining on original team

---

### 4. **Player Trajectory Over Multiple Years**
📁 Location: `notebooks/outputs/player_trajectory.html`

**Visualization Type:** Multi-line chart tracking individual players across years

**Top 5 Recurring Players:**
- **Aaron Miller**: Years [2015, 2017, 2021] - trajectory shows spikes
- **Calvin White**: Years [2018, 2020, 2021, 2022] - consistent 4-year pattern
- **Kirk Harris**: Years [2015, 2018, 2020, 2021] - scattered across decade
- **Julio Clark**: Years [2017, 2020, 2021, 2023] - recent activity
- **Larry Taylor**: Years [2016, 2017, 2020, 2022] - mid-range patterns

**Use Case:** Identify contract patterns and follow individual player dead money impact across their moves.

---

## 🎯 Key Statistical Findings

### Financial Overview (2015-2024)

| Metric | Value |
|--------|-------|
| **Total Dead Money** | $1,338.42M |
| **Average per Player** | $7.39M |
| **Median per Player** | $4.04M |
| **Max Single Instance** | $41.97M |
| **Dataset Size** | 181 records |

### Temporal Distribution

| Period | Total Dead Money | Avg per Year | Peak Year |
|--------|-----------------|--------------|-----------|
| 2015-2019 | $697.66M | $139.53M | 2017 ($208.65M) |
| 2020-2024 | $640.76M | $128.15M | 2020 ($183.77M) |
| **10-Year Avg** | - | **$133.84M** | - |

### Team Impact Rankings (All-Time)

Top teams affected by dead money obligations:

| Rank | Team | Total Dead Money | Years Impacted |
|------|------|-----------------|-----------------|
| 1 | NO (Saints) | $94.53M | 8 years |
| 2 | ARI (Cardinals) | $94.03M | 8 years |
| 3 | CLE (Browns) | $90.04M | 7 years |
| 4 | CIN (Bengals) | $84.65M | 7 years |
| 5 | PIT (Steelers) | $81.83M | 6 years |

---

## 💻 Technical Architecture

### Airflow Pipeline Execution

**Task Dependencies:**
```
snapshot_spotrac & snapshot_player_rankings
         ↓
  [Stage Raw Data]
         ↓
  [Validate Staging]
         ↓
  [dbt Seed Reference Data]
         ↓
  [dbt Staging Models]
         ↓
  [Normalize Data] (SQL)
         ↓
  [dbt Marts - Analytics Layer]
         ↓
  [Quality Validation]
         ↓
  [Merge Dead Money + Scrape Rosters]
```

**Execution Metrics:**
- **Scheduler**: Active with weekly schedule (`@weekly`)
- **Executor**: CeleryExecutor (distributed)
- **Message Broker**: Redis (localhost:6379)
- **Retry Policy**: 2 retries, 5-minute delay
- **Data Backend**: DuckDB with parquet storage

### Data Flow

```
Raw Data (CSV/HTML)
     ↓
[Staging Layer] - Validation & Type Casting
     ↓
[Normalization] - Business Logic & Aggregations
     ↓
[Analytics Marts] - Fact & Dimension Tables
     ↓
[Quality Checks] - Data Completeness & Integrity
     ↓
[Output] - Dead Money Summaries
```

---

## 🚀 Updating the Visualizations

### Regenerating All Dashboards

Run the complete notebook:
```bash
cd notebooks
jupyter nbconvert --to notebook --execute 08_pipeline_dag_and_dead_money_viz.ipynb
```

Or run individual visualization cells in the notebook:

```python
# DAG Visualization
# Cell: Build DAG using networkx

# Year Trends
# Cell: Dead money by year

# Top Players
# Cell: Top 10 players visualization

# Player Trajectories
# Cell: Interactive player tracking

# Statistics
# Cell: Summary statistics
```

### Key Data Sources

- **DuckDB Table**: `spotrac_dead_money`
- **Database Location**: `nfl_dead_money.duckdb`
- **Fields**: player_id, player_name, position, team, year, dead_cap_hit, is_king

---

## 📋 Visualization File Reference

| File | Type | Size | Last Updated | Purpose |
|------|------|------|-------------|---------|
| `pipeline_dag.png` | PNG | 183KB | 2024-12-20 | DAG architecture visual |
| `dead_money_by_year.html` | Plotly Interactive | 4.8MB | 2024-12-20 | Yearly trends chart |
| `top_players_dead_money.html` | Plotly Interactive | 4.8MB | 2024-12-20 | Top 10 players bar chart |
| `player_trajectory.html` | Plotly Interactive | 4.8MB | 2024-12-20 | Multi-year player tracking |

---

## 🔍 How to Use These Visualizations

### For Executive Reporting:
1. Use `pipeline_dag.png` to show infrastructure architecture
2. Use `dead_money_by_year.html` to show trend analysis
3. Reference statistics for cap planning decisions

### For Data Analysis:
1. Use `top_players_dead_money.html` to identify high-impact players
2. Use `player_trajectory.html` to understand contract patterns
3. Cross-reference with team performance metrics

### For Engineering Reviews:
1. Show `pipeline_dag.png` to illustrate data flow
2. Reference PIPELINE_DOCUMENTATION.md for detailed architecture
3. Use DuckDB queries for real-time data access

---

## 📚 Related Documentation

- **Pipeline Architecture**: See [PIPELINE_DOCUMENTATION.md](PIPELINE_DOCUMENTATION.md)
- **Data Quality Report**: See [DATA_QUALITY_REPORT.md](DATA_QUALITY_REPORT.md)
- **Notebook Source**: See [notebooks/08_pipeline_dag_and_dead_money_viz.ipynb](notebooks/08_pipeline_dag_and_dead_money_viz.ipynb)
- **DAG Code**: See [dags/nfl_dead_money_pipeline.py](dags/nfl_dead_money_pipeline.py)

---

## 🔗 Quick Links

- **View DAG Diagram**: Open `notebooks/outputs/pipeline_dag.png` in any image viewer
- **Interact with Dashboards**: Open `.html` files in any web browser
- **Live Data Queries**: Connect to DuckDB: `duckdb_open('nfl_dead_money.duckdb')`

---

**Last Updated**: December 20, 2024  
**Pipeline Status**: ✅ Running (CeleryExecutor + Redis)  
**Data Coverage**: 2015-2024 (10 years)  
**Records**: 181 player-year combinations across 32 NFL teams
