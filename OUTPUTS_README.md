# Airflow Pipeline DAG Visualization & Dead Money Analysis

## ✅ Deliverables Complete

### 1. 📊 **Pipeline DAG Architecture Diagram**

**File**: [notebooks/outputs/pipeline_dag.png](notebooks/outputs/pipeline_dag.png)

Visual representation of the Airflow DAG showing:
- **11 Pipeline Stages**: From data ingestion to final output
- **Color-Coded Layers**: 
  - 🔵 Blue = Data Ingestion (Snapshots, Rankings)
  - 🟠 Orange = Staging & Validation
  - 🟢 Green = Transformation (dbt models)
  - 🔴 Red = Quality & Output
- **Task Dependencies**: Explicit flow showing which tasks depend on others

This DAG runs on **CeleryExecutor with Redis** for distributed, fault-tolerant execution.

---

### 2. 📈 **Interactive Dead Money Dashboards**

#### Dead Money by Year
**File**: [notebooks/outputs/dead_money_by_year.html](notebooks/outputs/dead_money_by_year.html)

- **Metric 1**: Total dead money per year (blue line)
- **Metric 2**: Average per player (orange dashed line)
- **Time Period**: 2015-2024
- **Key Finding**: 43.7% decline from peak ($208.65M in 2017 to $93.27M in 2024)

#### Top 10 Players
**File**: [notebooks/outputs/top_players_dead_money.html](notebooks/outputs/top_players_dead_money.html)

Bar chart ranking players by total dead money across the decade:
1. Aaron Miller - $93.06M
2. Calvin White - $90.05M
3. Kirk Harris - $87.85M
4. Julio Clark - $87.71M
5. Larry Taylor - $76.18M

#### Player Trajectory
**File**: [notebooks/outputs/player_trajectory.html](notebooks/outputs/player_trajectory.html)

Line chart tracking the top 5 recurring players across multiple years, showing:
- How dead money impacts follow players across teams
- Year-over-year changes in their cap hits
- Multi-year contract patterns

---

## 📊 Key Findings

### Financial Summary
- **Total Dead Money (2015-2024)**: $1,338.42M
- **Average per Player**: $7.39M
- **Average per Year**: $133.84M
- **Peak Year**: 2017 with $208.65M
- **Recent Trend**: Declining (~$93-102M in 2023-2024)

### Top Teams Affected
1. New Orleans Saints: $94.53M
2. Arizona Cardinals: $94.03M
3. Cleveland Browns: $90.04M
4. Cincinnati Bengals: $84.65M
5. Pittsburgh Steelers: $81.83M

### Notable Patterns
- **Serial Offenders**: 5 players account for $434.85M (32.5% of total)
- **Multi-Year Burden**: Players like Calvin White, Kirk Harris show repeated cap hits
- **Recent Improvement**: Dead money obligations down 43.7% over the decade

---

## 📁 Project Structure

```
nfl-dead-money/
├── dags/
│   └── nfl_dead_money_pipeline.py          # Airflow DAG definition
├── notebooks/
│   ├── 08_pipeline_dag_and_dead_money_viz.ipynb   # Source notebook
│   └── outputs/
│       ├── pipeline_dag.png                # DAG architecture diagram
│       ├── dead_money_by_year.html         # Yearly trends chart
│       ├── top_players_dead_money.html     # Top 10 players chart
│       └── player_trajectory.html          # Player tracking chart
├── PIPELINE_DOCUMENTATION.md               # Comprehensive architecture guide
├── VISUALIZATION_GUIDE.md                  # This visualization suite guide
└── README.md                               # Project overview
```

---

## 🚀 Infrastructure Stack

### Deployment Configuration
- **Orchestrator**: Apache Airflow 3.x
- **Executor**: CeleryExecutor (distributed task execution)
- **Message Broker**: Redis 8.4.0 (localhost:6379)
- **Data Warehouse**: DuckDB (analytical focus)
- **Transformations**: dbt (data build tool)
- **Visualization**: Plotly (interactive charts), NetworkX (DAG diagram)

### Pipeline Flow
```
Data Sources → Staging → Validation → dbt Seed/Staging → 
Normalization → dbt Marts → Quality Checks → Output
```

---

## 📚 Documentation

### For Quick Overview
- Start with [VISUALIZATION_GUIDE.md](VISUALIZATION_GUIDE.md)
- View the 4 interactive HTML dashboards in `notebooks/outputs/`
- Check the PNG DAG diagram

### For Technical Deep Dive
- Read [PIPELINE_DOCUMENTATION.md](PIPELINE_DOCUMENTATION.md)
- Review [dags/nfl_dead_money_pipeline.py](dags/nfl_dead_money_pipeline.py)
- Examine the Jupyter notebook: [notebooks/08_pipeline_dag_and_dead_money_viz.ipynb](notebooks/08_pipeline_dag_and_dead_money_viz.ipynb)

### For Data Exploration
- Query DuckDB directly: `duckdb_open('nfl_dead_money.duckdb')`
- Table: `spotrac_dead_money` (181 records, 2015-2024)
- Fields: player_id, player_name, position, team, year, dead_cap_hit, is_king

---

## 💡 Use Cases

### Executive Reporting
→ Use yearly trend chart to show declining dead money burden
→ Reference top teams to discuss cap management strategy

### Analytics & Research
→ Use player trajectory chart to understand contract patterns
→ Analyze top affected teams for front office insights
→ Track multi-year players to identify serialized commitments

### System Monitoring
→ View DAG diagram to understand pipeline architecture
→ Check PIPELINE_DOCUMENTATION for scheduler/executor status
→ Monitor data freshness (updated weekly via Airflow)

---

## ✨ Recent Execution

**Date**: December 20, 2024  
**Status**: ✅ All visualizations generated successfully  
**Notebook Cells**: 11 cells executed  
**Data Loaded**: 181 player-year records from DuckDB  
**Outputs Generated**: 4 HTML dashboards + 1 PNG diagram  

---

## 🔄 Next Steps

To update visualizations with fresh data:
```bash
# Option 1: Run full notebook
cd notebooks
jupyter nbconvert --to notebook --execute 08_pipeline_dag_and_dead_money_viz.ipynb

# Option 2: Manually run cells in Jupyter/VSCode
# Open the notebook and execute cells sequentially
```

To trigger a new pipeline run:
```bash
# Via Airflow CLI
airflow dags test nfl_dead_money_pipeline

# Or via the Airflow Web UI
# http://localhost:8080 → DAGs → nfl_dead_money_pipeline → Trigger DAG
```

---

**Last Updated**: December 20, 2024  
**Airflow Status**: ✅ Running (CeleryExecutor)  
**Data Coverage**: 2015-2024 (181 records)  
**Commit**: Airflow 3.x with Redis broker, visualizations complete
