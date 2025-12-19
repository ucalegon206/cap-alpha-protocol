Checklist

- [x] Verify copilot-instructions.md exists in `.github`.

- [x] Clarify Project Requirements
	Project type: Data pipeline + analytics. Language: Python. Frameworks: dbt (DuckDB), Airflow, Plotly.

- [x] Scaffold the Project
	Structure present with `dbt/`, `dags/`, `src/`, `scripts/`, `notebooks/`, `data/`.

- [x] Customize the Project
	Implemented dbt models, DuckDB mart, notebooks, E2E test, Airflow DAG.

- [x] Install Required Extensions
	No additional extensions required per setup info.

- [x] Compile the Project
	Verified notebooks run with DuckDB backend; dbt parsing succeeds; Postgres skipped.

- [x] Create and Run Task
	Not required; project runs via notebooks/dbt CLI and Airflow DAG.

- [ ] Launch the Project
	Pending user confirmation for debug mode before launch.

- [x] Ensure Documentation is Complete
	README present; this file cleaned of HTML comments and updated.

Execution Guidelines

- Use workspace root `.` for commands.
- Keep explanations concise; state skips briefly.
- Do not install extensions beyond those specified by project setup info.
- Changes should be minimal, focused, and aligned with current project.
