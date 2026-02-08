# Cap Alpha Protocol: Distributed Architecture & Data Flow

**Version:** 1.0
**Date:** February 8, 2026
**Status:** IMPLEMENTED

## Executive Summary

The **Cap Alpha Protocol** employs a **Hybrid Static/Dynamic Architecture** designed for maximum stability, zero-latency user experience, and minimal operational overhead.

Instead of a traditional 3-tier architecture (Client <-> Server <-> Database) which introduces latency and connection risks, we utilize a **"Push-Based" Data Hydration** strategy. The heavy computational lifting (Risk Modeling, Trade Simulations) occurs in the offline Python pipeline, while the frontend consumes pre-computed, verified data snapshots.

## System Components

### 1. The Engine (Data Pipeline)
*   **Role:** The Source of Truth.
*   **Tech Stack:** Python 3.10+, DuckDB (Native), Pandas, Scikit-Learn.
*   **Location:** Local Environment / CI Runner.
*   **Responsibilities:**
    *   Ingesting raw data from Spotrac/OTC.
    *   Training ML models (`edce_risk`).
    *   Simulating adversarial trade scenarios (`Monte Carlo Tree Search`).
    *   **OUTPUT:** A fully materialized `nfl_production.db` (Gold Layer).

### 2. The Archive (Motherduck)
*   **Role:** Cloud Data Warehouse & Analytics Backend.
*   **Tech Stack:** Motherduck (Serverless DuckDB).
*   **Responsibilities:**
    *   Long-term storage of historical data.
    *   Supporting ad-hoc SQL analysis for "The Round Table" (Analysts).
    *   Serving as the "Off-site Backup" for the local pipeline.

### 3. The Connector (JSON Bridge)
*   **Role:** The Operational Data Link.
*   **Mechanism:** `scripts/sync_to_system.py` (formerly `sync_to_motherduck.py`).
*   **Function:**
    *   Extracts "Live" roster data (Latest Season) from the local DuckDB.
    *   Serializes it into a highly optimized static file: `web/data/roster_dump.json`.
    *   Injects this file directly into the Frontend's build path.

### 4. The Presentation (Vercel)
*   **Role:** User Interface & Trade Simulator.
*   **Tech Stack:** Next.js 14, React, Tailwind CSS.
*   **Data Source:** Imports `roster_dump.json` as a native module.
*   **Responsibilities:**
    *   **Zero-Latency Roster Browsing:** Data is bundled with the code.
    *   **Client-Side Simulation:** Simple trade math runs in the browser.
    *   **Visuals:** Drag-and-drop "War Room" interface.

## Data Flow Diagram

```mermaid
graph TD
    subgraph "Local / Compute Environment"
        Scrapers[Data Scrapers] -->|Raw HTML| Bronze[Bronze Layer]
        Bronze -->|SQL| Silver[Silver Layer]
        Silver -->|Feature Eng| Gold[Gold Layer (DuckDB)]
        Gold -->|Training| ML[XGBoost Risk Model]
        ML -->|Inference| Gold
    end

    subgraph "Synchronization (The Bridge)"
        Gold -->|Sync Script| Motherduck[(Motherduck Cloud)]
        Gold -->|JSON Dump| BridgeFile(web/data/roster_dump.json)
    end

    subgraph "Presentation (Vercel)"
        BridgeFile -->|Build Time| NextJS[Next.js App]
        NextJS -->|Deploy| LiveSite[nfl-dead-money.vercel.app]
        User((User)) -->|Interact| LiveSite
    end
```

## Architectural Decisions & Rationale

### Decision: The JSON Bridge (Static Injection)
**Context:** We initially attempted to connect the Next.js frontend directly to the DuckDB database file (`.db`) or the Motherduck cloud instance.
**Problem:**
1.  **Native Binding Conflicts:** `duckdb` (Node.js) requires native C++ bindings that notoriously fail in serverless/edge environments (like Vercel) and causes webpack bundling errors.
2.  **Connection Latency:** Querying a cloud DB for every page load slows down the "Trade Machine" experience.
3.  **File Locks:** Local development was blocked because the Python pipeline and Node.js server fought over the `.db` file lock.

**Solution:**
We decoupled the **Reader** from the **Writer**.
*   The Python pipeline **Writes** the database.
*   The Sync Script **Exports** a clean, read-only JSON snapshot.
*   The Frontend **Reads** the JSON.

**Benefits:**
*   **Stability:** The frontend *cannot* crash due to database connection errors.
*   **Performance:** Roster data loads instantly (memory-mapped).
*   **Simplicity:** No need to manage database credentials or VPCs on Vercel.

## Future Scaling Considerations

If the data grows beyond ~50MB (currently ~7MB), the JSON approach may impact initial load times.
*   **Phase 2 Mitigation:** Split JSON by Team (e.g., `kc_roster.json`, `lv_roster.json`) and lazy-load.
*   **Phase 3 Upgrade:** Only then, consider a dedicated API service (e.g., FastAPI) to query Motherduck, but *only* if real-time interaction complexity demands it.
