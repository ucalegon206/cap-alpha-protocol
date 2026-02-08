# Frontend Architecture: The "Cap Alpha" Dashboard

**Goal**: Build a world-class, executive-grade interface for NFL General Managers to visualize "Dead Money" efficiency and simulated trade scenarios.
**Aesthetic**: "Bloomberg Terminal meets ESPN Dark Mode." High contrast, data density, immediate clarity.

## 1. Technology Stack
- **Framework**: Next.js 14 (App Router) - For server-side rendering and speed.
- **Language**: TypeScript - For type safety matching our Python strictness.
- **Styling**: Tailwind CSS - For rapid, system-based utility styling.
- **Components**: Shadcn UI (Radix Primitives) - For accessible, high-quality interactive elements.
- **State**: React Query (TanStack Query) - For client-side data fetching/caching.
- **Charts**: Recharts - For responsive, composable D3 visualizations.

## 2. Design System ("The War Room")
- **Color Palette**:
  - **Background**: `Zinc 950` (Deepest Charcoal) - Not pure black.
  - **Primary**: `Emerald 500` (Cap Surplus / Value)
  - **Destructive**: `Rose 500` (Dead Money / Liability)
  - **Accent**: `Amber 400` (Draft Capital / Potential)
  - **Text**: `Zinc 100` (High readability)
- **Typography**: `Inter` (Clean, tabular numbers for financial data).
- **Layout**: "Dashboard First". No scrolling if possible. Dense grids.

## 3. Core Features (Phase 1)
### A. The "Roster Portfolio" View
- **Concept**: Treat the roster like a stock portfolio.
- **Visuals**:
  - **Treemap**: Size = Cap Hit. Color = Efficiency (Green=Good, Red=Bad).
  - **Table**: Sortable list of all players with "Risk Score" and "Surplus Value".

### B. The Trade Simulator (" The Machine")
- **Interaction**: Drag-and-drop assets between two teams.
- **Feedback**: Real-time recalculation of "Win Probability Added" or "Cap Space Cleared".
- **AI Insight**: "The engine rejects this trade because Denver takes on $40M dead money."

## 4. Implementation Steps
1. **Initialize**: `npx create-next-app@latest web --typescript --tailwind --eslint`
2. **Scaffold**: Install Shadcn UI (`button`, `card`, `table`, `dialog`).
3. **Data Layer**: Create API routes in Next.js that read from `data/duckdb/nfl_production.db` (Read-Only Mode).
4. **Develop**: Build the "Roster Portfolio" page first.

## 5. Hosting Strategy
- **Local**: `npm run dev` (Port 3000).
- **Self-Hosted**: Docker container exposing Port 3000.
