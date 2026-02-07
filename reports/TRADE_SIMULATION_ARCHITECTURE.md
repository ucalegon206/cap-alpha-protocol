# Architecture Design: Adversarial Trade Simulation Engine

**Objective**: Model NFL roster construction as a multi-agent adversarial game to identify optimal trade scenarios.

## 1. Core Concept: The "Cap Space" State Machine

We treat the NFL league state as a **Markov Decision Process (MDP)**.
*   **State ($S_t$)**: The complete configuration of all 32 rosters, cap space, and draft capital at time $t$.
*   **Action ($A$)**: A transaction (Trade, Cut, Sign, Restructure).
*   **Transition ($T(s'|s,a)$)**: The probability that a trade is accepted or a player signs.
*   **Reward ($R$)**: Change in `Team_Win_Probability` or `Long_Term_Cap_Health`.

## 2. The "Adversarial" Component
Trades are not unilateral. A trade requires **Mutual Maximization**.
*   **Team A (Buyer)**: Wants to maximize $P(SuperBowl)$.
*   **Team B (Seller)**: Wants to maximize $Val(DraftPicks) + CapSpace$.

We model this as a **Non-Zero-Sum Game**. A trade occurs only if:
$$ \Delta Value(Team A) > 0 \land \Delta Value(Team B) > 0 $$

## 3. Algorithm: Monte Carlo Tree Search (MCTS)

Simulating "all possible trades" is computationally impossible ($O(N!)$). We use **MCTS** to prune the search space.

### Phase A: Candidate Generation (Heuristic Policy)
Instead of random trades, we use our **Red List** and **Surplus Value** metrics to propose logical trades.
*   *Heuristic*: "Team A needs a WR (Gap Analysis) and has Cap Space. Team B has a Surplus WR and needs Cap Space."

### Phase B: Simulation (Rollout)
1.  **Propose Trade**: Team A offers Player X + Pick Y for Player Z.
2.  **Adversarial Evaluation**: Team B evaluates the offer using its own utility function (e.g., "Am I rebuilding?").
3.  **Accept/Reject**: If accepted, state $S$ transitions to $S'$.
4.  **Recursion**: Repeat for $N$ steps (seasons).

### Phase C: Value Backpropagation
Update the "Win Probability" of the initial state based on the long-term simulation results.

## 4. Technical Implementation Plan

### Stack
*   **State Store**: DuckDB (Fast in-memory cloning of "League State").
*   **Logic Engine**: Rust (for MCTS performance) or Python (multiprocessing).
*   **Agent Personas**: Each team is assigned a "Persona" (Contender, Rebuilder, Tanking) that dictates their utility function.

### Feasibility Prototype ("The Sandbox")
1.  **Limit Scope**: Simulate only 1 position group (e.g., Quarterbacks).
2.  **Define Utility**: $U(Team) = \alpha \cdot Wins + \beta \cdot CapSpace$.
3.  **Run**: Simulate 1000 seasons to find the "Nash Equilibrium" of QB movement.

## 5. Potential "Expensive Operation" Mitigation
*   **Pruning**: Only consider trades involving players with $Value > Threshold$.
*   **Clustering**: Group similar players (e.g., "Replacement Level WR") into generic assets.
*   **Parallelization**: Run independent simulations on distributed workers (Ray/Dask).
