#!/usr/bin/env python3
"""
Trade Simulator: Orchestrator
Author: Cap Alpha Protocol

Objective: Run a sandbox simulation to demonstrate the Adversarial Trade Engine.
Scenario:
- KC (Contender): High Win Weight, Needs WR.
- LV (Rebuilder): High Cap Weight, Surplus WR (Davante Adams).
"""
import sys
from trade_simulator import LeagueState, TeamState, Agent, CONTENDER, MCTS, TeamPersona

def run_sandbox():
    print("🏈 Initializing Adversarial Trade Engine (Sandbox Mode)...")
    
    # 1. Setup Mock League State
    teams = {
        "KC": TeamState(
            name="KC",
            cap_space=15.0, # Tight Cap
            needs={"WR": 0.9, "CB": 0.2}, # Desperate for WR
            roster_value=120.0 # Super Bowl Caliber
        ),
        "LV": TeamState(
            name="LV",
            cap_space=40.0, # Rebuilding
            needs={"QB": 0.8, "DT": 0.5},
            roster_value=60.0 # Struggling
        ),
        "BUF": TeamState( # Control Team
            name="BUF",
            cap_space=5.0,
            needs={"S": 0.6},
            roster_value=110.0
        )
    }
    
    initial_state = LeagueState(teams)
    
    # Inject Mock Market Players (simplification for POC)
    initial_state.market_players = [
        {"id": "p1", "name": "Davante Adams", "team": "LV", "position": "WR", "value": 15.0, "cap_hit": 25.0},
        {"id": "p2", "name": "Stefon Diggs", "team": "BUF", "position": "WR", "value": 14.0, "cap_hit": 20.0},
        {"id": "p3", "name": "Maxx Crosby", "team": "LV", "position": "ED", "value": 18.0, "cap_hit": 22.0}
    ]
    
    print(f"Teams Loaded: {list(teams.keys())}")
    
    # 2. Setup Adversarial Agents
    # KC = Contender (Values Wins)
    # LV = Rebuilder (Values Cap/Picks)
    agents = {
        "KC": Agent("KC", CONTENDER),
        "LV": Agent("LV", TeamPersona(win_weight=0.1, cap_weight=0.8, draft_weight=0.1)), # Aggressive Seller
        "BUF": Agent("BUF", CONTENDER)
    }
    
    # 3. Initialize MCTS
    mcts = MCTS(initial_state, agents)
    
    print("\n🔮 Running Monte Carlo Tree Search (500 Iterations)...")
    best_action = mcts.search(iterations=500)
    
    # 4. Results
    if best_action:
        print("\n✅ OPTIMAL TRADE IDENTIFIED:")
        print(f"   Source: {best_action.source_team}")
        print(f"   Target: {best_action.target_team} (Active Need: WR)")
        print(f"   Asset:  {best_action.player_name} (${best_action.cap_hit}M)")
        print(f"   Cost:   {best_action.compensation_picks}")
        print("\n   Reasoning: Maximizes KC Win Probability while solving LV Cap Crisis.")
    else:
        print("\n❌ No mutually beneficial trades found.")

if __name__ == "__main__":
    run_sandbox()
