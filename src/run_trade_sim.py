#!/usr/bin/env python3
"""
Trade Simulator: League-Wide Trade Scanner
Author: Cap Alpha Protocol

Objective: Scan ALL 32 teams for mutually beneficial trades based on finding "Surplus" to meet "Needs".
Constraint: No forced narratives. Data driven only.
"""
import sys
from trade_simulator import StateLoader, Agent, TeamPersona, CONTENDER

import pandas as pd

def run_league_scan():
    print("🏈 Initializing League-Wide Trade Scanner (No Narratives)...")
    
    # 1. Hydrate Real State
    DB_PATH = "data/duckdb/nfl_production.db"
    # Use Dynamic Threshold (Default ~0.75% of Cap)
    loader = StateLoader(DB_PATH, year=2025)
    initial_state = loader.load_league_state()
    
    # 2. Setup Agents (Dynamic Thresholds)
    # Calculate Cap Percentiles
    caps = [t.cap_space for t in initial_state.teams.values()]
    cap_series = pd.Series(caps)
    low_cap_threshold = cap_series.quantile(0.25) # Bottom 25% = Cap Stressed
    high_cap_threshold = cap_series.quantile(0.75) # Top 25% = Cap Rich
    
    print(f"💰 Dynamic Thresholds | Stressed < ${low_cap_threshold:.1f}M | Rich > ${high_cap_threshold:.1f}M")
    
    agents = {}
    for team_name, team_data in initial_state.teams.items():
        if team_data.cap_space < low_cap_threshold:
            # Cap Stressed / Contender Mode
            agents[team_name] = Agent(team_name, CONTENDER)
        elif team_data.cap_space > high_cap_threshold:
            # Rebuilder Mode
            agents[team_name] = Agent(team_name, TeamPersona(win_weight=0.2, cap_weight=0.6, draft_weight=0.2))
        else:
            # Balanced
            agents[team_name] = Agent(team_name, TeamPersona(win_weight=0.5, cap_weight=0.3, draft_weight=0.2))

    print(f"🕵️ Scanning {len(initial_state.teams)} Teams for Trade Opportunities...")
    
    potential_trades = []

    # 3. Iterate Every Team as a Potential "Buyer"
    for buyer_name in initial_state.teams.keys():
        buyer_agent = agents[buyer_name]
        
        # Get Candidate Trades (Buyer Needs matching Market Surplus)
        candidates = initial_state.get_legal_actions(buyer_name)
        
        for trade in candidates:
            # 4. Evaluate Mutual Benefit
            # Buyer's Perspective
            # Apply Action to get "Post-Trade State"
            # NOTE: For speed, we just approximate utility delta here
            # But the 'State.apply_action' is robust.
            
            post_trade_state = initial_state.apply_action(trade)
            
            buyer_delta = buyer_agent.evaluate_trade(initial_state, post_trade_state)
            
            # Seller's Perspective
            seller_name = trade.source_team
            seller_agent = agents[seller_name]
            seller_delta = seller_agent.evaluate_trade(initial_state, post_trade_state)
            
            # 5. Filter: MUST be positive for BOTH
            # (Adversarial Logic: No one makes a losing trade)
            if buyer_delta > 0 and seller_delta > 0:
                potential_trades.append({
                    "buyer": buyer_name,
                    "seller": seller_name,
                    "player": trade.player_name,
                    "cap": trade.cap_hit,
                    "cost": trade.compensation_picks,
                    "buyer_gain": buyer_delta,
                    "seller_gain": seller_delta
                })

    # 6. Report
    print(f"\n✅ Scan Complete. Found {len(potential_trades)} Mutually Beneficial Scenarios.\n")
    
    # Sort by Aggregate Utility (Maximize League Value)
    potential_trades.sort(key=lambda x: x['buyer_gain'] + x['seller_gain'], reverse=True)
    
    print(f"{'BUYER':<5} | {'SELLER':<6} | {'PLAYER':<20} | {'CAP':<6} | {'COST':<15} | {'SCORE'}")
    print("-" * 75)
    
    for pt in potential_trades[:50]: # Top 50
        score = pt['buyer_gain'] + pt['seller_gain']
        print(f"{pt['buyer']:<5} | {pt['seller']:<6} | {pt['player']:<20} | ${int(pt['cap'])}M | {str(pt['cost']):<15} | {score:.2f}")

if __name__ == "__main__":
    run_league_scan()
