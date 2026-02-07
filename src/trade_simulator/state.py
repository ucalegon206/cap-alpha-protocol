"""
Trade Simulator: State Representation (MDP)
Author: Cap Alpha Protocol

This module defines the 'State' of the NFL League for the Monte Carlo Tree Search.
A State captures:
1. Cap Space for all 32 teams.
2. Roster Composition (Signings/Cuts).
3. Draft Capital.
"""
import copy
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

@dataclass
class TeamState:
    name: str # e.g. "ARI"
    cap_space: float # Millions
    needs: Dict[str, float] # {"QB": 0.9, "WR": 0.4} (0-1 Severity)
    roster_value: float # Aggregate AV
    
@dataclass
class TradeAction:
    source_team: str
    target_team: str
    player_id: str
    player_name: str
    player_value: float
    cap_hit: float
    compensation_picks: List[str] # ["2026-R1", "2026-R3"]

class LeagueState:
    def __init__(self, teams: Dict[str, TeamState], constraints: Dict = None):
        self.teams = teams
        self.constraints = constraints or {}
        # Hashable ID for MCTS caching
        self.id = self._generate_id()

    def _generate_id(self) -> str:
        # Simple hash of team states
        return str(hash(tuple(sorted((t.name, t.cap_space) for t in self.teams.values()))))

    def get_legal_actions(self, active_team: str) -> List[TradeAction]:
        """
        Generate potential trades for the active team.
        Heuristic: "I have needs. Find someone with surplus."
        """
        legal_trades = []
        team = self.teams[active_team]
        
        # 1. Identify my biggest need
        # e.g., "WR": 0.9 severity
        if not team.needs:
            return []
            
        target_pos = max(team.needs, key=team.needs.get)
        
        # 2. Scan other teams for surplus at this position
        # Simplified: Just find any player with Value > 10 at that position
        for other_name, other_team in self.teams.items():
            if other_name == active_team:
                continue
                
            # Assume we have access to a player list (in reality, this would be in TeamState)
            # For POC, we'll iterate a mock list attached to this state
            if not hasattr(self, 'market_players'):
                return []
                
            for player in self.market_players:
                if player['team'] == other_name and player['position'] == target_pos:
                    # found a match!
                    # Construct Trade Action
                    # "I give nothing (for now), you give me player"
                    action = TradeAction(
                        source_team=other_name,
                        target_team=active_team,
                        player_id=player['id'],
                        player_name=player['name'],
                        player_value=player['value'],
                        cap_hit=player['cap_hit'],
                        compensation_picks=["2026-R3"] # Default cost
                    )
                    legal_trades.append(action)
                    
        return legal_trades

    def apply_action(self, action: TradeAction) -> 'LeagueState':
        """
        Return a NEW State resulting from the action (Immutable Transition).
        """
        new_teams = copy.deepcopy(self.teams)
        
        # 1. Move Player
        # Remove from Source, Add to Target (Logic placeholder)
        
        # 2. Move Cap Hit
        new_teams[action.source_team].cap_space += action.cap_hit # Dead Cap logic omitted for POC
        new_teams[action.target_team].cap_space -= action.cap_hit

        return LeagueState(new_teams, self.constraints)

    def is_terminal(self) -> bool:
        """
        Has the simulation reached a conclusion? (e.g. Deadline Passed)
        """
        return False # Placeholder
        
    def get_result(self, team: str) -> float:
        """
        Calculate the Reward for a specific team in this state.
        Reward = WinProb + (CapSpace * Beta)
        """
        return self.teams[team].roster_value # Placeholder
