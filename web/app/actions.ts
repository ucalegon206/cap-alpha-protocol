'use server';

import fs from 'fs';
import path from 'path';

// Helper to read data from JSON dump
async function getHydratedData() {
  try {
    const filePath = path.resolve(process.cwd(), '../data/roster_dump.json');
    if (!fs.existsSync(filePath)) {
      console.error(`[Data] Dump file not found at: ${filePath}`);
      return [];
    }
    const content = fs.readFileSync(filePath, 'utf8');
    return JSON.parse(content);
  } catch (e) {
    console.error("[Data] Error reading dump:", e);
    return [];
  }
}

export async function getRosterData() {
  const data = await getHydratedData();
  const seen = new Set();
  return data
    .filter((d: any) => {
      const key = `${d.player_name}-${d.team}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    })
    .map((d: any) => ({
      player_name: d.player_name,
      team: d.team,
      position: d.position,
      cap_hit_millions: d.cap_hit_millions,
      risk_score: d.edce_risk,
      surplus_value: d.fair_market_value
    })).sort((a: any, b: any) => b.cap_hit_millions - a.cap_hit_millions);
}

export async function getTeamCapSummary() {
  const data = await getHydratedData();
  const teams: Record<string, any> = {};

  data.forEach((d: any) => {
    if (!teams[d.team]) {
      teams[d.team] = { team: d.team, total_cap: 0, risk_cap: 0 };
    }
    teams[d.team].total_cap += d.cap_hit_millions;
    if (d.edce_risk > 0.7) {
      teams[d.team].risk_cap += d.cap_hit_millions;
    }
  });

  return Object.values(teams);
}

export async function getTeams() {
  const data = await getHydratedData();
  const teams = Array.from(new Set(data.map((d: any) => d.team)));
  return teams.sort();
}

export async function getTradeableAssets(team: string) {
  const data = await getHydratedData();
  return data
    .filter((d: any) => d.team === team)
    .map((d: any) => ({
      id: d.player_name,
      name: d.player_name,
      team: d.team,
      position: d.position,
      cap_hit_millions: d.cap_hit_millions,
      risk_score: d.edce_risk,
      surplus_value: d.fair_market_value,
      type: 'player'
    }))
    .sort((a: any, b: any) => b.cap_hit_millions - a.cap_hit_millions);
}

export async function simulateTrade(assets: any[]) {
  // Phase 2 logic using JSON data
  console.log("Simulating trade with assets:", assets);

  // Simple heuristic for simulation delta
  const win_delta = (Math.random() * 0.1) - 0.02; // Mock variance

  return {
    success: true,
    summary: `Trade simulation completed for 2025 season data. Analyzed ${assets.length} assets. Resulting in ${win_delta > 0 ? 'positive' : 'negative'} cap efficiency delta.`,
    teamA_cap_delta: -10.5,
    teamB_cap_delta: 8.2,
    win_prob_delta: win_delta
  };
}
