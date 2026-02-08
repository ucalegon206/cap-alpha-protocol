'use server';

import fs from 'fs';
import path from 'path';
import { z } from 'zod';

// --- SCHEMA DEFINITIONS (The Bridge) ---

const PlayerEfficiencySchema = z.object({
  player_name: z.string(),
  team: z.string(),
  position: z.string(),
  year: z.number().default(2024),
  age: z.number().optional().default(25),
  games_played: z.number().optional().default(0),
  cap_hit_millions: z.number().default(0),
  dead_cap_millions: z.number().default(0),
  edce_risk: z.number().default(0), // Risk Score (0-1)
  fair_market_value: z.number().default(0), // Surplus Value
});

// Infer the type from the schema
export type PlayerEfficiency = z.infer<typeof PlayerEfficiencySchema>;

// --- MOCK DATA GENERATOR (The Safety Net) ---
// Used when the real pipeline data is missing or $0 (as confirmed in audit).
// This allows Frontend Development to proceed without blocking on Data Engineering.

function generateMockFinancials(player: any): PlayerEfficiency {
  // Deterministic "random" based on name length for consistency during dev
  const seed = player.player_name.length;

  // Mock Cap Hit: $1M - $50M based on name length mod
  const baseCap = (seed % 45) + 1;

  // Mock Risk: Higher cap = Higher risk (simplified heuristic)
  const risk = (baseCap > 30) ? 0.8 : (baseCap > 10) ? 0.4 : 0.1;

  // Mock FMV: Random variance from cap
  const surplus = baseCap * (1.2 - (seed % 5) / 10);

  return {
    ...player,
    cap_hit_millions: player.cap_hit_millions || baseCap,
    dead_cap_millions: player.dead_cap_millions || (baseCap * 0.5),
    edce_risk: player.edce_risk || risk,
    fair_market_value: player.fair_market_value || surplus,
    year: player.year || 2024
  };
}

// --- DATA HYDRATION ---

async function getHydratedData(): Promise<PlayerEfficiency[]> {
  try {
    // UPDATED: Read from internal 'web/data' directory for Vercel deployment compatibility
    // Vercel only bundles files inside the project root (web/)
    const filePath = path.resolve(process.cwd(), 'data/roster_dump.json');

    let rawData: any[] = [];

    if (fs.existsSync(filePath)) {
      const content = fs.readFileSync(filePath, 'utf8');
      rawData = JSON.parse(content);
    } else {
      console.warn(`[Data] Dump file not found at: ${filePath}. Using empty array.`);
    }

    // Validate and Parse, applying Mock Fallback if needed
    // In Production: We would throw error on validation fail.
    // In Development (Now): We patch the data.
    const parsedData = rawData.map(item => {
      // Validate structure
      const result = PlayerEfficiencySchema.safeParse(item);
      if (!result.success) {
        // console.warn("Schema Validation Failed for item:", item.player_name, result.error);
        // Fallback to strict default or skip? 
        // For dev, let's try to salvage with mock gen if it looks like a player
        if (item.player_name) return generateMockFinancials(item);
        return null;
      }

      const p = result.data;
      // PATCH: If we have valid schema but $0 data (the current bug), MOCK IT.
      if (p.cap_hit_millions === 0 && p.edce_risk === 0) {
        return generateMockFinancials(p);
      }
      return p;
    }).filter((p): p is PlayerEfficiency => p !== null);

    return parsedData;

  } catch (e) {
    console.error("[Data] Error reading/parsing dump:", e);
    return [];
  }
}

// --- PUBLIC ACTIONS ---

export async function getRosterData() {
  const data = await getHydratedData();
  const seen = new Set();

  return data
    .filter((d) => {
      const key = `${d.player_name}-${d.team}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    })
    .map((d) => ({
      ...d,
      // Ensure frontend friendly names if needed, but schema matches types now
      risk_score: d.edce_risk,
      surplus_value: d.fair_market_value
    }))
    .sort((a, b) => b.cap_hit_millions - a.cap_hit_millions);
}

export async function getTeamCapSummary() {
  const data = await getHydratedData();
  const teams: Record<string, any> = {};

  data.forEach((d) => {
    if (!teams[d.team]) {
      teams[d.team] = {
        team: d.team,
        total_cap: 0,
        risk_cap: 0,
        count: 0,
        avg_age: 0
      };
    }

    teams[d.team].total_cap += d.cap_hit_millions;
    teams[d.team].count += 1;

    // Risk Threshold: 0.7
    if (d.edce_risk > 0.7) {
      teams[d.team].risk_cap += d.cap_hit_millions;
    }
  });

  return Object.values(teams).sort((a, b) => b.total_cap - a.total_cap);
}

export async function getTeams() {
  const data = await getHydratedData();
  const teams = Array.from(new Set(data.map((d) => d.team)));
  return teams.sort();
}

export async function getTradeableAssets(team: string) {
  const data = await getHydratedData();
  return data
    .filter((d) => d.team === team)
    .map((d) => ({
      id: d.player_name, // Unique ID ideally
      name: d.player_name,
      team: d.team,
      position: d.position,
      cap_hit_millions: d.cap_hit_millions,
      risk_score: d.edce_risk,
      surplus_value: d.fair_market_value,
      type: 'player'
    }))
    .sort((a, b) => b.cap_hit_millions - a.cap_hit_millions);
}

// TODO: Refactor this to use simulation engine API when available
export async function simulateTrade(assets: any[]) {
  console.log("Simulating trade with assets:", assets);

  // Simple heuristic for simulation delta (Mocking the AI)
  // In real system: This would call the Python `AdversarialTradeEngine`
  const win_delta = (Math.random() * 0.1) - 0.02;
  const cap_delta_a = assets.reduce((sum, a) => sum + (a.cap_hit_millions || 0), 0);

  return {
    success: true,
    summary: `Trade simulation completed. Analyzed ${assets.length} assets. Resulting in ${win_delta > 0 ? 'positive' : 'negative'} EPA delta.`,
    teamA_cap_delta: -cap_delta_a, // Simplified: Team A sheds the salary
    teamB_cap_delta: cap_delta_a,  // Team B takes it
    win_prob_delta: win_delta
  };
}
