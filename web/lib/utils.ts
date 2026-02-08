import { type ClassValue, clsx } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
    return twMerge(clsx(inputs))
}

export const NFL_TEAMS: Record<string, 'AFC' | 'NFC'> = {
    // AFC East
    'BUF': 'AFC', 'MIA': 'AFC', 'NE': 'AFC', 'NYJ': 'AFC',
    // AFC North
    'BAL': 'AFC', 'CIN': 'AFC', 'CLE': 'AFC', 'PIT': 'AFC',
    // AFC South
    'HOU': 'AFC', 'IND': 'AFC', 'JAX': 'AFC', 'TEN': 'AFC',
    // AFC West
    'DEN': 'AFC', 'KC': 'AFC', 'LV': 'AFC', 'LAC': 'AFC',
    'KAN': 'AFC', 'LVR': 'AFC', // Normalize variants

    // NFC East
    'DAL': 'NFC', 'NYG': 'NFC', 'PHI': 'NFC', 'WAS': 'NFC',
    // NFC North
    'CHI': 'NFC', 'DET': 'NFC', 'GB': 'NFC', 'MIN': 'NFC',
    'GNB': 'NFC', // Normalize variants
    // NFC South
    'ATL': 'NFC', 'CAR': 'NFC', 'NO': 'NFC', 'TB': 'NFC',
    'NOR': 'NFC', 'TAM': 'NFC', // Normalize variants
    // NFC West
    'ARI': 'NFC', 'LAR': 'NFC', 'SF': 'NFC', 'SEA': 'NFC',
    'SFO': 'NFC', // Normalize variants
}

export function formatCurrency(amount: number): string {
    return new Intl.NumberFormat("en-US", {
        style: "currency",
        currency: "USD",
        maximumFractionDigits: 1,
    }).format(amount) + "M"
}

export function getRiskColor(risk: number): string {
    if (risk > 0.7) return "text-rose-500"
    if (risk < 0.3) return "text-emerald-500"
    return "text-amber-500" // Use amber for middle ground
}
