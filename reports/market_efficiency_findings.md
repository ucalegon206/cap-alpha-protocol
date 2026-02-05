# Market Efficiency Audit: The Disciplinary Pricing Test

## Test 1: Does the Market 'Price In' Discipline?
**Hypothesis**: High previous penalties should correlate with LOWER salary (Negative Coefficient).

**Observations**: 4186
**R-squared**: 0.053

| Feature | Coefficient | P-Value | Interpretation |
|---|---|---|---|
| prev_games | 0.0154 | 0.0000 | Significant |
| total_production_yards | 0.0004 | 0.0000 | Significant |
| experience_years | 0.0000 | nan | Not Significant |
| prev_penalty_yards | 0.0051 | 0.0000 | Significant |

**Conclusion**: ❌ **Market is INEFFICIENT**: Teams DO NOT significantly discount salary for disciplinary issues.

## Test 2: Do Penalties Predict Future Decline?
**Hypothesis**: High penalties predict a steeper decline in availability (Negative Games Change).

| Feature | Coefficient | P-Value |
|---|---|---|
| Total Penalty Yards | -0.1249 | 0.0000 |

**Conclusion**: ✅ **True Risk Vector**: High penalties are a leading indicator of availability decline.

## Strategic Synthesis
**💎 THE SMOKING GUN FOUND**: Penalties predict decline, but the market DOES NOT price them in. This is a massive arbitrage opportunity.