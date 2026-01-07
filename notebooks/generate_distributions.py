import pandas as pd
import numpy as np
from pathlib import Path
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Resolve project root whether invoked from repo root or notebooks/
cwd = Path.cwd()
project_root = cwd if (cwd / 'data').exists() else cwd.parent

data_dir = project_root / 'data/processed/compensation'
outputs_dir = project_root / 'notebooks/outputs'
outputs_dir.mkdir(parents=True, exist_ok=True)

print(f"Project root: {project_root}")

# Load datasets
rosters_path = data_dir / 'raw_rosters_2015_2024.csv'
dead_path = data_dir / 'player_dead_money.csv'
rosters_df = pd.read_csv(rosters_path)
dead_df = pd.read_csv(dead_path)

# Parse salaries

def parse_salary(val):
    if pd.isna(val) or val == 'NaN':
        return np.nan
    if isinstance(val, str):
        val = val.replace('$', '').replace(',', '')
        try:
            return float(val)
        except ValueError:
            return np.nan
    return float(val)

rosters_df['Salary_numeric'] = rosters_df['Salary'].apply(parse_salary)
rosters_df['Salary_M'] = rosters_df['Salary_numeric'] / 1e6

# Merge dead money onto rosters by player/team/year
merged = rosters_df.copy()
merged['dead_cap_millions'] = 0.0

for _, row in dead_df.iterrows():
    mask = (
        merged['Player'].str.lower() == row['player_name'].lower()
    ) & (merged['team'] == row['team']) & (merged['year'] == row['year'])
    if mask.any():
        merged.loc[mask, 'dead_cap_millions'] = row['dead_cap_millions']

# Build common bins
years = sorted(merged['year'].dropna().unique())
years = [int(y) for y in years]
min_year, max_year = min(years), max(years)

x_min, x_max = 0.0, 50.0
n_bins = 100
bin_edges = np.linspace(x_min, x_max, n_bins + 1)
bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
bin_width = bin_edges[1] - bin_edges[0]

# Colors: older -> lighter grey, recent -> darker

def grey_for_year(y: int) -> str:
    if max_year == min_year:
        g = 120
    else:
        t = (y - min_year) / (max_year - min_year)
        g = int(220 - t * 140)  # 220 (light) -> 80 (dark)
    return f"rgb({g},{g},{g})"

# Salary probability distributions (normalized density)
with_salary = merged[merged['Salary_M'].notna()].copy()

salary_fig = go.Figure()
for y in years:
    vals = with_salary.loc[with_salary['year'] == y, 'Salary_M'].values
    vals = vals[(vals >= x_min) & (vals <= x_max)]
    if len(vals) == 0:
        continue
    counts, _ = np.histogram(vals, bins=bin_edges)
    if counts.sum() == 0:
        continue
    density = counts / counts.sum() / bin_width
    salary_fig.add_trace(
        go.Scatter(
            x=bin_centers,
            y=density,
            name=str(y),
            mode='lines',
            line=dict(color=grey_for_year(y), width=2),
        )
    )

salary_fig.update_layout(
    title='Salary Probability Distribution by Year (Normalized Density)',
    xaxis_title='Salary Cap Hit ($M)',
    yaxis_title='Probability Density',
    template='plotly_white',
    height=420,
    legend_title_text='Year',
)
salary_fig.write_html(outputs_dir / 'salary_probability_distribution_by_year.html')

# Dead money distribution: sum per salary bin
dead_fig = go.Figure()
for y in years:
    df_y = dead_df[dead_df['year'] == y]
    if df_y.empty:
        continue
    vals_dm = df_y['dead_cap_millions'].values
    # Bin by dead money amount using same X scale as salary (0-50M)
    sums = np.zeros(len(bin_centers))
    idxs = np.clip(np.floor((vals_dm - x_min) / bin_width).astype(int), 0, len(bin_centers) - 1)
    for i, idx in enumerate(idxs):
        val = vals_dm[i]
        if isinstance(val, (int, float)) and np.isfinite(val):
            sums[idx] += float(val)
    dead_fig.add_trace(
        go.Scatter(
            x=bin_centers,
            y=sums,
            name=str(y),
            mode='lines',
            line=dict(color=grey_for_year(y), width=2),
        )
    )

dead_fig.update_layout(
    title='Dead Money Distribution by Year (Sum per Salary Bin)',
    xaxis_title='Salary Cap Hit ($M)',
    yaxis_title='Dead Money Sum ($M)',
    template='plotly_white',
    height=420,
    legend_title_text='Year',
)
dead_fig.write_html(outputs_dir / 'dead_money_distribution_by_year.html')

# Combined figure
comb_fig = make_subplots(
    rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08,
    subplot_titles=(
        'Salary Probability Distribution by Year',
        'Dead Money Distribution by Year (Sum per Salary Bin)'
    )
)
for tr in salary_fig.data:
    comb_fig.add_trace(tr, row=1, col=1)
for tr in dead_fig.data:
    comb_fig.add_trace(tr, row=2, col=1)

comb_fig.update_layout(
    template='plotly_white',
    height=860,
    xaxis=dict(title='Salary Cap Hit ($M)', range=[x_min, x_max]),
    xaxis2=dict(title='Salary Cap Hit ($M)'),
    yaxis=dict(title='Probability Density'),
    yaxis2=dict(title='Dead Money Sum ($M)'),
    legend_title_text='Year',
)
comb_fig.write_html(outputs_dir / 'salary_and_dead_money_distributions.html')

print('Saved outputs:')
print(' -', outputs_dir / 'salary_probability_distribution_by_year.html')
print(' -', outputs_dir / 'dead_money_distribution_by_year.html')
print(' -', outputs_dir / 'salary_and_dead_money_distributions.html')

# --- Additional analysis: Are dead money sums concentrated among lower or higher paid players? ---
# Compute overall dead money share by salary tier across all years
tiers = [0, 2, 5, 10, 20, float('inf')]  # millions
tier_labels = ['0-2M', '2-5M', '5-10M', '10-20M', '20M+']

with_dm_overall = with_salary[with_salary['dead_cap_millions'] > 0].copy()
if with_dm_overall.empty:
    print('\nDead money by salary tier: not available (no matched salary for dead money records).')
else:
    with_dm_overall['salary_tier'] = pd.cut(
        with_dm_overall['Salary_M'], bins=tiers, labels=tier_labels, right=False
    )
    tier_sums = (
        with_dm_overall.groupby('salary_tier')['dead_cap_millions']
        .sum()
        .reindex(tier_labels)
    )
    total_dead = tier_sums.sum()
    tier_share = (tier_sums / total_dead * 100).round(2)
    # Build bar chart for tier shares
    import plotly.express as px
    tier_fig = px.bar(
        tier_share.reset_index(name='share_pct'),
        x='share_pct', y='salary_tier', orientation='h',
        labels={'share_pct': 'Share of Dead Money (%)', 'salary_tier': 'Salary Tier'},
        title='Dead Money Share by Salary Tier (All Years)', template='plotly_white'
    )
    tier_fig.write_html(outputs_dir / 'dead_money_share_by_salary_tier.html')

# Overall dead money sum by common salary bins (aggregate across years)
overall_vals_dm = dead_df['dead_cap_millions'].values
overall_sums = np.zeros(len(bin_centers))
overall_idxs = np.clip(
    np.floor((overall_vals_dm - x_min) / bin_width).astype(int), 0, len(bin_centers) - 1
)
for i, idx in enumerate(overall_idxs):
    val = overall_vals_dm[i]
    if isinstance(val, (int, float)) and np.isfinite(val):
        overall_sums[idx] += float(val)

overall_fig = go.Figure(
    data=[go.Scatter(x=bin_centers, y=overall_sums, mode='lines', line=dict(color='black', width=2))],
)
overall_fig.update_layout(
    title='Dead Money Sum by Amount Bin (All Years)',
    xaxis_title='Salary Cap Hit ($M)',
    yaxis_title='Dead Money Sum ($M)',
    template='plotly_white',
    height=420,
)
overall_fig.write_html(outputs_dir / 'dead_money_sum_by_amount_bin_overall.html')

# Text summary for quick insight
if not with_dm_overall.empty:
    share_above_10 = with_dm_overall.loc[with_dm_overall['Salary_M'] >= 10, 'dead_cap_millions'].sum()
    share_below_10 = with_dm_overall.loc[with_dm_overall['Salary_M'] < 10, 'dead_cap_millions'].sum()
    pct_above_10 = (share_above_10 / (share_above_10 + share_below_10) * 100) if (share_above_10 + share_below_10) > 0 else 0.0
    pct_below_10 = 100 - pct_above_10
    print('\nDead money share by salary tier (%):')
    for tier, pct in tier_share.items():
        print(f' - {tier}: {pct}%')
    print(f"\nDead money above $10M salary: {pct_above_10:.2f}%")
    print(f"Dead money below $10M salary: {pct_below_10:.2f}%")
print('\nAdditional outputs:')
if not with_dm_overall.empty:
    print(' -', outputs_dir / 'dead_money_share_by_salary_tier.html')
print(' -', outputs_dir / 'dead_money_sum_by_amount_bin_overall.html')
