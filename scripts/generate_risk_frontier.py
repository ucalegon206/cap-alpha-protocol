
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

DB_PATH = "data/nfl_data.db"
OUTPUT_DIR = "reports/visuals/"

def generate_risk_frontier(year=2023):
    con = duckdb.connect(DB_PATH)
    
    # Query the Gold Layer
    query = f"""
    SELECT 
        player_name, 
        position, 
        cap_hit_millions, 
        edce_risk,
        team
    FROM fact_player_efficiency
    WHERE year = {year} 
      AND cap_hit_millions > 2.0
    """
    
    df = con.execute(query).df()
    con.close()
    
    if df.empty:
        print(f"No data for {year}")
        return

    # Plotting
    plt.figure(figsize=(12, 8))
    sns.set_style("whitegrid")
    
    # Scatter plot
    scatter = sns.scatterplot(
        data=df, 
        x="cap_hit_millions", 
        y="edce_risk", 
        hue="position", 
        size="edce_risk",
        sizes=(50, 400),
        alpha=0.6,
        palette="viridis"
    )
    
    # Label top risks (High Burden + High Regret)
    threshold_x = df["cap_hit_millions"].quantile(0.9)
    threshold_y = df["edce_risk"].quantile(0.9)
    
    top_players = df[(df["cap_hit_millions"] > threshold_x) | (df["edce_risk"] > threshold_y)]
    
    for i, row in top_players.iterrows():
        plt.text(
            row["cap_hit_millions"] + 0.5, 
            row["edce_risk"] + 0.1, 
            f"{row['player_name']} ({row['team']})", 
            fontsize=9, 
            alpha=0.8
        )
        
    # Formatting
    plt.title(f"NFL Risk Frontier: Cap Burden vs. Expected Regret ({year})", fontsize=16, fontweight='bold')
    plt.xlabel("Cap Burden (Cap Hit in $M)", fontsize=12)
    plt.ylabel("Expected Regret (EDCE Risk Score)", fontsize=12)
    plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
    
    # Draw "Disaster Zone" boundary
    plt.axvspan(threshold_x, df["cap_hit_millions"].max() + 5, color='red', alpha=0.05)
    plt.axhspan(threshold_y, df["edce_risk"].max() + 1, color='red', alpha=0.05)
    plt.text(df["cap_hit_millions"].max() - 10, df["edce_risk"].max() - 1, "THE DISASTER ZONE", color='red', alpha=0.5, fontweight='bold')

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = f"{OUTPUT_DIR}risk_frontier_{year}.png"
    plt.tight_layout()
    plt.savefig(out_path, dpi=300)
    print(f"✓ Risk Frontier saved to {out_path}")
    
    return out_path

if __name__ == "__main__":
    generate_risk_frontier(2023)
    generate_risk_frontier(2024)
    generate_risk_frontier(2025)
