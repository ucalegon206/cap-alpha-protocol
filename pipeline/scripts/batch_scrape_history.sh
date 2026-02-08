#!/bin/bash
# Batch Scrape History (2015-2024)
# Usage: ./scripts/batch_scrape_history.sh

# Ensure strict mode
set -euo pipefail

echo "Starting Historical Contract Scrape (2015-2024)..."

# Active years for Dead Money Analysis
# We go backwards to favor recent data first? Or forward?
# Forward looks more natural.

for year in {2015..2024}
do
    echo "------------------------------------------------"
    echo "Scraping Season: $year"
    echo "------------------------------------------------"
    
    # Run the python module
    # We use the Refactored CLI args
    /Users/andrewsmith/Documents/portfolio/nfl-dead-money/.venv/bin/python -m src.run_contract_details_scrape --year $year
    
    # Sleep to be polite to Spotrac servers
    echo "Sleeping for 10 seconds..."
    sleep 10
done

echo "Batch Scrape Complete."
