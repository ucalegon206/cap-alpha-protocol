#!/bin/bash
set -e

# Activate venv if needed
# source .venv/bin/activate

export PYTHONPATH=.:src
PYTHON=./.venv/bin/python

START=2015
END=2024

echo "Starting Historical Scrape ($START - $END)..."

for year in $(seq $START $END); do
    echo "------------------------------------------------"
    echo "Scraping Year: $year"
    echo "------------------------------------------------"
    $PYTHON src/run_historical_scrape.py --year $year --source spotrac --force
    # Sleep to be nice? Selenium is slow enough not to trigger rate limits usually, but safe side.
    sleep 5
done

echo "Historical Scrape Complete."
