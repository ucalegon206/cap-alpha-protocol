#!/bin/bash
set -e

echo "🏈 Setting up NFL Dead Money Environment..."

# Check Python version
python3 -c "import sys; assert sys.version_info >= (3,9), 'Python 3.9+ required'"

# Create venv if not exists
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
else
    echo "📦 Virtual environment already exists."
fi

# Activate venv
source venv/bin/activate

# Install dependencies
echo "⬇️ Installing fixed dependencies..."
pip install --upgrade pip
pip install -r requirements-frozen.txt

echo "✅ Environment Ready!"
echo "👉 To activate: source venv/bin/activate"
echo "👉 To run: python src/run_trade_sim.py"
