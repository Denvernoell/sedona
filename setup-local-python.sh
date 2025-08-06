#!/bin/bash

# Local Python Environment Setup for Polars + polars-st + Iceberg
# Run this script to set up your local Python environment

echo "🚀 Setting up local Python environment for Polars + polars-st + Iceberg..."

# Check if virtual environment exists, create if not
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python -m venv venv
fi

# Activate virtual environment
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
    # Windows
    source venv/Scripts/activate
else
    # macOS/Linux
    source venv/bin/activate
fi

echo "✅ Virtual environment activated"

# Upgrade pip
pip install --upgrade pip

echo "📥 Installing core packages..."

# Install core packages
pip install polars==0.20.7
pip install pyarrow==15.0.0
pip install pyiceberg==0.6.1

echo "🗺️  Installing geospatial packages..."

# Install geospatial packages
pip install geopandas==0.14.1
pip install shapely==2.0.2
pip install fiona==1.9.5
pip install pyproj==3.6.1
pip install rtree==1.1.0

echo "📊 Installing visualization packages..."

# Install visualization packages
pip install folium==0.15.1
pip install matplotlib==3.8.2
pip install plotly==5.17.0
pip install contextily==1.4.0

echo "🔧 Installing additional tools..."

# Additional tools
pip install geopy==2.4.1
pip install h3==3.7.6
pip install duckdb==0.9.2
pip install boto3==1.34.0
pip install s3fs==2023.12.2

echo "📓 Installing Jupyter..."

# Jupyter
pip install jupyter==1.0.0
pip install jupyterlab==4.0.9
pip install ipywidgets==8.1.1

echo "🦀 Installing polars-st from GitHub..."

# Install polars-st from GitHub
pip install git+https://github.com/Oreilles/polars-st.git

echo "✅ Installation complete!"
echo ""
echo "🎯 Next steps:"
echo "1. Start the Iceberg service: docker-compose up -d"
echo "2. Launch Jupyter Lab locally: jupyter lab"
echo "3. Open the notebooks and start analyzing spatial data!"
echo ""
echo "📁 Your Iceberg warehouse will be available at: ./data (shared with Docker)"
echo "🌐 Access Jupyter Lab at: http://localhost:8888"
echo "🐳 Iceberg service status: docker-compose ps"
