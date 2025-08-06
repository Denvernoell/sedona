# Local Python Environment Setup for Polars + polars-st + Iceberg
# Run this PowerShell script to set up your local Python environment

Write-Host "🚀 Setting up local Python environment for Polars + polars-st + Iceberg..." -ForegroundColor Green

# Check if virtual environment exists, create if not
if (-not (Test-Path "venv")) {
    Write-Host "📦 Creating virtual environment..." -ForegroundColor Yellow
    python -m venv venv
}

# Activate virtual environment
Write-Host "✅ Activating virtual environment..." -ForegroundColor Green
.\venv\Scripts\Activate.ps1

# Upgrade pip
Write-Host "⬆️ Upgrading pip..." -ForegroundColor Yellow
pip install --upgrade pip

Write-Host "📥 Installing core packages..." -ForegroundColor Yellow

# Install core packages
pip install polars==0.20.7
pip install pyarrow==15.0.0
pip install pyiceberg==0.6.1

Write-Host "🗺️ Installing geospatial packages..." -ForegroundColor Yellow

# Install geospatial packages
pip install geopandas==0.14.1
pip install shapely==2.0.2
pip install fiona==1.9.5
pip install pyproj==3.6.1
pip install rtree==1.1.0

Write-Host "📊 Installing visualization packages..." -ForegroundColor Yellow

# Install visualization packages
pip install folium==0.15.1
pip install matplotlib==3.8.2
pip install plotly==5.17.0
pip install contextily==1.4.0

Write-Host "🔧 Installing additional tools..." -ForegroundColor Yellow

# Additional tools
pip install geopy==2.4.1
pip install h3==3.7.6
pip install duckdb==0.9.2
pip install boto3==1.34.0
pip install s3fs==2023.12.2

Write-Host "📓 Installing Jupyter..." -ForegroundColor Yellow

# Jupyter
pip install jupyter==1.0.0
pip install jupyterlab==4.0.9
pip install ipywidgets==8.1.1

Write-Host "🦀 Installing polars-st from GitHub..." -ForegroundColor Yellow

# Install polars-st from GitHub
pip install git+https://github.com/Oreilles/polars-st.git

Write-Host "✅ Installation complete!" -ForegroundColor Green
Write-Host ""
Write-Host "🎯 Next steps:" -ForegroundColor Cyan
Write-Host "1. Start the Iceberg service: docker-compose up -d" -ForegroundColor White
Write-Host "2. Launch Jupyter Lab locally: jupyter lab" -ForegroundColor White
Write-Host "3. Open the notebooks and start analyzing spatial data!" -ForegroundColor White
Write-Host ""
Write-Host "📁 Your Iceberg warehouse will be available at: ./data (shared with Docker)" -ForegroundColor Cyan
Write-Host "🌐 Access Jupyter Lab at: http://localhost:8888" -ForegroundColor Cyan
Write-Host "🐳 Iceberg service status: docker-compose ps" -ForegroundColor Cyan
