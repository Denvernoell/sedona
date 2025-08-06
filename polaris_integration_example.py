#!/usr/bin/env python3
"""
Complete Polaris + Polars + polars-st Integration Example

This script demonstrates the full workflow:
1. Apache Polaris catalog management
2. High-performance spatial operations with Polars  
3. Iceberg table creation and management
4. Spatial analysis with polars-st

Run after: docker-compose up -d && python test_services.py
"""

import sys
import traceback
from pathlib import Path

# Add config to path
sys.path.append(str(Path(__file__).parent / "config"))

def create_sample_spatial_data():
    """Create sample spatial data using Polars + polars-st"""
    try:
        import polars as pl
        import polars_st as st
        
        print("🗺️ Creating sample spatial data with Polars...")
        
        # Create sample cities data
        cities_df = pl.DataFrame({
            "city_id": [1, 2, 3, 4, 5],
            "city_name": [
                "San Francisco", "New York", "Chicago", 
                "Los Angeles", "Seattle"
            ],
            "longitude": [-122.4194, -74.0060, -87.6298, -118.2437, -122.3321],
            "latitude": [37.7749, 40.7128, 41.8781, 34.0522, 47.6062],
            "population": [874961, 8336817, 2693976, 3979576, 753675],
            "area_km2": [121.4, 783.8, 606.1, 1302.0, 369.2]
        }).with_columns([
            # Create Point geometries from coordinates
            st.from_xy("longitude", "latitude").alias("geometry"),
            # Calculate population density
            (pl.col("population") / pl.col("area_km2")).alias("density_per_km2")
        ])
        
        print(f"✅ Created DataFrame with {len(cities_df)} cities")
        return cities_df
        
    except ImportError as e:
        print(f"❌ Missing Polars or polars-st: {e}")
        print("💡 Run: pip install polars 'polars-st @ git+https://github.com/Oreilles/polars-st'")
        return None
    except Exception as e:
        print(f"❌ Error creating spatial data: {e}")
        return None


def perform_spatial_analysis(df):
    """Perform spatial analysis using polars-st"""
    try:
        import polars as pl
        import polars_st as st
        
        print("\n🔍 Performing spatial analysis...")
        
        # Reference point (approximate center of continental US)
        reference_point = st.point(-98.5795, 39.8283)
        
        # Perform spatial operations
        analysis_result = df.with_columns([
            # Convert to WKT for display
            st.to_wkt(pl.col("geometry")).alias("wkt_geometry"),
            # Extract coordinates
            st.x(pl.col("geometry")).alias("lon"),
            st.y(pl.col("geometry")).alias("lat"), 
            # Calculate distance from reference point
            st.distance(pl.col("geometry"), reference_point).alias("distance_from_center"),
            # Create buffer zones (0.5 degree radius)
            st.buffer(pl.col("geometry"), 0.5).alias("buffer_zone")
        ]).with_columns([
            # Calculate buffer area
            st.area(pl.col("buffer_zone")).alias("buffer_area"),
            # Classification based on distance from center
            pl.when(pl.col("distance_from_center") < 10.0)
            .then(pl.lit("Central"))
            .when(pl.col("distance_from_center") < 20.0)
            .then(pl.lit("Regional"))
            .otherwise(pl.lit("Coastal"))
            .alias("location_category")
        ])
        
        print("✅ Spatial analysis complete!")
        
        # Display results
        print("\n📊 Analysis Results:")
        result_summary = analysis_result.select([
            "city_name", "population", "density_per_km2", 
            "distance_from_center", "location_category", "buffer_area"
        ]).sort("distance_from_center")
        
        print(result_summary)
        return analysis_result
        
    except Exception as e:
        print(f"❌ Error in spatial analysis: {e}")
        traceback.print_exc()
        return None


def test_polaris_integration(df):
    """Test Apache Polaris catalog integration"""
    try:
        from iceberg_spatial_config import (
            create_polaris_catalog,
            create_iceberg_table_with_polaris,
            list_polaris_tables,
            get_polaris_status
        )
        
        print("\n🌟 Testing Apache Polaris integration...")
        
        # Check Polaris status first
        polaris_status = get_polaris_status()
        if not polaris_status.get("available", False):
            print(f"❌ Polaris not available: {polaris_status.get('error', 'Unknown error')}")
            print("💡 Make sure Docker services are running: docker-compose up -d")
            return False
        
        print("✅ Apache Polaris is available")
        
        # Connect to Polaris catalog
        catalog = create_polaris_catalog()
        if not catalog:
            print("❌ Failed to connect to Polaris catalog")
            return False
            
        print("✅ Connected to Polaris catalog")
        
        # Create table in Polaris
        success = create_iceberg_table_with_polaris(
            catalog=catalog,
            namespace="spatial_demo",
            table_name="cities_analysis", 
            df=df
        )
        
        if success:
            print("✅ Successfully created table in Polaris!")
            
            # List tables
            tables = list_polaris_tables(catalog)
            print(f"📋 Available tables: {tables}")
            return True
        else:
            print("❌ Failed to create table in Polaris")
            return False
            
    except Exception as e:
        print(f"❌ Error testing Polaris: {e}")
        traceback.print_exc()
        return False


def main():
    """Run the complete integration example"""
    print("🚀 Polaris + Polars + polars-st Integration Example")
    print("=" * 60)
    
    # Step 1: Create spatial data
    spatial_df = create_sample_spatial_data()
    if spatial_df is None:
        print("\n❌ Failed to create spatial data")
        return
    
    # Step 2: Perform spatial analysis  
    analysis_df = perform_spatial_analysis(spatial_df)
    if analysis_df is None:
        print("\n❌ Failed to perform spatial analysis")
        return
        
    # Step 3: Test Polaris integration
    polaris_success = test_polaris_integration(analysis_df)
    
    # Final summary
    print(f"\n🎯 Integration Test Summary:")
    print(f"   ✅ Spatial Data Creation: Success")
    print(f"   ✅ Spatial Analysis (polars-st): Success") 
    print(f"   {'✅' if polaris_success else '❌'} Polaris Integration: {'Success' if polaris_success else 'Failed'}")
    
    if polaris_success:
        print(f"\n🎉 Complete integration successful!")
        print(f"\n🔗 Next Steps:")
        print(f"   • Open notebooks/ for interactive examples")
        print(f"   • Check Polaris UI: http://localhost:8181")
        print(f"   • Explore Iceberg service: http://localhost:8080")
        print(f"   • Run system status: python -c \"from config.iceberg_spatial_config import print_system_status; print_system_status()\"")
    else:
        print(f"\n⚠️ Polaris integration had issues - check service status")
        print(f"💡 Run: python test_services.py")


if __name__ == "__main__":
    main()
