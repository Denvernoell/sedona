"""
Configuration for Apache Iceberg with Polars and polars-st spatial capabilities
Works with local Python environment and containerized Iceberg service
"""

import polars as pl
from pathlib import Path
import os
import json
from typing import Dict, Any, Optional

try:
    import polars_st as st
    HAS_POLARS_ST = True
except ImportError:
    HAS_POLARS_ST = False
    print("polars-st not installed. Some spatial operations will use fallback methods.")


def create_polars_config() -> Dict[str, Any]:
    """
    Create Polars configuration for spatial operations
    
    Returns:
        Dict: Polars configuration
    """
    return {
        "lazy": True,  # Use lazy evaluation for better performance
        "string_cache": True,  # Enable string cache for better memory usage
        "streaming": True,  # Enable streaming for large datasets
    }


def setup_polars_spatial() -> None:
    """
    Setup Polars with spatial extensions
    """
    # Set Polars configuration for optimal performance
    pl.Config.set_streaming_chunk_size(10_000)
    pl.Config.set_tbl_rows(20)
    pl.Config.set_tbl_cols(10)
    
    if HAS_POLARS_ST:
        print("✅ Polars-ST spatial functions registered")
    else:
        print("⚠️  polars-st not available - using fallback spatial operations")


def get_iceberg_warehouse_path() -> str:
    """
    Get the path to the Iceberg warehouse (shared with Docker container)
    
    Returns:
        str: Path to Iceberg warehouse
    """
    # Use local data directory that's mounted in Docker container
    local_data_path = Path(__file__).parent.parent / "data"
    local_data_path.mkdir(exist_ok=True)
    return str(local_data_path)


def create_iceberg_catalog_config() -> Dict[str, Any]:
    """
    Create Iceberg catalog configuration for local use with Apache Polaris
    
    Returns:
        Dict: Catalog configuration
    """
    warehouse_path = get_iceberg_warehouse_path()
    
    return {
        "type": "rest",  # Use REST catalog (Apache Polaris)
        "uri": "http://localhost:8181/api/catalog",  # Polaris REST API endpoint
        "warehouse": warehouse_path,
        "credential": "polaris-client:polaris-secret",
        "scope": "PRINCIPAL_ROLE:ALL",
        # Fallback to Hadoop catalog if Polaris is not available
        "fallback": {
            "type": "hadoop",
            "warehouse": warehouse_path,
        }
    }


def create_polaris_client_config() -> Dict[str, Any]:
    """
    Create configuration for Apache Polaris REST catalog client
    
    Returns:
        Dict: Polaris client configuration
    """
    return {
        "polaris_url": "http://localhost:8181",
        "client_id": "polaris-client", 
        "client_secret": "polaris-secret",
        "realm": "default-realm",
        "warehouse": get_iceberg_warehouse_path()
    }


def get_iceberg_catalog_config() -> Dict[str, Any]:
    """
    Get Iceberg catalog configuration (legacy function for compatibility)
    
    Returns:
        Dict: Catalog configuration
    """
    return create_iceberg_catalog_config()


def create_polaris_catalog():
    """
    Create a Polaris REST catalog client
    
    Returns:
        Catalog instance or None if not available
    """
    try:
        from pyiceberg.catalog.rest import RestCatalog
        
        config = create_polaris_client_config()
        
        catalog = RestCatalog(
            name="polaris",
            **{
                "uri": f"{config['polaris_url']}/api/catalog",
                "credential": f"{config['client_id']}:{config['client_secret']}",
                "warehouse": config["warehouse"]
            }
        )
        
        print("✅ Connected to Apache Polaris catalog")
        return catalog
        
    except ImportError:
        print("⚠️  PyIceberg not available - cannot connect to Polaris")
        return None
    except Exception as e:
        print(f"❌ Error connecting to Polaris: {e}")
        return None


def create_iceberg_table_with_polaris(
    catalog, 
    namespace: str, 
    table_name: str, 
    df: pl.DataFrame
) -> bool:
    """
    Create an Iceberg table using Apache Polaris catalog
    
    Args:
        catalog: Polaris catalog instance
        namespace: Table namespace
        table_name: Name of the table
        df: Polars DataFrame with data
        
    Returns:
        bool: Success status
    """
    if not catalog:
        print("❌ No catalog available")
        return False
        
    try:
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, StringType, LongType, DoubleType, TimestampType
        
        # Create namespace if it doesn't exist
        try:
            catalog.create_namespace(namespace)
            print(f"✅ Created namespace: {namespace}")
        except Exception:
            print(f"📁 Namespace {namespace} already exists")
        
        # Define schema based on DataFrame
        fields = []
        field_id = 1
        
        for col, dtype in zip(df.columns, df.dtypes):
            if dtype == pl.Int64 or dtype == pl.Int32:
                field_type = LongType()
            elif dtype == pl.Float64 or dtype == pl.Float32:
                field_type = DoubleType()
            elif dtype == pl.Utf8:
                field_type = StringType()
            elif dtype == pl.Datetime:
                field_type = TimestampType()
            else:
                field_type = StringType()  # Default to string
            
            fields.append(NestedField(field_id, col, field_type, required=False))
            field_id += 1
        
        schema = Schema(*fields)
        
        # Create table
        full_table_name = f"{namespace}.{table_name}"
        table = catalog.create_table(full_table_name, schema)
        
        print(f"✅ Created Iceberg table: {full_table_name}")
        
        # Write data to table
        arrow_table = df.to_arrow()
        table.append(arrow_table)
        
        print(f"✅ Data written to table: {len(df)} rows")
        return True
        
    except Exception as e:
        print(f"❌ Error creating table: {e}")
        return False


def list_polaris_tables(catalog) -> list:
    """
    List all tables in the Polaris catalog
    
    Args:
        catalog: Polaris catalog instance
        
    Returns:
        list: List of table identifiers
    """
    if not catalog:
        return []
        
    try:
        tables = []
        namespaces = catalog.list_namespaces()
        
        for namespace in namespaces:
            namespace_tables = catalog.list_tables(namespace)
            tables.extend(namespace_tables)
            
        return tables
    except Exception as e:
        print(f"❌ Error listing tables: {e}")
        return []


def create_spatial_iceberg_table_example(catalog: Any, table_name: str = "spatial_data.locations") -> pl.DataFrame:
    """
    Example of creating a spatial Iceberg table using Polars
    
    Args:
        catalog: Iceberg catalog instance
        table_name: Name of the table to create
    """
    # Create sample spatial data with Polars
    df = pl.DataFrame({
        "id": [1, 2, 3, 4],
        "name": ["San Francisco", "New York", "Chicago", "Los Angeles"],
        "longitude": [-122.4194, -74.0060, -87.6298, -118.2437],
        "latitude": [37.7749, 40.7128, 41.8781, 34.0522],
        "category": ["urban", "urban", "urban", "urban"],
        "population": [884363, 8336817, 2746388, 3898747]
    })
    
    # Create spatial geometries using polars-st
    spatial_df = df.with_columns([
        st.from_xy("longitude", "latitude").alias("geometry")
    ])
    
    # Write to Iceberg table (this would require additional Iceberg integration)
    print(f"Created spatial dataframe with {len(spatial_df)} rows")
    return spatial_df


def spatial_query_example(df: pl.DataFrame) -> pl.DataFrame:
    """
    Example spatial queries using Polars and polars-st
    
    Args:
        df: Polars DataFrame with spatial data
        
    Returns:
        pl.DataFrame: Results of spatial operations
    """
    # Create a reference point (San Francisco area)
    reference_point = st.point(-122.0, 37.0)
    
    # Perform spatial operations using polars-st
    result = df.with_columns([
        # Convert geometry to WKT text
        st.to_wkt(pl.col("geometry")).alias("wkt_geometry"),
        # Extract coordinates
        st.x(pl.col("geometry")).alias("longitude"),
        st.y(pl.col("geometry")).alias("latitude"),
        # Calculate distance from reference point
        st.distance(pl.col("geometry"), reference_point).alias("distance_from_sf")
    ]).filter(
        # Filter points within approximately 100 units (degrees) 
        pl.col("distance_from_sf") < 10.0
    ).sort("distance_from_sf")
    
    return result


def advanced_spatial_operations(df: pl.DataFrame) -> pl.DataFrame:
    """
    Advanced spatial operations using polars-st
    
    Args:
        df: Polars DataFrame with spatial data
        
    Returns:
        pl.DataFrame: Results with advanced spatial calculations
    """
    # Create buffer zones around points
    buffered_df = df.with_columns([
        st.buffer(pl.col("geometry"), 0.5).alias("buffer_zone"),
        st.area(st.buffer(pl.col("geometry"), 0.5)).alias("buffer_area")
    ])
    
    # Create convex hull of all points
    all_points = df.select("geometry").to_series()
    convex_hull = st.convex_hull(st.collect(all_points))
    
    # Check if points are within the convex hull
    result = buffered_df.with_columns([
        st.within(pl.col("geometry"), convex_hull).alias("within_convex_hull"),
        st.contains(pl.col("buffer_zone"), pl.col("geometry")).alias("buffer_contains_point")
    ])
    
    return result


def load_spatial_data_from_file(file_path: str) -> pl.DataFrame:
    """
    Load spatial data from various file formats
    
    Args:
        file_path: Path to the spatial data file
        
    Returns:
        pl.DataFrame: Loaded spatial data
    """
    file_extension = Path(file_path).suffix.lower()
    
    if file_extension == '.parquet':
        df = pl.read_parquet(file_path)
    elif file_extension == '.csv':
        df = pl.read_csv(file_path)
    elif file_extension in ['.geojson', '.json']:
        # For GeoJSON, you might need to use geopandas first then convert
        import geopandas as gpd
        gdf = gpd.read_file(file_path)
        df = pl.from_pandas(gdf)
        # Convert geometry column if present
        if 'geometry' in df.columns:
            df = df.with_columns([
                st.from_wkt(pl.col("geometry").cast(pl.Utf8)).alias("geometry")
            ])
    else:
        raise ValueError(f"Unsupported file format: {file_extension}")
    
    return df


def write_spatial_data_to_iceberg(df: pl.DataFrame, table_name: str, warehouse_path: str = "/tmp/warehouse") -> None:
    """
    Write spatial data to Iceberg table format
    
    Args:
        df: Polars DataFrame with spatial data
        table_name: Name of the Iceberg table
        warehouse_path: Path to Iceberg warehouse
    """
    # Convert to Arrow for Iceberg compatibility
    arrow_table = df.to_arrow()
    
    # This would require proper Iceberg integration
    # For now, we'll write as Parquet with partitioning
    output_path = Path(warehouse_path) / table_name
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Write as partitioned Parquet files (Iceberg-compatible format)
    df.write_parquet(
        output_path / "data.parquet",
        compression="snappy"
    )
    
    print(f"Spatial data written to {output_path}")


# Example usage configuration
ICEBERG_WAREHOUSE_PATH = "/tmp/warehouse"
METASTORE_URI = "thrift://localhost:9083"

# Polars configuration for optimal spatial performance
POLARS_CONFIG = {
    "streaming_chunk_size": 10_000,
    "string_cache": True,
    "lazy_evaluation": True,
    "parallel_execution": True,
    "memory_efficient": True
}


# Spatial operation examples
SPATIAL_OPERATIONS = [
    "st.point(x, y)",
    "st.from_xy(x_col, y_col)", 
    "st.from_wkt(wkt_string)",
    "st.to_wkt(geometry)",
    "st.distance(geom1, geom2)",
    "st.buffer(geometry, distance)",
    "st.area(geometry)",
    "st.within(geom1, geom2)",
    "st.contains(geom1, geom2)",
    "st.intersects(geom1, geom2)",
    "st.convex_hull(geometry)",
    "st.centroid(geometry)"
]


def check_dependencies() -> Dict[str, bool]:
    """
    Check if required dependencies are available
    
    Returns:
        Dict[str, bool]: Status of each dependency
    """
    status = {}
    
    # Check Polars
    try:
        import polars
        status["polars"] = True
    except ImportError:
        status["polars"] = False
    
    # Check polars-st
    try:
        import polars_st
        status["polars_st"] = True
    except ImportError:
        status["polars_st"] = False
    
    # Check PyIceberg
    try:
        import pyiceberg
        status["pyiceberg"] = True
    except ImportError:
        status["pyiceberg"] = False
    
    # Check Docker connection
    import subprocess
    try:
        result = subprocess.run(
            ["docker", "compose", "ps", "--services"], 
            capture_output=True, 
            text=True, 
            timeout=5
        )
        status["docker_compose"] = result.returncode == 0
    except:
        status["docker_compose"] = False
    
    # Check Apache Polaris availability
    try:
        import requests
        response = requests.get("http://localhost:8181/management/v1/config", timeout=5)
        status["polaris"] = response.status_code == 200
    except:
        status["polaris"] = False
    
    return status


def print_system_status():
    """
    Print system status and dependency information
    """
    print("🔧 System Status Check")
    print("=" * 60)
    
    deps = check_dependencies()
    
    for dep, available in deps.items():
        status = "✅" if available else "❌"
        print(f"{status} {dep}: {'Available' if available else 'Not Available'}")
    
    print(f"\n📁 Warehouse Path: {get_iceberg_warehouse_path()}")
    print("🐳 Docker Services:")
    print("   • Iceberg: http://localhost:8080")
    print("   • Apache Polaris: http://localhost:8181")
    
    if deps.get("polaris", False):
        print("\n🌟 Apache Polaris Features Available:")
        print("   • REST Catalog API")
        print("   • Table versioning and time travel")
        print("   • Multi-tenant catalog management")
        print("   • RBAC (Role-Based Access Control)")
    
    if deps.get("polars_st", False):
        print("\n🗺️ Spatial Operations Available:")
        for op in SPATIAL_OPERATIONS:
            print(f"   • {op}")


def get_polaris_status() -> Dict[str, Any]:
    """
    Get detailed status of Apache Polaris service
    
    Returns:
        Dict: Polaris status information
    """
    try:
        import requests
        
        # Check management endpoint
        management_url = "http://localhost:8181/management/v1/config"
        response = requests.get(management_url, timeout=5)
        
        if response.status_code == 200:
            config_data = response.json()
            
            # Check catalog endpoint
            catalog_url = "http://localhost:8181/api/catalog/v1/config"
            catalog_response = requests.get(catalog_url, timeout=5)
            
            return {
                "available": True,
                "management_endpoint": management_url,
                "catalog_endpoint": catalog_url,
                "catalog_accessible": catalog_response.status_code == 200,
                "config": config_data
            }
        else:
            return {
                "available": False,
                "error": f"Management endpoint returned {response.status_code}"
            }
            
    except Exception as e:
        return {
            "available": False,
            "error": str(e)
        }
