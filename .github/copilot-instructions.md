# Copilot Instructions for Apache Sedona Geospatial Workflows

## Project Context
This project uses Apache Sedona for large-scale geospatial data processing with Python. Focus on distributed spatial analytics, GeoDataFrames, and spatial SQL operations.

## Key Libraries and Frameworks
- **Apache Sedona**: Core geospatial processing engine
- **PySpark**: Distributed computing framework
- **Geopandas**: For local geospatial data manipulation
- **Shapely**: Geometric operations
- **Folium/Matplotlib**: Visualization

## Code Style Preferences
- Use type hints for all function parameters and returns
- Prefer functional programming patterns for Spark operations
- Use descriptive variable names for geospatial objects (e.g., `geometry_df`, `spatial_rdd`)
- Follow PEP 8 naming conventions

## Common Patterns

### Sedona Session Setup
```python
from sedona.spark import SedonaContext
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("GeospatialAnalysis") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator") \
    .getOrCreate()

sedona = SedonaContext.create(spark)
```

### Spatial DataFrame Operations
- Always register Sedona SQL functions: `sedona.sql("SELECT ST_Point(x, y) as geometry")`
- Use `ST_` prefixed functions for spatial operations
- Chain spatial transformations efficiently
- Cache intermediate results for complex workflows

### Performance Considerations
- Partition spatial data by spatial proximity when possible
- Use broadcast joins for small reference datasets
- Prefer spatial indexes (R-tree, Quad-tree) for large datasets
- Monitor Spark UI for spatial operation performance

## File Organization
- `/src/spatial/`: Core spatial processing modules
- `/src/etl/`: Data ingestion and transformation
- `/src/analysis/`: Spatial analysis workflows
- `/notebooks/`: Jupyter notebooks for exploration
- `/config/`: Spark and Sedona configuration files

## Testing Patterns
- Use small synthetic geometries for unit tests
- Mock large datasets with representative samples
- Test spatial operations with edge cases (empty geometries, invalid shapes)
- Validate CRS transformations carefully

## Common Spatial Operations to Suggest
- Spatial joins (ST_Intersects, ST_Contains, ST_Within)
- Spatial aggregations (ST_Union, ST_ConvexHull)
- Coordinate transformations (ST_Transform)
- Spatial indexing and partitioning
- Distance calculations (ST_Distance, ST_DWithin)

## Error Handling
- Always validate geometry inputs with ST_IsValid
- Handle CRS mismatches gracefully
- Catch and log Spark SQL spatial function errors
- Provide meaningful error messages for invalid spatial operations
