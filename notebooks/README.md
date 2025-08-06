# Apache Sedona Example Notebooks

This directory contains Jupyter notebooks demonstrating various Apache Sedona geospatial workflows.

## Notebooks Overview

### 01_sedona_setup_basics.ipynb
- Sedona session initialization
- Basic spatial operations (ST_Point, ST_Distance)
- Geometry validation
- Sample data creation

### 02_spatial_joins_analysis.ipynb
- Spatial joins (ST_Contains, ST_Intersects)
- Buffer operations (ST_Buffer)
- Spatial aggregations (ST_ConvexHull, ST_Union)
- Performance optimization techniques

### 03_coordinate_transformations.ipynb
- CRS transformations (WGS84, Web Mercator, UTM)
- Distance calculations in different projections
- UTM zone selection
- Transformation validation

## Getting Started

1. Ensure Apache Sedona is properly installed and configured
2. Start Jupyter Lab/Notebook in this directory
3. Run notebooks in numerical order for progressive learning
4. Modify sample data to experiment with your own datasets

## Prerequisites

```python
# Required libraries
from sedona.spark import SedonaContext
from pyspark.sql import SparkSession
import pandas as pd
import geopandas as gpd  # Optional, for local operations
```

## Common Patterns Used

- Type hints for all functions
- Descriptive variable names (geometry_df, spatial_rdd)
- Functional programming patterns for Spark operations
- Error handling with geometry validation
- Performance optimization with caching

## Next Steps

- Explore larger datasets from your domain
- Implement spatial indexing for performance
- Add visualization with Folium or Matplotlib
- Create custom spatial analysis workflows
