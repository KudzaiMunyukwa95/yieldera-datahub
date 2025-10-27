"""
Spatial and temporal reduction utilities for GEE operations
"""

import ee
from typing import Literal, Dict, Any, Optional
from shapely import wkt
from shapely.geometry import Point, Polygon, LineString, MultiPoint
import json


# Constants
NODATA_VALUE = -999
CHIRPS_SCALE = 5566  # ~5km native resolution
ERA5_SCALE = 11132   # ~11km native resolution


def get_spatial_reducer(stat: Literal["mean", "median", "sum", "min", "max"]) -> ee.Reducer:
    """Get Earth Engine reducer for spatial aggregation"""
    reducers = {
        "mean": ee.Reducer.mean(),
        "median": ee.Reducer.median(),
        "sum": ee.Reducer.sum(),
        "min": ee.Reducer.min(),
        "max": ee.Reducer.max()
    }
    return reducers.get(stat, ee.Reducer.mean())


def parse_geometry(geom_input: Dict[str, Any]) -> ee.Geometry:
    """
    Parse geometry input and convert to ee.Geometry
    
    Args:
        geom_input: Dictionary with type, lat/lon or wkt, and optional buffer
        
    Returns:
        ee.Geometry object
        
    Raises:
        ValueError: If geometry is invalid
    """
    geom_type = geom_input.get("type")
    buffer_m = geom_input.get("buffer_m", 0)
    
    if geom_type == "point":
        lat = geom_input["lat"]
        lon = geom_input["lon"]
        geom = ee.Geometry.Point([lon, lat])
        
        if buffer_m > 0:
            geom = geom.buffer(buffer_m)
            
    elif geom_type == "wkt":
        wkt_string = geom_input["wkt"]
        
        # Validate with shapely
        try:
            shapely_geom = wkt.loads(wkt_string)
        except Exception as e:
            raise ValueError(f"Invalid WKT string: {e}")
        
        # Apply buffer if needed (before converting to GEE)
        if buffer_m > 0 and isinstance(shapely_geom, (LineString, MultiPoint, Point)):
            shapely_geom = shapely_geom.buffer(buffer_m / 111320)  # Convert m to degrees roughly
        
        # Convert to GeoJSON then to ee.Geometry
        geojson = json.loads(json.dumps(shapely_geom.__geo_interface__))
        geom = ee.Geometry(geojson)
        
    else:
        raise ValueError(f"Unsupported geometry type: {geom_type}")
    
    return geom


def get_geometry_summary(geom_input: Dict[str, Any]) -> Dict[str, Any]:
    """
    Get summary information about a geometry
    
    Args:
        geom_input: Geometry input dictionary
        
    Returns:
        Dictionary with geometry summary
    """
    if geom_input.get("type") == "point":
        lat = geom_input["lat"]
        lon = geom_input["lon"]
        buffer_m = geom_input.get("buffer_m", 0)
        
        if buffer_m > 0:
            # Approximate area of buffered point
            area_km2 = (3.14159 * (buffer_m ** 2)) / 1_000_000
            return {
                "type": "BufferedPoint",
                "centroid": [lon, lat],
                "area_km2": round(area_km2, 4)
            }
        else:
            return {
                "type": "Point",
                "centroid": [lon, lat],
                "area_km2": None
            }
    
    elif geom_input.get("type") == "wkt":
        wkt_string = geom_input["wkt"]
        shapely_geom = wkt.loads(wkt_string)
        
        # Apply buffer if specified
        buffer_m = geom_input.get("buffer_m", 0)
        if buffer_m > 0 and isinstance(shapely_geom, (LineString, MultiPoint, Point)):
            shapely_geom = shapely_geom.buffer(buffer_m / 111320)
        
        centroid = shapely_geom.centroid
        # Rough area calculation in km² (assuming lat/lon in degrees)
        area_deg2 = shapely_geom.area
        area_km2 = area_deg2 * (111.32 ** 2)  # Rough conversion at equator
        
        return {
            "type": shapely_geom.geom_type,
            "centroid": [centroid.x, centroid.y],
            "area_km2": round(area_km2, 4) if area_km2 > 0.0001 else None
        }
    
    return {
        "type": "Unknown",
        "centroid": [0, 0],
        "area_km2": None
    }


def reduce_image_over_region(
    image: ee.Image,
    geometry: ee.Geometry,
    reducer: ee.Reducer,
    scale: int,
    band_name: str
) -> Optional[float]:
    """
    Reduce an image over a region with proper error handling
    
    Args:
        image: ee.Image to reduce
        geometry: ee.Geometry region
        reducer: ee.Reducer to use
        scale: Scale in meters
        band_name: Band name to extract
        
    Returns:
        Reduced value or None if no data
    """
    try:
        result = image.reduceRegion(
            reducer=reducer,
            geometry=geometry,
            scale=scale,
            maxPixels=1e13,
            bestEffort=True
        ).get(band_name)
        
        # Handle None/null values
        value = result.getInfo() if result else None
        
        # Return NODATA_VALUE for None or invalid values
        if value is None or (isinstance(value, (int, float)) and (value < -900 or value > 1e6)):
            return NODATA_VALUE
            
        return float(value)
        
    except Exception as e:
        print(f"Warning: Error reducing image: {e}")
        return NODATA_VALUE


def sample_image_at_point(
    image: ee.Image,
    point: ee.Geometry.Point,
    scale: int,
    band_name: str
) -> Optional[float]:
    """
    Sample an image at a point location
    
    Args:
        image: ee.Image to sample
        point: ee.Geometry.Point location
        scale: Scale in meters
        band_name: Band name to extract
        
    Returns:
        Sampled value or None if no data
    """
    try:
        # Use reduceRegion for point sampling (more reliable than sample)
        result = image.reduceRegion(
            reducer=ee.Reducer.first(),
            geometry=point,
            scale=scale,
            maxPixels=1
        ).get(band_name)
        
        value = result.getInfo() if result else None
        
        if value is None or (isinstance(value, (int, float)) and (value < -900 or value > 1e6)):
            return NODATA_VALUE
            
        return float(value)
        
    except Exception as e:
        print(f"Warning: Error sampling at point: {e}")
        return NODATA_VALUE


def validate_date_range(start_date: str, end_date: str, max_days: int = 5000) -> None:
    """
    Validate date range doesn't exceed limits
    
    Args:
        start_date: Start date string (YYYY-MM-DD)
        end_date: End date string (YYYY-MM-DD)
        max_days: Maximum allowed days
        
    Raises:
        ValueError: If range is invalid
    """
    from datetime import datetime
    
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    days = (end - start).days + 1
    
    if days > max_days:
        raise ValueError(f"Date range too long: {days} days (max: {max_days})")
    
    if days < 1:
        raise ValueError("Invalid date range: start must be before or equal to end")


def cap_end_date_to_present(end_date: str) -> str:
    """
    Cap end date to present if it's in the future
    
    Args:
        end_date: End date string (YYYY-MM-DD)
        
    Returns:
        Capped date string
    """
    from datetime import datetime, timedelta
    
    end = datetime.strptime(end_date, '%Y-%m-%d')
    # Use yesterday to account for GEE update lag
    today = datetime.now() - timedelta(days=1)
    
    if end > today:
        return today.strftime('%Y-%m-%d')
    
    return end_date


def estimate_area_km2(geometry: ee.Geometry) -> float:
    """
    Estimate area of a geometry in km²
    
    Args:
        geometry: ee.Geometry
        
    Returns:
        Area in km²
    """
    try:
        area_m2 = geometry.area(maxError=100).getInfo()
        return area_m2 / 1_000_000
    except:
        return 0.0
