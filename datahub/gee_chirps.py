"""
CHIRPS rainfall data extraction via Google Earth Engine
"""

import ee
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from .reducers import (
    get_spatial_reducer,
    NODATA_VALUE,
    CHIRPS_SCALE
)
from .errors import GEEError


# CHIRPS dataset configuration
CHIRPS_COLLECTION = "UCSB-CHG/CHIRPS/DAILY"
CHIRPS_BAND = "precipitation"
CHIRPS_LICENSE = "CC-BY-4.0"
CHIRPS_CITATION = "Funk, C. et al. (2015). The climate hazards infrared precipitation with stations—a new environmental record for monitoring extremes. Scientific Data, 2, 150066."


class CHIRPSExtractor:
    """Extract CHIRPS rainfall data via Google Earth Engine"""
    
    def __init__(self):
        self.collection_id = CHIRPS_COLLECTION
        self.band_name = CHIRPS_BAND
        self.scale = CHIRPS_SCALE
        self.native_resolution_deg = 0.05
        
    def get_timeseries(
        self,
        geometry: ee.Geometry,
        start_date: str,
        end_date: str,
        spatial_stat: str = "mean",
        is_point: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Extract daily rainfall timeseries
        
        Args:
            geometry: ee.Geometry (point or polygon)
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            spatial_stat: Spatial aggregation method
            is_point: Whether geometry is a point
            
        Returns:
            List of daily data dictionaries
            
        Raises:
            GEEError: If extraction fails
        """
        try:
            # Get collection
            collection = ee.ImageCollection(self.collection_id) \
                .filterDate(start_date, end_date) \
                .filterBounds(geometry) \
                .select(self.band_name)
            
            # Get reducer
            reducer = get_spatial_reducer(spatial_stat)
            
            # Extract daily values using server-side operations
            def extract_daily(image):
                value = image.reduceRegion(
                    reducer=reducer,
                    geometry=geometry,
                    scale=self.scale,
                    maxPixels=1e13
                ).get(self.band_name)
                
                return ee.Feature(None, {
                    'date': image.date().format('YYYY-MM-dd'),
                    'rainfall': value
                })
            
            features = collection.map(extract_daily)
            feature_collection = ee.FeatureCollection(features)
            
            # Retrieve data
            data = feature_collection.getInfo()
            
            # Process results
            daily_data = []
            for feature in data.get('features', []):
                props = feature['properties']
                rainfall = props.get('rainfall')
                
                # Handle None/null values
                if rainfall is None:
                    value = NODATA_VALUE
                else:
                    value = round(float(rainfall), 2)
                
                daily_data.append({
                    'date': props['date'],
                    'precip_mm': value
                })
            
            return sorted(daily_data, key=lambda x: x['date'])
            
        except ee.EEException as e:
            raise GEEError(f"GEE error extracting CHIRPS timeseries: {str(e)}")
        except Exception as e:
            raise GEEError(f"Error extracting CHIRPS timeseries: {str(e)}")
    
    def export_geotiff(
        self,
        geometry: ee.Geometry,
        start_date: str,
        end_date: str,
        resolution_deg: float = 0.05,
        clip_to_geometry: bool = True,
        export_mode: str = "multiband",
        band_selection: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Export CHIRPS data as GeoTIFF
        
        Args:
            geometry: ee.Geometry for export region
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            resolution_deg: Output resolution in degrees
            clip_to_geometry: Whether to clip to geometry bounds
            export_mode: 'multiband' or 'zip'
            band_selection: Optional single band to export
            
        Returns:
            Dictionary with export configuration
            
        Raises:
            GEEError: If export configuration fails
        """
        try:
            # Get collection
            collection = ee.ImageCollection(self.collection_id) \
                .filterDate(start_date, end_date) \
                .filterBounds(geometry) \
                .select(self.band_name)
            
            # Get list of images
            image_list = collection.toList(collection.size())
            count = image_list.size().getInfo()
            
            if count == 0:
                raise GEEError("No CHIRPS data found for specified date range and location")
            
            # For multiband mode, stack all images
            if export_mode == "multiband":
                # Create a list to hold all bands
                bands = []
                dates = []
                
                for i in range(min(count, 366)):  # Limit to 366 bands
                    img = ee.Image(image_list.get(i))
                    date_str = ee.Date(img.get('system:time_start')).format('YYYY-MM-dd').getInfo()
                    dates.append(date_str)
                    
                    # Rename band to date
                    band = img.select([self.band_name]).rename(date_str)
                    bands.append(band)
                
                # Combine all bands into single image
                multiband = ee.Image.cat(bands)
                
                # Get region bounds
                region = geometry.bounds() if clip_to_geometry else geometry.buffer(10000).bounds()
                
                # Get download URL
                url = multiband.getDownloadURL({
                    'region': region,
                    'scale': resolution_deg * 111320,  # Convert degrees to meters
                    'crs': 'EPSG:4326',
                    'format': 'GEO_TIFF'
                })
                
                return {
                    'mode': 'multiband',
                    'url': url,
                    'bands': dates,
                    'count': len(dates),
                    'resolution_deg': resolution_deg
                }
            
            else:
                # For zip mode, return list of individual image URLs
                urls = []
                
                for i in range(min(count, 31)):  # Limit to 31 days for zip mode
                    img = ee.Image(image_list.get(i))
                    date_str = ee.Date(img.get('system:time_start')).format('YYYY-MM-dd').getInfo()
                    
                    region = geometry.bounds() if clip_to_geometry else geometry.buffer(10000).bounds()
                    
                    url = img.getDownloadURL({
                        'region': region,
                        'scale': resolution_deg * 111320,
                        'crs': 'EPSG:4326',
                        'format': 'GEO_TIFF'
                    })
                    
                    urls.append({
                        'date': date_str,
                        'url': url
                    })
                
                return {
                    'mode': 'zip',
                    'files': urls,
                    'count': len(urls),
                    'resolution_deg': resolution_deg
                }
                
        except ee.EEException as e:
            raise GEEError(f"GEE error exporting CHIRPS GeoTIFF: {str(e)}")
        except Exception as e:
            raise GEEError(f"Error exporting CHIRPS GeoTIFF: {str(e)}")
    
    def get_metadata(self) -> Dict[str, Any]:
        """Get CHIRPS dataset metadata"""
        return {
            "source": f"GEE: {self.collection_id}",
            "variable": "precipitation",
            "units": "mm/day",
            "native_resolution_deg": self.native_resolution_deg,
            "scale_m": self.scale,
            "temporal_resolution": "daily",
            "spatial_coverage": "global (50°S to 50°N)",
            "temporal_coverage": "1981-present",
            "update_frequency": "daily",
            "license": CHIRPS_LICENSE,
            "citation": CHIRPS_CITATION,
            "nodata_value": NODATA_VALUE,
            "dataset_doi": "10.1038/sdata.2015.66"
        }
