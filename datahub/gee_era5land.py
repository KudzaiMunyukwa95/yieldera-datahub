"""
ERA5-Land temperature data extraction via Google Earth Engine
"""

import ee
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from .reducers import (
    get_spatial_reducer, 
    reduce_image_over_region,
    sample_image_at_point,
    NODATA_VALUE,
    ERA5_SCALE
)
from .errors import GEEError


# ERA5-Land dataset configuration
ERA5_COLLECTION = "ECMWF/ERA5_LAND/HOURLY"
ERA5_TEMP_BAND = "temperature_2m"
ERA5_LICENSE = "https://cds.climate.copernicus.eu/api/v2/terms/static/licence-to-use-copernicus-products.pdf"
ERA5_CITATION = "Muñoz Sabater, J. (2019): ERA5-Land hourly data from 1950 to present. Copernicus Climate Change Service (C3S) Climate Data Store (CDS)."


class ERA5LandExtractor:
    """Extract ERA5-Land temperature data via Google Earth Engine"""
    
    def __init__(self):
        self.collection_id = ERA5_COLLECTION
        self.band_name = ERA5_TEMP_BAND
        self.scale = ERA5_SCALE
        self.native_resolution_deg = 0.1
        
    def get_timeseries(
        self,
        geometry: ee.Geometry,
        start_date: str,
        end_date: str,
        spatial_stat: str = "mean",
        is_point: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Extract daily temperature timeseries (Tmin, Tmax, Tavg) from hourly data
        
        Args:
            geometry: ee.Geometry (point or polygon)
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            spatial_stat: Spatial aggregation method
            is_point: Whether geometry is a point
            
        Returns:
            List of daily data dictionaries with tmin_c, tmax_c, tavg_c
            
        Raises:
            GEEError: If extraction fails
        """
        try:
            # Parse dates
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            # Get reducer
            reducer = get_spatial_reducer(spatial_stat)
            
            daily_data = []
            current_date = start_dt
            
            # Process day by day to aggregate hourly to daily
            while current_date <= end_dt:
                date_str = current_date.strftime('%Y-%m-%d')
                next_date = current_date + timedelta(days=1)
                next_date_str = next_date.strftime('%Y-%m-%d')
                
                try:
                    # Get hourly collection for this day
                    daily_collection = ee.ImageCollection(self.collection_id) \
                        .filterDate(date_str, next_date_str) \
                        .filterBounds(geometry) \
                        .select(self.band_name)
                    
                    # Check if we have data
                    count = daily_collection.size().getInfo()
                    
                    if count == 0:
                        # No data for this day
                        daily_data.append({
                            'date': date_str,
                            'tmin_c': NODATA_VALUE,
                            'tmax_c': NODATA_VALUE,
                            'tavg_c': NODATA_VALUE
                        })
                        current_date = next_date
                        continue
                    
                    # Compute daily min, max, mean from hourly images
                    if is_point:
                        # For points, sample and aggregate client-side
                        hourly_temps = []
                        
                        images = daily_collection.toList(24)
                        for i in range(min(count, 24)):
                            img = ee.Image(images.get(i))
                            temp_k = sample_image_at_point(
                                img, geometry, self.scale, self.band_name
                            )
                            if temp_k is not None and temp_k != NODATA_VALUE:
                                hourly_temps.append(temp_k - 273.15)  # Convert K to C
                        
                        if hourly_temps:
                            tmin = min(hourly_temps)
                            tmax = max(hourly_temps)
                            tavg = sum(hourly_temps) / len(hourly_temps)
                        else:
                            tmin = tmax = tavg = NODATA_VALUE
                    
                    else:
                        # For polygons, use server-side reduction
                        # Compute daily statistics
                        daily_min = daily_collection.min()
                        daily_max = daily_collection.max()
                        daily_mean = daily_collection.mean()
                        
                        # Reduce over region
                        tmin_k = reduce_image_over_region(
                            daily_min, geometry, reducer, self.scale, self.band_name
                        )
                        tmax_k = reduce_image_over_region(
                            daily_max, geometry, reducer, self.scale, self.band_name
                        )
                        tavg_k = reduce_image_over_region(
                            daily_mean, geometry, reducer, self.scale, self.band_name
                        )
                        
                        # Convert K to C
                        if tmin_k != NODATA_VALUE and tmin_k > 0:
                            tmin = tmin_k - 273.15
                        else:
                            tmin = NODATA_VALUE
                            
                        if tmax_k != NODATA_VALUE and tmax_k > 0:
                            tmax = tmax_k - 273.15
                        else:
                            tmax = NODATA_VALUE
                            
                        if tavg_k != NODATA_VALUE and tavg_k > 0:
                            tavg = tavg_k - 273.15
                        else:
                            tavg = NODATA_VALUE
                    
                    daily_data.append({
                        'date': date_str,
                        'tmin_c': round(tmin, 2) if tmin != NODATA_VALUE else NODATA_VALUE,
                        'tmax_c': round(tmax, 2) if tmax != NODATA_VALUE else NODATA_VALUE,
                        'tavg_c': round(tavg, 2) if tavg != NODATA_VALUE else NODATA_VALUE
                    })
                    
                except Exception as day_error:
                    print(f"Warning: Error processing {date_str}: {day_error}")
                    daily_data.append({
                        'date': date_str,
                        'tmin_c': NODATA_VALUE,
                        'tmax_c': NODATA_VALUE,
                        'tavg_c': NODATA_VALUE
                    })
                
                current_date = next_date
            
            return daily_data
            
        except ee.EEException as e:
            raise GEEError(f"GEE error extracting ERA5-Land timeseries: {str(e)}")
        except Exception as e:
            raise GEEError(f"Error extracting ERA5-Land timeseries: {str(e)}")
    
    def export_geotiff(
        self,
        geometry: ee.Geometry,
        start_date: str,
        end_date: str,
        resolution_deg: float = 0.1,
        clip_to_geometry: bool = True,
        export_mode: str = "multiband",
        band_selection: str = "tavg"  # 'tmin', 'tmax', or 'tavg'
    ) -> Dict[str, Any]:
        """
        Export ERA5-Land temperature data as GeoTIFF
        
        Args:
            geometry: ee.Geometry for export region
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            resolution_deg: Output resolution in degrees
            clip_to_geometry: Whether to clip to geometry bounds
            export_mode: 'multiband' or 'zip'
            band_selection: Which temperature band ('tmin', 'tmax', 'tavg')
            
        Returns:
            Dictionary with export configuration
            
        Raises:
            GEEError: If export configuration fails
        """
        try:
            # Parse dates
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            # Compute daily images
            daily_images = []
            dates = []
            current_date = start_dt
            
            while current_date <= end_dt and len(daily_images) < 366:
                date_str = current_date.strftime('%Y-%m-%d')
                next_date = current_date + timedelta(days=1)
                next_date_str = next_date.strftime('%Y-%m-%d')
                
                # Get hourly collection for this day
                daily_collection = ee.ImageCollection(self.collection_id) \
                    .filterDate(date_str, next_date_str) \
                    .filterBounds(geometry) \
                    .select(self.band_name)
                
                # Compute selected statistic
                if band_selection == "tmin":
                    daily_img = daily_collection.min()
                elif band_selection == "tmax":
                    daily_img = daily_collection.max()
                else:  # tavg
                    daily_img = daily_collection.mean()
                
                # Convert K to C: (temp_k - 273.15)
                daily_img = daily_img.subtract(273.15).rename(date_str)
                
                daily_images.append(daily_img)
                dates.append(date_str)
                
                current_date = next_date
            
            if not daily_images:
                raise GEEError("No ERA5-Land data found for specified date range")
            
            # Get region bounds
            region = geometry.bounds() if clip_to_geometry else geometry.buffer(10000).bounds()
            
            if export_mode == "multiband":
                # Combine all bands
                multiband = ee.Image.cat(daily_images)
                
                url = multiband.getDownloadURL({
                    'region': region,
                    'scale': resolution_deg * 111320,
                    'crs': 'EPSG:4326',
                    'format': 'GEO_TIFF'
                })
                
                return {
                    'mode': 'multiband',
                    'url': url,
                    'bands': dates,
                    'count': len(dates),
                    'resolution_deg': resolution_deg,
                    'variable': band_selection
                }
            
            else:
                # Zip mode - individual files
                urls = []
                
                for i, (img, date_str) in enumerate(zip(daily_images[:31], dates[:31])):
                    url = img.getDownloadURL({
                        'region': region,
                        'scale': resolution_deg * 111320,
                        'crs': 'EPSG:4326',
                        'format': 'GEO_TIFF'
                    })
                    
                    urls.append({
                        'date': date_str,
                        'url': url,
                        'variable': band_selection
                    })
                
                return {
                    'mode': 'zip',
                    'files': urls,
                    'count': len(urls),
                    'resolution_deg': resolution_deg,
                    'variable': band_selection
                }
                
        except ee.EEException as e:
            raise GEEError(f"GEE error exporting ERA5-Land GeoTIFF: {str(e)}")
        except Exception as e:
            raise GEEError(f"Error exporting ERA5-Land GeoTIFF: {str(e)}")
    
    def get_metadata(self) -> Dict[str, Any]:
        """Get ERA5-Land dataset metadata"""
        return {
            "source": f"GEE: {self.collection_id}",
            "variable": "temperature_2m (derived: tmin, tmax, tavg)",
            "units": {
                "tmin": "°C",
                "tmax": "°C",
                "tavg": "°C"
            },
            "native_resolution_deg": self.native_resolution_deg,
            "scale_m": self.scale,
            "temporal_resolution": "hourly (aggregated to daily)",
            "spatial_coverage": "global",
            "temporal_coverage": "1950-present",
            "update_frequency": "~5 days behind real-time",
            "license": ERA5_LICENSE,
            "citation": ERA5_CITATION,
            "nodata_value": NODATA_VALUE,
            "dataset_doi": "10.24381/cds.e2161bac"
        }
