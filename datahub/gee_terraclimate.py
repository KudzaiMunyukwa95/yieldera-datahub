"""
TerraClimate monthly temperature data extraction via Google Earth Engine
Dataset: IDAHO_EPSCOR/TERRACLIMATE (1958-present, monthly)

Variables:
- tmin: Minimum temperature (°C)
- tmax: Maximum temperature (°C)
- tavg: Average temperature (°C) - calculated from (tmin + tmax) / 2

Temporal Coverage: 1958-01-01 to present (~2-3 months lag)
Spatial Resolution: 4 km (0.04166 degrees)
Temporal Resolution: Monthly

Author: Yieldera Climate Intelligence
Created: 2025-11-01
"""

import ee
from datetime import datetime
from typing import List, Dict, Any, Optional


class TerraClimateExtractor:
    """Extract TerraClimate monthly temperature data via Google Earth Engine"""
    
    def __init__(self):
        """Initialize TerraClimate extractor with dataset configuration"""
        self.dataset_id = 'IDAHO_EPSCOR/TERRACLIMATE'
        self.scale = 4638  # ~4km native resolution in meters
        self.native_resolution_deg = 0.04166
        
    def get_metadata(self):
        """
        Return metadata about the TerraClimate dataset
        
        Returns:
            dict: Dataset metadata including source, variables, coverage, etc.
        """
        return {
            "source": f"GEE: {self.dataset_id}",
            "variables": ["tmin", "tmax", "tavg"],
            "variable_descriptions": {
                "tmin": "Minimum temperature",
                "tmax": "Maximum temperature",
                "tavg": "Average temperature (calculated from tmin and tmax)"
            },
            "units": "degrees Celsius (°C)",
            "native_resolution_deg": self.native_resolution_deg,
            "native_resolution_km": 4,
            "spatial_coverage": "global (land areas)",
            "temporal_resolution": "monthly",
            "temporal_coverage": "1958-present",
            "update_frequency": "monthly (~2-3 months lag)",
            "typical_values": "−50°C to 50°C globally, 15-30°C for African croplands",
            "license": "CC BY 4.0",
            "citation": "Abatzoglou, J.T., Dobrowski, S.Z., Parks, S.A. et al. TerraClimate, a high-resolution global dataset of monthly climate and climatic water balance from 1958–2015. Sci Data 5, 170191 (2018).",
            "dataset_doi": "10.1038/sdata.2017.191",
            "references": [
                "https://developers.google.com/earth-engine/datasets/catalog/IDAHO_EPSCOR_TERRACLIMATE",
                "https://www.climatologylab.org/terraclimate.html"
            ]
        }
    
    def _get_collection(self, start_date, end_date, geometry=None):
        """
        Get TerraClimate collection filtered by date range and geometry
        
        Args:
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            geometry (ee.Geometry, optional): Geometry to filter collection
            
        Returns:
            ee.ImageCollection: Filtered TerraClimate collection
        """
        collection = ee.ImageCollection(self.dataset_id)
        
        # Filter by date range
        collection = collection.filterDate(start_date, end_date)
        
        # Filter by geometry if provided
        if geometry:
            collection = collection.filterBounds(geometry)
        
        return collection
    
    def _get_spatial_reducer(self, spatial_stat):
        """
        Get Earth Engine reducer for spatial statistics
        
        Args:
            spatial_stat (str): Type of statistic (mean, median, max, min, sum)
            
        Returns:
            ee.Reducer: Earth Engine reducer
        """
        reducers = {
            'mean': ee.Reducer.mean(),
            'median': ee.Reducer.median(),
            'max': ee.Reducer.max(),
            'min': ee.Reducer.min(),
            'sum': ee.Reducer.sum()
        }
        
        return reducers.get(spatial_stat.lower(), ee.Reducer.mean())
    
    def _reduce_image_over_region(self, image, geometry, reducer, scale=None):
        """
        Reduce an image over a region using specified reducer
        
        Args:
            image (ee.Image): Image to reduce
            geometry (ee.Geometry): Region to reduce over
            reducer (ee.Reducer): Reducer to use
            scale (int, optional): Scale in meters
            
        Returns:
            dict: Reduced values for each band
        """
        if scale is None:
            scale = self.scale
        
        reduction = image.reduceRegion(
            reducer=reducer,
            geometry=geometry,
            scale=scale,
            maxPixels=1e13,
            bestEffort=True
        )
        
        return reduction.getInfo()
    
    def _sample_image_at_point(self, image, point, scale=None):
        """
        Sample an image at a point location
        
        Args:
            image (ee.Image): Image to sample
            point (ee.Geometry.Point): Point location
            scale (int, optional): Scale in meters
            
        Returns:
            dict: Sampled values for each band
        """
        if scale is None:
            scale = self.scale
        
        sample = image.sample(
            region=point,
            scale=scale,
            projection='EPSG:4326',
            factor=1,
            numPixels=1
        ).first()
        
        return sample.getInfo()['properties'] if sample.getInfo() else {}
    
    def get_timeseries(self, geometry, start_date, end_date, spatial_stat='mean', is_point=False):
        """
        Extract monthly temperature timeseries (tmin, tmax, tavg)
        
        Args:
            geometry (ee.Geometry): Point or polygon geometry
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            spatial_stat (str): Spatial statistic for polygons (mean, median, max, min, sum)
            is_point (bool): Whether geometry is a point
            
        Returns:
            list: Array of monthly data dictionaries with date, tmin_c, tmax_c, tavg_c
            
        Example:
            [
                {"date": "2024-01", "tmin_c": 15.2, "tmax_c": 28.5, "tavg_c": 21.8},
                {"date": "2024-02", "tmin_c": 16.1, "tmax_c": 29.2, "tavg_c": 22.6}
            ]
        """
        # Get collection
        collection = self._get_collection(start_date, end_date, geometry)
        
        # Select temperature bands
        collection = collection.select(['tmmn', 'tmmx'])
        
        # Get reducer for polygons
        reducer = self._get_spatial_reducer(spatial_stat)
        
        # Convert collection to list
        image_list = collection.toList(collection.size())
        num_images = image_list.size().getInfo()
        
        if num_images == 0:
            raise ValueError("No TerraClimate data found for specified date range and location")
        
        # Extract data for each month
        timeseries = []
        
        for i in range(num_images):
            image = ee.Image(image_list.get(i))
            
            # Get date in YYYY-MM format
            date = ee.Date(image.get('system:time_start'))
            year = date.get('year').getInfo()
            month = date.get('month').getInfo()
            date_str = f"{year}-{month:02d}"
            
            # Extract values based on geometry type
            if is_point:
                values = self._sample_image_at_point(image, geometry)
            else:
                values = self._reduce_image_over_region(image, geometry, reducer)
            
            # Get temperature values and handle None
            tmin = values.get('tmmn')
            tmax = values.get('tmmx')
            
            # Convert from Kelvin to Celsius if needed (TerraClimate is already in Celsius * 10)
            # TerraClimate stores temperatures multiplied by 10, so divide by 10
            if tmin is not None:
                tmin_c = round(float(tmin) / 10.0, 2)
            else:
                tmin_c = -999.0
            
            if tmax is not None:
                tmax_c = round(float(tmax) / 10.0, 2)
            else:
                tmax_c = -999.0
            
            # Calculate average temperature
            if tmin_c != -999.0 and tmax_c != -999.0:
                tavg_c = round((tmin_c + tmax_c) / 2.0, 2)
            else:
                tavg_c = -999.0
            
            timeseries.append({
                'date': date_str,
                'tmin_c': tmin_c,
                'tmax_c': tmax_c,
                'tavg_c': tavg_c
            })
        
        return timeseries
    
    def get_statistics(self, geometry, start_date, end_date, spatial_stat='mean', is_point=False):
        """
        Get summary statistics for temperature over a time period
        
        Args:
            geometry (ee.Geometry): Point or polygon geometry
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            spatial_stat (str): Spatial statistic for polygons
            is_point (bool): Whether geometry is a point
            
        Returns:
            dict: Summary statistics for tmin, tmax, and tavg
            
        Example:
            {
                "tmin": {"mean": 16.3, "min": 15.2, "max": 17.5, "median": 16.1, "num_months": 12},
                "tmax": {"mean": 29.3, "min": 28.5, "max": 30.1, "median": 29.2, "num_months": 12},
                "tavg": {"mean": 22.8, "min": 21.8, "max": 23.8, "median": 22.6, "num_months": 12},
                "date_range": {"start": "2024-01", "end": "2024-12", "total_months": 12}
            }
        """
        # Get timeseries
        timeseries = self.get_timeseries(geometry, start_date, end_date, spatial_stat, is_point)
        
        # Filter out nodata values
        valid_tmin = [d['tmin_c'] for d in timeseries if d['tmin_c'] != -999.0]
        valid_tmax = [d['tmax_c'] for d in timeseries if d['tmax_c'] != -999.0]
        valid_tavg = [d['tavg_c'] for d in timeseries if d['tavg_c'] != -999.0]
        
        if not valid_tmin or not valid_tmax or not valid_tavg:
            raise ValueError("No valid data points found")
        
        # Helper function to calculate statistics
        def calc_stats(values):
            sorted_values = sorted(values)
            return {
                'mean': round(sum(values) / len(values), 2),
                'min': round(min(values), 2),
                'max': round(max(values), 2),
                'median': round(sorted_values[len(sorted_values)//2], 2),
                'num_months': len(values)
            }
        
        # Calculate statistics for each variable
        stats = {
            'tmin': calc_stats(valid_tmin),
            'tmax': calc_stats(valid_tmax),
            'tavg': calc_stats(valid_tavg),
            'date_range': {
                'start': timeseries[0]['date'] if timeseries else None,
                'end': timeseries[-1]['date'] if timeseries else None,
                'total_months': len(timeseries)
            }
        }
        
        return stats
    
    def export_geotiff(self, geometry, start_date, end_date, resolution_deg=0.04166, 
                      clip_to_geometry=True, export_mode='multiband', band_selection='tavg'):
        """
        Export TerraClimate temperature data as GeoTIFF
        
        Args:
            geometry (ee.Geometry): Region to export
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            resolution_deg (float): Output resolution in degrees (default: 0.04166 = ~4km)
            clip_to_geometry (bool): Whether to clip to geometry bounds
            export_mode (str): 'multiband' or 'zip'
            band_selection (str): Which temperature band ('tmin', 'tmax', or 'tavg')
            
        Returns:
            dict: Export configuration with download URL(s)
        """
        # Validate inputs
        valid_bands = ['tmin', 'tmax', 'tavg']
        if band_selection not in valid_bands:
            raise ValueError(f"band_selection must be one of: {', '.join(valid_bands)}")
        
        # Get collection
        collection = self._get_collection(start_date, end_date, geometry)
        
        # Map to select appropriate band
        band_map = {
            'tmin': 'tmmn',
            'tmax': 'tmmx',
            'tavg': ['tmmn', 'tmmx']  # Will average these
        }
        
        # Process images
        def process_image(image):
            if band_selection == 'tavg':
                # Calculate average from min and max
                tmin = image.select('tmmn')
                tmax = image.select('tmmx')
                tavg = tmin.add(tmax).divide(2.0)
                processed = tavg.divide(10.0).rename('tavg')  # Convert to Celsius
            else:
                # Select and convert single band
                band = band_map[band_selection]
                processed = image.select(band).divide(10.0).rename(band_selection)
            
            # Get date
            date = ee.Date(image.get('system:time_start'))
            year = date.get('year')
            month = date.get('month')
            date_str = ee.String(year).cat('-').cat(
                ee.Algorithms.String(month).slice(0, 2)
            )
            
            return processed.set('date', date_str).rename(date_str)
        
        processed_collection = collection.map(process_image)
        
        # Convert resolution from degrees to meters
        resolution_m = int(resolution_deg * 111320)
        
        # Get geometry bounds
        bounds = geometry.bounds() if clip_to_geometry else geometry
        
        # Get list of images
        image_list = processed_collection.toList(processed_collection.size())
        num_images = image_list.size().getInfo()
        
        if num_images == 0:
            raise ValueError("No TerraClimate data found for specified date range")
        
        if export_mode == 'multiband':
            # Combine all bands into single image
            def add_band(current, previous):
                return ee.Image(previous).addBands(ee.Image(current))
            
            first_image = ee.Image(image_list.get(0))
            multiband = ee.Image(image_list.slice(1).iterate(add_band, first_image))
            
            # Generate download URL
            url = multiband.getDownloadURL({
                'region': bounds,
                'scale': resolution_m,
                'crs': 'EPSG:4326',
                'fileFormat': 'GeoTIFF',
                'formatOptions': {
                    'cloudOptimized': True
                }
            })
            
            # Get band names (dates)
            band_names = []
            for i in range(num_images):
                img = ee.Image(image_list.get(i))
                date_str = img.get('date').getInfo()
                band_names.append(date_str)
            
            return {
                'mode': 'multiband',
                'filename': f"terraclimate_{band_selection}_{start_date}_{end_date}.tif",
                'download_url': url,
                'num_months': num_images,
                'bands': band_names,
                'variable': band_selection,
                'date_range': {
                    'start': start_date,
                    'end': end_date
                },
                'resolution_m': resolution_m,
                'crs': 'EPSG:4326'
            }
        
        else:  # zip mode
            # Export individual monthly GeoTIFFs
            downloads = []
            
            for i in range(min(num_images, 60)):  # Limit to 60 months
                image = ee.Image(image_list.get(i))
                date = image.get('date').getInfo()
                
                filename = f"terraclimate_{band_selection}_{date}"
                
                url = image.getDownloadURL({
                    'region': bounds,
                    'scale': resolution_m,
                    'crs': 'EPSG:4326',
                    'fileFormat': 'GeoTIFF'
                })
                
                downloads.append({
                    'date': date,
                    'filename': filename + '.tif',
                    'download_url': url
                })
            
            return {
                'mode': 'zip',
                'num_files': len(downloads),
                'variable': band_selection,
                'date_range': {
                    'start': start_date,
                    'end': end_date
                },
                'resolution_m': resolution_m,
                'crs': 'EPSG:4326',
                'files': downloads
            }


# Example usage
if __name__ == '__main__':
    # Initialize Earth Engine
    try:
        ee.Initialize()
    except:
        ee.Authenticate()
        ee.Initialize()
    
    # Create extractor
    extractor = TerraClimateExtractor()
    
    # Test location: Harare, Zimbabwe
    harare = ee.Geometry.Point([31.0530, -17.8249])
    
    # Get metadata
    print("Dataset Metadata:")
    metadata = extractor.get_metadata()
    print(f"Source: {metadata['source']}")
    print(f"Variables: {metadata['variables']}")
    print(f"Resolution: {metadata['native_resolution_km']} km")
    
    # Extract timeseries for 12 months
    print("\nExtracting timeseries for Harare (2024-01-01 to 2024-12-31)...")
    timeseries = extractor.get_timeseries(
        geometry=harare,
        start_date='2024-01-01',
        end_date='2024-12-31',
        is_point=True
    )
    
    print(f"\nFirst 3 months:")
    for month in timeseries[:3]:
        print(f"  {month['date']}: Tmin={month['tmin_c']}°C, Tmax={month['tmax_c']}°C, Tavg={month['tavg_c']}°C")
    
    # Get statistics
    print("\nCalculating statistics...")
    stats = extractor.get_statistics(
        geometry=harare,
        start_date='2024-01-01',
        end_date='2024-12-31',
        is_point=True
    )
    
    print(f"\nAverage Temperature:")
    print(f"  Mean: {stats['tavg']['mean']}°C")
    print(f"  Range: {stats['tavg']['min']}°C - {stats['tavg']['max']}°C")
