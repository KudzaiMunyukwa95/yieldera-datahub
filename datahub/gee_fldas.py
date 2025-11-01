"""
FLDAS monthly soil moisture data extraction via Google Earth Engine
Dataset: NASA/FLDAS/NOAH01/C/GL/M/V001 (1982-present, monthly)

Variables:
- sm_surface: Surface soil moisture (0-10cm depth)
- sm_rootzone: Root zone soil moisture (10-40cm depth)

Temporal Coverage: 1982-01-01 to present (~1-2 months lag)
Spatial Resolution: 11 km (0.1 degrees)
Temporal Resolution: Monthly

Author: Yieldera Climate Intelligence
Created: 2025-11-01
"""

import ee
from datetime import datetime
from typing import List, Dict, Any, Optional


class FLDASExtractor:
    """Extract FLDAS monthly soil moisture data via Google Earth Engine"""
    
    def __init__(self):
        """Initialize FLDAS extractor with dataset configuration"""
        self.dataset_id = 'NASA/FLDAS/NOAH01/C/GL/M/V001'
        self.scale = 11000  # ~11km native resolution in meters
        self.native_resolution_deg = 0.1
        
    def get_metadata(self):
        """
        Return metadata about the FLDAS dataset
        
        Returns:
            dict: Dataset metadata including source, variables, coverage, etc.
        """
        return {
            "source": f"GEE: {self.dataset_id}",
            "variables": ["sm_surface", "sm_rootzone"],
            "variable_descriptions": {
                "sm_surface": "Surface soil moisture (0-10cm depth)",
                "sm_rootzone": "Root zone soil moisture (10-40cm depth)"
            },
            "units": "percent (%)",
            "native_units": "kg/m² (converted to % by formula: value/400*100)",
            "native_resolution_deg": self.native_resolution_deg,
            "native_resolution_km": 11,
            "spatial_coverage": "global (60°S to 90°N)",
            "temporal_resolution": "monthly",
            "temporal_coverage": "1982-present",
            "update_frequency": "monthly (~1-2 months lag)",
            "typical_values": "10-40% for African croplands, 5-60% globally",
            "license": "NASA Open Data",
            "citation": "McNally, A., et al. (2017). A land data assimilation system for sub-Saharan Africa food and water security applications. Scientific Data, 4, 170012.",
            "dataset_doi": "10.1038/sdata.2017.12",
            "references": [
                "https://developers.google.com/earth-engine/datasets/catalog/NASA_FLDAS_NOAH01_C_GL_M_V001",
                "https://ldas.gsfc.nasa.gov/fldas"
            ]
        }
    
    def _get_collection(self, start_date, end_date, geometry=None):
        """
        Get FLDAS collection filtered by date range and geometry
        
        Args:
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            geometry (ee.Geometry, optional): Geometry to filter collection
            
        Returns:
            ee.ImageCollection: Filtered FLDAS collection
        """
        collection = ee.ImageCollection(self.dataset_id)
        
        # Filter by date range
        collection = collection.filterDate(start_date, end_date)
        
        # Filter by geometry if provided
        if geometry:
            collection = collection.filterBounds(geometry)
        
        return collection
    
    def _convert_to_percentage(self, value):
        """
        Convert FLDAS soil moisture from kg/m² to percentage
        
        FLDAS values typically range from 10-400 kg/m²
        We convert to volumetric percentage (0-100%) using: value / 400 * 100
        
        Args:
            value: Soil moisture in kg/m²
            
        Returns:
            float: Soil moisture in percentage
        """
        if value is None or value == -999:
            return -999.0
        
        # Convert kg/m² to percentage
        # Typical max saturation ~400 kg/m² = 100%
        percentage = (float(value) / 400.0) * 100.0
        
        # Clamp to reasonable range (0-100%)
        percentage = max(0.0, min(100.0, percentage))
        
        return round(percentage, 2)
    
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
        Extract monthly soil moisture timeseries (surface and root zone)
        
        Args:
            geometry (ee.Geometry): Point or polygon geometry
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            spatial_stat (str): Spatial statistic for polygons (mean, median, max, min, sum)
            is_point (bool): Whether geometry is a point
            
        Returns:
            list: Array of monthly data dictionaries with date, sm_surface, sm_rootzone
            
        Example:
            [
                {"date": "2024-01", "sm_surface": 15.2, "sm_rootzone": 25.8},
                {"date": "2024-02", "sm_surface": 12.3, "sm_rootzone": 22.1}
            ]
        """
        # Get collection
        collection = self._get_collection(start_date, end_date, geometry)
        
        # Select soil moisture bands
        collection = collection.select(['SoilMoi00_10cm_tavg', 'SoilMoi10_40cm_tavg'])
        
        # Get reducer for polygons
        reducer = self._get_spatial_reducer(spatial_stat)
        
        # Convert collection to list
        image_list = collection.toList(collection.size())
        num_images = image_list.size().getInfo()
        
        if num_images == 0:
            raise ValueError("No FLDAS data found for specified date range and location")
        
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
            
            # Get soil moisture values (in kg/m²) and convert to percentage
            sm_surface_raw = values.get('SoilMoi00_10cm_tavg')
            sm_rootzone_raw = values.get('SoilMoi10_40cm_tavg')
            
            # Convert to percentage
            sm_surface = self._convert_to_percentage(sm_surface_raw)
            sm_rootzone = self._convert_to_percentage(sm_rootzone_raw)
            
            timeseries.append({
                'date': date_str,
                'sm_surface': sm_surface,
                'sm_rootzone': sm_rootzone
            })
        
        return timeseries
    
    def get_statistics(self, geometry, start_date, end_date, spatial_stat='mean', is_point=False):
        """
        Get summary statistics for soil moisture over a time period
        
        Args:
            geometry (ee.Geometry): Point or polygon geometry
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            spatial_stat (str): Spatial statistic for polygons
            is_point (bool): Whether geometry is a point
            
        Returns:
            dict: Summary statistics for surface and root zone soil moisture
            
        Example:
            {
                "sm_surface": {"mean": 15.2, "min": 12.3, "max": 18.5, "median": 15.1, "num_months": 12},
                "sm_rootzone": {"mean": 25.8, "min": 22.1, "max": 28.9, "median": 25.5, "num_months": 12},
                "date_range": {"start": "2024-01", "end": "2024-12", "total_months": 12}
            }
        """
        # Get timeseries
        timeseries = self.get_timeseries(geometry, start_date, end_date, spatial_stat, is_point)
        
        # Filter out nodata values
        valid_surface = [d['sm_surface'] for d in timeseries if d['sm_surface'] != -999.0]
        valid_rootzone = [d['sm_rootzone'] for d in timeseries if d['sm_rootzone'] != -999.0]
        
        if not valid_surface or not valid_rootzone:
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
            'sm_surface': calc_stats(valid_surface),
            'sm_rootzone': calc_stats(valid_rootzone),
            'date_range': {
                'start': timeseries[0]['date'] if timeseries else None,
                'end': timeseries[-1]['date'] if timeseries else None,
                'total_months': len(timeseries)
            }
        }
        
        return stats
    
    def export_geotiff(self, geometry, start_date, end_date, resolution_deg=0.1, 
                      clip_to_geometry=True, export_mode='multiband', band_selection='both'):
        """
        Export FLDAS soil moisture data as GeoTIFF
        
        Args:
            geometry (ee.Geometry): Region to export
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            resolution_deg (float): Output resolution in degrees (default: 0.1 = ~11km)
            clip_to_geometry (bool): Whether to clip to geometry bounds
            export_mode (str): 'multiband' or 'zip'
            band_selection (str): Which band ('sm_surface', 'sm_rootzone', or 'both')
            
        Returns:
            dict: Export configuration with download URL(s)
        """
        # Validate inputs
        valid_bands = ['sm_surface', 'sm_rootzone', 'both']
        if band_selection not in valid_bands:
            raise ValueError(f"band_selection must be one of: {', '.join(valid_bands)}")
        
        # Get collection
        collection = self._get_collection(start_date, end_date, geometry)
        
        # Select bands based on band_selection
        if band_selection == 'sm_surface':
            collection = collection.select(['SoilMoi00_10cm_tavg'])
            export_bands = ['sm_surface']
        elif band_selection == 'sm_rootzone':
            collection = collection.select(['SoilMoi10_40cm_tavg'])
            export_bands = ['sm_rootzone']
        else:  # both
            collection = collection.select(['SoilMoi00_10cm_tavg', 'SoilMoi10_40cm_tavg'])
            export_bands = ['sm_surface', 'sm_rootzone']
        
        # Process images: convert kg/m² to percentage
        def process_image(image):
            # Convert from kg/m² to percentage
            processed = image.divide(400.0).multiply(100.0)
            
            # Clamp to 0-100%
            processed = processed.clamp(0, 100)
            
            # Get date
            date = ee.Date(image.get('system:time_start'))
            year = date.get('year')
            month = date.get('month')
            date_str = ee.String(year).cat('-').cat(
                ee.Algorithms.String(month).slice(0, 2)
            )
            
            # Rename bands
            if band_selection == 'sm_surface':
                processed = processed.select(['SoilMoi00_10cm_tavg']).rename('sm_surface')
            elif band_selection == 'sm_rootzone':
                processed = processed.select(['SoilMoi10_40cm_tavg']).rename('sm_rootzone')
            else:
                processed = processed.select(
                    ['SoilMoi00_10cm_tavg', 'SoilMoi10_40cm_tavg'],
                    ['sm_surface', 'sm_rootzone']
                )
            
            return processed.set('date', date_str)
        
        processed_collection = collection.map(process_image)
        
        # Convert resolution from degrees to meters
        resolution_m = int(resolution_deg * 111320)
        
        # Get geometry bounds
        bounds = geometry.bounds() if clip_to_geometry else geometry
        
        # Get list of images
        image_list = processed_collection.toList(processed_collection.size())
        num_images = image_list.size().getInfo()
        
        if num_images == 0:
            raise ValueError("No FLDAS data found for specified date range")
        
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
                'filename': f"fldas_{band_selection}_{start_date}_{end_date}.tif",
                'download_url': url,
                'num_months': num_images,
                'bands': band_names,
                'variables': export_bands,
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
                
                filename = f"fldas_{band_selection}_{date}"
                
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
                'variables': export_bands,
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
    extractor = FLDASExtractor()
    
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
        print(f"  {month['date']}: Surface={month['sm_surface']}%, RootZone={month['sm_rootzone']}%")
    
    # Get statistics
    print("\nCalculating statistics...")
    stats = extractor.get_statistics(
        geometry=harare,
        start_date='2024-01-01',
        end_date='2024-12-31',
        is_point=True
    )
    
    print(f"\nSurface Soil Moisture (0-10cm):")
    print(f"  Mean: {stats['sm_surface']['mean']}%")
    print(f"  Range: {stats['sm_surface']['min']}% - {stats['sm_surface']['max']}%")
    
    print(f"\nRoot Zone Soil Moisture (10-40cm):")
    print(f"  Mean: {stats['sm_rootzone']['mean']}%")
    print(f"  Range: {stats['sm_rootzone']['min']}% - {stats['sm_rootzone']['max']}%")
