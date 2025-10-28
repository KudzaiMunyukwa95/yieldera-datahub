"""
NASA SMAP L4 Soil Moisture Extractor for Google Earth Engine
Dataset: NASA/SMAP/SPL4SMGP/005 (SMAP L4 Global 3-hourly 9 km EASE-Grid Surface and Root Zone Soil Moisture)

Variables:
- sm_surface: Surface soil moisture (0-5 cm) in m³/m³ (converted to %)
- sm_rootzone: Root zone soil moisture (0-100 cm) in m³/m³ (converted to %)

Temporal Coverage: 2015-03-31 to present (near real-time with ~3 day latency)
Spatial Resolution: 9 km (0.09 degrees)
Temporal Resolution: 3-hourly (aggregated to daily mean)

Author: Yieldera Climate Intelligence
Created: 2025-10-28
"""

import ee
from datetime import datetime, timedelta
import os


class SMAPExtractor:
    """Extract NASA SMAP L4 soil moisture data from Google Earth Engine"""
    
    def __init__(self):
        """Initialize SMAP extractor with dataset configuration"""
        self.dataset_id = 'NASA/SMAP/SPL4SMGP/007'  # Version 7 (latest)
        self.bands = ['sm_surface', 'sm_rootzone']
        self.scale = 9000  # 9 km native resolution
        self.cache_dir = 'cache/smap'
        
        # Ensure cache directory exists
        os.makedirs(self.cache_dir, exist_ok=True)
    
    
    def get_metadata(self):
        """
        Return metadata about the SMAP L4 dataset
        
        Returns:
            dict: Dataset metadata including source, variables, coverage, etc.
        """
        return {
            "source": "GEE: NASA/SMAP/SPL4SMGP/007",
            "variables": ["sm_surface", "sm_rootzone"],
            "variable_descriptions": {
                "sm_surface": "Surface soil moisture (0-5 cm depth)",
                "sm_rootzone": "Root zone soil moisture (0-100 cm depth)"
            },
            "units": "percent (%)",
            "native_units": "m³/m³ (converted to % by multiplying by 100)",
            "native_resolution_deg": 0.09,
            "native_resolution_km": 9,
            "spatial_coverage": "global (−90° – 90° lat, −180° – 180° lon)",
            "temporal_resolution": "3-hourly (aggregated to daily mean)",
            "temporal_coverage": "2015-03-31 to present",
            "update_frequency": "daily (~3 day latency)",
            "typical_values": "10-45% for African croplands, 5-60% globally",
            "license": "NASA Open Data",
            "citation": "Reichle et al. (2019), NASA SMAP L4 Global Soil Moisture Data Assimilation Product, Version 7, Goddard Earth Sciences Data and Information Services Center (GES DISC)",
            "dataset_doi": "10.5067/EVKPQZ4AFC4D",
            "references": [
                "https://developers.google.com/earth-engine/datasets/catalog/NASA_SMAP_SPL4SMGP_007",
                "https://smap.jpl.nasa.gov/",
                "https://gmao.gsfc.nasa.gov/research/land/"
            ]
        }
    
    
    def _get_collection(self, start_date, end_date, geometry=None):
        """
        Get SMAP collection filtered by date range and geometry
        
        Args:
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            geometry (ee.Geometry, optional): Geometry to filter collection
            
        Returns:
            ee.ImageCollection: Filtered SMAP collection
        """
        # Load SMAP L4 collection
        collection = ee.ImageCollection(self.dataset_id)
        
        # Filter by date range
        collection = collection.filterDate(start_date, end_date)
        
        # Filter by geometry if provided
        if geometry:
            collection = collection.filterBounds(geometry)
        
        return collection
    
    
    def _convert_to_percentage(self, image):
        """
        Convert soil moisture from m³/m³ to percentage (%)
        
        Args:
            image (ee.Image): SMAP image with values in m³/m³
            
        Returns:
            ee.Image: Image with values converted to percentage
        """
        # Multiply by 100 to convert m³/m³ to %
        converted = image.select(self.bands).multiply(100)
        
        # Copy properties from original image
        return converted.copyProperties(image, ['system:time_start', 'system:time_end'])
    
    
    def _aggregate_daily(self, collection):
        """
        Aggregate 3-hourly SMAP data to daily mean
        
        Args:
            collection (ee.ImageCollection): 3-hourly SMAP collection
            
        Returns:
            ee.ImageCollection: Daily aggregated collection
        """
        # Get date range
        date_range = collection.aggregate_array('system:time_start')
        start_millis = date_range.reduce(ee.Reducer.min())
        end_millis = date_range.reduce(ee.Reducer.max())
        
        start_date = ee.Date(start_millis)
        end_date = ee.Date(end_millis).advance(1, 'day')
        
        # Calculate number of days
        num_days = end_date.difference(start_date, 'day').toInt()
        
        # Create daily sequence
        day_sequence = ee.List.sequence(0, num_days.subtract(1))
        
        def aggregate_day(day_offset):
            """Aggregate images for a single day"""
            day_offset = ee.Number(day_offset)
            day_start = start_date.advance(day_offset, 'day')
            day_end = day_start.advance(1, 'day')
            
            # Filter images for this day
            day_images = collection.filterDate(day_start, day_end)
            
            # Calculate daily mean
            daily_mean = day_images.mean()
            
            # Convert to percentage
            daily_mean_pct = self._convert_to_percentage(daily_mean)
            
            # Set date property
            return daily_mean_pct.set({
                'system:time_start': day_start.millis(),
                'date': day_start.format('YYYY-MM-dd')
            })
        
        # Map over days
        daily_collection = ee.ImageCollection.fromImages(
            day_sequence.map(aggregate_day)
        )
        
        return daily_collection
    
    
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
        
        # Reduce region
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
        
        # Sample point
        sample = image.sample(
            region=point,
            scale=scale,
            geometries=False
        ).first()
        
        if sample is None:
            return {band: None for band in self.bands}
        
        # Get values
        values = {}
        for band in self.bands:
            values[band] = sample.get(band).getInfo()
        
        return values
    
    
    def get_timeseries(self, geometry, start_date, end_date, spatial_stat='mean', is_point=False):
        """
        Extract daily soil moisture timeseries for a geometry
        
        Args:
            geometry (ee.Geometry): Point or polygon geometry
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            spatial_stat (str): Spatial statistic (mean, median, max, min, sum)
            is_point (bool): Whether geometry is a point
            
        Returns:
            list: List of daily soil moisture values
            
        Example:
            >>> extractor = SMAPExtractor()
            >>> geometry = ee.Geometry.Point([31.0530, -17.8249])
            >>> data = extractor.get_timeseries(geometry, '2024-01-01', '2024-01-07', is_point=True)
            >>> print(data[0])
            {'date': '2024-01-01', 'sm_surface': 28.5, 'sm_rootzone': 35.2}
        """
        # Validate date range (SMAP starts March 31, 2015)
        smap_start = datetime(2015, 3, 31)
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        
        if start_dt < smap_start:
            raise ValueError(f"SMAP data only available from {smap_start.strftime('%Y-%m-%d')} onwards")
        
        # Get collection
        collection = self._get_collection(start_date, end_date, geometry)
        
        # Check if collection is empty
        size = collection.size().getInfo()
        if size == 0:
            raise ValueError(f"No SMAP data available for date range {start_date} to {end_date}")
        
        # Aggregate to daily
        daily_collection = self._aggregate_daily(collection)
        
        # Get list of images
        images = daily_collection.toList(daily_collection.size())
        num_images = images.size().getInfo()
        
        print(f"Processing {num_images} days of SMAP data...")
        
        # Extract values for each day
        timeseries = []
        
        for i in range(num_images):
            image = ee.Image(images.get(i))
            date = image.get('date').getInfo()
            
            # Extract values based on geometry type
            if is_point:
                values = self._sample_image_at_point(image, geometry)
            else:
                reducer = self._get_spatial_reducer(spatial_stat)
                values = self._reduce_image_over_region(image, geometry, reducer)
            
            # Extract soil moisture values
            sm_surface = values.get('sm_surface')
            sm_rootzone = values.get('sm_rootzone')
            
            # Handle None values (nodata)
            if sm_surface is None or sm_surface == 'None':
                sm_surface = -999
            else:
                sm_surface = round(float(sm_surface), 2)
            
            if sm_rootzone is None or sm_rootzone == 'None':
                sm_rootzone = -999
            else:
                sm_rootzone = round(float(sm_rootzone), 2)
            
            # Add to timeseries
            timeseries.append({
                'date': date,
                'sm_surface': sm_surface,
                'sm_rootzone': sm_rootzone
            })
        
        # Sort by date
        timeseries.sort(key=lambda x: x['date'])
        
        print(f"Extracted {len(timeseries)} days of soil moisture data")
        
        return timeseries
    
    
    def export_geotiff(self, geometry, start_date, end_date, resolution_deg=0.09, 
                       clip_to_geometry=True, export_mode='multiband', 
                       band_selection='both'):
        """
        Export SMAP soil moisture data as GeoTIFF
        
        Args:
            geometry (ee.Geometry): Region to export
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            resolution_deg (float): Output resolution in degrees (default: 0.09 for 9km)
            clip_to_geometry (bool): Whether to clip to geometry bounds
            export_mode (str): 'multiband' (one file) or 'zip' (daily files, max 31 days)
            band_selection (str): 'sm_surface', 'sm_rootzone', or 'both'
            
        Returns:
            dict: Export configuration with download URL
            
        Note:
            - Multiband mode: One file with all days as bands
            - Zip mode: Separate GeoTIFF for each day (limited to 31 days)
        """
        # Validate date range
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        num_days = (end_dt - start_dt).days + 1
        
        if export_mode == 'zip' and num_days > 31:
            raise ValueError("Zip export mode limited to 31 days maximum")
        
        # Validate band selection
        valid_bands = ['sm_surface', 'sm_rootzone', 'both']
        if band_selection not in valid_bands:
            raise ValueError(f"band_selection must be one of: {', '.join(valid_bands)}")
        
        # Get collection and aggregate to daily
        collection = self._get_collection(start_date, end_date, geometry)
        daily_collection = self._aggregate_daily(collection)
        
        # Select bands based on band_selection
        if band_selection == 'sm_surface':
            daily_collection = daily_collection.select(['sm_surface'])
            export_bands = ['sm_surface']
        elif band_selection == 'sm_rootzone':
            daily_collection = daily_collection.select(['sm_rootzone'])
            export_bands = ['sm_rootzone']
        else:  # both
            export_bands = self.bands
        
        # Convert resolution from degrees to meters (approximate)
        # At equator: 1 degree ≈ 111,320 meters
        resolution_m = int(resolution_deg * 111320)
        
        # Get geometry bounds
        if clip_to_geometry:
            bounds = geometry.bounds()
        else:
            bounds = geometry
        
        # Prepare export based on mode
        if export_mode == 'multiband':
            # Combine all daily images into multiband image
            image_list = daily_collection.toList(daily_collection.size())
            num_images = image_list.size().getInfo()
            
            # Create multiband image
            def add_band(current, previous):
                return ee.Image(previous).addBands(ee.Image(current))
            
            if num_images > 0:
                first_image = ee.Image(image_list.get(0))
                multiband = ee.Image(image_list.slice(1).iterate(add_band, first_image))
                
                # Generate filename
                filename = f"smap_soilmoisture_{start_date}_{end_date}"
                
                # Create download URL
                url = multiband.getDownloadURL({
                    'region': bounds,
                    'scale': resolution_m,
                    'crs': 'EPSG:4326',
                    'fileFormat': 'GeoTIFF',
                    'formatOptions': {
                        'cloudOptimized': True
                    }
                })
                
                return {
                    'mode': 'multiband',
                    'filename': filename + '.tif',
                    'download_url': url,
                    'num_days': num_images,
                    'bands': export_bands,
                    'date_range': {
                        'start': start_date,
                        'end': end_date
                    },
                    'resolution_m': resolution_m,
                    'crs': 'EPSG:4326'
                }
            else:
                raise ValueError("No images found in collection")
        
        else:  # zip mode
            # Export individual daily GeoTIFFs
            image_list = daily_collection.toList(daily_collection.size())
            num_images = image_list.size().getInfo()
            
            if num_images == 0:
                raise ValueError("No images found in collection")
            
            # Create list of download URLs for each day
            downloads = []
            
            for i in range(num_images):
                image = ee.Image(image_list.get(i))
                date = image.get('date').getInfo()
                
                filename = f"smap_soilmoisture_{date}"
                
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
                'bands': export_bands,
                'date_range': {
                    'start': start_date,
                    'end': end_date
                },
                'resolution_m': resolution_m,
                'crs': 'EPSG:4326',
                'files': downloads
            }
    
    
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
        """
        # Get timeseries
        timeseries = self.get_timeseries(geometry, start_date, end_date, spatial_stat, is_point)
        
        # Filter out nodata values
        valid_surface = [d['sm_surface'] for d in timeseries if d['sm_surface'] != -999]
        valid_rootzone = [d['sm_rootzone'] for d in timeseries if d['sm_rootzone'] != -999]
        
        if not valid_surface or not valid_rootzone:
            raise ValueError("No valid data points found")
        
        # Calculate statistics
        stats = {
            'sm_surface': {
                'mean': round(sum(valid_surface) / len(valid_surface), 2),
                'min': round(min(valid_surface), 2),
                'max': round(max(valid_surface), 2),
                'median': round(sorted(valid_surface)[len(valid_surface)//2], 2),
                'num_days': len(valid_surface)
            },
            'sm_rootzone': {
                'mean': round(sum(valid_rootzone) / len(valid_rootzone), 2),
                'min': round(min(valid_rootzone), 2),
                'max': round(max(valid_rootzone), 2),
                'median': round(sorted(valid_rootzone)[len(valid_rootzone)//2], 2),
                'num_days': len(valid_rootzone)
            },
            'date_range': {
                'start': start_date,
                'end': end_date,
                'total_days': len(timeseries)
            }
        }
        
        return stats


# Example usage
if __name__ == '__main__':
    # Initialize Earth Engine
    try:
        ee.Initialize()
    except:
        ee.Authenticate()
        ee.Initialize()
    
    # Create extractor
    extractor = SMAPExtractor()
    
    # Test location: Harare, Zimbabwe
    harare = ee.Geometry.Point([31.0530, -17.8249])
    
    # Get metadata
    print("Dataset Metadata:")
    print(extractor.get_metadata())
    
    # Extract timeseries for 7 days
    print("\nExtracting timeseries for Harare (2024-10-01 to 2024-10-07)...")
    timeseries = extractor.get_timeseries(
        geometry=harare,
        start_date='2024-10-01',
        end_date='2024-10-07',
        is_point=True
    )
    
    print(f"\nFirst 3 days:")
    for day in timeseries[:3]:
        print(f"  {day['date']}: Surface={day['sm_surface']}%, RootZone={day['sm_rootzone']}%")
    
    # Get statistics
    print("\nCalculating statistics...")
    stats = extractor.get_statistics(
        geometry=harare,
        start_date='2024-10-01',
        end_date='2024-10-07',
        is_point=True
    )
    
    print(f"\nSurface Soil Moisture (0-5cm):")
    print(f"  Mean: {stats['sm_surface']['mean']}%")
    print(f"  Range: {stats['sm_surface']['min']}% - {stats['sm_surface']['max']}%")
    
    print(f"\nRoot Zone Soil Moisture (0-100cm):")
    print(f"  Mean: {stats['sm_rootzone']['mean']}%")
    print(f"  Range: {stats['sm_rootzone']['min']}% - {stats['sm_rootzone']['max']}%")
