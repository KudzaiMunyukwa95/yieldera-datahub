"""
Yieldera DataHub Python Client

Simple wrapper for accessing DataHub climate data API
"""

import requests
import time
from typing import Dict, List, Optional, Any
import pandas as pd


class DataHubClient:
    """Client for Yieldera DataHub API"""
    
    def __init__(self, base_url: str, api_key: Optional[str] = None):
        """
        Initialize DataHub client
        
        Args:
            base_url: API base URL (e.g., 'https://api.yieldera.co.zw')
            api_key: Optional API key for authentication
        """
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.session = requests.Session()
        
        if api_key:
            self.session.headers.update({
                'Authorization': f'Bearer {api_key}'
            })
    
    def get_chirps_timeseries(
        self,
        lat: float,
        lon: float,
        start_date: str,
        end_date: str,
        spatial_stat: str = "mean"
    ) -> pd.DataFrame:
        """
        Get CHIRPS rainfall timeseries for a point
        
        Args:
            lat: Latitude
            lon: Longitude
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            spatial_stat: Spatial aggregation ('mean', 'median', 'sum', 'min', 'max')
            
        Returns:
            pandas DataFrame with date and precip_mm columns
        """
        payload = {
            "geometry": {
                "type": "point",
                "lat": lat,
                "lon": lon
            },
            "date_range": {
                "start": start_date,
                "end": end_date
            },
            "spatial_stat": spatial_stat
        }
        
        response = self.session.post(
            f"{self.base_url}/api/data/chirps/timeseries",
            json=payload
        )
        response.raise_for_status()
        
        data = response.json()
        df = pd.DataFrame(data['data'])
        df['date'] = pd.to_datetime(df['date'])
        
        return df
    
    def get_chirps_polygon(
        self,
        wkt: str,
        start_date: str,
        end_date: str,
        spatial_stat: str = "mean"
    ) -> pd.DataFrame:
        """
        Get CHIRPS rainfall for a polygon
        
        Args:
            wkt: WKT polygon string
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            spatial_stat: Spatial aggregation
            
        Returns:
            pandas DataFrame
        """
        payload = {
            "geometry": {
                "type": "wkt",
                "wkt": wkt
            },
            "date_range": {
                "start": start_date,
                "end": end_date
            },
            "spatial_stat": spatial_stat
        }
        
        response = self.session.post(
            f"{self.base_url}/api/data/chirps/timeseries",
            json=payload
        )
        response.raise_for_status()
        
        data = response.json()
        df = pd.DataFrame(data['data'])
        df['date'] = pd.to_datetime(df['date'])
        
        return df
    
    def get_era5land_timeseries(
        self,
        lat: float,
        lon: float,
        start_date: str,
        end_date: str,
        spatial_stat: str = "mean"
    ) -> pd.DataFrame:
        """
        Get ERA5-Land temperature timeseries
        
        Args:
            lat: Latitude
            lon: Longitude
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            spatial_stat: Spatial aggregation
            
        Returns:
            pandas DataFrame with tmin_c, tmax_c, tavg_c columns
        """
        payload = {
            "geometry": {
                "type": "point",
                "lat": lat,
                "lon": lon
            },
            "date_range": {
                "start": start_date,
                "end": end_date
            },
            "spatial_stat": spatial_stat
        }
        
        response = self.session.post(
            f"{self.base_url}/api/data/era5land/timeseries",
            json=payload
        )
        response.raise_for_status()
        
        data = response.json()
        df = pd.DataFrame(data['data'])
        df['date'] = pd.to_datetime(df['date'])
        
        return df
    
    def export_chirps_geotiff(
        self,
        wkt: str,
        start_date: str,
        end_date: str,
        resolution_deg: float = 0.05,
        tiff_mode: str = "multiband"
    ) -> str:
        """
        Export CHIRPS data as GeoTIFF
        
        Args:
            wkt: WKT polygon string
            start_date: Start date
            end_date: End date
            resolution_deg: Output resolution in degrees
            tiff_mode: 'multiband' or 'zip'
            
        Returns:
            Job ID
        """
        payload = {
            "geometry": {
                "type": "wkt",
                "wkt": wkt
            },
            "date_range": {
                "start": start_date,
                "end": end_date
            },
            "resolution_deg": resolution_deg,
            "tiff_mode": tiff_mode
        }
        
        response = self.session.post(
            f"{self.base_url}/api/data/chirps/geotiff",
            json=payload
        )
        response.raise_for_status()
        
        return response.json()['job_id']
    
    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """
        Get job status
        
        Args:
            job_id: Job identifier
            
        Returns:
            Job status dictionary
        """
        response = self.session.get(
            f"{self.base_url}/api/data/jobs/{job_id}/status"
        )
        response.raise_for_status()
        
        return response.json()
    
    def wait_for_job(self, job_id: str, timeout: int = 300, poll_interval: int = 5) -> Dict[str, Any]:
        """
        Wait for job to complete
        
        Args:
            job_id: Job identifier
            timeout: Maximum wait time in seconds
            poll_interval: Polling interval in seconds
            
        Returns:
            Completed job data
            
        Raises:
            TimeoutError: If job doesn't complete in time
            RuntimeError: If job fails
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            status = self.get_job_status(job_id)
            
            if status['status'] == 'done':
                return status
            elif status['status'] == 'error':
                raise RuntimeError(f"Job failed: {status.get('error', 'Unknown error')}")
            
            time.sleep(poll_interval)
        
        raise TimeoutError(f"Job {job_id} did not complete within {timeout} seconds")
    
    def download_geotiff(self, job_id: str, output_path: Optional[str] = None) -> str:
        """
        Download GeoTIFF from completed job
        
        Args:
            job_id: Job identifier
            output_path: Optional output file path
            
        Returns:
            Download URL or file path
        """
        # Wait for job to complete
        job_data = self.wait_for_job(job_id)
        
        # Get download URL
        download_url = job_data['download_urls']['tif']
        
        if output_path:
            # Download file
            response = requests.get(download_url, stream=True)
            response.raise_for_status()
            
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            return output_path
        else:
            return download_url
    
    def health_check(self) -> Dict[str, Any]:
        """Check API health status"""
        response = self.session.get(f"{self.base_url}/api/data/health")
        response.raise_for_status()
        return response.json()
    
    def list_datasets(self) -> Dict[str, Any]:
        """List available datasets"""
        response = self.session.get(f"{self.base_url}/api/data/datasets")
        response.raise_for_status()
        return response.json()


# Example usage
if __name__ == "__main__":
    # Initialize client
    client = DataHubClient(
        base_url="https://api.yieldera.co.zw",
        api_key="your_api_key_here"
    )
    
    # Health check
    health = client.health_check()
    print(f"API Status: {health['status']}")
    
    # Get CHIRPS rainfall for Harare
    df_rain = client.get_chirps_timeseries(
        lat=-17.8249,
        lon=31.0530,
        start_date="2024-10-01",
        end_date="2024-12-31"
    )
    print(f"\nRainfall data: {len(df_rain)} days")
    print(df_rain.head())
    
    # Get temperature
    df_temp = client.get_era5land_timeseries(
        lat=-17.8249,
        lon=31.0530,
        start_date="2024-01-01",
        end_date="2024-01-31"
    )
    print(f"\nTemperature data: {len(df_temp)} days")
    print(df_temp.head())
    
    # Export GeoTIFF
    polygon_wkt = "POLYGON((31.0 -17.9, 31.2 -17.9, 31.2 -17.7, 31.0 -17.7, 31.0 -17.9))"
    job_id = client.export_chirps_geotiff(
        wkt=polygon_wkt,
        start_date="2024-01-01",
        end_date="2024-01-31"
    )
    print(f"\nGeoTIFF export job created: {job_id}")
    
    # Download
    download_url = client.download_geotiff(job_id)
    print(f"Download URL: {download_url}")
