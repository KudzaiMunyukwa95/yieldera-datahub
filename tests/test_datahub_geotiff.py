"""
DataHub GeoTIFF and Job Management Tests
"""

import pytest
import json
import time


class TestGeoTIFFExport:
    """Test GeoTIFF export functionality"""
    
    def test_chirps_geotiff_job_creation(self, client, gee_initialized):
        """Test CHIRPS GeoTIFF export job creation"""
        payload = {
            "geometry": {
                "type": "wkt",
                "wkt": "POLYGON((31.0 -17.9, 31.1 -17.9, 31.1 -17.8, 31.0 -17.8, 31.0 -17.9))"
            },
            "date_range": {
                "start": "2024-01-01",
                "end": "2024-01-10"
            },
            "resolution_deg": 0.05,
            "clip_to_geometry": True,
            "tiff_mode": "multiband"
        }
        
        response = client.post(
            '/api/data/chirps/geotiff',
            data=json.dumps(payload),
            content_type='application/json'
        )
        
        assert response.status_code == 202  # Accepted
        data = response.get_json()
        
        assert "job_id" in data
        assert data["status"] == "queued"
        
        # Return job_id for status check
        return data["job_id"]
    
    def test_era5_geotiff_job_creation(self, client, gee_initialized):
        """Test ERA5-Land GeoTIFF export job creation"""
        payload = {
            "geometry": {
                "type": "point",
                "lat": -17.8249,
                "lon": 31.0530,
                "buffer_m": 5000
            },
            "date_range": {
                "start": "2024-01-01",
                "end": "2024-01-07"
            },
            "resolution_deg": 0.1,
            "band": "tavg"
        }
        
        response = client.post(
            '/api/data/era5land/geotiff',
            data=json.dumps(payload),
            content_type='application/json'
        )
        
        assert response.status_code == 202
        data = response.get_json()
        
        assert "job_id" in data
        return data["job_id"]


class TestJobManagement:
    """Test job status and management"""
    
    def test_job_status(self, client, gee_initialized):
        """Test job status endpoint"""
        # Create a job first
        payload = {
            "geometry": {
                "type": "point",
                "lat": -17.8249,
                "lon": 31.0530
            },
            "date_range": {
                "start": "2024-01-01",
                "end": "2024-01-05"
            }
        }
        
        create_response = client.post(
            '/api/data/chirps/geotiff',
            data=json.dumps(payload),
            content_type='application/json'
        )
        
        job_id = create_response.get_json()["job_id"]
        
        # Check status
        status_response = client.get(f'/api/data/jobs/{job_id}/status')
        
        assert status_response.status_code == 200
        status_data = status_response.get_json()
        
        assert status_data["job_id"] == job_id
        assert "status" in status_data
        assert "progress" in status_data
    
    def test_job_not_found(self, client):
        """Test job not found error"""
        fake_job_id = "00000000-0000-0000-0000-000000000000"
        
        response = client.get(f'/api/data/jobs/{fake_job_id}/status')
        
        assert response.status_code == 404
        data = response.get_json()
        
        assert "error" in data
    
    def test_job_download_not_ready(self, client, gee_initialized):
        """Test downloading from a queued/running job"""
        # Create job
        payload = {
            "geometry": {
                "type": "point",
                "lat": -17.8249,
                "lon": 31.0530
            },
            "date_range": {
                "start": "2024-01-01",
                "end": "2024-01-03"
            }
        }
        
        create_response = client.post(
            '/api/data/chirps/geotiff',
            data=json.dumps(payload),
            content_type='application/json'
        )
        
        job_id = create_response.get_json()["job_id"]
        
        # Try to download immediately (might still be processing)
        # In real scenario with async workers, this would return "not ready"
        download_response = client.get(f'/api/data/jobs/{job_id}/download?format=tif')
        
        # Either 200 (done) or 400 (not ready) depending on execution speed
        assert download_response.status_code in [200, 400]


class TestValidation:
    """Test input validation"""
    
    def test_invalid_geometry_type(self, client):
        """Test invalid geometry type"""
        payload = {
            "geometry": {
                "type": "invalid_type",
                "lat": -17.8249,
                "lon": 31.0530
            },
            "date_range": {
                "start": "2024-01-01",
                "end": "2024-01-05"
            }
        }
        
        response = client.post(
            '/api/data/chirps/timeseries',
            data=json.dumps(payload),
            content_type='application/json'
        )
        
        assert response.status_code == 400
    
    def test_invalid_wkt(self, client):
        """Test invalid WKT string"""
        payload = {
            "geometry": {
                "type": "wkt",
                "wkt": "INVALID WKT STRING"
            },
            "date_range": {
                "start": "2024-01-01",
                "end": "2024-01-05"
            }
        }
        
        response = client.post(
            '/api/data/chirps/timeseries',
            data=json.dumps(payload),
            content_type='application/json'
        )
        
        assert response.status_code in [400, 500]  # Validation or GEE error
    
    def test_invalid_date_format(self, client):
        """Test invalid date format"""
        payload = {
            "geometry": {
                "type": "point",
                "lat": -17.8249,
                "lon": 31.0530
            },
            "date_range": {
                "start": "01-01-2024",  # Wrong format
                "end": "2024-01-05"
            }
        }
        
        response = client.post(
            '/api/data/chirps/timeseries',
            data=json.dumps(payload),
            content_type='application/json'
        )
        
        assert response.status_code == 400
    
    def test_coordinates_out_of_range(self, client):
        """Test coordinates out of valid range"""
        payload = {
            "geometry": {
                "type": "point",
                "lat": 95.0,  # > 90
                "lon": 31.0530
            },
            "date_range": {
                "start": "2024-01-01",
                "end": "2024-01-05"
            }
        }
        
        response = client.post(
            '/api/data/chirps/timeseries',
            data=json.dumps(payload),
            content_type='application/json'
        )
        
        assert response.status_code == 400


class TestUtilityEndpoints:
    """Test utility endpoints"""
    
    def test_health_check(self, client, gee_initialized):
        """Test health check endpoint"""
        response = client.get('/api/data/health')
        
        assert response.status_code == 200
        data = response.get_json()
        
        assert data["status"] == "healthy"
        assert data["gee"] == "connected"
    
    def test_list_datasets(self, client):
        """Test datasets listing"""
        response = client.get('/api/data/datasets')
        
        assert response.status_code == 200
        data = response.get_json()
        
        assert "datasets" in data
        assert len(data["datasets"]) >= 2  # CHIRPS and ERA5-Land
        
        # Check dataset structure
        chirps = next(d for d in data["datasets"] if d["id"] == "chirps")
        assert "metadata" in chirps
        assert "source" in chirps["metadata"]


# Import fixtures from test_datahub_timeseries
from .test_datahub_timeseries import client, gee_initialized
