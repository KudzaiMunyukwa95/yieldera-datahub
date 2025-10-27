"""
DataHub Timeseries Tests
"""

import pytest
import json
from datetime import datetime, timedelta


class TestCHIRPSTimeseries:
    """Test CHIRPS timeseries extraction"""
    
    def test_point_timeseries(self, client, gee_initialized):
        """Test CHIRPS timeseries for a point"""
        payload = {
            "geometry": {
                "type": "point",
                "lat": -17.8249,
                "lon": 31.0530
            },
            "date_range": {
                "start": "2024-01-01",
                "end": "2024-01-31"
            },
            "spatial_stat": "mean",
            "temporal_stat": "daily"
        }
        
        response = client.post(
            '/api/data/chirps/timeseries',
            data=json.dumps(payload),
            content_type='application/json'
        )
        
        assert response.status_code == 200
        data = response.get_json()
        
        assert data["dataset"] == "CHIRPS"
        assert data["variable"] == "precip"
        assert len(data["data"]) == 31
        assert "precip_mm" in data["data"][0]
        assert data["geometry_summary"]["type"] == "Point"
    
    def test_polygon_timeseries(self, client, gee_initialized):
        """Test CHIRPS timeseries for a polygon"""
        payload = {
            "geometry": {
                "type": "wkt",
                "wkt": "POLYGON((31.0 -17.9, 31.1 -17.9, 31.1 -17.8, 31.0 -17.8, 31.0 -17.9))"
            },
            "date_range": {
                "start": "2024-01-01",
                "end": "2024-01-10"
            },
            "spatial_stat": "mean"
        }
        
        response = client.post(
            '/api/data/chirps/timeseries',
            data=json.dumps(payload),
            content_type='application/json'
        )
        
        assert response.status_code == 200
        data = response.get_json()
        
        assert data["dataset"] == "CHIRPS"
        assert len(data["data"]) == 10
        assert data["geometry_summary"]["area_km2"] is not None
    
    def test_invalid_date_range(self, client):
        """Test validation of invalid date range"""
        payload = {
            "geometry": {
                "type": "point",
                "lat": -17.8249,
                "lon": 31.0530
            },
            "date_range": {
                "start": "2024-01-31",
                "end": "2024-01-01"  # End before start
            }
        }
        
        response = client.post(
            '/api/data/chirps/timeseries',
            data=json.dumps(payload),
            content_type='application/json'
        )
        
        assert response.status_code == 400
    
    def test_missing_geometry(self, client):
        """Test validation of missing geometry"""
        payload = {
            "date_range": {
                "start": "2024-01-01",
                "end": "2024-01-31"
            }
        }
        
        response = client.post(
            '/api/data/chirps/timeseries',
            data=json.dumps(payload),
            content_type='application/json'
        )
        
        assert response.status_code == 400


class TestERA5Timeseries:
    """Test ERA5-Land timeseries extraction"""
    
    def test_point_temperature(self, client, gee_initialized):
        """Test ERA5-Land temperature for a point"""
        payload = {
            "geometry": {
                "type": "point",
                "lat": -17.8249,
                "lon": 31.0530
            },
            "date_range": {
                "start": "2024-01-01",
                "end": "2024-01-07"  # One week
            },
            "spatial_stat": "mean"
        }
        
        response = client.post(
            '/api/data/era5land/timeseries',
            data=json.dumps(payload),
            content_type='application/json'
        )
        
        assert response.status_code == 200
        data = response.get_json()
        
        assert data["dataset"] == "ERA5-Land"
        assert data["variable"] == "t2m"
        assert len(data["data"]) == 7
        
        # Check temperature fields
        first_day = data["data"][0]
        assert "tmin_c" in first_day
        assert "tmax_c" in first_day
        assert "tavg_c" in first_day
    
    def test_polygon_temperature(self, client, gee_initialized):
        """Test ERA5-Land temperature for a polygon"""
        payload = {
            "geometry": {
                "type": "wkt",
                "wkt": "POLYGON((31.0 -17.9, 31.2 -17.9, 31.2 -17.7, 31.0 -17.7, 31.0 -17.9))"
            },
            "date_range": {
                "start": "2024-01-01",
                "end": "2024-01-05"
            },
            "spatial_stat": "mean"
        }
        
        response = client.post(
            '/api/data/era5land/timeseries',
            data=json.dumps(payload),
            content_type='application/json'
        )
        
        assert response.status_code == 200
        data = response.get_json()
        
        assert len(data["data"]) == 5
        assert data["aggregation"]["spatial"] == "mean"
    
    def test_date_range_limit(self, client):
        """Test ERA5-Land date range validation (max 366 days)"""
        payload = {
            "geometry": {
                "type": "point",
                "lat": -17.8249,
                "lon": 31.0530
            },
            "date_range": {
                "start": "2023-01-01",
                "end": "2024-12-31"  # > 366 days
            }
        }
        
        response = client.post(
            '/api/data/era5land/timeseries',
            data=json.dumps(payload),
            content_type='application/json'
        )
        
        assert response.status_code == 400


class TestCaching:
    """Test request caching"""
    
    def test_cache_hit(self, client, gee_initialized):
        """Test that repeated requests hit cache"""
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
        
        # First request - should hit GEE
        response1 = client.post(
            '/api/data/chirps/timeseries',
            data=json.dumps(payload),
            content_type='application/json'
        )
        assert response1.status_code == 200
        
        # Second request - should hit cache (much faster)
        response2 = client.post(
            '/api/data/chirps/timeseries',
            data=json.dumps(payload),
            content_type='application/json'
        )
        assert response2.status_code == 200
        
        # Data should be identical
        assert response1.get_json()["data"] == response2.get_json()["data"]


# Pytest fixtures
@pytest.fixture
def client():
    """Flask test client"""
    from flask import Flask
    from app.datahub import datahub_bp
    
    app = Flask(__name__)
    app.register_blueprint(datahub_bp)
    
    with app.test_client() as client:
        yield client


@pytest.fixture
def gee_initialized():
    """Initialize Google Earth Engine"""
    import ee
    import os
    import json
    from google.oauth2 import service_account
    
    creds_json = os.getenv('GEE_SERVICE_ACCOUNT_CREDENTIALS_JSON')
    if creds_json:
        creds = service_account.Credentials.from_service_account_info(
            json.loads(creds_json),
            scopes=['https://www.googleapis.com/auth/earthengine.readonly']
        )
        ee.Initialize(credentials=creds, opt_url='https://earthengine-highvolume.googleapis.com')
    
    return True
