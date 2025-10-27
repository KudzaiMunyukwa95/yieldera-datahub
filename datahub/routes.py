"""
DataHub API routes
"""

from flask import Blueprint, request, jsonify, send_file
from pydantic import ValidationError as PydanticValidationError
from typing import Dict, Any
import ee

from .schemas import (
    TimeseriesRequest,
    GeoTIFFRequest,
    TimeseriesResponse,
    JobCreateResponse,
    JobStatusResponse
)
from .gee_chirps import CHIRPSExtractor
from .gee_era5land import ERA5LandExtractor
from .reducers import (
    parse_geometry,
    get_geometry_summary,
    validate_date_range,
    cap_end_date_to_present
)
from .jobs import JobStore, JobExecutor
from .storage import FileStorage
from .caching import RequestCache
from .errors import (
    DataHubError,
    ValidationError,
    GEEError,
    JobNotFoundError,
    handle_datahub_error
)


# Create blueprint
datahub_bp = Blueprint('datahub', __name__, url_prefix='/api/data')

# Initialize services
job_store = JobStore()
file_storage = FileStorage()
request_cache = RequestCache()


# Error handlers
@datahub_bp.errorhandler(DataHubError)
def handle_error(error):
    return handle_datahub_error(error)


@datahub_bp.errorhandler(PydanticValidationError)
def handle_validation_error(error):
    return jsonify({
        "error": "ValidationError",
        "message": "Invalid request data",
        "details": error.errors()
    }), 400


# Helper function to validate and parse requests
def validate_request(schema_class, data: Dict[str, Any]):
    """Validate request data using Pydantic schema"""
    try:
        return schema_class(**data)
    except PydanticValidationError as e:
        raise ValidationError(
            "Request validation failed",
            hint="Check request format and required fields",
            details={"errors": e.errors()}
        )


# =====================
# CHIRPS Endpoints
# =====================

@datahub_bp.route('/chirps/timeseries', methods=['POST'])
def chirps_timeseries():
    """
    Extract CHIRPS rainfall timeseries
    
    POST /api/data/chirps/timeseries
    """
    try:
        # Validate request
        req_data = validate_request(TimeseriesRequest, request.get_json())
        
        # Validate date range
        validate_date_range(
            req_data.date_range.start,
            req_data.date_range.end,
            max_days=int(os.getenv('MAX_DAYS', '5000'))
        )
        
        # Cap end date if in future
        end_date = cap_end_date_to_present(req_data.date_range.end)
        
        # Check cache
        cache_key = {
            "dataset": "chirps",
            "geometry": req_data.geometry.model_dump(),
            "start": req_data.date_range.start,
            "end": end_date,
            "spatial_stat": req_data.spatial_stat
        }
        
        cached_data = request_cache.get(cache_key, format="json")
        if cached_data:
            print("Cache hit for CHIRPS timeseries")
            return jsonify(cached_data)
        
        # Parse geometry
        geometry = parse_geometry(req_data.geometry.model_dump())
        geom_summary = get_geometry_summary(req_data.geometry.model_dump())
        is_point = req_data.geometry.type == "point"
        
        # Extract data
        extractor = CHIRPSExtractor()
        data = extractor.get_timeseries(
            geometry,
            req_data.date_range.start,
            end_date,
            req_data.spatial_stat,
            is_point
        )
        
        # Build response
        response_data = {
            "dataset": "CHIRPS",
            "variable": "precip",
            "aggregation": {
                "spatial": req_data.spatial_stat,
                "temporal": "daily"
            },
            "units": {
                "precip": "mm/day"
            },
            "geometry_summary": geom_summary,
            "date_range": {
                "start": req_data.date_range.start,
                "end": end_date
            },
            "data": data,
            "meta": extractor.get_metadata()
        }
        
        # Cache response
        request_cache.set(cache_key, response_data, format="json")
        
        return jsonify(response_data)
        
    except DataHubError:
        raise
    except Exception as e:
        print(f"Error in chirps_timeseries: {e}")
        raise GEEError(f"Failed to extract CHIRPS data: {str(e)}")


@datahub_bp.route('/chirps/geotiff', methods=['POST'])
def chirps_geotiff():
    """
    Export CHIRPS rainfall as GeoTIFF (async job)
    
    POST /api/data/chirps/geotiff
    """
    try:
        # Validate request
        req_data = validate_request(GeoTIFFRequest, request.get_json())
        
        # Validate date range
        validate_date_range(
            req_data.date_range.start,
            req_data.date_range.end,
            max_days=366  # Limit GeoTIFF exports
        )
        
        # Create job
        job_id = job_store.create_job(
            job_type="chirps_geotiff",
            request_data=request.get_json(),
            user_id=request.headers.get('X-User-ID')
        )
        
        # Execute job synchronously (in production, use background worker)
        JobExecutor.execute_geotiff_job(job_id, job_store)
        
        return jsonify({
            "job_id": job_id,
            "status": "queued",
            "message": "GeoTIFF export job created"
        }), 202
        
    except DataHubError:
        raise
    except Exception as e:
        print(f"Error in chirps_geotiff: {e}")
        raise GEEError(f"Failed to create CHIRPS GeoTIFF job: {str(e)}")


# =====================
# ERA5-Land Endpoints
# =====================

@datahub_bp.route('/era5land/timeseries', methods=['POST'])
def era5land_timeseries():
    """
    Extract ERA5-Land temperature timeseries
    
    POST /api/data/era5land/timeseries
    """
    try:
        # Validate request
        req_data = validate_request(TimeseriesRequest, request.get_json())
        
        # Validate date range - ERA5 is more expensive, limit to 1 year
        validate_date_range(
            req_data.date_range.start,
            req_data.date_range.end,
            max_days=366
        )
        
        # Cap end date if in future
        end_date = cap_end_date_to_present(req_data.date_range.end)
        
        # Check cache
        cache_key = {
            "dataset": "era5land",
            "geometry": req_data.geometry.model_dump(),
            "start": req_data.date_range.start,
            "end": end_date,
            "spatial_stat": req_data.spatial_stat
        }
        
        cached_data = request_cache.get(cache_key, format="json")
        if cached_data:
            print("Cache hit for ERA5-Land timeseries")
            return jsonify(cached_data)
        
        # Parse geometry
        geometry = parse_geometry(req_data.geometry.model_dump())
        geom_summary = get_geometry_summary(req_data.geometry.model_dump())
        is_point = req_data.geometry.type == "point"
        
        # Extract data
        extractor = ERA5LandExtractor()
        data = extractor.get_timeseries(
            geometry,
            req_data.date_range.start,
            end_date,
            req_data.spatial_stat,
            is_point
        )
        
        # Build response
        response_data = {
            "dataset": "ERA5-Land",
            "variable": "t2m",
            "aggregation": {
                "spatial": req_data.spatial_stat,
                "temporal": "daily"
            },
            "units": {
                "tmin": "°C",
                "tmax": "°C",
                "tavg": "°C"
            },
            "geometry_summary": geom_summary,
            "date_range": {
                "start": req_data.date_range.start,
                "end": end_date
            },
            "data": data,
            "meta": extractor.get_metadata()
        }
        
        # Cache response
        request_cache.set(cache_key, response_data, format="json")
        
        return jsonify(response_data)
        
    except DataHubError:
        raise
    except Exception as e:
        print(f"Error in era5land_timeseries: {e}")
        raise GEEError(f"Failed to extract ERA5-Land data: {str(e)}")


@datahub_bp.route('/era5land/geotiff', methods=['POST'])
def era5land_geotiff():
    """
    Export ERA5-Land temperature as GeoTIFF (async job)
    
    POST /api/data/era5land/geotiff
    """
    try:
        # Validate request
        req_data = validate_request(GeoTIFFRequest, request.get_json())
        
        # Validate date range
        validate_date_range(
            req_data.date_range.start,
            req_data.date_range.end,
            max_days=366
        )
        
        # Create job
        job_id = job_store.create_job(
            job_type="era5land_geotiff",
            request_data=request.get_json(),
            user_id=request.headers.get('X-User-ID')
        )
        
        # Execute job synchronously (in production, use background worker)
        JobExecutor.execute_geotiff_job(job_id, job_store)
        
        return jsonify({
            "job_id": job_id,
            "status": "queued",
            "message": "GeoTIFF export job created"
        }), 202
        
    except DataHubError:
        raise
    except Exception as e:
        print(f"Error in era5land_geotiff: {e}")
        raise GEEError(f"Failed to create ERA5-Land GeoTIFF job: {str(e)}")


# =====================
# Job Management
# =====================

@datahub_bp.route('/jobs/<job_id>/status', methods=['GET'])
def get_job_status(job_id: str):
    """
    Get job status
    
    GET /api/data/jobs/<job_id>/status
    """
    try:
        job_data = job_store.get_job(job_id)
        
        if not job_data:
            raise JobNotFoundError(
                f"Job {job_id} not found",
                hint="Check job ID or the job may have expired"
            )
        
        return jsonify(job_data)
        
    except DataHubError:
        raise
    except Exception as e:
        print(f"Error getting job status: {e}")
        raise DataHubError(f"Failed to get job status: {str(e)}")


@datahub_bp.route('/jobs/<job_id>/download', methods=['GET'])
def download_job_output(job_id: str):
    """
    Download job output
    
    GET /api/data/jobs/<job_id>/download?format=csv|tif
    """
    try:
        job_data = job_store.get_job(job_id)
        
        if not job_data:
            raise JobNotFoundError(f"Job {job_id} not found")
        
        if job_data["status"] != "done":
            return jsonify({
                "error": "JobNotReady",
                "message": f"Job is {job_data['status']}, not ready for download"
            }), 400
        
        format_type = request.args.get('format', 'tif')
        download_urls = job_data.get("download_urls", {})
        
        if format_type not in download_urls:
            return jsonify({
                "error": "FormatNotAvailable",
                "message": f"Format {format_type} not available for this job"
            }), 400
        
        # Return download URL (GEE direct link)
        return jsonify({
            "download_url": download_urls[format_type],
            "format": format_type
        })
        
    except DataHubError:
        raise
    except Exception as e:
        print(f"Error downloading job output: {e}")
        raise DataHubError(f"Failed to download job output: {str(e)}")


# =====================
# Utility Endpoints
# =====================

@datahub_bp.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        # Test GEE connection
        ee.Number(1).getInfo()
        
        return jsonify({
            "status": "healthy",
            "gee": "connected",
            "cache_stats": request_cache.get_cache_stats(),
            "storage_stats": file_storage.get_storage_stats()
        })
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "gee": "disconnected",
            "error": str(e)
        }), 503


@datahub_bp.route('/datasets', methods=['GET'])
def list_datasets():
    """List available datasets and metadata"""
    chirps = CHIRPSExtractor()
    era5 = ERA5LandExtractor()
    
    return jsonify({
        "datasets": [
            {
                "id": "chirps",
                "name": "CHIRPS Daily Precipitation",
                "metadata": chirps.get_metadata()
            },
            {
                "id": "era5land",
                "name": "ERA5-Land Temperature",
                "metadata": era5.get_metadata()
            }
        ]
    })


# Import os for env vars
import os
