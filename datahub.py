"""
DataHub Blueprint - Fixed version with proper response structure
"""

from flask import Blueprint, request, jsonify
from datetime import datetime
import ee

datahub_bp = Blueprint('datahub', __name__, url_prefix='/api/data')


def parse_geometry(geometry_dict):
    """Parse geometry from request"""
    geom_type = geometry_dict.get('type', '').lower()
    
    if geom_type == 'point':
        lat = float(geometry_dict['lat'])
        lon = float(geometry_dict['lon'])
        return ee.Geometry.Point([lon, lat]), True
    
    elif geom_type == 'polygon':
        coords = geometry_dict.get('coordinates', [])
        return ee.Geometry.Polygon(coords), False
    
    else:
        raise ValueError(f"Unsupported geometry type: {geom_type}")


@datahub_bp.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    try:
        ee.Number(1).getInfo()
        return jsonify({
            "status": "healthy",
            "gee": "connected",
            "timestamp": datetime.utcnow().isoformat()
        })
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "gee": "disconnected",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }), 503


@datahub_bp.route('/datasets', methods=['GET'])
def list_datasets():
    """List available datasets"""
    return jsonify({
        "datasets": [
            {
                "id": "chirps",
                "name": "CHIRPS Daily Rainfall",
                "variable": "precipitation",
                "resolution": "0.05 degrees (~5.5km)",
                "temporal_coverage": "1981-present",
                "update_frequency": "daily"
            },
            {
                "id": "era5land",
                "name": "ERA5-Land Temperature",
                "variable": "temperature_2m",
                "resolution": "0.1 degrees (~11km)",
                "temporal_coverage": "1950-present",
                "update_frequency": "~5 days behind"
            }
        ]
    })


@datahub_bp.route('/chirps/timeseries', methods=['POST', 'OPTIONS'])
def chirps_timeseries():
    """Extract CHIRPS rainfall timeseries"""
    
    # Handle preflight OPTIONS request
    if request.method == 'OPTIONS':
        return '', 204
    
    try:
        from gee_chirps import CHIRPSExtractor
        
        data = request.get_json()
        
        # Validate request
        if not data or 'geometry' not in data or 'date_range' not in data:
            return jsonify({
                "error": "Missing required fields: geometry and date_range"
            }), 400
        
        # Parse geometry
        geometry, is_point = parse_geometry(data['geometry'])
        
        # Get parameters
        start_date = data['date_range']['start']
        end_date = data['date_range']['end']
        spatial_stat = data.get('spatial_stat', 'mean')
        
        # Extract data
        extractor = CHIRPSExtractor()
        timeseries = extractor.get_timeseries(
            geometry=geometry,
            start_date=start_date,
            end_date=end_date,
            spatial_stat=spatial_stat,
            is_point=is_point
        )
        
        # Return in consistent format
        return jsonify(timeseries), 200
        
    except ValueError as e:
        return jsonify({"error": f"Invalid input: {str(e)}"}), 400
    except Exception as e:
        return jsonify({"error": f"Server error: {str(e)}"}), 500


@datahub_bp.route('/era5land/timeseries', methods=['POST', 'OPTIONS'])
def era5land_timeseries():
    """Extract ERA5-Land temperature timeseries"""
    
    # Handle preflight OPTIONS request
    if request.method == 'OPTIONS':
        return '', 204
    
    try:
        from gee_era5land import ERA5LandExtractor
        
        data = request.get_json()
        
        # Validate request
        if not data or 'geometry' not in data or 'date_range' not in data:
            return jsonify({
                "error": "Missing required fields: geometry and date_range"
            }), 400
        
        # Parse geometry
        geometry, is_point = parse_geometry(data['geometry'])
        
        # Get parameters
        start_date = data['date_range']['start']
        end_date = data['date_range']['end']
        spatial_stat = data.get('spatial_stat', 'mean')
        
        # Validate date range (ERA5 limited to 366 days)
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        days_diff = (end_dt - start_dt).days
        
        if days_diff > 366:
            return jsonify({
                "error": "ERA5-Land limited to 366 days. Please reduce date range."
            }), 400
        
        # Extract data
        extractor = ERA5LandExtractor()
        timeseries = extractor.get_timeseries(
            geometry=geometry,
            start_date=start_date,
            end_date=end_date,
            spatial_stat=spatial_stat,
            is_point=is_point
        )
        
        # Return in consistent format
        return jsonify(timeseries), 200
        
    except ValueError as e:
        return jsonify({"error": f"Invalid input: {str(e)}"}), 400
    except Exception as e:
        return jsonify({"error": f"Server error: {str(e)}"}), 500


@datahub_bp.route('/chirps/geotiff', methods=['POST'])
def chirps_geotiff():
    """Export CHIRPS as GeoTIFF (async job)"""
    from jobs import JobStore
    
    try:
        data = request.get_json()
        
        job_store = JobStore()
        job_id = job_store.create_job(
            job_type='chirps_geotiff',
            request_data=data
        )
        
        return jsonify({
            "job_id": job_id,
            "status": "queued",
            "message": "GeoTIFF export job created. Check status at /api/data/jobs/{job_id}/status"
        }), 202
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@datahub_bp.route('/era5land/geotiff', methods=['POST'])
def era5land_geotiff():
    """Export ERA5-Land as GeoTIFF (async job)"""
    from jobs import JobStore
    
    try:
        data = request.get_json()
        
        job_store = JobStore()
        job_id = job_store.create_job(
            job_type='era5_geotiff',
            request_data=data
        )
        
        return jsonify({
            "job_id": job_id,
            "status": "queued",
            "message": "GeoTIFF export job created. Check status at /api/data/jobs/{job_id}/status"
        }), 202
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@datahub_bp.route('/jobs/<job_id>/status', methods=['GET'])
def job_status(job_id):
    """Get job status"""
    from jobs import JobStore
    
    job_store = JobStore()
    job = job_store.get_job(job_id)
    
    if not job:
        return jsonify({"error": "Job not found"}), 404
    
    return jsonify(job)


@datahub_bp.route('/jobs/<job_id>/download', methods=['GET'])
def job_download(job_id):
    """Download job result"""
    from jobs import JobStore
    from flask import redirect
    
    job_store = JobStore()
    job = job_store.get_job(job_id)
    
    if not job:
        return jsonify({"error": "Job not found"}), 404
    
    if job['status'] != 'done':
        return jsonify({
            "error": "Job not complete",
            "status": job['status']
        }), 400
    
    # Get format
    format_type = request.args.get('format', 'tif')
    
    if format_type == 'tif' and job.get('download_urls', {}).get('tif'):
        return redirect(job['download_urls']['tif'])
    
    return jsonify({"error": "Download not available"}), 404
