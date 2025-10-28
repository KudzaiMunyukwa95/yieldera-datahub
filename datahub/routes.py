"""
DataHub Blueprint - Complete version with CHIRPS, ERA5-Land, and SMAP L4
"""

from flask import Blueprint, request, jsonify, redirect
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
            },
            {
                "id": "smap",
                "name": "SMAP L4 Soil Moisture",
                "variables": ["sm_surface", "sm_rootzone"],
                "resolution": "0.09 degrees (~9km)",
                "temporal_coverage": "2015-03-31 to present",
                "update_frequency": "daily (~3 day latency)"
            }
        ]
    })


# ============================================================================
# CHIRPS ENDPOINTS
# ============================================================================

@datahub_bp.route('/chirps/timeseries', methods=['POST', 'OPTIONS'])
def chirps_timeseries():
    """Extract CHIRPS rainfall timeseries"""
    
    # Handle preflight OPTIONS request
    if request.method == 'OPTIONS':
        return '', 204
    
    try:
        from datahub.gee_chirps import CHIRPSExtractor
        
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


@datahub_bp.route('/chirps/geotiff', methods=['POST'])
def chirps_geotiff():
    """Export CHIRPS as GeoTIFF (async job)"""
    from datahub.jobs import JobStore
    
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


# ============================================================================
# ERA5-LAND ENDPOINTS
# ============================================================================

@datahub_bp.route('/era5land/timeseries', methods=['POST', 'OPTIONS'])
def era5land_timeseries():
    """Extract ERA5-Land temperature timeseries"""
    
    # Handle preflight OPTIONS request
    if request.method == 'OPTIONS':
        return '', 204
    
    try:
        from datahub.gee_era5land import ERA5LandExtractor
        
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


@datahub_bp.route('/era5land/geotiff', methods=['POST'])
def era5land_geotiff():
    """Export ERA5-Land as GeoTIFF (async job)"""
    from datahub.jobs import JobStore
    
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


# ============================================================================
# SMAP L4 SOIL MOISTURE ENDPOINTS
# ============================================================================

@datahub_bp.route('/smap/timeseries', methods=['POST', 'OPTIONS'])
def smap_timeseries():
    """
    Extract SMAP L4 soil moisture timeseries
    
    POST /api/data/smap/timeseries
    
    Request Body:
    {
        "geometry": {
            "type": "point",
            "lat": -17.8249,
            "lon": 31.0530
        },
        "date_range": {
            "start": "2024-10-01",
            "end": "2024-10-31"
        },
        "spatial_stat": "mean"  // optional: mean, median, max, min, sum
    }
    
    Response:
    [
        {
            "date": "2024-10-01",
            "sm_surface": 28.5,
            "sm_rootzone": 35.2
        },
        ...
    ]
    """
    # Handle preflight OPTIONS request
    if request.method == 'OPTIONS':
        return '', 204
    
    try:
        from datahub.gee_smap import SMAPExtractor
        
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
        
        # Validate date range (SMAP starts March 31, 2015)
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        smap_start = datetime(2015, 3, 31)
        
        if start_dt < smap_start:
            return jsonify({
                "error": f"SMAP data only available from 2015-03-31 onwards"
            }), 400
        
        # Check date range length (warn if too long)
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        days_diff = (end_dt - start_dt).days
        
        if days_diff > 366:
            return jsonify({
                "error": "Date range limited to 366 days for performance. Please reduce range."
            }), 400
        
        # Extract data
        extractor = SMAPExtractor()
        timeseries = extractor.get_timeseries(
            geometry=geometry,
            start_date=start_date,
            end_date=end_date,
            spatial_stat=spatial_stat,
            is_point=is_point
        )
        
        # Return array directly (matching CHIRPS and ERA5 format)
        return jsonify(timeseries), 200
        
    except ValueError as e:
        return jsonify({"error": f"Invalid input: {str(e)}"}), 400
    except Exception as e:
        print(f"SMAP Error: {str(e)}")
        return jsonify({"error": f"Server error: {str(e)}"}), 500


@datahub_bp.route('/smap/statistics', methods=['POST', 'OPTIONS'])
def smap_statistics():
    """
    Get summary statistics for SMAP soil moisture over a time period
    
    POST /api/data/smap/statistics
    
    Request Body:
    {
        "geometry": {
            "type": "point",
            "lat": -17.8249,
            "lon": 31.0530
        },
        "date_range": {
            "start": "2024-10-01",
            "end": "2024-10-31"
        },
        "spatial_stat": "mean"
    }
    
    Response:
    {
        "sm_surface": {
            "mean": 28.5,
            "min": 15.2,
            "max": 42.3,
            "median": 29.1,
            "num_days": 31
        },
        "sm_rootzone": {
            "mean": 35.2,
            "min": 28.1,
            "max": 45.6,
            "median": 36.0,
            "num_days": 31
        },
        "date_range": {
            "start": "2024-10-01",
            "end": "2024-10-31",
            "total_days": 31
        }
    }
    """
    # Handle preflight OPTIONS request
    if request.method == 'OPTIONS':
        return '', 204
    
    try:
        from datahub.gee_smap import SMAPExtractor
        
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
        
        # Extract statistics
        extractor = SMAPExtractor()
        stats = extractor.get_statistics(
            geometry=geometry,
            start_date=start_date,
            end_date=end_date,
            spatial_stat=spatial_stat,
            is_point=is_point
        )
        
        return jsonify(stats), 200
        
    except ValueError as e:
        return jsonify({"error": f"Invalid input: {str(e)}"}), 400
    except Exception as e:
        print(f"SMAP Statistics Error: {str(e)}")
        return jsonify({"error": f"Server error: {str(e)}"}), 500


@datahub_bp.route('/smap/export', methods=['POST', 'OPTIONS'])
def smap_export():
    """
    Export SMAP soil moisture data as GeoTIFF
    
    POST /api/data/smap/export
    
    Request Body:
    {
        "geometry": {
            "type": "polygon",
            "coordinates": [[[lon1, lat1], [lon2, lat2], ...]]
        },
        "date_range": {
            "start": "2024-10-01",
            "end": "2024-10-07"
        },
        "resolution_deg": 0.09,  // optional: default 0.09 (9km)
        "clip_to_geometry": true,  // optional: default true
        "export_mode": "multiband",  // optional: "multiband" or "zip"
        "band_selection": "both"  // optional: "sm_surface", "sm_rootzone", or "both"
    }
    
    Response (multiband mode):
    {
        "mode": "multiband",
        "filename": "smap_soilmoisture_2024-10-01_2024-10-07.tif",
        "download_url": "https://...",
        "num_days": 7,
        "bands": ["sm_surface", "sm_rootzone"],
        "date_range": {"start": "2024-10-01", "end": "2024-10-07"},
        "resolution_m": 9000,
        "crs": "EPSG:4326"
    }
    """
    # Handle preflight OPTIONS request
    if request.method == 'OPTIONS':
        return '', 204
    
    try:
        from datahub.gee_smap import SMAPExtractor
        
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
        resolution_deg = data.get('resolution_deg', 0.09)
        clip_to_geometry = data.get('clip_to_geometry', True)
        export_mode = data.get('export_mode', 'multiband')
        band_selection = data.get('band_selection', 'both')
        
        # Export GeoTIFF
        extractor = SMAPExtractor()
        export_config = extractor.export_geotiff(
            geometry=geometry,
            start_date=start_date,
            end_date=end_date,
            resolution_deg=resolution_deg,
            clip_to_geometry=clip_to_geometry,
            export_mode=export_mode,
            band_selection=band_selection
        )
        
        return jsonify(export_config), 200
        
    except ValueError as e:
        return jsonify({"error": f"Invalid input: {str(e)}"}), 400
    except Exception as e:
        print(f"SMAP Export Error: {str(e)}")
        return jsonify({"error": f"Server error: {str(e)}"}), 500


@datahub_bp.route('/smap/metadata', methods=['GET', 'OPTIONS'])
def smap_metadata():
    """
    Get SMAP L4 dataset metadata
    
    GET /api/data/smap/metadata
    
    Response:
    {
        "source": "GEE: NASA/SMAP/SPL4SMGP/007",
        "variables": ["sm_surface", "sm_rootzone"],
        "units": "percent (%)",
        "native_resolution_deg": 0.09,
        "temporal_coverage": "2015-03-31 to present",
        ...
    }
    """
    # Handle preflight OPTIONS request
    if request.method == 'OPTIONS':
        return '', 204
    
    try:
        from datahub.gee_smap import SMAPExtractor
        
        extractor = SMAPExtractor()
        metadata = extractor.get_metadata()
        
        return jsonify(metadata), 200
        
    except Exception as e:
        print(f"SMAP Metadata Error: {str(e)}")
        return jsonify({"error": f"Server error: {str(e)}"}), 500


# ============================================================================
# JOB MANAGEMENT ENDPOINTS
# ============================================================================

@datahub_bp.route('/jobs/<job_id>/status', methods=['GET'])
def job_status(job_id):
    """Get job status"""
    from datahub.jobs import JobStore
    
    job_store = JobStore()
    job = job_store.get_job(job_id)
    
    if not job:
        return jsonify({"error": "Job not found"}), 404
    
    return jsonify(job)


@datahub_bp.route('/jobs/<job_id>/download', methods=['GET'])
def job_download(job_id):
    """Download job result"""
    from datahub.jobs import JobStore
    
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
