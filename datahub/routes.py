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
        
        # Validate date range (ERA5 limited to 30 days for monthly analysis)
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        days_diff = (end_dt - start_dt).days
        
        if days_diff > 30:
            return jsonify({
                "error": "ERA5-Land limited to 30 days for optimal performance. Please reduce date range.",
                "suggestion": "For longer periods (60-90 days), use the statistics endpoint which is faster for seasonal analysis.",
                "max_days": 30,
                "requested_days": days_diff
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
        
        if days_diff > 30:
            return jsonify({
                "error": "SMAP limited to 30 days for optimal performance. Please reduce date range.",
                "suggestion": "For longer periods (60-90 days), use the statistics endpoint which is faster for seasonal analysis.",
                "max_days": 30,
                "requested_days": days_diff
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


# ============================================================================
# COMPARISON/PHASE ANALYSIS ENDPOINT
# ============================================================================

@datahub_bp.route('/compare/timeseries', methods=['POST', 'OPTIONS'])
def compare_timeseries():
    """
    Compare two time periods for any dataset (phase analysis)
    
    POST /api/data/compare/timeseries
    
    Request Body:
    {
        "dataset": "smap",  // or "chirps", "era5land"
        "geometry": {
            "type": "point",
            "lat": -17.8249,
            "lon": 31.0530
        },
        "period_1": {
            "start": "2022-11-01",
            "end": "2022-11-30",
            "label": "November 2022"  // optional
        },
        "period_2": {
            "start": "2023-11-01",
            "end": "2023-11-30",
            "label": "November 2023"  // optional
        },
        "spatial_stat": "mean"
    }
    
    Response:
    {
        "dataset": "smap",
        "period_1": {
            "label": "November 2022",
            "data": [...],
            "statistics": {"mean": 24.3, "min": 18.5, ...}
        },
        "period_2": {
            "label": "November 2023",
            "data": [...],
            "statistics": {"mean": 18.5, "min": 12.3, ...}
        },
        "comparison": {
            "mean_difference": -5.8,
            "mean_percent_change": -23.9,
            "status": "Period 2 is drier",
            "severity": "moderate",
            "interpretation": "Period 2 had 23.9% less soil moisture..."
        },
        "aligned_data": [
            {"day": 1, "value_1": 34.2, "value_2": 28.4, "difference": -5.8},
            ...
        ]
    }
    """
    
    # Handle preflight OPTIONS request
    if request.method == 'OPTIONS':
        return '', 204
    
    try:
        import numpy as np
        
        data = request.get_json()
        
        # Validate request
        required_fields = ['dataset', 'geometry', 'period_1', 'period_2']
        if not all(field in data for field in required_fields):
            return jsonify({
                "error": "Missing required fields",
                "required": required_fields
            }), 400
        
        dataset = data['dataset']
        geometry_data = data['geometry']
        period_1 = data['period_1']
        period_2 = data['period_2']
        spatial_stat = data.get('spatial_stat', 'mean')
        
        # Validate dataset
        valid_datasets = ['chirps', 'era5land', 'smap']
        if dataset not in valid_datasets:
            return jsonify({
                "error": f"Invalid dataset. Must be one of: {', '.join(valid_datasets)}"
            }), 400
        
        # Parse geometry
        geometry, is_point = parse_geometry(geometry_data)
        
        # Import appropriate extractor
        if dataset == 'chirps':
            from datahub.gee_chirps import CHIRPSExtractor
            extractor = CHIRPSExtractor()
            variable_name = 'precipitation'
        elif dataset == 'era5land':
            from datahub.gee_era5land import ERA5LandExtractor
            extractor = ERA5LandExtractor()
            variable_name = 'temperature'
        elif dataset == 'smap':
            from datahub.gee_smap import SMAPExtractor
            extractor = SMAPExtractor()
            variable_name = 'soil_moisture'
        
        # Extract data for period 1
        print(f"Extracting {dataset} data for period 1: {period_1['start']} to {period_1['end']}")
        data_1 = extractor.get_timeseries(
            geometry=geometry,
            start_date=period_1['start'],
            end_date=period_1['end'],
            spatial_stat=spatial_stat,
            is_point=is_point
        )
        
        # Extract data for period 2
        print(f"Extracting {dataset} data for period 2: {period_2['start']} to {period_2['end']}")
        data_2 = extractor.get_timeseries(
            geometry=geometry,
            start_date=period_2['start'],
            end_date=period_2['end'],
            spatial_stat=spatial_stat,
            is_point=is_point
        )
        
        # Calculate statistics for each period
        stats_1 = calculate_statistics(data_1, dataset)
        stats_2 = calculate_statistics(data_2, dataset)
        
        # Calculate comparison metrics
        comparison = calculate_comparison(stats_1, stats_2, dataset)
        
        # Align data by day of month for comparison
        aligned_data = align_timeseries(data_1, data_2, dataset)
        
        # Build response
        response = {
            "dataset": dataset,
            "variable": variable_name,
            "period_1": {
                "label": period_1.get('label', f"{period_1['start']} to {period_1['end']}"),
                "start": period_1['start'],
                "end": period_1['end'],
                "data": data_1,
                "statistics": stats_1
            },
            "period_2": {
                "label": period_2.get('label', f"{period_2['start']} to {period_2['end']}"),
                "start": period_2['start'],
                "end": period_2['end'],
                "data": data_2,
                "statistics": stats_2
            },
            "comparison": comparison,
            "aligned_data": aligned_data
        }
        
        return jsonify(response), 200
        
    except ValueError as e:
        return jsonify({"error": f"Invalid input: {str(e)}"}), 400
    except Exception as e:
        print(f"Comparison Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Server error: {str(e)}"}), 500


def calculate_statistics(data, dataset):
    """Calculate statistics for a timeseries dataset"""
    import numpy as np
    
    if not data:
        return None
    
    # Extract values based on dataset
    if dataset == 'chirps':
        values = [d.get('precip_mm', 0) for d in data if d.get('precip_mm') is not None]
    elif dataset == 'era5land':
        # For temperature, use average temperature
        values = [d.get('tavg_c', 0) for d in data if d.get('tavg_c') is not None]
    elif dataset == 'smap':
        # For SMAP, use root zone as primary indicator
        values = [d.get('sm_rootzone', 0) for d in data if d.get('sm_rootzone') is not None]
    
    if not values:
        return None
    
    return {
        "mean": round(float(np.mean(values)), 2),
        "min": round(float(np.min(values)), 2),
        "max": round(float(np.max(values)), 2),
        "median": round(float(np.median(values)), 2),
        "std": round(float(np.std(values)), 2),
        "sum": round(float(np.sum(values)), 2),
        "num_days": len(values)
    }


def calculate_comparison(stats_1, stats_2, dataset):
    """Calculate comparison metrics between two periods"""
    
    if not stats_1 or not stats_2:
        return {
            "error": "Cannot compare - missing data for one or both periods"
        }
    
    # Calculate differences
    mean_diff = stats_2['mean'] - stats_1['mean']
    mean_pct = (mean_diff / stats_1['mean'] * 100) if stats_1['mean'] != 0 else 0
    
    median_diff = stats_2['median'] - stats_1['median']
    sum_diff = stats_2['sum'] - stats_1['sum']
    
    # Determine status and severity
    if dataset == 'chirps':
        # For rainfall, lower is worse
        if mean_pct < -30:
            status = "Period 2 is much drier"
            severity = "severe"
        elif mean_pct < -15:
            status = "Period 2 is drier"
            severity = "moderate"
        elif mean_pct > 30:
            status = "Period 2 is much wetter"
            severity = "high"
        elif mean_pct > 15:
            status = "Period 2 is wetter"
            severity = "moderate"
        else:
            status = "Periods are similar"
            severity = "none"
        
        interpretation = f"Period 2 had {abs(mean_pct):.1f}% {'less' if mean_pct < 0 else 'more'} rainfall than Period 1 ({abs(sum_diff):.1f}mm difference)."
    
    elif dataset == 'era5land':
        # For temperature, higher can be worse (heat stress)
        if mean_pct > 10:
            status = "Period 2 is much hotter"
            severity = "severe"
        elif mean_pct > 5:
            status = "Period 2 is hotter"
            severity = "moderate"
        elif mean_pct < -10:
            status = "Period 2 is much cooler"
            severity = "severe"
        elif mean_pct < -5:
            status = "Period 2 is cooler"
            severity = "moderate"
        else:
            status = "Periods are similar"
            severity = "none"
        
        interpretation = f"Period 2 was {abs(mean_diff):.1f}°C {'warmer' if mean_diff > 0 else 'cooler'} than Period 1."
    
    elif dataset == 'smap':
        # For soil moisture, lower is worse
        if mean_pct < -30:
            status = "Period 2 is much drier"
            severity = "severe"
        elif mean_pct < -15:
            status = "Period 2 is drier"
            severity = "moderate"
        elif mean_pct > 30:
            status = "Period 2 is much wetter"
            severity = "high"
        elif mean_pct > 15:
            status = "Period 2 is wetter"
            severity = "moderate"
        else:
            status = "Periods are similar"
            severity = "none"
        
        interpretation = f"Period 2 had {abs(mean_pct):.1f}% {'less' if mean_pct < 0 else 'more'} soil moisture than Period 1, indicating significantly {'drier' if mean_pct < 0 else 'wetter'} conditions."
    
    return {
        "mean_difference": round(mean_diff, 2),
        "mean_percent_change": round(mean_pct, 2),
        "median_difference": round(median_diff, 2),
        "sum_difference": round(sum_diff, 2),
        "status": status,
        "severity": severity,
        "interpretation": interpretation,
        "period_1_mean": stats_1['mean'],
        "period_2_mean": stats_2['mean']
    }


def align_timeseries(data_1, data_2, dataset):
    """Align two timeseries by day of month for comparison"""
    
    aligned = []
    
    # Get the shorter length
    min_length = min(len(data_1), len(data_2))
    
    for i in range(min_length):
        item_1 = data_1[i]
        item_2 = data_2[i]
        
        # Extract values based on dataset
        if dataset == 'chirps':
            value_1 = item_1.get('precip_mm')
            value_2 = item_2.get('precip_mm')
            variable = 'precip_mm'
        elif dataset == 'era5land':
            value_1 = item_1.get('tavg_c')
            value_2 = item_2.get('tavg_c')
            variable = 'tavg_c'
        elif dataset == 'smap':
            value_1 = item_1.get('sm_rootzone')
            value_2 = item_2.get('sm_rootzone')
            variable = 'sm_rootzone'
        
        if value_1 is not None and value_2 is not None:
            difference = value_2 - value_1
            percent_change = (difference / value_1 * 100) if value_1 != 0 else 0
            
            aligned.append({
                "day": i + 1,
                "date_1": item_1.get('date'),
                "date_2": item_2.get('date'),
                "value_1": round(value_1, 2),
                "value_2": round(value_2, 2),
                "difference": round(difference, 2),
                "percent_change": round(percent_change, 2),
                "variable": variable
            })
    
    return aligned
