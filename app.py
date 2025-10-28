"""
Yieldera DataHub API - Main Application
Complete climate data platform with CHIRPS, ERA5-Land, and SMAP L4
"""

from flask import Flask, jsonify
from flask_cors import CORS
from datetime import datetime
import ee
import json
import os
from google.oauth2 import service_account

# Initialize Flask app
app = Flask(__name__)

# Configure CORS - CRITICAL for frontend to work
CORS(app, resources={
    r"/api/*": {
        "origins": "*",
        "methods": ["GET", "POST", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"]
    }
})

# ============================================================================
# Initialize Google Earth Engine
# ============================================================================

def initialize_gee():
    """Initialize GEE with service account credentials from environment"""
    try:
        # Get credentials from environment variable
        creds_json = os.getenv('GEE_SERVICE_ACCOUNT_CREDENTIALS_JSON')
        
        if not creds_json:
            raise EnvironmentError(
                "GEE_SERVICE_ACCOUNT_CREDENTIALS_JSON environment variable not set. "
                "Please set it in Render dashboard."
            )
        
        # Parse JSON credentials
        creds_dict = json.loads(creds_json)
        
        # Create credentials object
        credentials = service_account.Credentials.from_service_account_info(
            creds_dict,
            scopes=['https://www.googleapis.com/auth/earthengine.readonly']
        )
        
        # Initialize Earth Engine
        ee.Initialize(
            credentials=credentials,
            opt_url='https://earthengine-highvolume.googleapis.com'
        )
        
        print("✓ Google Earth Engine initialized successfully")
        
        # Test connection
        test_value = ee.Number(1).getInfo()
        print(f"✓ GEE connection test passed (result: {test_value})")
        
        return True
        
    except json.JSONDecodeError as e:
        print(f"✗ Invalid JSON in GEE_SERVICE_ACCOUNT_CREDENTIALS_JSON: {e}")
        raise
    except Exception as e:
        print(f"✗ Failed to initialize Google Earth Engine: {e}")
        raise

# Initialize GEE when app starts
initialize_gee()

# Register DataHub blueprint
from datahub import datahub_bp
app.register_blueprint(datahub_bp)

print("✓ DataHub blueprint registered at /api/data")


# ============================================================================
# ROOT ENDPOINTS
# ============================================================================

@app.route('/')
def index():
    """Root endpoint"""
    return jsonify({
        "name": "Yieldera DataHub API",
        "version": "1.0.0",
        "description": "Climate data extraction API for parametric insurance",
        "status": "operational",
        "documentation": "/api",
        "endpoints": {
            "api_index": "/api",
            "health_check": "/api/data/health",
            "datasets": "/api/data/datasets"
        }
    })


@app.route('/api')
def api_index():
    """API documentation and endpoint list"""
    return jsonify({
        "name": "Yieldera DataHub API",
        "version": "1.0.0",
        "description": "Climate data extraction API for parametric insurance",
        
        "endpoints": {
            # CHIRPS Endpoints
            "/api/data/chirps/timeseries": {
                "methods": ["POST"],
                "description": "Extract CHIRPS rainfall timeseries",
                "parameters": {
                    "geometry": "Point or polygon geometry",
                    "date_range": "Start and end dates (1981-present)",
                    "spatial_stat": "Spatial aggregation method (mean, median, max, min, sum)"
                },
                "returns": "Array of daily rainfall values in mm"
            },
            "/api/data/chirps/geotiff": {
                "methods": ["POST"],
                "description": "Export CHIRPS rainfall as GeoTIFF (async job)",
                "parameters": {
                    "geometry": "Region to export",
                    "date_range": "Start and end dates",
                    "resolution_deg": "Output resolution in degrees"
                },
                "returns": "Job ID for async processing"
            },
            
            # ERA5-Land Endpoints
            "/api/data/era5land/timeseries": {
                "methods": ["POST"],
                "description": "Extract ERA5-Land temperature timeseries",
                "parameters": {
                    "geometry": "Point or polygon geometry",
                    "date_range": "Start and end dates (1950-present, max 366 days)",
                    "spatial_stat": "Spatial aggregation method"
                },
                "returns": "Array of daily temperature values (tmin_c, tmax_c, tavg_c)"
            },
            "/api/data/era5land/geotiff": {
                "methods": ["POST"],
                "description": "Export ERA5-Land temperature as GeoTIFF (async job)",
                "parameters": {
                    "geometry": "Region to export",
                    "date_range": "Start and end dates",
                    "resolution_deg": "Output resolution in degrees"
                },
                "returns": "Job ID for async processing"
            },
            
            # SMAP L4 Endpoints
            "/api/data/smap/timeseries": {
                "methods": ["POST"],
                "description": "Extract SMAP L4 soil moisture timeseries",
                "parameters": {
                    "geometry": "Point or polygon geometry",
                    "date_range": "Start and end dates (2015-03-31 to present, max 366 days)",
                    "spatial_stat": "Spatial aggregation method (mean, median, max, min, sum)"
                },
                "returns": "Array of daily soil moisture values (sm_surface and sm_rootzone in %)"
            },
            "/api/data/smap/statistics": {
                "methods": ["POST"],
                "description": "Get summary statistics for SMAP soil moisture",
                "parameters": {
                    "geometry": "Point or polygon geometry",
                    "date_range": "Start and end dates",
                    "spatial_stat": "Spatial aggregation method"
                },
                "returns": "Summary statistics (mean, min, max, median) for both variables"
            },
            "/api/data/smap/export": {
                "methods": ["POST"],
                "description": "Export SMAP soil moisture as GeoTIFF",
                "parameters": {
                    "geometry": "Region to export",
                    "date_range": "Start and end dates",
                    "resolution_deg": "Output resolution in degrees (default: 0.09)",
                    "clip_to_geometry": "Whether to clip to geometry (default: true)",
                    "export_mode": "multiband or zip (default: multiband)",
                    "band_selection": "sm_surface, sm_rootzone, or both (default: both)"
                },
                "returns": "Download URL(s) for GeoTIFF file(s)"
            },
            "/api/data/smap/metadata": {
                "methods": ["GET"],
                "description": "Get SMAP L4 dataset metadata",
                "parameters": "None",
                "returns": "Dataset metadata (source, variables, coverage, citation, etc.)"
            },
            
            # Utility Endpoints
            "/api/data/health": {
                "methods": ["GET"],
                "description": "Health check and GEE connection status",
                "returns": "Service health status"
            },
            "/api/data/datasets": {
                "methods": ["GET"],
                "description": "List all available datasets",
                "returns": "Array of dataset information"
            },
            "/api/data/jobs/<job_id>/status": {
                "methods": ["GET"],
                "description": "Get status of async job",
                "returns": "Job status and details"
            },
            "/api/data/jobs/<job_id>/download": {
                "methods": ["GET"],
                "description": "Download completed job result",
                "returns": "Redirect to download URL"
            }
        },
        
        "datasets": {
            "CHIRPS": {
                "name": "Climate Hazards Group InfraRed Precipitation with Station data",
                "temporal_coverage": "1981-present",
                "spatial_resolution": "5.5 km (0.05 degrees)",
                "update_frequency": "daily",
                "variables": ["precipitation"],
                "units": "mm/day",
                "provider": "UC Santa Barbara / USGS"
            },
            "ERA5-Land": {
                "name": "ECMWF Reanalysis v5 Land",
                "temporal_coverage": "1950-present",
                "spatial_resolution": "9 km (0.1 degrees)",
                "update_frequency": "~5 days behind real-time",
                "variables": ["temperature_2m_min", "temperature_2m_max", "temperature_2m_mean"],
                "units": "°C",
                "provider": "European Centre for Medium-Range Weather Forecasts"
            },
            "SMAP-L4": {
                "name": "NASA SMAP L4 Global Soil Moisture",
                "temporal_coverage": "2015-03-31 to present",
                "spatial_resolution": "9 km (0.09 degrees)",
                "update_frequency": "daily (~3 day latency)",
                "variables": ["sm_surface (0-5cm)", "sm_rootzone (0-100cm)"],
                "units": "percent (%)",
                "typical_values": "10-45% for African croplands, 5-60% globally",
                "provider": "NASA Goddard Space Flight Center"
            }
        },
        
        "usage_examples": {
            "chirps_timeseries": {
                "endpoint": "POST /api/data/chirps/timeseries",
                "body": {
                    "geometry": {"type": "point", "lat": -17.8249, "lon": 31.0530},
                    "date_range": {"start": "2024-01-01", "end": "2024-01-31"},
                    "spatial_stat": "mean"
                }
            },
            "era5land_timeseries": {
                "endpoint": "POST /api/data/era5land/timeseries",
                "body": {
                    "geometry": {"type": "point", "lat": -17.8249, "lon": 31.0530},
                    "date_range": {"start": "2024-01-01", "end": "2024-01-07"},
                    "spatial_stat": "mean"
                }
            },
            "smap_timeseries": {
                "endpoint": "POST /api/data/smap/timeseries",
                "body": {
                    "geometry": {"type": "point", "lat": -17.8249, "lon": 31.0530},
                    "date_range": {"start": "2024-10-01", "end": "2024-10-31"},
                    "spatial_stat": "mean"
                }
            }
        },
        
        "authentication": "None required (public API)",
        "rate_limits": "None (subject to Google Earth Engine quotas)",
        "contact": "support@yieldera.co.zw",
        "website": "https://yieldera.co.zw",
        "documentation": "https://docs.yieldera.co.zw"
    })


@app.route('/health')
def health():
    """Global health check"""
    try:
        # Test GEE connection
        ee.Number(1).getInfo()
        gee_status = "connected"
        
        # Test all extractors
        services = {}
        
        # Test CHIRPS
        try:
            from datahub.gee_chirps import CHIRPSExtractor
            CHIRPSExtractor()
            services['chirps'] = 'ok'
        except Exception as e:
            services['chirps'] = f'error: {str(e)}'
        
        # Test ERA5-Land
        try:
            from datahub.gee_era5land import ERA5LandExtractor
            ERA5LandExtractor()
            services['era5land'] = 'ok'
        except Exception as e:
            services['era5land'] = f'error: {str(e)}'
        
        # Test SMAP
        try:
            from datahub.gee_smap import SMAPExtractor
            SMAPExtractor()
            services['smap'] = 'ok'
        except Exception as e:
            services['smap'] = f'error: {str(e)}'
        
        # Overall status
        all_ok = all(v == 'ok' for v in services.values()) and gee_status == 'connected'
        
        return jsonify({
            "status": "healthy" if all_ok else "degraded",
            "gee": gee_status,
            "services": services,
            "timestamp": datetime.utcnow().isoformat(),
            "version": "1.0.0"
        }), 200 if all_ok else 503
        
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "gee": "disconnected",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }), 500


# ============================================================================
# ERROR HANDLERS
# ============================================================================

@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    return jsonify({
        "error": "Endpoint not found",
        "status": 404,
        "available_endpoints": "/api"
    }), 404


@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors"""
    return jsonify({
        "error": "Internal server error",
        "status": 500,
        "message": str(error)
    }), 500


# ============================================================================
# RUN APPLICATION
# ============================================================================

if __name__ == '__main__':
    import os
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
