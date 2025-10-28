"""
Yieldera API - Main Application
Integrates DataHub climate data module with Flask
"""

from flask import Flask, jsonify
from flask_cors import CORS
import ee
import json
import os
from google.oauth2 import service_account

# Import DataHub blueprint
from datahub import datahub_bp

# Create Flask app
app = Flask(__name__)

# Configure CORS - Allow all origins for testing
# TODO: Restrict to specific domains in production
CORS(app, resources={
    r"/api/*": {
        "origins": "*",  # Allow all origins for now
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

# ============================================================================
# Register Blueprints
# ============================================================================

# Register DataHub blueprint
app.register_blueprint(datahub_bp)

print("✓ DataHub blueprint registered at /api/data/*")

# ============================================================================
# Root Routes
# ============================================================================

@app.route('/')
def index():
    """API homepage"""
    return jsonify({
        "message": "Yieldera API",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "datahub": {
                "health": "/api/data/health",
                "datasets": "/api/data/datasets",
                "chirps_timeseries": "/api/data/chirps/timeseries",
                "era5land_timeseries": "/api/data/era5land/timeseries",
                "chirps_geotiff": "/api/data/chirps/geotiff",
                "era5land_geotiff": "/api/data/era5land/geotiff"
            }
        },
        "documentation": "https://docs.yieldera.co.zw"
    })

@app.route('/health')
def health():
    """Overall health check"""
    try:
        # Test GEE connection
        ee.Number(1).getInfo()
        gee_status = "connected"
    except:
        gee_status = "disconnected"
    
    return jsonify({
        "status": "healthy" if gee_status == "connected" else "degraded",
        "gee": gee_status,
        "message": "Yieldera API is running"
    })

# ============================================================================
# Error Handlers
# ============================================================================

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "error": "Not Found",
        "message": "The requested endpoint does not exist",
        "status": 404
    }), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        "error": "Internal Server Error",
        "message": "An unexpected error occurred",
        "status": 500
    }), 500

# ============================================================================
# Run the app (for local testing)
# ============================================================================

if __name__ == '__main__':
    # For local development only
    app.run(debug=True, host='0.0.0.0', port=5000)
