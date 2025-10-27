"""
Quick Start: Integrating DataHub into Your Existing Flask App

This file shows the minimal changes needed to add DataHub to your current backend.
"""

# ============================================================================
# STEP 1: Add to your main app.py (or __init__.py)
# ============================================================================

from flask import Flask
from flask_cors import CORS  # If you need CORS
import ee
import json
import os
from google.oauth2 import service_account

# Import DataHub blueprint
from app.datahub import datahub_bp

# Create Flask app
app = Flask(__name__)

# Configure CORS if needed (for frontend access)
CORS(app, resources={
    r"/api/data/*": {
        "origins": ["https://yieldera.co.zw", "https://dashboard.yieldera.co.zw"]
    }
})

# ============================================================================
# STEP 2: Initialize Google Earth Engine (ONCE at startup)
# ============================================================================

def initialize_gee():
    """Initialize GEE with service account credentials from environment"""
    try:
        # Get credentials from environment variable
        creds_json = os.getenv('GEE_SERVICE_ACCOUNT_CREDENTIALS_JSON')
        
        if not creds_json:
            raise EnvironmentError(
                "GEE_SERVICE_ACCOUNT_CREDENTIALS_JSON environment variable not set"
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
        print(f"✓ GEE connection test passed (got {test_value})")
        
    except Exception as e:
        print(f"✗ Failed to initialize Google Earth Engine: {e}")
        raise

# Initialize GEE when app starts
initialize_gee()

# ============================================================================
# STEP 3: Register DataHub Blueprint
# ============================================================================

app.register_blueprint(datahub_bp)

print("✓ DataHub blueprint registered at /api/data/*")

# ============================================================================
# STEP 4: Your existing routes continue as normal
# ============================================================================

@app.route('/')
def index():
    """Your existing homepage"""
    return {
        "message": "Yieldera API",
        "endpoints": {
            "datahub": "/api/data/health",
            "your_endpoints": "..."
        }
    }

@app.route('/api/quotes', methods=['POST'])
def create_quote():
    """Your existing quote endpoint"""
    # Your existing code unchanged
    pass

# ... rest of your existing routes ...

# ============================================================================
# OPTIONAL: Add health check that includes DataHub
# ============================================================================

@app.route('/health')
def health():
    """Overall health check"""
    from app.datahub.routes import health_check as datahub_health
    
    return {
        "app": "healthy",
        "datahub": datahub_health()
    }

# ============================================================================
# Run the app
# ============================================================================

if __name__ == '__main__':
    app.run(debug=False)


# ============================================================================
# ENVIRONMENT VARIABLES NEEDED (in Render dashboard or .env)
# ============================================================================

"""
Required:
---------
GEE_SERVICE_ACCOUNT_CREDENTIALS_JSON={"type":"service_account","project_id":"your-project",...}

Optional (with defaults):
-------------------------
DATA_CACHE_DIR=/mnt/data/cache
DATA_JOBS_DIR=/mnt/data/jobs
DATA_OUTPUTS_DIR=/mnt/data/outputs
MAX_AREA_KM2=10000
MAX_DAYS=5000
RATE_LIMIT_PER_MIN=60
FLASK_ENV=production
"""


# ============================================================================
# TESTING YOUR INTEGRATION
# ============================================================================

"""
1. Start your app:
   python app.py

2. Test health endpoint:
   curl http://localhost:5000/api/data/health

3. Test CHIRPS endpoint:
   curl -X POST http://localhost:5000/api/data/chirps/timeseries \
     -H "Content-Type: application/json" \
     -d '{
       "geometry": {"type": "point", "lat": -17.8249, "lon": 31.0530},
       "date_range": {"start": "2024-01-01", "end": "2024-01-10"}
     }'

4. If all works, deploy to Render!
"""


# ============================================================================
# TROUBLESHOOTING
# ============================================================================

"""
Error: "GEE_SERVICE_ACCOUNT_CREDENTIALS_JSON environment variable not set"
Solution: Set the environment variable in Render dashboard or your .env file

Error: "Failed to initialize Google Earth Engine"
Solution: 
  1. Check JSON is valid (paste into jsonlint.com)
  2. Verify service account has Earth Engine API enabled
  3. Check service account email in GEE asset manager

Error: "Module 'app.datahub' not found"
Solution: Ensure you copied the /app/datahub/ directory to your repo

Error: "No module named 'pydantic'"
Solution: Install dependencies:
  pip install pydantic==2.5.0 shapely==2.0.2

Error: "Health check returns 503"
Solution: GEE initialization failed - check credentials and network
"""


# ============================================================================
# NEXT STEPS AFTER INTEGRATION
# ============================================================================

"""
1. Test all endpoints with your auth middleware
2. Update frontend to call new DataHub endpoints
3. Add DataHub URLs to your API documentation
4. Set up monitoring for /api/data/health endpoint
5. Configure rate limiting if needed
6. Test client libraries (Python/R) with your production API
7. Train your team on new DataHub capabilities
"""
