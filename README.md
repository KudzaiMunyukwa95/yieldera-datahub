# Yieldera DataHub Module

**Production-ready climate data extraction API for parametric insurance pricing and index analytics**

## Overview

DataHub is a Flask-based API that provides access to high-quality climate datasets via Google Earth Engine for agricultural insurance applications. It delivers daily rainfall and temperature data with flexible geometry support (points, polygons, buffered lines) and multiple output formats.

## Datasets

### 1. CHIRPS (Climate Hazards Group InfraRed Precipitation with Station data)

- **Variable**: Daily precipitation
- **Resolution**: ~5.5 km (0.05°)
- **Coverage**: Global (50°S to 50°N)
- **Period**: 1981 - present
- **Update**: Daily
- **Units**: mm/day
- **Source**: `UCSB-CHG/CHIRPS/DAILY` on GEE
- **License**: CC-BY-4.0
- **Citation**: Funk, C. et al. (2015). The climate hazards infrared precipitation with stations—a new environmental record for monitoring extremes. Scientific Data, 2, 150066.
- **DOI**: 10.1038/sdata.2015.66

### 2. ERA5-Land (ECMWF Reanalysis v5 - Land)

- **Variable**: 2-meter temperature (hourly → daily Tmin/Tmax/Tavg)
- **Resolution**: ~11 km (0.1°)
- **Coverage**: Global
- **Period**: 1950 - present
- **Update**: ~5 days behind real-time
- **Units**: °C (converted from Kelvin)
- **Source**: `ECMWF/ERA5_LAND/HOURLY` on GEE
- **License**: Copernicus License
- **Citation**: Muñoz Sabater, J. (2019): ERA5-Land hourly data from 1950 to present. Copernicus Climate Change Service (C3S) Climate Data Store (CDS).
- **DOI**: 10.24381/cds.e2161bac

## Installation

### Prerequisites

```bash
# Python 3.8+
pip install --break-system-packages flask pydantic earthengine-api shapely google-auth numpy
```

### Environment Variables

Create `.env` or configure in Render:

```bash
# Google Earth Engine (Required)
GEE_SERVICE_ACCOUNT_CREDENTIALS_JSON='{"type":"service_account","project_id":"..."}'

# Data Storage (Optional - defaults shown)
DATA_CACHE_DIR=/mnt/data/cache
DATA_JOBS_DIR=/mnt/data/jobs
DATA_OUTPUTS_DIR=/mnt/data/outputs

# Limits (Optional)
MAX_AREA_KM2=10000              # Maximum polygon area
MAX_DAYS=5000                   # Maximum date range for CHIRPS
RATE_LIMIT_PER_MIN=60           # API rate limit

# Flask
FLASK_ENV=production
```

### Integration with Existing Flask App

```python
# In your main app.py or __init__.py

from app.datahub import datahub_bp
from flask import Flask

app = Flask(__name__)

# Register DataHub blueprint
app.register_blueprint(datahub_bp)

# Initialize GEE (only once at startup)
from app.datahub.gee_chirps import initialize_gee
initialize_gee()

if __name__ == '__main__':
    app.run()
```

## API Endpoints

### Base URL
```
https://api.yieldera.co.zw/api/data
```

### Endpoints

#### 1. CHIRPS Timeseries
```http
POST /api/data/chirps/timeseries
```

**Request:**
```json
{
  "geometry": {
    "type": "point",
    "lat": -17.8249,
    "lon": 31.0530
  },
  "date_range": {
    "start": "2024-01-01",
    "end": "2024-12-31"
  },
  "spatial_stat": "mean",
  "temporal_stat": "daily"
}
```

**Response:**
```json
{
  "dataset": "CHIRPS",
  "variable": "precip",
  "aggregation": {"spatial": "mean", "temporal": "daily"},
  "units": {"precip": "mm/day"},
  "geometry_summary": {
    "type": "Point",
    "centroid": [31.053, -17.8249],
    "area_km2": null
  },
  "date_range": {"start": "2024-01-01", "end": "2024-12-31"},
  "data": [
    {"date": "2024-01-01", "precip_mm": 2.1},
    {"date": "2024-01-02", "precip_mm": 0.0}
  ],
  "meta": {
    "source": "GEE: UCSB-CHG/CHIRPS/DAILY",
    "native_resolution_deg": 0.05,
    "nodata_value": -999
  }
}
```

#### 2. ERA5-Land Timeseries
```http
POST /api/data/era5land/timeseries
```

**Request:**
```json
{
  "geometry": {
    "type": "wkt",
    "wkt": "POLYGON((31.0 -17.9, 31.2 -17.9, 31.2 -17.7, 31.0 -17.7, 31.0 -17.9))"
  },
  "date_range": {
    "start": "2024-01-01",
    "end": "2024-01-31"
  },
  "spatial_stat": "mean"
}
```

**Response:**
```json
{
  "dataset": "ERA5-Land",
  "variable": "t2m",
  "units": {"tmin": "°C", "tmax": "°C", "tavg": "°C"},
  "data": [
    {"date": "2024-01-01", "tmin_c": 18.5, "tmax_c": 32.1, "tavg_c": 25.3}
  ]
}
```

#### 3. CHIRPS GeoTIFF Export
```http
POST /api/data/chirps/geotiff
```

**Request:**
```json
{
  "geometry": {
    "type": "wkt",
    "wkt": "POLYGON((31.0 -17.9, 31.1 -17.9, 31.1 -17.8, 31.0 -17.8, 31.0 -17.9))"
  },
  "date_range": {
    "start": "2024-01-01",
    "end": "2024-01-31"
  },
  "resolution_deg": 0.05,
  "clip_to_geometry": true,
  "tiff_mode": "multiband"
}
```

**Response:**
```json
{
  "job_id": "a1b2c3d4-5678-90ab-cdef-1234567890ab",
  "status": "queued",
  "message": "GeoTIFF export job created"
}
```

#### 4. Job Status
```http
GET /api/data/jobs/{job_id}/status
```

**Response:**
```json
{
  "job_id": "a1b2c3d4-5678-90ab-cdef-1234567890ab",
  "status": "done",
  "progress": 100,
  "download_urls": {
    "tif": "https://earthengine.googleapis.com/..."
  },
  "created_at": "2025-01-15T10:30:00Z",
  "updated_at": "2025-01-15T10:32:15Z"
}
```

#### 5. Download Job Output
```http
GET /api/data/jobs/{job_id}/download?format=tif
```

#### 6. Health Check
```http
GET /api/data/health
```

#### 7. List Datasets
```http
GET /api/data/datasets
```

## Geometry Types

### 1. Point
```json
{
  "type": "point",
  "lat": -17.8249,
  "lon": 31.0530,
  "buffer_m": 0
}
```

### 2. Polygon (WKT)
```json
{
  "type": "wkt",
  "wkt": "POLYGON((31.0 -17.9, 31.2 -17.9, 31.2 -17.7, 31.0 -17.7, 31.0 -17.9))"
}
```

### 3. LineString with Buffer
```json
{
  "type": "wkt",
  "wkt": "LINESTRING(31.0 -17.9, 31.2 -17.7)",
  "buffer_m": 1000
}
```

## Spatial Statistics

- `mean`: Average value over region (default)
- `median`: Median value
- `sum`: Total (useful for rainfall accumulation)
- `min`: Minimum value
- `max`: Maximum value

## Limits & Performance

| Constraint | CHIRPS | ERA5-Land | Notes |
|------------|--------|-----------|-------|
| Max date range | 5,000 days | 366 days | ERA5 is more expensive to process |
| Max area | 10,000 km² | 10,000 km² | Configurable via `MAX_AREA_KM2` |
| Timeseries cache | 24 hours | 24 hours | Based on request hash |
| GeoTIFF timeout | 5 minutes | 5 minutes | For very large exports |
| Rate limit | 60 req/min | 60 req/min | Per API key |

### Performance Tips

1. **Use caching**: Identical requests return cached results
2. **Start with small date ranges**: Test with 30-90 days first
3. **Limit polygon complexity**: Simplify geometries when possible
4. **Use points for quick tests**: Faster than polygons
5. **Batch requests**: Group related queries

## Error Handling

All errors return consistent JSON:

```json
{
  "error": "ValidationError",
  "message": "Invalid date format",
  "code": 400,
  "hint": "Use YYYY-MM-DD format",
  "details": {...}
}
```

### Common Error Codes

- `400` - Validation error (bad request format)
- `404` - Job not found
- `429` - Rate limit exceeded
- `500` - Internal server error
- `502` - GEE service error
- `503` - Service unhealthy

## Data Quality Notes

### CHIRPS

- **Missing data**: Represented as `-999`
- **Spatial interpolation**: Uses satellite + station data
- **Best for**: Sub-Saharan Africa, with good station coverage in Zimbabwe
- **Limitations**: Less accurate in mountainous terrain

### ERA5-Land

- **Missing data**: Represented as `-999`
- **Temperature derivation**: Daily Tmin/Tmax/Tavg from 24 hourly images
- **Best for**: Large-scale analysis, trend detection
- **Limitations**: Lower spatial resolution than CHIRPS

## Usage Examples

### Python Client

```python
import requests

API_URL = "https://api.yieldera.co.zw/api/data"
headers = {"Authorization": "Bearer YOUR_TOKEN"}

# Get rainfall timeseries
response = requests.post(
    f"{API_URL}/chirps/timeseries",
    json={
        "geometry": {"type": "point", "lat": -17.8249, "lon": 31.0530},
        "date_range": {"start": "2024-10-01", "end": "2025-03-31"},
        "spatial_stat": "mean"
    },
    headers=headers
)

data = response.json()
print(f"Retrieved {len(data['data'])} days of rainfall")

# Export to CSV
import pandas as pd
df = pd.DataFrame(data['data'])
df.to_csv('rainfall.csv', index=False)
```

### R Client

```r
library(httr)
library(jsonlite)

api_url <- "https://api.yieldera.co.zw/api/data"
token <- "YOUR_TOKEN"

# Get temperature timeseries
response <- POST(
  paste0(api_url, "/era5land/timeseries"),
  body = list(
    geometry = list(type = "point", lat = -17.8249, lon = 31.0530),
    date_range = list(start = "2024-01-01", end = "2024-12-31"),
    spatial_stat = "mean"
  ),
  encode = "json",
  add_headers(Authorization = paste("Bearer", token))
)

data <- content(response, "parsed")
df <- do.call(rbind, lapply(data$data, as.data.frame))
write.csv(df, "temperature.csv", row.names = FALSE)
```

### cURL

```bash
# CHIRPS rainfall
curl -X POST https://api.yieldera.co.zw/api/data/chirps/timeseries \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "geometry": {"type": "point", "lat": -17.8249, "lon": 31.0530},
    "date_range": {"start": "2024-10-01", "end": "2025-03-31"}
  }'

# Check job status
curl https://api.yieldera.co.zw/api/data/jobs/JOB_ID/status \
  -H "Authorization: Bearer YOUR_TOKEN"
```

## Testing

```bash
# Install test dependencies
pip install pytest pytest-flask

# Run tests
pytest tests/test_datahub_timeseries.py -v
pytest tests/test_datahub_geotiff.py -v

# Run with coverage
pytest --cov=app/datahub tests/
```

## Deployment Checklist

### Pre-Deployment

- [ ] Set `GEE_SERVICE_ACCOUNT_CREDENTIALS_JSON` in environment
- [ ] Create directories: `/mnt/data/cache`, `/mnt/data/jobs`, `/mnt/data/outputs`
- [ ] Verify GEE service account has Earth Engine permissions
- [ ] Test GEE connection with health endpoint
- [ ] Configure rate limiting if using reverse proxy
- [ ] Set up CORS for frontend domains

### Render Deployment

```bash
# In Render dashboard:
1. Add environment variable: GEE_SERVICE_ACCOUNT_CREDENTIALS_JSON
2. Set FLASK_ENV=production
3. Configure health check: GET /api/data/health
4. Set instance type: At least 1 GB RAM
5. Enable auto-deploy from main branch
```

### Post-Deployment

- [ ] Test `/api/data/health` returns 200
- [ ] Test CHIRPS timeseries with known location
- [ ] Test ERA5-Land timeseries
- [ ] Monitor logs for GEE quota warnings
- [ ] Set up cache cleanup cron (daily)
- [ ] Monitor disk usage for `/mnt/data/*`

## Monitoring

### Key Metrics

- **GEE API calls**: Track via Earth Engine dashboard
- **Cache hit rate**: Check logs for "Cache hit" messages
- **Response times**: Monitor P50, P95, P99
- **Error rates**: Track 5xx errors
- **Disk usage**: Monitor `/mnt/data` directories

### Logs

```python
# Enable debug logging
import logging
logging.basicConfig(level=logging.INFO)
```

## Maintenance

### Cache Cleanup

```python
from app.datahub.caching import RequestCache
cache = RequestCache()
cache.clear_expired()  # Remove files > 24 hours old
```

### Job Cleanup

```python
from app.datahub.jobs import JobStore
jobs = JobStore()
jobs.cleanup_old_jobs(days=7)  # Remove jobs > 7 days old
```

### Storage Cleanup

```python
from app.datahub.storage import FileStorage
storage = FileStorage()
storage.cleanup_old_files(days=7)
```

## Troubleshooting

### "GEE initialization failed"

- Check `GEE_SERVICE_ACCOUNT_CREDENTIALS_JSON` is valid JSON
- Verify service account has Earth Engine API enabled
- Test credentials with: `ee.Number(1).getInfo()`

### "No data returned for date range"

- CHIRPS coverage: 50°S to 50°N
- ERA5-Land lags ~5 days behind real-time
- Check for dataset outages: https://status.earthengine.app

### "Job stuck in 'queued' status"

- Check logs for GEE errors
- Verify geometry is valid
- Reduce date range or resolution

### "Rate limit exceeded"

- Implement request throttling in client
- Use caching to avoid repeated requests
- Contact support for quota increase

## Support

- **Documentation**: https://docs.yieldera.co.zw
- **Email**: support@yieldera.co.zw
- **Issues**: GitHub repository

## License

Proprietary - Yieldera (Private) Limited

© 2025 Yieldera. All rights reserved.

---

**Version**: 1.0.0  
**Last Updated**: January 2025  
**Author**: Yieldera Engineering Team
