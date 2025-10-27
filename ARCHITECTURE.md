# DataHub Architecture Overview

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Yieldera Platform                         │
│                                                                   │
│  ┌────────────┐      ┌──────────────────────────────────────┐  │
│  │  Frontend  │◄────►│      Flask Backend (app.py)          │  │
│  │            │      │                                       │  │
│  │ Dashboard  │      │  ┌────────────────────────────────┐  │  │
│  │ Web App    │      │  │      DataHub Blueprint         │  │  │
│  │ Mobile     │      │  │   (/api/data/*)                │  │  │
│  └────────────┘      │  │                                │  │  │
│                      │  │  ┌──────────┐  ┌─────────────┐ │  │  │
│  ┌────────────┐      │  │  │  Routes  │  │  Schemas    │ │  │  │
│  │ R/Python   │      │  │  │          │  │ (Pydantic)  │ │  │  │
│  │ Clients    │◄────►│  │  └──────────┘  └─────────────┘ │  │  │
│  │            │      │  │         │              │        │  │  │
│  └────────────┘      │  │         ▼              ▼        │  │  │
│                      │  │  ┌──────────────────────────┐   │  │  │
│  ┌────────────┐      │  │  │   GEE Extractors         │   │  │  │
│  │  Insurance │      │  │  │                          │   │  │  │
│  │  Analysts  │      │  │  │  ┌────────────────────┐ │   │  │  │
│  │            │      │  │  │  │  CHIRPS Extractor  │ │   │  │  │
│  └────────────┘      │  │  │  │  - Timeseries      │ │   │  │  │
│                      │  │  │  │  - GeoTIFF         │ │   │  │  │
│                      │  │  │  └────────────────────┘ │   │  │  │
│                      │  │  │                          │   │  │  │
│                      │  │  │  ┌────────────────────┐ │   │  │  │
│                      │  │  │  │ ERA5Land Extractor │ │   │  │  │
│                      │  │  │  │  - Timeseries      │ │   │  │  │
│                      │  │  │  │  - GeoTIFF         │ │   │  │  │
│                      │  │  │  └────────────────────┘ │   │  │  │
│                      │  │  └──────────────────────────┘   │  │  │
│                      │  │                                  │  │  │
│                      │  │  ┌──────────┐  ┌──────────┐    │  │  │
│                      │  │  │ Job Mgmt │  │  Caching │    │  │  │
│                      │  │  │ (FS)     │  │  (24hr)  │    │  │  │
│                      │  │  └──────────┘  └──────────┘    │  │  │
│                      │  └────────────────────────────────┘  │  │
│                      └──────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────┘
                                    │
                                    │ GEE API
                                    ▼
┌─────────────────────────────────────────────────────────────────┐
│              Google Earth Engine (GEE)                           │
│                                                                   │
│  ┌─────────────────┐              ┌────────────────────┐        │
│  │  CHIRPS Daily   │              │  ERA5-Land Hourly  │        │
│  │  Precipitation  │              │  Temperature       │        │
│  │  (UCSB-CHG)     │              │  (ECMWF)           │        │
│  │                 │              │                    │        │
│  │  • 5.5 km res   │              │  • 11 km res       │        │
│  │  • 1981-present │              │  • 1950-present    │        │
│  │  • Daily update │              │  • ~5 day lag      │        │
│  └─────────────────┘              └────────────────────┘        │
└───────────────────────────────────────────────────────────────────┘
```

## Data Flow

### 1. Timeseries Request Flow

```
User Request
    │
    ├─► Validate Request (Pydantic schemas)
    │
    ├─► Check Cache
    │   ├─► HIT: Return cached JSON (< 100ms)
    │   └─► MISS: Continue to GEE
    │
    ├─► Parse Geometry (WKT → ee.Geometry)
    │
    ├─► Extract Data from GEE
    │   ├─► CHIRPS: Daily precipitation
    │   │   └─► Single ImageCollection query
    │   │
    │   └─► ERA5-Land: Hourly → Daily temp
    │       └─► 24 hourly images per day
    │
    ├─► Process Results
    │   ├─► Handle missing data (-999)
    │   ├─► Convert units (K → °C)
    │   └─► Format as JSON
    │
    ├─► Cache Response (24hr TTL)
    │
    └─► Return to User
```

### 2. GeoTIFF Export Flow

```
User Request
    │
    ├─► Validate Request
    │
    ├─► Create Job (UUID)
    │   └─► Status: queued
    │
    ├─► Execute Job (sync or async)
    │   │
    │   ├─► Parse Geometry
    │   │
    │   ├─► Extract from GEE
    │   │   ├─► Multiband mode: Single TIFF with date-named bands
    │   │   └─► Zip mode: Multiple daily TIFFs
    │   │
    │   ├─► Get Download URL from GEE
    │   │
    │   └─► Update Job: status=done, download_urls={...}
    │
    └─► User polls /jobs/{id}/status
        └─► Download via returned URL
```

## Module Responsibilities

### routes.py (API Layer)
- HTTP request handling
- Blueprint registration
- Error handling
- Request/response formatting

### schemas.py (Validation Layer)
- Pydantic models for type safety
- Input validation (dates, coordinates, etc.)
- Request/response serialization

### gee_chirps.py & gee_era5land.py (Data Layer)
- Google Earth Engine queries
- Data extraction logic
- Unit conversions
- Dataset-specific handling

### reducers.py (Utility Layer)
- Spatial aggregation (mean/median/sum/min/max)
- Geometry parsing (WKT, Point)
- Date validation and capping
- GEE reducer utilities

### jobs.py (Job Management Layer)
- Async job queue
- Status tracking
- Job persistence (filesystem)
- Background execution framework

### caching.py (Caching Layer)
- Request-level caching
- Hash-based key generation
- TTL management (24hr default)
- Cache statistics

### storage.py (Storage Layer)
- CSV export utilities
- File management
- Download URL generation
- Cleanup utilities

### errors.py (Error Handling Layer)
- Custom exception classes
- Consistent error responses
- Error hints for troubleshooting

## Technology Stack

```
┌──────────────────────────────────────────────────────┐
│ Frontend                                              │
│ - React/Vue/Angular (your choice)                    │
│ - Axios/Fetch for API calls                          │
└──────────────────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────┐
│ API Layer (Flask)                                     │
│ - Flask 3.0.0                                        │
│ - Pydantic 2.5.0 (validation)                        │
│ - Flask-CORS (if needed)                             │
└──────────────────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────┐
│ Business Logic (DataHub)                             │
│ - Custom Python modules                              │
│ - Shapely 2.0.2 (geometry)                           │
│ - NumPy 1.26.2 (arrays)                              │
└──────────────────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────┐
│ Google Earth Engine                                  │
│ - earthengine-api 0.1.388                            │
│ - google-auth 2.25.2                                 │
│ - Service Account authentication                     │
└──────────────────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────┐
│ Storage                                               │
│ - Filesystem (cache, jobs, outputs)                 │
│ - /mnt/data/* directories                            │
│ - Ready for S3/Redis upgrade                         │
└──────────────────────────────────────────────────────┘
```

## Deployment Architecture (Render)

```
┌─────────────────────────────────────────────────────┐
│                   Render Platform                    │
│                                                       │
│  ┌────────────────────────────────────────────────┐ │
│  │         Web Service (yieldera-api)             │ │
│  │                                                 │ │
│  │  • Instance: Standard (1 GB RAM)               │ │
│  │  • Python 3.10+                                │ │
│  │  • Gunicorn server                             │ │
│  │  • Health check: /api/data/health              │ │
│  │                                                 │ │
│  │  Environment Variables:                        │ │
│  │  - GEE_SERVICE_ACCOUNT_CREDENTIALS_JSON        │ │
│  │  - FLASK_ENV=production                        │ │
│  │  - DATA_CACHE_DIR=/mnt/data/cache              │ │
│  │  - MAX_DAYS=5000                               │ │
│  │                                                 │ │
│  │  Persistent Disk:                              │ │
│  │  - /mnt/data (10 GB)                           │ │
│  │    ├─ /cache  (request cache)                  │ │
│  │    ├─ /jobs   (job tracking)                   │ │
│  │    └─ /outputs (CSV exports)                   │ │
│  └────────────────────────────────────────────────┘ │
│                        │                             │
│                        │ HTTPS                       │
│                        ▼                             │
│  ┌────────────────────────────────────────────────┐ │
│  │         Load Balancer / CDN                    │ │
│  │  • HTTPS termination                           │ │
│  │  • Rate limiting (optional)                    │ │
│  │  • Caching (optional)                          │ │
│  └────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────┘
                         │
                         │ Internet
                         ▼
                    [End Users]
```

## Security Considerations

### Authentication
- DataHub relies on your existing auth middleware
- Add Bearer token validation in routes if needed
- Example: JWT middleware before DataHub blueprint

### Authorization
- Rate limiting configurable (`RATE_LIMIT_PER_MIN`)
- User-specific job tracking via `X-User-ID` header
- GEE service account has read-only scope

### Data Protection
- No PII stored in cache/jobs
- GeoTIFF URLs are time-limited (GEE default)
- Geometry data sanitized via Pydantic

### Secrets Management
- GEE credentials via environment variable
- Never logged or exposed in responses
- Service account JSON not in source control

## Scalability Considerations

### Current Limits (Filesystem-based)
- **Cache**: ~10,000 requests (at 100KB avg = 1GB)
- **Jobs**: ~1,000 concurrent (at 5KB avg = 5MB)
- **Outputs**: Depends on disk (10GB = ~1,000 CSVs)

### Scaling Path
1. **Phase 1** (0-1K users): Filesystem (current)
2. **Phase 2** (1K-10K users): Redis cache + RQ jobs
3. **Phase 3** (10K+ users): S3 storage + Celery + Redis

### Upgrade Checklist (when needed)
- [ ] Swap `caching.py` to use Redis
- [ ] Swap `jobs.py` to use RQ/Celery
- [ ] Swap `storage.py` to use S3/Spaces
- [ ] Add CDN for GeoTIFF downloads
- [ ] Horizontal scaling (multiple instances)

## Monitoring & Observability

### Key Metrics
- **Request rate**: `/api/data/*` endpoints
- **Cache hit rate**: Log "Cache hit" messages
- **GEE quota**: Check Earth Engine console
- **Response times**: P50, P95, P99
- **Error rate**: 4xx, 5xx responses
- **Disk usage**: `/mnt/data` directories

### Health Checks
- `/api/data/health` - Overall system health
- GEE connectivity test (ee.Number(1).getInfo())
- Cache/storage statistics

### Logging
- Structured logs (JSON format recommended)
- Log levels: INFO for requests, ERROR for failures
- Correlation IDs for request tracing

## Cost Considerations

### Google Earth Engine
- **Free tier**: 50,000 requests/day
- **Typical usage**: 
  - CHIRPS 30-day query: ~30 requests
  - ERA5 7-day query: ~168 requests
- **Estimate**: ~1,600 ERA5 week-long queries/day on free tier

### Render
- **Standard instance** (1 GB): ~$7/month
- **Persistent disk** (10 GB): ~$1/month
- **Total**: ~$8/month for solo operation

### Bandwidth
- **Timeseries JSON**: ~10-50KB per response
- **GeoTIFF**: Varies (1MB - 100MB+)
- **Cache savings**: 70%+ reduction in GEE calls

## Future Enhancements

### Additional Datasets
- MODIS NDVI (vegetation health)
- Soil moisture (SMAP or ERA5-Land)
- Evapotranspiration (MODIS ET)
- Land surface temperature (MODIS LST)

### Advanced Features
- Historical burn-cost analysis endpoints
- Automated trigger detection
- Spatial interpolation for sparse data
- Multi-location batch processing
- WebSocket for real-time job updates

### Analytics
- Usage dashboard (requests per user/dataset)
- Data quality reports (missing data %)
- Performance analytics (query times)
- Cost optimization recommendations

---

**Architecture Version**: 1.0  
**Last Updated**: January 2025  
**Status**: Production-ready
