"""
DataHub request/response schemas using Pydantic
"""

from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional, Literal, List, Dict, Any
from datetime import datetime, date


class GeometryInput(BaseModel):
    """Geometry specification"""
    type: Literal["point", "wkt"] = Field(..., description="Geometry type")
    lat: Optional[float] = Field(None, ge=-90, le=90, description="Latitude for point")
    lon: Optional[float] = Field(None, ge=-180, le=180, description="Longitude for point")
    wkt: Optional[str] = Field(None, description="WKT string for polygon/line")
    buffer_m: float = Field(0, ge=0, le=100000, description="Buffer in meters for lines/points")
    
    @model_validator(mode='after')
    def validate_geometry(self):
        if self.type == "point":
            if self.lat is None or self.lon is None:
                raise ValueError("lat and lon required for point geometry")
        elif self.type == "wkt":
            if not self.wkt:
                raise ValueError("wkt string required for wkt geometry")
        return self


class DateRangeInput(BaseModel):
    """Date range specification"""
    start: str = Field(..., description="Start date YYYY-MM-DD")
    end: str = Field(..., description="End date YYYY-MM-DD")
    
    @field_validator('start', 'end')
    @classmethod
    def validate_date_format(cls, v):
        try:
            datetime.strptime(v, '%Y-%m-%d')
            return v
        except ValueError:
            raise ValueError(f"Invalid date format: {v}. Use YYYY-MM-DD")
    
    @model_validator(mode='after')
    def validate_date_order(self):
        start_dt = datetime.strptime(self.start, '%Y-%m-%d')
        end_dt = datetime.strptime(self.end, '%Y-%m-%d')
        if start_dt > end_dt:
            raise ValueError("start date must be before or equal to end date")
        return self


class TimeseriesRequest(BaseModel):
    """Request schema for timeseries data"""
    geometry: GeometryInput
    date_range: DateRangeInput
    spatial_stat: Literal["mean", "median", "sum", "min", "max"] = Field("mean")
    temporal_stat: Literal["daily"] = Field("daily")
    crs: str = Field("EPSG:4326", description="Coordinate reference system")
    tz: str = Field("Africa/Harare", description="Timezone for daily boundaries")
    
    @field_validator('tz')
    @classmethod
    def validate_timezone(cls, v):
        # Basic validation - can expand with pytz if needed
        if not v or len(v) < 3:
            raise ValueError("Invalid timezone")
        return v


class GeoTIFFRequest(BaseModel):
    """Request schema for GeoTIFF export"""
    geometry: GeometryInput
    date_range: DateRangeInput
    spatial_stat: Optional[Literal["mean", "median", "sum", "min", "max"]] = Field(None)
    crs: str = Field("EPSG:4326")
    resolution_deg: float = Field(0.05, ge=0.01, le=0.5, description="Output resolution in degrees")
    clip_to_geometry: bool = Field(True, description="Clip output to geometry bounds")
    tiff_mode: Literal["multiband", "zip"] = Field("multiband", description="Multi-band TIFF or ZIP of daily TIFFs")
    band: Optional[Literal["precip", "tmin", "tmax", "tavg"]] = Field(None, description="Single band to export")


class GeometrySummary(BaseModel):
    """Summary of processed geometry"""
    type: str
    centroid: List[float]
    area_km2: Optional[float] = None


class TimeseriesDataPoint(BaseModel):
    """Single timeseries data point"""
    date: str
    precip_mm: Optional[float] = None
    tmin_c: Optional[float] = None
    tmax_c: Optional[float] = None
    tavg_c: Optional[float] = None


class TimeseriesResponse(BaseModel):
    """Response schema for timeseries"""
    dataset: Literal["CHIRPS", "ERA5-Land"]
    variable: Literal["precip", "t2m"]
    aggregation: Dict[str, str]
    units: Dict[str, str]
    geometry_summary: GeometrySummary
    date_range: Dict[str, str]
    data: List[TimeseriesDataPoint]
    meta: Dict[str, Any]


class JobStatusResponse(BaseModel):
    """Job status response"""
    job_id: str
    status: Literal["queued", "running", "done", "error"]
    progress: int = Field(ge=0, le=100)
    error: Optional[str] = None
    download_urls: Optional[Dict[str, str]] = None
    created_at: str
    updated_at: str


class JobCreateResponse(BaseModel):
    """Job creation response"""
    job_id: str
    status: Literal["queued"]
    message: str = "Job created successfully"
