"""
Yieldera DataHub Module
Climate data extraction and delivery for parametric insurance pricing
"""
from .routes import datahub_bp
from .errors import DataHubError, ValidationError, GEEError

# Import all extractors for direct access
from .gee_chirps import CHIRPSExtractor
from .gee_era5land import ERA5LandExtractor
from .gee_smap import SMAPExtractor

__version__ = "1.0.0"
__all__ = [
    "datahub_bp", 
    "DataHubError", 
    "ValidationError", 
    "GEEError",
    "CHIRPSExtractor",
    "ERA5LandExtractor",
    "SMAPExtractor"
]
