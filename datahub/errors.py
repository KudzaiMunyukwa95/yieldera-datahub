"""
DataHub error handling utilities
"""

from flask import jsonify
from typing import Dict, Any, Optional


class DataHubError(Exception):
    """Base exception for DataHub errors"""
    status_code = 500
    
    def __init__(self, message: str, hint: Optional[str] = None, details: Optional[Dict] = None):
        super().__init__(message)
        self.message = message
        self.hint = hint
        self.details = details or {}
    
    def to_dict(self) -> Dict[str, Any]:
        response = {
            "error": self.__class__.__name__,
            "message": self.message,
            "code": self.status_code
        }
        if self.hint:
            response["hint"] = self.hint
        if self.details:
            response["details"] = self.details
        return response


class ValidationError(DataHubError):
    """Request validation errors"""
    status_code = 400


class GEEError(DataHubError):
    """Google Earth Engine errors"""
    status_code = 502
    
    def __init__(self, message: str, hint: Optional[str] = None):
        super().__init__(
            message,
            hint or "Check GEE service status and credentials",
            {}
        )


class GeometryError(DataHubError):
    """Geometry validation/processing errors"""
    status_code = 400


class JobNotFoundError(DataHubError):
    """Job not found"""
    status_code = 404


class RateLimitError(DataHubError):
    """Rate limit exceeded"""
    status_code = 429


def handle_datahub_error(error: DataHubError):
    """Flask error handler for DataHub errors"""
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response
