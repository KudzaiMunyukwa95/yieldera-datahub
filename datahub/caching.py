"""
Request caching for DataHub
"""

import os
import hashlib
import json
import pickle
from pathlib import Path
from typing import Any, Optional, Dict
from datetime import datetime, timedelta


class RequestCache:
    """Simple file-based cache for DataHub requests"""
    
    def __init__(self, cache_dir: str = "/mnt/data/cache", ttl_hours: int = 24):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.ttl = timedelta(hours=ttl_hours)
        
    def _compute_cache_key(self, request_data: Dict[str, Any]) -> str:
        """Compute cache key from request parameters"""
        # Sort keys for consistent hashing
        sorted_json = json.dumps(request_data, sort_keys=True)
        hash_obj = hashlib.sha256(sorted_json.encode())
        return hash_obj.hexdigest()
    
    def get(self, request_data: Dict[str, Any], format: str = "json") -> Optional[Any]:
        """
        Get cached data if available and not expired
        
        Args:
            request_data: Request parameters to hash
            format: 'json' or 'tiff'
            
        Returns:
            Cached data or None
        """
        try:
            cache_key = self._compute_cache_key(request_data)
            
            if format == "json":
                cache_file = self.cache_dir / f"{cache_key}.json"
            elif format == "csv":
                cache_file = self.cache_dir / f"{cache_key}.csv"
            elif format == "tiff":
                cache_file = self.cache_dir / f"{cache_key}.tif"
            else:
                return None
            
            if not cache_file.exists():
                return None
            
            # Check if expired
            file_mtime = datetime.fromtimestamp(cache_file.stat().st_mtime)
            if datetime.now() - file_mtime > self.ttl:
                # Expired - delete
                cache_file.unlink()
                return None
            
            # Read cached data
            if format == "json":
                with open(cache_file, 'r') as f:
                    return json.load(f)
            elif format in ["csv", "tiff"]:
                # Return file path for binary data
                return str(cache_file)
            
            return None
            
        except Exception as e:
            print(f"Cache read error: {e}")
            return None
    
    def set(self, request_data: Dict[str, Any], data: Any, format: str = "json") -> str:
        """
        Cache data
        
        Args:
            request_data: Request parameters to hash
            data: Data to cache (dict for json, filepath for csv/tiff)
            format: 'json', 'csv', or 'tiff'
            
        Returns:
            Cache file path
        """
        try:
            cache_key = self._compute_cache_key(request_data)
            
            if format == "json":
                cache_file = self.cache_dir / f"{cache_key}.json"
                with open(cache_file, 'w') as f:
                    json.dump(data, f)
            elif format == "csv":
                cache_file = self.cache_dir / f"{cache_key}.csv"
                # Copy file to cache
                import shutil
                shutil.copy2(data, cache_file)
            elif format == "tiff":
                cache_file = self.cache_dir / f"{cache_key}.tif"
                import shutil
                shutil.copy2(data, cache_file)
            else:
                return ""
            
            return str(cache_file)
            
        except Exception as e:
            print(f"Cache write error: {e}")
            return ""
    
    def clear_expired(self):
        """Remove expired cache files"""
        try:
            now = datetime.now()
            for cache_file in self.cache_dir.glob("*"):
                if cache_file.is_file():
                    file_mtime = datetime.fromtimestamp(cache_file.stat().st_mtime)
                    if now - file_mtime > self.ttl:
                        cache_file.unlink()
                        print(f"Removed expired cache: {cache_file.name}")
        except Exception as e:
            print(f"Cache cleanup error: {e}")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        try:
            files = list(self.cache_dir.glob("*"))
            total_size = sum(f.stat().st_size for f in files if f.is_file())
            
            return {
                "total_files": len(files),
                "total_size_mb": round(total_size / (1024 * 1024), 2),
                "cache_dir": str(self.cache_dir)
            }
        except:
            return {"total_files": 0, "total_size_mb": 0}
