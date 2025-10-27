"""
Job queue and status management for async DataHub operations
"""

import json
import uuid
from pathlib import Path
from typing import Dict, Any, Optional, Literal
from datetime import datetime
from enum import Enum


class JobStatus(str, Enum):
    """Job status enumeration"""
    QUEUED = "queued"
    RUNNING = "running"
    DONE = "done"
    ERROR = "error"


class JobStore:
    """Simple file-based job tracking"""
    
    def __init__(self, jobs_dir: str = "/mnt/data/jobs"):
        self.jobs_dir = Path(jobs_dir)
        self.jobs_dir.mkdir(parents=True, exist_ok=True)
        
    def create_job(
        self,
        job_type: str,
        request_data: Dict[str, Any],
        user_id: Optional[str] = None
    ) -> str:
        """
        Create a new job
        
        Args:
            job_type: Type of job (e.g., 'chirps_geotiff', 'era5_geotiff')
            request_data: Request parameters
            user_id: Optional user identifier
            
        Returns:
            Job ID
        """
        job_id = str(uuid.uuid4())
        
        job_data = {
            "job_id": job_id,
            "job_type": job_type,
            "status": JobStatus.QUEUED,
            "progress": 0,
            "request_data": request_data,
            "user_id": user_id,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "error": None,
            "download_urls": None
        }
        
        self._save_job(job_id, job_data)
        return job_id
    
    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job status and details"""
        job_file = self.jobs_dir / f"{job_id}.json"
        
        if not job_file.exists():
            return None
        
        try:
            with open(job_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading job {job_id}: {e}")
            return None
    
    def update_job(
        self,
        job_id: str,
        status: Optional[JobStatus] = None,
        progress: Optional[int] = None,
        error: Optional[str] = None,
        download_urls: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Update job status
        
        Args:
            job_id: Job identifier
            status: New status
            progress: Progress percentage (0-100)
            error: Error message if failed
            download_urls: Download URLs when complete
            
        Returns:
            True if successful
        """
        job_data = self.get_job(job_id)
        
        if not job_data:
            return False
        
        # Update fields
        if status is not None:
            job_data["status"] = status
        if progress is not None:
            job_data["progress"] = max(0, min(100, progress))
        if error is not None:
            job_data["error"] = error
        if download_urls is not None:
            job_data["download_urls"] = download_urls
        
        job_data["updated_at"] = datetime.utcnow().isoformat()
        
        self._save_job(job_id, job_data)
        return True
    
    def mark_running(self, job_id: str) -> bool:
        """Mark job as running"""
        return self.update_job(job_id, status=JobStatus.RUNNING, progress=10)
    
    def mark_done(self, job_id: str, download_urls: Dict[str, str]) -> bool:
        """Mark job as completed"""
        return self.update_job(
            job_id,
            status=JobStatus.DONE,
            progress=100,
            download_urls=download_urls
        )
    
    def mark_error(self, job_id: str, error: str) -> bool:
        """Mark job as failed"""
        return self.update_job(
            job_id,
            status=JobStatus.ERROR,
            error=error
        )
    
    def _save_job(self, job_id: str, job_data: Dict[str, Any]):
        """Save job data to file"""
        job_file = self.jobs_dir / f"{job_id}.json"
        
        with open(job_file, 'w') as f:
            json.dump(job_data, f, indent=2)
    
    def cleanup_old_jobs(self, days: int = 7):
        """Remove job files older than specified days"""
        from datetime import timedelta
        
        cutoff = datetime.utcnow() - timedelta(days=days)
        
        for job_file in self.jobs_dir.glob("*.json"):
            try:
                with open(job_file, 'r') as f:
                    job_data = json.load(f)
                
                created_at = datetime.fromisoformat(job_data["created_at"])
                
                if created_at < cutoff:
                    job_file.unlink()
                    print(f"Cleaned up old job: {job_data['job_id']}")
                    
            except Exception as e:
                print(f"Error cleaning up {job_file}: {e}")
    
    def get_user_jobs(self, user_id: str, limit: int = 50) -> list:
        """Get jobs for a specific user"""
        jobs = []
        
        for job_file in self.jobs_dir.glob("*.json"):
            try:
                with open(job_file, 'r') as f:
                    job_data = json.load(f)
                
                if job_data.get("user_id") == user_id:
                    jobs.append(job_data)
                    
            except Exception as e:
                print(f"Error reading job file: {e}")
        
        # Sort by created_at descending
        jobs.sort(key=lambda x: x["created_at"], reverse=True)
        
        return jobs[:limit]


class JobExecutor:
    """Execute async jobs (placeholder for actual worker)"""
    
    @staticmethod
    def execute_geotiff_job(job_id: str, job_store: JobStore):
        """
        Execute a GeoTIFF export job
        
        This is a synchronous execution. In production, this should be
        run in a background worker (Celery, RQ, or threading)
        """
        import time
        from .gee_chirps import CHIRPSExtractor
        from .gee_era5land import ERA5LandExtractor
        from .reducers import parse_geometry
        from .storage import FileStorage
        
        try:
            job_data = job_store.get_job(job_id)
            if not job_data:
                return
            
            job_store.mark_running(job_id)
            
            # Parse request
            request = job_data["request_data"]
            job_type = job_data["job_type"]
            
            # Parse geometry
            geometry = parse_geometry(request["geometry"])
            
            # Extract parameters
            start_date = request["date_range"]["start"]
            end_date = request["date_range"]["end"]
            resolution_deg = request.get("resolution_deg", 0.05)
            clip = request.get("clip_to_geometry", True)
            tiff_mode = request.get("tiff_mode", "multiband")
            band = request.get("band", "tavg" if "era5" in job_type else None)
            
            # Execute based on job type
            if "chirps" in job_type:
                extractor = CHIRPSExtractor()
                result = extractor.export_geotiff(
                    geometry, start_date, end_date,
                    resolution_deg, clip, tiff_mode, band
                )
            elif "era5" in job_type:
                extractor = ERA5LandExtractor()
                result = extractor.export_geotiff(
                    geometry, start_date, end_date,
                    resolution_deg, clip, tiff_mode, band or "tavg"
                )
            else:
                raise ValueError(f"Unknown job type: {job_type}")
            
            job_store.update_job(job_id, progress=80)
            
            # Store result
            storage = FileStorage()
            
            if result["mode"] == "multiband":
                # Single GeoTIFF URL from GEE
                download_urls = {
                    "tif": result["url"]
                }
            else:
                # Multiple files - create zip
                # For simplicity, just return first file URL
                # In production, create actual zip file
                download_urls = {
                    "tif": result["files"][0]["url"] if result["files"] else None
                }
            
            job_store.mark_done(job_id, download_urls)
            
        except Exception as e:
            print(f"Job {job_id} failed: {e}")
            job_store.mark_error(job_id, str(e))
