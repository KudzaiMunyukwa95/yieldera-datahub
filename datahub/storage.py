"""
File storage utilities for DataHub outputs
"""

import os
import csv
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime


class FileStorage:
    """Manage file storage for DataHub outputs"""
    
    def __init__(self, output_dir: str = "/tmp/datahub/outputs"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def save_csv(
        self,
        data: List[Dict[str, Any]],
        filename: str,
        headers: List[str]
    ) -> str:
        """
        Save data as CSV file
        
        Args:
            data: List of dictionaries
            filename: Output filename
            headers: CSV column headers
            
        Returns:
            Path to saved file
        """
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(data)
        
        return str(filepath)
    
    def save_timeseries_csv(
        self,
        data: List[Dict[str, Any]],
        dataset: str,
        job_id: str = None
    ) -> str:
        """
        Save timeseries data as CSV
        
        Args:
            data: Timeseries data
            dataset: Dataset name ('chirps' or 'era5land')
            job_id: Optional job ID for filename
            
        Returns:
            Path to CSV file
        """
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        
        if job_id:
            filename = f"{dataset}_{job_id}.csv"
        else:
            filename = f"{dataset}_{timestamp}.csv"
        
        # Determine headers based on dataset
        if dataset.lower() == "chirps":
            headers = ["date", "precip_mm"]
        else:  # ERA5-Land
            headers = ["date", "tmin_c", "tmax_c", "tavg_c"]
        
        return self.save_csv(data, filename, headers)
    
    def get_file_url(self, filepath: str, base_url: str = "") -> str:
        """
        Generate download URL for a file
        
        Args:
            filepath: Path to file
            base_url: Base API URL
            
        Returns:
            Download URL
        """
        filename = Path(filepath).name
        
        if base_url:
            return f"{base_url}/api/data/download/{filename}"
        else:
            return f"/api/data/download/{filename}"
    
    def cleanup_old_files(self, days: int = 7):
        """Remove files older than specified days"""
        from datetime import timedelta
        
        cutoff = datetime.utcnow() - timedelta(days=days)
        
        for filepath in self.output_dir.glob("*"):
            if filepath.is_file():
                file_mtime = datetime.fromtimestamp(filepath.stat().st_mtime)
                
                if file_mtime < cutoff:
                    filepath.unlink()
                    print(f"Cleaned up old file: {filepath.name}")
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """Get storage statistics"""
        try:
            files = list(self.output_dir.glob("*"))
            total_size = sum(f.stat().st_size for f in files if f.is_file())
            
            return {
                "total_files": len(files),
                "total_size_mb": round(total_size / (1024 * 1024), 2),
                "output_dir": str(self.output_dir)
            }
        except:
            return {"total_files": 0, "total_size_mb": 0}
