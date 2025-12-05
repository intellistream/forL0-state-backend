#!/usr/bin/env python3
"""
Flink REST API client for job submission and monitoring.
"""

import requests  # type: ignore[import-untyped]
import time
from typing import Optional, Dict, Any, List


class FlinkClient:
    """Client for interacting with Flink REST API."""
    
    def __init__(self, rest_url: str = "http://localhost:8081"):
        self.rest_url = rest_url.rstrip('/')
    
    def is_available(self) -> bool:
        """Check if Flink cluster is available."""
        try:
            response = requests.get(f"{self.rest_url}/overview", timeout=5)
            return response.status_code == 200
        except requests.RequestException:
            return False
    
    def get_cluster_info(self) -> Optional[Dict[str, Any]]:
        """Get cluster overview information."""
        try:
            response = requests.get(f"{self.rest_url}/overview")
            if response.status_code == 200:
                return response.json()
        except requests.RequestException:
            pass
        return None
    
    def submit_job(self, jar_path: str, entry_class: Optional[str] = None, 
                   program_args: Optional[List[str]] = None, parallelism: Optional[int] = None) -> Optional[str]:
        """
        Submit a job to Flink cluster.
        
        Returns job ID if successful, None otherwise.
        """
        # First upload the JAR
        jar_id = self._upload_jar(jar_path)
        if not jar_id:
            return None
        
        # Then run the JAR
        run_url = f"{self.rest_url}/jars/{jar_id}/run"
        params = {}
        if entry_class:
            params['entry-class'] = entry_class
        if parallelism:
            params['parallelism'] = parallelism
        if program_args:
            params['program-args'] = ' '.join(program_args)
        
        try:
            response = requests.post(run_url, json=params)
            if response.status_code == 200:
                result = response.json()
                return result.get('jobid')
        except requests.RequestException as e:
            print(f"Error submitting job: {e}")
        
        return None
    
    def _upload_jar(self, jar_path: str) -> Optional[str]:
        """Upload a JAR file to Flink cluster."""
        upload_url = f"{self.rest_url}/jars/upload"
        try:
            with open(jar_path, 'rb') as f:
                files = {'jarfile': (jar_path, f, 'application/java-archive')}
                response = requests.post(upload_url, files=files)
                if response.status_code == 200:
                    result = response.json()
                    # Extract JAR ID from filename
                    filename = result.get('filename', '')
                    return filename.split('/')[-1] if '/' in filename else filename
        except Exception as e:
            print(f"Error uploading JAR: {e}")
        return None
    
    def get_job_status(self, job_id: str) -> Optional[str]:
        """Get status of a job."""
        try:
            response = requests.get(f"{self.rest_url}/jobs/{job_id}")
            if response.status_code == 200:
                return response.json().get('state')
        except requests.RequestException:
            pass
        return None
    
    def wait_for_job(self, job_id: str, timeout: int = 3600, 
                     poll_interval: int = 5) -> str:
        """
        Wait for a job to complete.
        
        Returns final job status.
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            status = self.get_job_status(job_id)
            if status in ['FINISHED', 'FAILED', 'CANCELED']:
                return status
            print(f"Job {job_id} status: {status}")
            time.sleep(poll_interval)
        return 'TIMEOUT'
    
    def get_job_metrics(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get metrics for a job."""
        try:
            response = requests.get(f"{self.rest_url}/jobs/{job_id}/metrics")
            if response.status_code == 200:
                return response.json()
        except requests.RequestException:
            pass
        return None
    
    def cancel_job(self, job_id: str) -> bool:
        """Cancel a running job."""
        try:
            response = requests.patch(f"{self.rest_url}/jobs/{job_id}")
            return response.status_code == 202
        except requests.RequestException:
            return False
