#!/usr/bin/env python3
"""
Flink REST API client for job submission and monitoring.

Uses urllib3 (bundled with requests) because the ``requests`` library has a
compatibility issue with the Flink REST server (RemoteDisconnected errors).
"""

import json
import time
import urllib3  # type: ignore[import-untyped]
from typing import Optional, Dict, Any, List

# Silence urllib3 warnings about unverified HTTPS (we only use HTTP)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_http = urllib3.PoolManager(timeout=urllib3.Timeout(connect=10, read=60))


def _get(url: str, timeout: int = 60) -> Optional[Any]:
    """GET request → parsed JSON body, or None on failure."""
    try:
        resp = _http.request('GET', url, timeout=urllib3.Timeout(connect=5, read=timeout))
        if resp.status == 200:
            return json.loads(resp.data.decode('utf-8'))
    except Exception:
        pass
    return None


def _post_json(url: str, body: dict, timeout: int = 120) -> Optional[Any]:
    """POST JSON body → parsed JSON response, or None."""
    try:
        resp = _http.request(
            'POST', url,
            body=json.dumps(body).encode('utf-8'),
            headers={'Content-Type': 'application/json'},
            timeout=urllib3.Timeout(connect=10, read=timeout),
        )
        if resp.status == 200:
            return json.loads(resp.data.decode('utf-8'))
    except Exception as e:
        print(f"POST {url} failed: {e}")
    return None


def _post_multipart(url: str, fields: dict, timeout: int = 120) -> Optional[Any]:
    """POST multipart/form-data → parsed JSON response, or None."""
    try:
        resp = _http.request(
            'POST', url,
            fields=fields,
            timeout=urllib3.Timeout(connect=10, read=timeout),
        )
        if resp.status == 200:
            return json.loads(resp.data.decode('utf-8'))
    except Exception as e:
        print(f"POST (multipart) {url} failed: {e}")
    return None


def _patch(url: str, timeout: int = 30) -> int:
    """PATCH request → HTTP status code (0 on failure)."""
    try:
        resp = _http.request('PATCH', url, timeout=urllib3.Timeout(connect=5, read=timeout))
        return resp.status
    except Exception:
        return 0


class FlinkClient:
    """Client for interacting with Flink REST API."""

    def __init__(self, rest_url: str = "http://localhost:8081"):
        self.rest_url = rest_url.rstrip('/')

    def is_available(self) -> bool:
        """Check if Flink cluster is available."""
        return _get(f"{self.rest_url}/overview", timeout=5) is not None

    def get_cluster_info(self) -> Optional[Dict[str, Any]]:
        """Get cluster overview information."""
        return _get(f"{self.rest_url}/overview")

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
        params: Dict[str, Any] = {}
        if entry_class:
            params['entry-class'] = entry_class
        if parallelism:
            params['parallelism'] = parallelism
        if program_args:
            params['program-args'] = ' '.join(program_args)

        result = _post_json(run_url, params)
        if result:
            return result.get('jobid')
        return None

    def _upload_jar(self, jar_path: str) -> Optional[str]:
        """Upload a JAR file to Flink cluster."""
        upload_url = f"{self.rest_url}/jars/upload"
        try:
            with open(jar_path, 'rb') as f:
                jar_data = f.read()
            fields = {
                'jarfile': (jar_path.split('/')[-1], jar_data, 'application/java-archive'),
            }
            result = _post_multipart(upload_url, fields)
            if result:
                filename = result.get('filename', '')
                return filename.split('/')[-1] if '/' in filename else filename
        except Exception as e:
            print(f"Error uploading JAR: {e}")
        return None

    def get_job_status(self, job_id: str) -> Optional[str]:
        """Get status of a job."""
        data = _get(f"{self.rest_url}/jobs/{job_id}")
        if data:
            return data.get('state')
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
        return _get(f"{self.rest_url}/jobs/{job_id}/metrics")

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a running job."""
        return _patch(f"{self.rest_url}/jobs/{job_id}") == 202
