"""
Wrapper Lambda para el job de refresh.
El event puede traer job_id (lo envía el backend).
"""
import json
import os
from refresh_star_job import run

def handler(event, context):
    job_id = None
    if isinstance(event, dict):
        job_id = event.get("job_id")
    if not job_id:
        job_id = os.environ.get("JOB_ID", "")
    try:
        result = run(job_id=job_id)
        return {
            "statusCode": 200,
            "body": json.dumps(result),
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"status": "FAILED", "error": str(e)}),
        }
