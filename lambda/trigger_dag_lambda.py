"""
trigger_dag_lambda.py - starts the novacart_medallion DAG when a batch's _SUCCESS marker lands in S3.

Flow: s3://novacart-lakehouse/input/batch_<n>/_SUCCESS  ->  EventBridge rule (Object Created)
      ->  this Lambda  ->  MWAA CLI endpoint: airflow dags trigger novacart_medallion -c {"batch_id": "<n>"}

Environment variables:
    MWAA_ENV_NAME   name of the MWAA environment (required)
    DAG_ID          default novacart_medallion
Runtime: Python 3.12, standard library + boto3 (included in the Lambda runtime). Timeout 30 s.
"""

import base64
import json
import os
import re
import urllib.request

import boto3

MWAA_ENV_NAME = os.environ.get("MWAA_ENV_NAME", "")
DAG_ID = os.environ.get("DAG_ID", "novacart_medallion")
KEY_PATTERN = re.compile(r"^input/batch_(\d+)/_SUCCESS$")


def extract_key(event):
    """EventBridge 'Object Created' events carry the key in detail.object.key."""
    detail = event.get("detail", {})
    key = detail.get("object", {}).get("key")
    if key:
        return key
    records = event.get("Records", [])          # also accept classic S3 notification format
    if records:
        return records[0]["s3"]["object"]["key"]
    raise ValueError(f"No S3 object key in event: {json.dumps(event)[:500]}")


def trigger_dag(batch_id):
    mwaa = boto3.client("mwaa")
    token = mwaa.create_cli_token(Name=MWAA_ENV_NAME)
    conf = json.dumps({"batch_id": batch_id})
    run_id = f"s3_success_batch_{batch_id}_{context_time()}"
    command = f"dags trigger {DAG_ID} -r {run_id} -c '{conf}'"
    req = urllib.request.Request(
        url=f"https://{token['WebServerHostname']}/aws_mwaa/cli",
        data=command.encode("utf-8"),
        method="POST",
        headers={"Authorization": f"Bearer {token['CliToken']}", "Content-Type": "text/plain"},
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        body = json.loads(resp.read().decode("utf-8"))
    stdout = base64.b64decode(body.get("stdout", "")).decode("utf-8")
    stderr = base64.b64decode(body.get("stderr", "")).decode("utf-8")
    return run_id, stdout, stderr


def context_time():
    import datetime as dt
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S")


def lambda_handler(event, context):
    if not MWAA_ENV_NAME:
        raise RuntimeError("Set the MWAA_ENV_NAME environment variable")
    key = extract_key(event)
    match = KEY_PATTERN.match(key)
    if not match:
        print(f"Ignoring object {key}: not an input/batch_<n>/_SUCCESS marker")
        return {"status": "ignored", "key": key}

    batch_id = match.group(1)
    run_id, stdout, stderr = trigger_dag(batch_id)
    print(f"Triggered {DAG_ID} run_id={run_id} batch_id={batch_id}\nstdout: {stdout}\nstderr: {stderr}")
    if "Traceback" in stderr:
        raise RuntimeError(f"MWAA CLI reported an error: {stderr}")
    return {"status": "triggered", "dag_id": DAG_ID, "run_id": run_id, "batch_id": batch_id}
