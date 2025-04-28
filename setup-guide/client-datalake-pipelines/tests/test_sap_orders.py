"""
Test stub for sap-orders pipeline
"""
from common.utils import build_s3_ingest_path

def test_build_path():
    path = build_s3_ingest_path("sap", "orders", "2025-01-01")
    assert path.startswith("s3://")
