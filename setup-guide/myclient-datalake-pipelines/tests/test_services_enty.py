"""
Test stub for services-enty pipeline
"""
from common.utils import build_s3_ingest_path

def test_build_path():
    path = build_s3_ingest_path("services", "enty", "2025-01-01")
    assert path.startswith("s3://")
