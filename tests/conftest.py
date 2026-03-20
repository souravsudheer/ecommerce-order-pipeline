"""
conftest.py
Shared pytest fixtures for the test suite.
Uses in-memory DuckDB connections so tests are fast,
isolated, and require no file system setup.
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import pytest
import duckdb


@pytest.fixture
def mem_conn():
    """
    A fresh in-memory DuckDB connection for each test.
    Automatically closed after the test completes.
    """
    conn = duckdb.connect()
    yield conn
    conn.close()
