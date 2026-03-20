import pytest
import duckdb


@pytest.fixture
def mem_conn():
    conn = duckdb.connect()
    yield conn
    conn.close()
