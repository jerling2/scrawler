import pytest
from source import MongoConnection


@pytest.fixture(scope='session')
def conn():
    conn = MongoConnection()
    conn.connect()
    yield conn
    conn.close()


def test_init_conn(conn):
    assert conn

# Deleted dependency on E1 Repo (no longer exists)