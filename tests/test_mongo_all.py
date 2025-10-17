import pytest
from source import MongoConnection


@pytest.fixture(scope='session')
def conn():
    conn = MongoConnection()
    yield conn


def test_init(conn):
    assert conn

def test_connect(conn):
    conn.connect()