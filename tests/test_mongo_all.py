import pytest
import os
from source import MongoConnection


@pytest.fixture(scope='session')
def conn():
    conn = MongoConnection()
    conn.connect()
    yield conn
    conn.close()

def test_get_database(conn):
    assert conn.get_database().name == os.environ['SCRAWLER_MONGO_DATABASE']

def test_get_collection(conn):
    assert conn.get_collection('pytest').name == 'pytest'