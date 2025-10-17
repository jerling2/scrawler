import pytest
import os
from source import MongoConnection, HandshakeRawJobListingsRepo


@pytest.fixture(scope='session')
def conn():
    conn = MongoConnection()
    conn.connect()
    yield conn
    conn.close()

@pytest.fixture(scope='session')
def repo(conn):
    repo = HandshakeRawJobListingsRepo('pytest', conn)
    yield repo
    conn.get_collection('pytest').drop()
    
def test_init_conn(conn):
    assert conn

def test_init_repo(repo):
    assert repo

def test_repo_insert_document(repo):
    repo.insert_raw_job_listings(
        url='https://example.com',
        html="<html>test</hmtl>"
    )