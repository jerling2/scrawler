import pytest
import random
from datetime import datetime
from source import PostgresApplicationEntity, PostgresConnection, PostgresApplicationsRepo


@pytest.fixture(scope="session")
def conn():
    conn = PostgresConnection()
    conn.connect()
    yield conn
    conn.close()

@pytest.fixture(scope="session")
def repo(conn):
    repo = PostgresApplicationsRepo('pytest_applications_table', conn)
    yield repo

@pytest.fixture
def random_data():
    return \
    [
        PostgresApplicationEntity(
            datetime.now(),
            random.choice(['full-time', 'part-time', 'internship']),
            'software engineer',
            'remote',
            'Company A',
            (50_000, 90_000)
        )
        for _ in range(10)
    ]

def test_conn(conn):
    assert conn

def test_repo(repo):
    assert repo

def test_create(repo, conn):
    with conn.client.cursor():
        repo.create_table()
        conn.client.commit()

def test_drop(repo, conn):
    with conn.client.cursor():
        repo.drop_table()
        conn.client.commit()

def test_insert(repo, conn, random_data):
    with conn.client.cursor():
        for application in random_data:
            repo.insert(application)
        conn.client.commit()
