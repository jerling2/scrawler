import pytest
import random
from datetime import datetime
from psycopg2.errors import DuplicateTable, UndefinedTable
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
    try:
        repo.drop_table()
        conn.client.commit()
    except:
        conn.client.rollback()

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
    try:
        repo.create_table()
        conn.client.commit()
    except DuplicateTable:
        conn.client.rollback()

def test_drop(repo, conn):
    try:
        repo.drop_table()
        conn.client.commit()
    except UndefinedTable:
        conn.client.rollback()

def test_insert(repo, conn, random_data):
    try:
        repo.create_table()
        conn.client.commit()
    except DuplicateTable:
        conn.client.rollback()

    for application in random_data:
        repo.insert(application)
    conn.client.commit()