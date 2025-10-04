import pytest
from source.utilities.database.logging.logger import get_database_logger
from source.utilities.database.base.postgres_database import PSQLDatabase, SQLStatement

logger = get_database_logger()

@pytest.fixture()
def conn():
    db = PSQLDatabase()
    db.connect()
    yield db
    db.close()

def test_connect_and_close(conn):
    assert conn

def test_context():
    with PSQLDatabase() as db:
        assert db

def test_health_check(conn):
    conn.health_check()

def test_execute(conn):
    conn.execute(SQLStatement("SELECT %s;", (1,)))

def test_transaction(conn):
    conn.transaction([
        SQLStatement("SELECT %s;", (1,)),
        SQLStatement("SELECT %s;", (1,)),
    ])

def test_fetch_one(conn):
    row = conn.fetch_one(SQLStatement("SELECT 1;"))
    assert row == (1,)

def test_fetch_many(conn):
    rows = conn.fetch_many(SQLStatement("SELECT 1;"), 1)
    assert rows == [(1,)]

def test_fetch(conn):
    rows = conn.fetch(SQLStatement("SELECT 1;"))
    assert rows == [(1,)]