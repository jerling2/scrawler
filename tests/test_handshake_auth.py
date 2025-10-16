import pytest
from source import HandshakeAuth

@pytest.fixture(scope='session')
def crawler():
    crawler = HandshakeAuth()
    yield crawler

def test_init(crawler):
    assert crawler

@pytest.mark.asyncio
async def test_login(crawler):
    await crawler.login()

@pytest.mark.asyncio
async def test_check_auth(crawler):
    await crawler.check_auth()

@pytest.mark.asyncio
async def test_login_and_check(crawler):
    await crawler.login()
    assert await crawler.check_auth() == True
