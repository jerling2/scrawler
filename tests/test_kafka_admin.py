import pytest
from source import create_kafka_topics, get_kafka_admin_client

def test_get_admin_client():
    assert get_kafka_admin_client()

def test_create_kafka_topic():
    admin_client = get_kafka_admin_client()
    create_kafka_topics(admin_client, ['pytest'])
