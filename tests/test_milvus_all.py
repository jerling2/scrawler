import pytest
import time
import random
from source.utilities.embedding.embedder import SupportedEmbedding
from source.utilities.database.models.milvus_applications_model import MilvusApplicationsModel, MilvusApplicationEntity
from source.utilities.database.connections.milvus_connection import MilvusConnection
from source.utilities.database.repos.milvus_applications_repo import MilvusApplicationsRepo


WAIT_FOR_WRITE = 1 # seconds


@pytest.fixture
def model():
    return MilvusApplicationsModel(SupportedEmbedding.NOMIC_EMBED_TEXT.dim, 512)

@pytest.fixture
def conn():
    return MilvusConnection()

@pytest.fixture(scope="session")
def repo():
    repo = MilvusApplicationsRepo("pytest_application_repo", SupportedEmbedding.NOMIC_EMBED_TEXT.dim, 512)
    yield repo
    repo.drop_collection() 

@pytest.fixture
def random_data():
    return \
    [
        MilvusApplicationEntity(
            application_id=0,
            chunk_idx=i,
            chunk_txt=f"sample text {i}",
            chunk_vec=[
                random.random() for _ in range(SupportedEmbedding.NOMIC_EMBED_TEXT.dim)
            ]
        )
        for i in range(10)
    ]

@pytest.fixture
def random_vector():
    return [random.random() for _ in range(SupportedEmbedding.NOMIC_EMBED_TEXT.dim)]

def test_model_init(model):
    assert model

def test_conn_init(conn):
    assert conn

def test_create_collection_schema(model, conn):
    assert conn.create_collection_schema(model.collection_config, model.fields)

def test_create_index_params(model, conn):
    assert conn.create_index_params(model.indices)

def test_repo_init(repo):
    assert repo

def test_repo_create_collection(repo):
    repo.create_collection()

def test_repo_drop_collection(repo):
    repo.drop_collection()

def test_repo_insert(repo, random_data):
    repo.create_collection()
    repo.insert(random_data)

def test_repo_delete(repo, random_data):
    repo.create_collection()
    repo.insert(random_data)
    repo.delete([0])

def test_repo_search(repo, random_data, random_vector):
    repo.create_collection()
    repo.insert(random_data)
    time.sleep(WAIT_FOR_WRITE)
    hits = repo.search([random_vector], limit=5)
    assert len(hits[0]) == 5
