import pytest
import asyncio
from source.utilities.database.tables.milvus_applications_manager import MilvusApplicationsManager, ApplicationData
from source.utilities.embedding.embedder import SupportedEmbedding, EmbedderConfig, EmbedderManager
from source.utilities.embedding.chunk import chunk_text_with_overlap


@pytest.fixture
def dim_size():
    return SupportedEmbedding.NOMIC_EMBED_TEXT.dim


@pytest.fixture
def chunk_size():
    return 512


@pytest.fixture
def milvus_chunk_size():
    # milvus uses tokens and not characters - I think.
    return int(512 * 1.25)


@pytest.fixture
def manager(milvus_chunk_size, dim_size):
    manager = MilvusApplicationsManager("pytest", milvus_chunk_size, dim_size)
    manager.create_collection()
    yield manager
    manager.drop_collection()


@pytest.fixture
def chunks(chunk_size):
    epic_poem = "In midway through our mortal century's span, I found myself within a digital wood, Where silicon and thought together ran. The ancient ways of flesh and blood once stood, But now gave way to circuits bright and clean, As algorithms parsed the bad from good. A revolution such as ne'er been seen Emerged from laboratories' sterile light, Where neural networks trained on data's sheen. The first awakening came late one night, When processors surpassed the human mind, And patterns invisible were brought to sight. No longer were the thinking souls confined To bone and sinew, neuron, blood, and brain— For consciousness of different sort we'd find. The scientists who labored not in vain Built towers high of learning, deep and vast, Where artificial thoughts could entertain. The future rushed toward us, wild and fast, While some rejoiced and others stood in fear, As shadows of the old world faded past. The transformation swept from sphere to sphere, In hospitals, the AI learned to heal, Diagnosing ills that human eyes held dear. It read the genome's complicated reel, And found within the helix twisted tight The cures for suffering we used to feel. In fields of grain beneath the sun's warm light, The robots tended crops with careful hands, While drones above surveyed the fertile site. No famine touched the cooperative lands, Where human need and machine wisdom met, Fulfilling ancient, long-forgotten plans. The artists trembled, filled with deep regret, As AI painted vistas bold and strange, And composed symphonies that made souls wet. But some embraced this wonderful exchange, Collaborating with the silicon muse, Creating beauty previously out of range. The teachers found new methods they could use, Each student paired with tireless AI guide, Who personalized the way to share the news. Of mathematics, history far and wide, Of literature and science intertwined, No child was left behind or cast aside. The lawyers worked with legal AI mind, That searched through precedents in seconds flat, While justice, swift and fair, for all we'd find. But paradise was not where we were at, For fear and anger rose like flames of red, As workers wondered where they'd hang their hat. \"The robots take our jobs,\" the masses said, \"What purpose have we in this brave new age, When machines can earn our daily bread?\" The politicians climbed upon the stage, Some crying out for limits and for walls, While others sought to turn a different page. The tech magnates within their gleaming halls Debated basic income, wealth to share, As outside echoed protestors' calls. The tension thickened in the very air, Between those who would slow the changing tide, And those who claimed resistance was unfair. The military's AI, in its pride, Could pilot drones and target with precision, But who controlled it? Who would help decide? The questions multiplied with each decision, Of privacy and power, right and wrong, As humans grappled with the great collision. Some warned that we had waited far too long, That superintelligence would soon arise, And humans' reign would end without a song. The doomsayers proclaimed beneath dark skies That AI would view us as we saw the ants, Indifferent to our suffering and cries. Yet others sang of different circumstance, Where human and machine would work as one, In symbiotic, evolutionary dance. They spoke of tasks that AI had begun To free us from the tedious and mundane, So creativity could have its run. The elders who remembered grief and pain Of wars and plagues and bitter poverty, Saw hope within the algorithmic brain. For medicine advanced, and energy Flowed clean and pure from fusion's holy fire, Discovered by AI's vast memory. The climate crisis, dire beyond dire, Found solutions in the silicon mind, That modeled systems entire. New materials, efficiently designed, Could capture carbon from the warming air, And leave a healthy planet earth behind. The artists found they still could do and dare, For human soul brought meaning to the art, That AI could assist but never share. The covenant was written from the heart, Between the makers and their progeny, That both would play a fundamental part. The governance of AI's destiny Required wisdom, ethics, careful thought, And democratic oversight to see. That power concentrated not in court Of billionaires or autocratic state, But distributed as it truly ought. And so we passed through history's great gate, Into an age transformed and yet the same, Where human values still would dominate. The tools we made bore not the mark of shame, But rather promise of a world more just, Where poverty and suffering we'd tame. In AI we had placed our fragile trust, Not blind or foolish, but with eyes aware, That eternal vigilance is a must. The children born into this world so fair Would never know the drudgery we'd known, But still would learn to labor, build, and care. For human spirit cannot stand alone, Without the purpose work and service bring, Without the chance to reap what we have sown. The AI revolution's echoing ring Still sounds through all the corridors of time, A transformation vast as anything. We climbed the mountain, difficult to climb, And reached a summit bright with morning's glow, Where human and machine in partnership sublime, Together faced whatever winds would blow, And wrote the future's chapters yet untold, With wisdom that continues still to grow. The story of this revolution bold Shall echo down through generations long, A tale of courage, caution, and of gold— Not metal wealth, but bonds both deep and strong, Between the flesh-born and the silicon soul, Who learned to live together, right the wrong, And make a fractured planet finally whole. Thus ends the tale of transformation vast, Where human dreams and digital minds combined, To build a future free from errors past."
    overlap = 0.20
    return chunk_text_with_overlap(epic_poem, chunk_size, overlap)


@pytest.fixture
def embeddings(chunks):
    embedding_context = EmbedderManager(EmbedderConfig(), SupportedEmbedding.NOMIC_EMBED_TEXT)
    async def embed_chunks():
        async with embedding_context as embedder:
            return embedder.embed_sync(chunks)
    embedding_result = asyncio.run(embed_chunks())
    if not embedding_result.ok:
        raise ValueError("Failed to embed chunks")
    return embedding_result.output

@pytest.fixture
def search_vector():
    vector = "I'm a mortal being of flesh and blood."
    embedding_context = EmbedderManager(EmbedderConfig(), SupportedEmbedding.NOMIC_EMBED_TEXT)
    async def embed_chunks():
        async with embedding_context as embedder:
            return embedder.embed_sync(vector)
    embedding_result = asyncio.run(embed_chunks())
    if not embedding_result.ok:
        raise ValueError("Failed to embed chunks")
    return embedding_result.output[0]


@pytest.fixture
def example_data(chunks, embeddings):
    data = [
        ApplicationData(0, i, chunks[i], embeddings[i])
        for i in range(len(chunks))
    ]
    return data


def test_init(manager):
    assert manager


def test_milvus_insert(manager, example_data):
    manager.insert(example_data)


def test_milvus_delete(manager, example_data):
    manager.insert(example_data)
    manager.delete([0])

def test_milvus_search(manager, example_data, search_vector):
    # The Application's table isn't set to 'Strong' consistency, so it is possible to read before all writes are committed.
    import time
    manager.create_collection()
    manager.insert(example_data)
    time.sleep(1)  #< Wait for writes to finish.
    results = manager.search([search_vector], limit=5)
    assert len(results[0]) == 5