from vixion.constants import VECTOR_DIM
from vixion.services import embeddings_stub


def _cos_sim(a: list[float], b: list[float]) -> float:
    return sum(x * y for x, y in zip(a, b, strict=True))


def test_stub_embedding_dimension_and_normalized():
    v = embeddings_stub.stub_embedding_vector("hello world")
    assert len(v) == VECTOR_DIM
    s = sum(x * x for x in v)
    assert 0.99 < s < 1.01


def test_stub_embedding_deterministic():
    assert embeddings_stub.stub_embedding_vector("x") == embeddings_stub.stub_embedding_vector("x")


def test_stub_embedding_similar_headlines_closer_than_unrelated():
    a = embeddings_stub.stub_embedding_vector(
        "Israel and Gaza: ceasefire talks continue amid international pressure"
    )
    b = embeddings_stub.stub_embedding_vector(
        "Gaza crisis: Israel faces mounting pressure over humanitarian situation"
    )
    u = embeddings_stub.stub_embedding_vector(
        "Tokyo cherry blossom festival draws record crowds this spring weekend"
    )
    assert _cos_sim(a, b) > _cos_sim(a, u) + 0.05
    assert _cos_sim(a, b) > _cos_sim(b, u) + 0.05
