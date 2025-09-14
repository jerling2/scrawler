"""This module provides utilities to embed text"""
from openai import AsyncOpenAI
import ollama


def embed_texts_with_nomic(texts: list[str]) -> list[list[float]]:
    """
    Args:
        - texts (list[str]):
            A list of strings to embed.
    
    Returns:
        - Response containing embeddings as a list of float vectors, where each
            vector corresponds to the input text at the same index.
    
    Example:
        >>> result = embed_texts_with_nomic(['hello', 'world'])
        >>> len(response)
        2
        >>> len(response[0])  # Embedding dimension
        768
    """
    response = ollama.embed(
        model='nomic-embed-text',
        input=texts
    )
    return response["embeddings"]


async def embed_texts_with_openai(texts: list[str]):
    """
    Args:
        - texts (list[str]):
            A list of strings to embed.
    
    Returns:
        - OpenAI embeddings response object containing:
            - data: List of embedding objects with .embedding attribute
            - usage: Token usage information (prompt_tokens, total_tokens)
            - model: Model name used for embedding

    Raises:
        - OpenAIError: If environment variable `OPENAI_API_KEY` is not set.
    Example:
        >>> response = await embed_texts_with_openai(['hello', 'world'])
        >>> embeddings = [embedding.embedding for embedding in response.data]
        >>> usuage = response.usuage
        >>> model = response.model
    """
    client = AsyncOpenAI()
    response = await client.embeddings.create(
        model="text-embedding-3-small",
        input=texts
    )
    return response
