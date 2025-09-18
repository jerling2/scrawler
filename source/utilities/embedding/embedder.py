"""This module provides the Embedder class"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Protocol, Awaitable, Generic, TypeVar
from enum import Enum
import asyncio
import tiktoken
from transformers import AutoTokenizer
from aiolimiter import AsyncLimiter
import ollama
from openai import AsyncOpenAI, OpenAI


T = TypeVar("T", int, float)


class SyncEmbedChunks(Protocol):
    """Protocol for synchronous embedding functions that process text chunks."""

    def __call__(model: str, chunks: list[str]) -> EmbeddingResult: ...


class AsyncEmbedChunks(Protocol):
    """Protocol for asynchronous embedding functions that process text chunks."""

    def __call__(model: str, chunks: list[str]) -> Awaitable[EmbeddingResult]: ...


class Tokenize(Protocol):
    """Protocol for tokenization functions that convert text to token lists."""

    def __call__(text: str) -> list[float]: ...


@dataclass
class UsageResult:
    """Contains statistics on embedding usage.

    Attr:
    - model (str): The embedding model used
    - total_tokens (Optional[int]): Total number of tokens processed
    """
    model: str
    total_tokens: Optional[int]=None


@dataclass
class EmbeddingResult:
    """Represents the result of an embedding operation.

    Embedding results contain either full output or nothing at all to ensure consistency.

    Attr:
    - ok (bool): Whether the embedding operation succeeded
    - output (Optional[list[list[float]]]): The embedding vectors if successful
    - usage (Optional[UsageResult]): Usage statistics if available
    """
    ok: bool
    output: Optional[list[list[float]]] = None
    usage: Optional[UsageResult] = None

    @classmethod
    def success(cls, output: list[float], usage: UsageResult) -> EmbeddingResult:
        """Create a successful embedding result.

        Args:
        - output (list[float]): The embedding vectors
        - usage (UsageResult): Usage statistics

        Returns:
        - (EmbeddingResult): A successful embedding result
        """
        return EmbeddingResult(ok=True, output=output, usage=usage)

    @classmethod
    def failure(cls) -> EmbeddingResult:
        """Create a failed embedding result.

        Returns:
        - (EmbeddingResult): A failed embedding result with ok=False
        """
        return EmbeddingResult(ok=False)


@dataclass
class EmbeddingInfo:
    """Contains available information about an embedding model.

    Attr:
    - model (str): Model identifier
    - dim (int): Embedding dimension
    - cost_per_token (Optional[float]): Cost per token in USD
    - sync_embed (Optional[SyncEmbedChunks]): Synchronous embedding function
    - async_embed (Optional[AsyncEmbedChunks]): Asynchronous embedding function
    - tokenize (Optional[Tokenize]): Tokenization function
    - rpm_limit (int): Requests per minute limit
    - tpm_limit (int): Tokens per minute limit
    """
    model: str
    dim: int
    cost_per_token: Optional[float]=None
    sync_embed: Optional[SyncEmbedChunks]=None
    async_embed: Optional[AsyncEmbedChunks]=None
    tokenize: Optional[Tokenize]=None
    rpm_limit: int=None
    tpm_limit: int=None


@dataclass
class EmbedderConfig:
    """Configuration for rate limits and usage constraints.

    Attr:
    - tok_limiter (Optional[CountLimiter[int]]): Token usage limiter
    - usd_limiter (Optional[CountLimiter[float]]): USD cost limiter
    - rpm_limiter (Optional[AsyncLimiter]): Requests per minute limiter
    - tpm_limiter (Optional[AsyncLimiter]): Tokens per minute limiter
    """
    tok_limiter: Optional[CountLimiter[int]]=None
    usd_limiter: Optional[CountLimiter[float]]=None
    rpm_limiter: Optional[AsyncLimiter]=None
    tpm_limiter: Optional[AsyncLimiter]=None


def create_sync_embed_function(provider: str) -> SyncEmbedChunks:
    """Create a synchronous embedding function for the specified provider.

    Args:
    - provider (str): The embedding provider ('ollama' or 'openai')

    Returns:
    - (SyncEmbedChunks): A synchronous embedding function

    Raises:
    - ValueError: If the provider is not supported
    """
    if provider == "ollama":
        def embed_sync_with_ollama(model: str, chunks: list[str]):
            try:
                response = ollama.embed(model=model, input=chunks)
                return EmbeddingResult.success(response.embeddings, UsageResult(model=model))
            except Exception:
                return EmbeddingResult.failure()
        return embed_sync_with_ollama
    if provider == "openai":
        def embed_sync_with_openai(model: str, chunks: list[str]):
            try:
                client = OpenAI()
                response = client.embeddings.create(model=model, input=chunks)
                embeddings = [obj.embedding for obj in response.data]
                usage = UsageResult(model, total_tokens=response.usage.total_tokens)
                return EmbeddingResult.success(embeddings, usage)
            except Exception:
                return EmbeddingResult.failure()
        return embed_sync_with_openai
    raise ValueError(
        f"Unsupported embedding provider '{provider}'. Supported providers: 'ollama', 'openai'"
    )


def create_async_embed_function(provider: str) -> AsyncEmbedChunks:
    """Create an asynchronous embedding function for the specified provider.

    Args:
    - provider (str): The embedding provider ('openai' is currently supported)

    Returns:
    - (AsyncEmbedChunks): An asynchronous embedding function

    Raises:
    - ValueError: If the provider is not supported
    """
    if provider == "openai":
        async def embed_async_with_openai(model: str, chunks: list[str]):
            client = AsyncOpenAI()
            return client.embeddings.create(model=model, input=chunks)
        return embed_async_with_openai
    raise ValueError(
        f"Unsupported async embedding provider '{provider}'. Supported providers: 'openai'"
    )


def create_tokenizer_function(model: str) -> Tokenize:
    """Create a tokenization function for the specified model.

    Args:
    - model (str): The model name ('nomic-embed-text',
        'text-embedding-3-small', or 'text-embedding-3-large')

    Returns:
    - (Tokenize): A tokenization function for the model

    Raises:
    - ValueError: If the model is not supported
    """
    if model == "nomic-embed-text":
        def tokenize_nomic(text: str) -> list[float]:
            enc = AutoTokenizer.from_pretrained("nomic-ai/nomic-embed-text-v1")
            return enc.encode(text)
        return tokenize_nomic
    if model == "text-embedding-3-small":
        def tokenize_embedding_3_small(text: str) -> list[float]:
            enc = tiktoken.get_encoding('cl100k_base')
            return enc.encode(text)
        return tokenize_embedding_3_small
    if model == "text-embedding-3-large":
        def tokenize_embedding_3_large(text: str) -> list[float]:
            enc = tiktoken.get_encoding('cl100k_base')
            return enc.encode(text)
        return tokenize_embedding_3_large
    raise ValueError(
        f"Unsupported tokenizer model '{model}'. Supported models: "
        "'nomic-embed-text', 'text-embedding-3-small', 'text-embedding-3-large'"
    )


class SupportedEmbedding(Enum):
    """Enum of supported embedding models with their configuration.

    Each enum value contains complete embedding model information including dimensions,
    costs, and associated functions.

    Attr:
    - NOMIC_EMBED_TEXT: Nomic embedding model configuration
    - TEXT_EMBEDDING_3_SMALL: OpenAI small embedding model configuration  
    - TEXT_EMBEDDING_3_LARGE: OpenAI large embedding model configuration
    """
    NOMIC_EMBED_TEXT = EmbeddingInfo(
        model="nomic-embed-text",
        dim=768,
        cost_per_token=0.02e-6,
        sync_embed=create_sync_embed_function("ollama"),
        tokenize=create_tokenizer_function("nomic-embed-text")
    )
    TEXT_EMBEDDING_3_SMALL = EmbeddingInfo(
        model="text-embedding-3-small",
        dim=1536,
        cost_per_token=0.02e-6,
        sync_embed=create_sync_embed_function("openai"),
        async_embed=create_async_embed_function("openai"),
        tokenize=create_tokenizer_function("text-embedding-3-small"),
        rpm_limit=3000,
        tpm_limit=1e6
    )
    TEXT_EMBEDDING_3_LARGE = EmbeddingInfo(
        model="text-embedding-3-large",
        dim=3072,
        cost_per_token=0.13e-6,
        sync_embed=create_sync_embed_function("openai"),
        async_embed=create_async_embed_function("openai"),
        tokenize=create_tokenizer_function("text-embedding-3-large"),
        rpm_limit=3000,
        tpm_limit=1e6
    )

    def __init__(self, info: EmbeddingInfo):
        """Initialize the enum with embedding info and unpack fields for direct access.

        Args:
        - info (EmbeddingInfo): Complete embedding model information
        """
        self.info = info
        self.model = info.model
        self.dim = info.dim
        self.cost_per_token = info.cost_per_token
        self.sync_embed = info.sync_embed
        self.async_embed = info.async_embed
        self.tokenize = info.tokenize
        self.rpm_limit = info.rpm_limit
        self.tpm_limit = info.tpm_limit


class CountLimiter(Generic[T]):
    """Generic counter limiter for tracking usage with configurable limits.

    Provides thread-safe counting with both synchronous and asynchronous increment methods.

    Attr:
    - limit (T): Maximum allowed value
    - counter_lock (asyncio.Lock): Lock for thread-safe async operations
    - counter (T): Current counter value
    """
    def __init__(self, *, limit: T) -> None:
        """Initialize the counter limiter with a maximum limit.

        Args:
        - limit (T): Maximum allowed counter value
        """
        self.limit: T = limit
        self.counter_lock = asyncio.Lock()
        self.counter: T = 0

    def increment_sync(self, inc: T):
        """Increment the counter synchronously if within limit.

        Args:
        - inc (T): Amount to increment

        Raises:
        - ValueError: If incrementing would exceed the limit
        """
        new_counter = self.counter + inc
        if new_counter > self.limit:
            raise ValueError(
                f"Increment of {inc} would exceed limit of {self.limit} (current: {self.counter})"
            )
        self.counter = new_counter

    async def increment_async(self, inc: T):
        """Increment the counter asynchronously if within limit.

        Args:
        - inc (T): Amount to increment

        Raises:
        - ValueError: If incrementing would exceed the limit
        """
        async with self.counter_lock:
            new_counter = self.counter + inc
            if new_counter > self.limit:
                raise ValueError(
                    f"Async increment of {inc} would exceed "
                    f"limit of {self.limit} (current: {self.counter})"
                )
            self.counter = new_counter

    @property
    def remaining(self) -> T:
        """Get the remaining balance before hitting the limit.

        Returns:
        - (T): Remaining balance (limit - current counter)
        """
        return self.limit - self.counter

    @property
    def used(self) -> T:
        """Get the current used amount.

        Returns:
        - (T): Current counter value
        """
        return self.counter


class Embedder:
    """Main embedding class that handles text chunk embedding with rate limiting.

    Attr:
    - config (EmbedderConfig): Configuration for rate limits and constraints
    - embedding (SupportedEmbedding): The embedding model to use
    """
    def __init__(self, embedder_config: EmbedderConfig, embedding: SupportedEmbedding):
        """Initialize the embedder with configuration and embedding model.

        Args:
        - embedder_config (EmbedderConfig): Rate limiting and usage configuration
        - embedding (SupportedEmbedding): The embedding model to use
        """
        self.config = embedder_config or EmbedderConfig()
        self.embedding = embedding

    def count_tokens(self, chunks: list[str]) -> int:
        """Count the total tokens across all text chunks.

        Args:
        - chunks (list[str]): List of text chunks to tokenize

        Returns:
        - (int): Total number of tokens across all chunks
        """
        total_tokens = 0
        for chunk in chunks:
            total_tokens += len(self.embedding.tokenize(chunk))
        return total_tokens

    def embed_sync(self, chunks: list[str]) -> EmbeddingResult:
        """Embed text chunks synchronously with rate limiting and cost tracking.

        Args:
        - chunks (list[str]): List of text chunks to embed

        Returns:
        - (EmbeddingResult): Result containing embeddings or failure status

        Raises:
        - ValueError: If sync embedding is not supported, tokenizer is missing,
            or cost estimation fails
        """
        if self.embedding.sync_embed is None:
            raise ValueError(
                f"Model '{self.embedding.model}' does not support synchronous embedding"
            )
        if (self.config.tok_limiter or self.config.usd_limiter) and not self.embedding.tokenize:
            raise ValueError(
                "Token or USD limiting enabled but no tokenizer function attached to "
                f"model '{self.embedding.model}'"
            )
        if self.config.usd_limiter and self.embedding.cost_per_token is None:
            raise ValueError(
                "USD limiting enabled but no cost_per_token specified for model "
                f"'{self.embedding.model}'"
            )
        total_tokens: Optional[int|float] = None
        if self.config.tok_limiter or self.config.usd_limiter:
            total_tokens = self.count_tokens(chunks)
        if self.config.tok_limiter:
            try:
                self.config.tok_limiter.increment_sync(total_tokens)
            except Exception:
                return EmbeddingResult.failure()
        if self.config.usd_limiter:
            total_cost = total_tokens * self.embedding.cost_per_token
            try:
                self.config.usd_limiter.increment_sync(total_cost)
            except Exception:
                return EmbeddingResult.failure()
        return self.embedding.sync_embed(self.embedding.model , chunks)


class EmbedderManager:
    """Async context manager for Embedder resources and usage tracking.

    Provides convenient access to usage statistics and manages embedder lifecycle.

    Attr:
    - embedder_config (EmbedderConfig): Configuration for the embedder
    - embedding (SupportedEmbedding): The embedding model
    - embedder (Embedder): The managed embedder instance
    """
    def __init__(self, embedder_config: EmbedderConfig, embedding: SupportedEmbedding):
        """Initialize the embedder manager.

        Args:
        - embedder_config (EmbedderConfig): Configuration for rate limits and constraints
        - embedding (SupportedEmbedding): The embedding model to use
        """
        self.embedder_config = embedder_config
        self.embedding = embedding
        self.embedder = Embedder(embedder_config, embedding)

    @property
    def remaining_usd(self) -> float:
        """Get remaining USD balance.

        Returns:
        - (float): Remaining USD balance

        Raises:
        - ValueError: If no USD limiter is configured
        """
        if self.embedder_config.usd_limiter is None:
            raise ValueError("Cannot check USD balance without a configured USD limiter")
        return self.embedder_config.usd_limiter.remaining

    @property
    def used_usd(self) -> float:
        """Get used USD amount.

        Returns:
        - (float): Amount of USD used

        Raises:
        - ValueError: If no USD limiter is configured
        """
        if self.embedder_config.usd_limiter is None:
            raise ValueError("Cannot check used USD amount without a configured USD limiter")
        return self.embedder_config.usd_limiter.used

    @property
    def remaining_tokens(self) -> float:
        """Get remaining token balance.

        Returns:
        - (float): Remaining token balance

        Raises:
        - ValueError: If no token limiter is configured
        """
        if self.embedder_config.tok_limiter is None:
            raise ValueError("Cannot check remaining tokens without a configured token limiter")
        return self.embedder_config.tok_limiter.remaining

    @property
    def used_tokens(self) -> float:
        """Get used token count.

        Returns:
        - (float): Number of tokens used

        Raises:
        - ValueError: If no token limiter is configured
        """
        if self.embedder_config.tok_limiter is None:
            raise ValueError("Cannot check used tokens without a configured token limiter")
        return self.embedder_config.tok_limiter.used

    async def __aenter__(self):
        """Enter the async context and return the embedder.

        Returns:
        - (Embedder): The managed embedder instance
        """
        return self.embedder

    async def __aexit__(self, exc_type, exc, tb):
        """Exit the async context with no cleanup required.

        Args:
        - exc_type: Exception type if any
        - exc: Exception instance if any  
        - tb: Traceback if any

        Returns:
        - (bool): False to not suppress exceptions
        """
        _ = exc_type, exc, tb
        return False


async def main():
    """Run the main test demonstrating embedder usage.

    Creates an embedder with limits, processes text chunks,
    and displays results and usage statistics.
    
    Raises:
    - ValueError: If embedding operation fails
    """
    config = EmbedderConfig(
        tok_limiter=CountLimiter(limit=6),
        usd_limiter=CountLimiter(limit=1.0)
    )
    embedding_context = EmbedderManager(config, SupportedEmbedding.NOMIC_EMBED_TEXT)
    async with embedding_context as embedder:
        embedding_result = embedder.embed_sync(["Hello", "world"])
        if not embedding_result.ok:
            raise ValueError(
                "Embedding operation failed - check rate limits and model availability"
            )
        for embedding in embedding_result.output:
            print(f"{embedding[0:4]}") #< print the first 4 values
        print(f'{embedding_result.usage!r}')
    print(embedding_context.remaining_tokens)
    print(embedding_context.remaining_usd)


if __name__ == "__main__":
    asyncio.run(main())
