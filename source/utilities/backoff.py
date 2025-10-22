import asyncio
import random
from functools import wraps
from typing import Callable, Coroutine, Any, Optional, TypeAlias

async_cb: TypeAlias = Callable[[], Coroutine[Any, Any, None]]

def async_exponential_backoff_with_jitter(
        max_retries: int=5, 
        base_delay: int=1, 
        max_delay: int=60, 
        callback_on_retry: Optional[async_cb]=None
    ):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except:
                    if attempt == max_retries - 1:
                        raise
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    jittered_delay = delay * (0.5 + random.random() * 0.5) # 50-100% of delay
                    await asyncio.sleep(jittered_delay)
                    if callback_on_retry:
                        await callback_on_retry()
        return wrapper
    return decorator