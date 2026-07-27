import asyncio
import functools


def async_timeout_retry(retries=2, timeout=1.0):
    def deco(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):

            for i in range(retries):
                try:
                    # Exit immediately on Success
                    coroutine = func(*args, **kwargs)
                    res = await asyncio.wait_for(coroutine, timeout=timeout)

                    if res is not None:
                        return res

                except asyncio.TimeoutError as te:
                    if i < retries - 1:
                        print(
                            f"Coroutine {func.__name__} failed to perform in {timeout} sec. Start over . . ."
                        )

            print(
                f"Coroutine {func.__name__} failed to perform in {timeout} sec {retries} \
times. Escaping with None"
            )
            return None

        return wrapper

    return deco
