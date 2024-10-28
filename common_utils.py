import re
import time
from functools import wraps


def normalize_department_name(
        department_name: str
) -> str:
    """
    Normalizes a department name by replacing non-breaking spaces 
    and multiple consecutive spaces with a single space.

    :param department_name: The department name to normalize.
    :return: A normalized department name with single spaces.
    """
    return re.sub(r'\s+', ' ', department_name.replace('\u00A0', ' ')).strip()


def async_timeit(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = await func(*args, **kwargs)
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        print(f"{func.__name__} executed in {elapsed_time:.4f} seconds")
        return result
    return wrapper
