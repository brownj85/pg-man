import random
import string

_POPULATION = string.ascii_lowercase + string.digits


def short_uid(n: int = 8) -> str:
    """Generate a random string of length ``n``
    from the lowercase ascii letters and digits 0-9."""
    return "".join(random.choices(_POPULATION, k=n))
