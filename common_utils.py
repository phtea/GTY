import re


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
