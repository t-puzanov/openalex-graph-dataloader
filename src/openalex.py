"""Helpers for working with OpenAlex entity IDs.

OpenAlex IDs look like ``https://openalex.org/W2741610723`` or
``https://openalex.org/T10064``. Parsing them by URL structure is more
robust than relying on a magic slice offset.
"""


def extract_numeric_id(url):
    """Return the numeric part of an OpenAlex ID URL as an int."""
    return str(url).rsplit("/", 1)[-1][1:]
