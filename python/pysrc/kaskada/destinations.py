"""Destinations for writing query results directly to external stores."""


class Destination(object):
    """Base class for destinations."""


class ParquetDestination(Destination):
    """Destination for Parquet files."""

    def __init__(self, path: str):
        """Construct a new ParquetDestination.

        Args:
            path: The path to the directory to write Parquet files.
        """
        raise NotImplementedError
