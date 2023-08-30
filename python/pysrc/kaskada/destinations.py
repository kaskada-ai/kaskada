class Destination(object):
    """Base class for destinations."""
    pass

class ParquetDestination(Destination):
    """Destination for Parquet files."""
    def __init__(self, path: str):
        raise NotImplementedError