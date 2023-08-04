from dataclasses import dataclass
from typing import Optional

@dataclass
class ExecutionOptions:
    """Execution options for a query.

    Attributes
    ----------
    row_limit : Optional[int]
        The maximum number of rows to return.
        If not specified (the default), all rows are returned.
    """

    row_limit: Optional[int] = None