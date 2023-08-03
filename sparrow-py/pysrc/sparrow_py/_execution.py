from dataclasses import dataclass
from typing import Optional


@dataclass
class ExecutionOptions:
    """Execution options for a query.

    Attributes
    ----------
    row_limit : Optional[int]
        The maximum number of rows to return.
        If not specified, all rows are returned.

    max_batch_size : Optional[int]
        The maximum batch size to use when returning results.
        If not specified, the default batch size will be used.
    """

    row_limit: Optional[int] = None
    max_batch_size: Optional[int] = None
