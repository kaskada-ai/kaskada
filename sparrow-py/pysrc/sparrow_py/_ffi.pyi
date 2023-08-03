from typing import Optional
from typing import Sequence
from typing import List

import pyarrow as pa
from sparrow_py.udf import Udf

class Session:
    def __init__(self) -> None: ...

class ExecutionOptions(object):
    def __init__(self, row_limit: Optional[int] = None) -> None: ...
    row_limit: Optional[int]

class Execution(object):
    def collect_pyarrow(self) -> List[pa.RecordBatch]: ...

class Expr:
    def __init__(
        self,
        session: Session,
        operation: str,
        args: Sequence[Expr | int | float | str | None],
    ) -> None: ...
    def data_type(self) -> pa.DataType: ...
    def data_type_string(self) -> str: ...
    def equivalent(self, other: Expr) -> bool: ...
    def session(self) -> Session: ...
    def execute(self, options: Optional[ExecutionOptions] = None) -> Execution: ...

def call_udf(udf: Udf, result_type: pa.DataType, *args: pa.Array) -> pa.Array: ...

class Table(Expr):
    def __init__(
        self,
        session: Session,
        name: str,
        time_column_name: str,
        key_column_name: str,
        schema: pa.Schema,
        subsort_column_name: Optional[str],
        grouping_name: Optional[str],
    ) -> None: ...
    @property
    def name(self) -> str: ...
    def add_pyarrow(self, data: pa.RecordBatch) -> None: ...