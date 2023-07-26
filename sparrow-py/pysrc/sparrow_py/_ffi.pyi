from typing import Optional
from typing import Sequence

import pyarrow as pa
from sparrow_py.udf import Udf

class Session:
    def __init__(self) -> None: ...
    def add_table(
        self,
        name: str,
        time_column_name: str,
        key_column_name: str,
        schema: pa.Schema,
        subsort_column_name: Optional[str],
        grouping_name: Optional[str],
    ) -> Expr: ...

class Expr:
    def __init__(
        self, session: Session, operation: str, args: Sequence[Expr | int | float | str]
    ) -> None: ...
    def data_type(self) -> pa.DataType: ...
    def data_type_string(self) -> str: ...
    def equivalent(self, other: Expr) -> bool: ...
    def session(self) -> Session: ...

def call_udf(udf: Udf, result_type: pa.DataType, *args: pa.Array) -> pa.Array: ...
