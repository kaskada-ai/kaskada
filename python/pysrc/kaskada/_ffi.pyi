from typing import Callable
from typing import List
from typing import Optional
from typing import Sequence

import pyarrow as pa

from ._execution import ExecutionOptions

class Session:
    def __init__(self) -> None: ...

class Execution(object):
    def collect_pyarrow(self) -> List[pa.RecordBatch]: ...
    def next_pyarrow(self) -> Optional[pa.RecordBatch]: ...
    def stop(self) -> None: ...
    async def next_pyarrow_async(self) -> Optional[pa.RecordBatch]: ...

class Expr:
    @staticmethod
    def call(
        session: Session,
        operation: str,
        args: Sequence[Expr],
    ) -> Expr: ...
    @staticmethod
    def call_udf(
        session: Session,
        udf: Udf,
        args: Sequence[Expr],
    ) -> Expr: ...
    @staticmethod
    def literal(
        session: Session,
        value: int | float | str | None,
    ) -> Expr: ...
    def cast(self, data_type: pa.DataType) -> Expr: ...
    def data_type(self) -> pa.DataType: ...
    def is_continuous(self) -> bool: ...
    def session(self) -> Session: ...
    def execute(self, options: Optional[ExecutionOptions] = None) -> Execution: ...
    def grouping(self) -> Optional[str]: ...

class Table(Expr):
    def __init__(
        self,
        session: Session,
        name: str,
        time_column: str,
        key_column: str,
        schema: pa.Schema,
        subsort_column: Optional[str],
        grouping_name: Optional[str],
        time_unit: Optional[str],
    ) -> None: ...
    @property
    def name(self) -> str: ...
    def add_pyarrow(self, data: pa.RecordBatch) -> None: ...

class Udf(object):
    def __init__(self, result_ty: str, result_fn: Callable[..., pa.Array]) -> None: ...
