from typing import Callable, List, Literal, Optional, Sequence

import pyarrow as pa

from ._execution import _ExecutionOptions

class Session:
    def __init__(self) -> None: ...

class Execution(object):
    def schema(self) -> pa.Schema: ...
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
    @staticmethod
    def literal_timedelta(
        session: Session,
        s: int,
        ns: int,
    ) -> Expr: ...
    def cast(self, data_type: pa.DataType) -> Expr: ...
    def data_type(self) -> pa.DataType: ...
    def is_continuous(self) -> bool: ...
    def session(self) -> Session: ...
    def execute(self, options: Optional[_ExecutionOptions] = None) -> Execution: ...
    def grouping(self) -> Optional[str]: ...
    def plan(
        self,
        kind: Literal["initial_dfg", "final_dfg", "final_plan"],
        options: Optional[_ExecutionOptions],
    ) -> str: ...

class Table:
    def __init__(
        self,
        session: Session,
        name: str,
        time_column: str,
        key_column: str,
        schema: pa.Schema,
        queryable: bool,
        subsort_column: Optional[str],
        grouping_name: Optional[str],
        time_unit: Optional[str],
        source: Optional[str],
    ) -> None: ...
    @property
    def name(self) -> str: ...
    def expr(self) -> Expr: ...
    async def add_pyarrow(self, data: pa.RecordBatch) -> None: ...
    async def add_parquet(self, path: str) -> None: ...

class Udf(object):
    def __init__(self, result_ty: str, result_fn: Callable[..., pa.Array]) -> None: ...
