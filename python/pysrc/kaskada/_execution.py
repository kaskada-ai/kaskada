from dataclasses import dataclass
from typing import AsyncIterator, Callable, Iterator, Optional, TypeVar

import pyarrow as pa

from . import _ffi


T = TypeVar("T")


@dataclass
class _ExecutionOptions:
    """Execution options passed to the FFI layer.

    Attributes:
        row_limit: The maximum number of rows to return. If not specified, all rows are returned.
        max_batch_size: The maximum batch size to use when returning results.
          If not specified, the default batch size will be used.
        materialize: If true, the query will be a continuous materialization.
    """

    row_limit: Optional[int] = None
    max_batch_size: Optional[int] = None
    materialize: bool = False


class Execution:
    """Represents an execution of a TimeStream."""

    def __init__(self, ffi_execution: _ffi.Execution) -> None:
        """Create the execution for a given FFI call."""
        self._ffi_execution = ffi_execution

    def stop(self):
        """Stop or cancel the execution.

        After this call, the execution will no longer produce results.
        """
        self._ffi_execution.stop()


class ResultIterator(Execution, Iterator[T], AsyncIterator[T]):
    """An iterator over results from the TimeStream."""

    _items: Iterator[T]

    def __init__(
        self, ffi_execution: _ffi.Execution, f: Callable[[pa.Table], Iterator[T]]
    ) -> None:
        """Create the execution for a given FFI call."""
        super().__init__(ffi_execution)
        self._f = f
        self._items = iter(())

    def __iter__(self):
        """Return a synchronous iterator over results."""
        return self

    def __aiter__(self):
        """Return an asynchronous iterator over results."""
        return self

    def __next__(self) -> T:
        """Return the next item synchronously."""
        try:
            return next(self._items)
        except StopIteration:
            pass

        while True:
            next_batch = self._ffi_execution.next_pyarrow()
            if next_batch is None:
                raise StopIteration

            # Annoyingly, PyArrow doesn't suport `drop_columns` on batches.
            # So we need to convert to a Table (even if we're producing batches).
            table = pa.Table.from_batches([next_batch])
            table = table.drop_columns(["_subsort", "_key_hash"])
            self._items = self._f(table)
            try:
                return next(self._items)
            except StopIteration:
                continue

    async def __anext__(self) -> T:
        """Return the next item asynchronously."""
        try:
            return next(self._items)
        except StopIteration:
            pass

        while True:
            next_batch = await self._ffi_execution.next_pyarrow_async()
            if next_batch is None:
                raise StopAsyncIteration

            # Annoyingly, PyArrow doesn't suport `drop_columns` on batches.
            # So we need to convert to a Table (even if we're producing batches).
            table = pa.Table.from_batches([next_batch])
            table = table.drop_columns(["_subsort", "_key_hash"])
            self._items = self._f(table)

            try:
                return next(self._items)
            except StopIteration:
                continue
