from __future__ import annotations

from typing import (
    Optional,
    Union,
)

from . import Timestream, LiteralValue, record

import kaskada as kd
import pyarrow as pa

def _aggregation(
    op: str,
    input: Timestream,
    window: Optional[kd.windows.Window],
    *args: Union[Timestream, LiteralValue],
) -> Timestream:
    """Return the aggregation `op` with the given `input`, `window` and `args`.

    Args:
    op: The operation to create.
    input: The input to the aggregation.
    window: The window to use for the aggregation.
    *args: Additional arguments to provide after `input` and before the flattened window.

    Raises:
        NotImplementedError: If the window is not a known type.
    """
    if window is None:
        return Timestream._call(op, input, *args, None, None)
    elif isinstance(window, kd.windows.Since):
        predicate = window.predicate
        if callable(predicate):
            predicate = predicate(input)
        return Timestream._call(op, input, *args, predicate, None)
    elif isinstance(window, kd.windows.Sliding):
        predicate = window.predicate
        if callable(predicate):
            predicate = predicate(input)
        return Timestream._call(op, input, *args, predicate, window.duration)
    elif isinstance(window, kd.windows.Trailing):
        if op != "collect":
            raise NotImplementedError(
                f"Aggregation '{op} does not support trailing windows"
            )

        trailing_ns = int(window.duration.total_seconds() * 1e9)

        # Create the shifted-forward input
        input_shift = input.shift_by(window.duration)

        # Merge, then extract just the input.
        #
        # Note: Assumes the "input" is discrete. Can probably
        # use a transform to make it discrete (eg., `input.filter(True)`)
        # or a special function to do that.
        #
        # HACK: This places an extra null row in the input to `collect`
        # which allows us to "clear" the window when the appropriate
        # `duration` has passed with no "real" inputs.
        merged_input = record({"input": input, "shift": input_shift}).col("input")
        return Timestream._call("collect", merged_input, *args, None, trailing_ns)
    else:
        raise NotImplementedError(f"Unknown window type {window!r}")

def collect(
    self,
    *,
    max: Optional[int],
    min: Optional[int] = 0,
    window: Optional[kd.windows.Window] = None,
) -> Timestream:
    """Return a Timestream collecting up to the last `max` values in the `window`.

    Collects the values for each key separately.

    Args:
        max: The maximum number of values to collect.
            If `None` all values are collected.
        min: The minimum number of values to collect before producing a value.
            Defaults to 0.
        window: The window to use for the aggregation. If not specified,
            the entire Timestream is used.

    Returns:
        A Timestream containing the list of collected elements at each point.
    """
    if pa.types.is_list(self.data_type):
        return (
            record({"value": self})
            .collect(max=max, min=min, window=window)
            .col("value")
        )
    else:
        return _aggregation("collect", self, window, max, min)

def count(self, window: Optional[kd.windows.Window] = None) -> Timestream:
    """Return a Timestream containing the count value in the `window`.

    Computed for each key separately.

    Args:
        window: The window to use for the aggregation. Defaults to the entire Timestream.
    """
    return _aggregation("count", self, window)

def count_if(self, window: Optional[kd.windows.Window] = None) -> Timestream:
    """Return a Timestream containing the count of `true` values in `window`.

    Computed for each key separately.

    Args:
        window: The window to use for the aggregation. Defaults to the entire Timestream.
    """
    return _aggregation("count_if", self, window)

def first(self, *, window: Optional[kd.windows.Window] = None) -> Timestream:
    """Return a Timestream containing the first value in the `window`.

    Computed for each key separately.

    Args:
        window: The window to use for the aggregation. Defaults to the entire Timestream.
    """
    return _aggregation("first", self, window)

def last(self, window: Optional[kd.windows.Window] = None) -> Timestream:
    """Return a Timestream containing the last value in the `window`.

    Computed for each key separately.

    Args:
        window: The window to use for the aggregation. Defaults to the entire Timestream.
    """
    return _aggregation("last", self, window)

def max(self, window: Optional[kd.windows.Window] = None) -> Timestream:
    """Return a Timestream containing the max value in the `window`.

    Computed for each key separately.

    Args:
        window: The window to use for the aggregation. Defaults to the entire Timestream.

    See Also:
        This returns the maximum of values in a column. See
        :func:`greatest` to get the maximum value
        between Timestreams at each point.
    """
    return _aggregation("max", self, window)

def mean(self, window: Optional[kd.windows.Window] = None) -> Timestream:
    """Return a Timestream containing the mean value in the `window`.

    Computed for each key separately.

    Args:
        window: The window to use for the aggregation. Defaults to the entire Timestream.
    """
    return _aggregation("mean", self, window)

def min(self, window: Optional[kd.windows.Window] = None) -> Timestream:
    """Return a Timestream containing the min value in the `window`.

    Computed for each key separately.

    Args:
        window: The window to use for the aggregation. Defaults to the entire Timestream.

    See Also:
        This returns the minimum of values in a column. See
        :func:`least` to get the minimum value
        between Timestreams at each point.
    """
    return _aggregation("min", self, window)

def stddev(self, window: Optional[kd.windows.Window] = None) -> Timestream:
    """Return a Timestream containing the standard deviation in the `window`.

    Computed for each key separately.

    Args:
        window: The window to use for the aggregation. Defaults to the entire Timestream.
    """
    return _aggregation("stddev", self, window)

def sum(self, *, window: Optional[kd.windows.Window] = None) -> Timestream:
    """Return a Timestream summing the values in the `window`.

    Computes the sum for each key separately.

    Args:
        window: The window to use for the aggregation. Defaults to the entire Timestream.
    """
    return _aggregation("sum", self, window)

def variance(self, window: Optional[kd.windows.Window] = None) -> Timestream:
    """Return a Timestream containing the variance in the `window`.

    Computed for each key separately.

    Args:
        window: The window to use for the aggregation. Defaults to the entire Timestream.
    """
    return _aggregation("variance", self, window)
