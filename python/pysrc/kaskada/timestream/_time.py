from __future__ import annotations

from datetime import datetime, timedelta

import pyarrow as pa

from . import Timestream, Arg

def shift_by(self, delta: Arg) -> Timestream:
    """Return a Timestream shifting each point forward by the `delta`.

    If multiple values are shifted to the same time, they will be emitted in
    the order in which they originally occurred.

    Args:
        delta: The delta to shift the point forward by.
    """
    if isinstance(delta, timedelta):
        session = self._ffi_expr.session()
        seconds = Timestream._call(
            "seconds", int(delta.total_seconds()), session=session
        )
        return Timestream._call("shift_by", seconds, self, input=self)
    else:
        return Timestream._call("shift_by", delta, self, input=self)

def shift_to(self, time: Arg) -> Timestream:
    """Return a Timestream shifting each point forward to `time`.

    If multiple values are shifted to the same time, they will be emitted in
    the order in which they originally occurred.

    Args:
        time: The time to shift to. This must be a datetime or a Timestream of timestamp_ns.

    Raises:
        NotImplementedError: When `time` is a datetime (shift_to literal not yet implemented).
    """
    if isinstance(time, datetime):
        # session = self._ffi_expr.session()
        # time_ns = time.timestamp() * 1e9
        # time_ns = Timestream._literal(time_ns, session=session)
        # time_ns = Timestream.cast(time_ns, pa.timestamp('ns'))
        # return Timestream._call("shift_to", time_ns, self)
        raise NotImplementedError("shift_to with datetime literal unsupported")
    else:
        return Timestream._call("shift_to", time, self, input=self)

def shift_until(self, predicate: Arg) -> Timestream:
    """Return a Timestream shifting each point forward to the next time `predicate` is true.

    Note that if the `predicate` evaluates to true at the same time as `self`,
    the point will be emitted at that time.

    If multiple values are shifted to the same time, they will be emitted in
    the order in which they originally occurred.

    Args:
        predicate: The predicate to determine whether to emit shifted rows.
    """
    return Timestream._call("shift_until", predicate, self, input=self)

def time(self) -> Timestream:
    """Return a Timestream containing the time of each point."""
    return Timestream._call("time_of", self)

def seconds_since(self, time: Arg) -> Timestream:
    """Return a Timestream containing seconds between `time` and `self`.

    If `self.time()` is greater than `time`, the result will be positive.

    Args:
        time: The time to compute the seconds since.

            This can be either a stream of timestamps or a datetime literal.
            If `time` is a Timestream, the result will contain the seconds
            from `self.time()` to `time.time()` for each point.
    """
    if isinstance(time, datetime):
        session = self._ffi_expr.session()
        nanos = Timestream._literal(time.timestamp() * 1e9, session=session)
        nanos = Timestream.cast(nanos, pa.timestamp("ns", None))
        return Timestream._call("seconds_between", nanos, self)
    else:
        return Timestream._call("seconds_between", time, self)

def seconds_since_previous(self, n: int = 1) -> Timestream:
    """Return a Timestream containing seconds between `self` and the time `n` points ago.

    Args:
        n: The number of points to look back. For example, `n=1` refers to
            the previous point.

            Defaults to 1 (the previous point).
    """
    time_of_current = Timestream._call("time_of", self).cast(pa.int64())
    time_of_previous = Timestream._call("time_of", self).lag(n).cast(pa.int64())

    # `time_of` returns nanoseconds, so divide to get seconds
    return time_of_current.sub(time_of_previous).div(1e9).cast(pa.duration("s"))
