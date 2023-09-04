"""Defines Kaskada Timestreams."""

from __future__ import annotations

# this must be imported first to avoid circular imports
from ._timestream import Timestream, Arg, LiteralValue, record, Literal

# after Timestream is initialized, add all the methods
from ._aggregation import collect, count, count_if, first, last, max, mean, min, stddev, sum, variance
from ._arithmetic import add, add__, addr__, ceil, clamp, div, div__, divr__, exp, floor, greatest, least, mul, mul__, mulr__, neg, powf, round, sqrt, sub, sub__, subr__
from ._collection import getitem__, flatten, index, length, union
from ._comparison import eq, ge, ge__, gt, gt__, le, le__, lt, lt__, ne, is_not_null, is_null
from ._execution import preview, to_pandas, run_iter, write
from ._grouping import lookup, with_key
from ._logical import and_, or_, not_
from ._misc import cast, coalesce, else_, filter, hash, if_, lag, null_if, pipe
from ._records import col, extend, _record, remove, select
from ._string import lower, upper
from ._time import shift_by, shift_to, shift_until, time, seconds_since, seconds_since_previous

# aggregation
Timestream.collect = collect
Timestream.count = count
Timestream.count_if = count_if
Timestream.first = first
Timestream.last = last
Timestream.max = max
Timestream.mean = mean
Timestream.min = min
Timestream.stddev = stddev
Timestream.sum = sum
Timestream.variance = variance

# arithmetic
Timestream.add = add
Timestream.__add__ = add__
Timestream.__radd__ = addr__
Timestream.ceil = ceil
Timestream.clamp = clamp
Timestream.div = div
Timestream.__truediv__ = div__
Timestream.__rtruediv__ = divr__
Timestream.exp = exp
Timestream.floor = floor
Timestream.greatest = greatest
Timestream.least = least
Timestream.mul = mul
Timestream.__mul__ = mul__
Timestream.__rmul__ = mulr__
Timestream.neg = neg
Timestream.powf = powf
Timestream.round = round
Timestream.sqrt = sqrt
Timestream.sub = sub
Timestream.__sub__ = sub__
Timestream.__rsub__ = subr__

# collection
Timestream.__getitem__ = getitem__
Timestream.flatten = flatten
Timestream.index = index
Timestream.length = length
Timestream.union = union

# comparison
Timestream.eq = eq
Timestream.ge = ge
Timestream.__ge__ = ge__
Timestream.gt = gt
Timestream.__gt__ = gt__
Timestream.le = le
Timestream.__le__ = le__
Timestream.lt = lt
Timestream.__lt__ = lt__
Timestream.ne = ne
Timestream.is_not_null = is_not_null
Timestream.is_null = is_null

# execution
Timestream.preview = preview
Timestream.to_pandas = to_pandas
Timestream.run_iter = run_iter
Timestream.write = write

# grouping
Timestream.lookup = lookup
Timestream.with_key = with_key

# logical
Timestream.and_ = and_
Timestream.or_ = or_
Timestream.not_ = not_

# misc
Timestream.cast = cast
Timestream.coalesce = coalesce
Timestream.else_ = else_
Timestream.filter = filter
Timestream.hash = hash
Timestream.if_ = if_
Timestream.lag = lag
Timestream.null_if = null_if
Timestream.pipe = pipe

# records
Timestream.col = col
Timestream.extend = extend
Timestream.record = _record
Timestream.remove = remove
Timestream.select = select

# string
Timestream.lower = lower
Timestream.upper = upper

# time
Timestream.shift_by = shift_by
Timestream.shift_to = shift_to
Timestream.shift_until = shift_until
Timestream.time = time
Timestream.seconds_since = seconds_since
Timestream.seconds_since_previous = seconds_since_previous
