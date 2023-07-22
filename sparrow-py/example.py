"""Simple script for testing error handling."""

from sparrow_py import Expr
from sparrow_py.math import add
from sparrow_py.math import typeerror

foo = Expr('foo')
bar = Expr('bar')


print(str(foo['pipe']))

# foo.pipe(typeerror, bar)


foo + typeerror(foo, bar)