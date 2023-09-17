"""Windows to use for Timestream aggregations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Callable

from ._timestream import Timestream


@dataclass(frozen=True)
class Window(object):
    """Base class for window functions."""


@dataclass(frozen=True)
class Since(Window):
    """Since windows are a series of non-overlapping windows that reset each time a predicate evaluates to true.

    Aggregations will contain all values starting from the last time the predicate
    evaluated to true (inclusive).

    Aggregations will produce a new value for each input value and additionally when
    the current window closes.

    Since windows are analogous to Tumbling windows, aside from the output behavior.
    """

    #: The boolean Timestream to use as predicate for the window.
    #: Each time the predicate evaluates to true the window will be cleared.
    #:
    #: The predicate may be a callable which returns the boolean Timestream, in
    #: which case it is applied to the Timestream being aggregated.
    predicate: Timestream | Callable[..., Timestream] | bool

    @staticmethod
    def minutely() -> Since:
        """Return a window since the start of each minute."""
        return Since(predicate=lambda domain: Timestream._call("minutely", domain))

    @staticmethod
    def hourly() -> Since:
        """Return a window since the start of each hour."""
        return Since(predicate=lambda domain: Timestream._call("hourly", domain))

    @staticmethod
    def daily() -> Since:
        """Return a window since the start of each day."""
        return Since(predicate=lambda domain: Timestream._call("daily", domain))

    @staticmethod
    def monthly() -> Since:
        """Return a window since the start of each month."""
        return Since(predicate=lambda domain: Timestream._call("monthly", domain))

    @staticmethod
    def yearly() -> Since:
        """Return a window since the start of each year."""
        return Since(predicate=lambda domain: Timestream._call("yearly", domain))


@dataclass(frozen=True)
class Tumbling(Window):
    """Tumbling windows are a series of non-overlapping windows that reset each time a predicate evaluates to true.

    Aggregations will contain all values starting from the last time the predicate
    evaluated to true (inclusive).

    Aggregations will produce only produce a value when the current window closes.

    Tumbling windows are analogous to Since windows, aside from the output behavior.
    """

    #: The boolean Timestream to use as predicate for the window.
    #: Each time the predicate evaluates to true the window will be cleared.
    #:
    #: The predicate may be a callable which returns the boolean Timestream, in
    #: which case it is applied to the Timestream being aggregated.
    predicate: Timestream | Callable[..., Timestream] | bool

    @staticmethod
    def minutely() -> Tumbling:
        """Return a tumbling window that resets at the start of each minute."""
        return Tumbling(predicate=lambda domain: Timestream._call("minutely", domain))

    @staticmethod
    def hourly() -> Tumbling:
        """Return a tumbling window that resets at the start of each hour."""
        return Tumbling(predicate=lambda domain: Timestream._call("hourly", domain))

    @staticmethod
    def daily() -> Tumbling:
        """Return a tumbling window that resets at the start of each day."""
        return Tumbling(predicate=lambda domain: Timestream._call("daily", domain))

    @staticmethod
    def monthly() -> Tumbling:
        """Return a tumbling window that resets at the start of each month."""
        return Tumbling(predicate=lambda domain: Timestream._call("monthly", domain))

    @staticmethod
    def yearly() -> Tumbling:
        """Return a tumbling window that resets at the start of each year."""
        return Tumbling(predicate=lambda domain: Timestream._call("yearly", domain))


@dataclass(frozen=True)
class Sliding(Window):
    """Sliding windows are a series of overlapping windows that reset each time a predicate evaluates to true.

    The `duration` is the number of active windows at any given time.

    Aggregations will produce a new value for each input value and additionally when
    the current window closes.
    """

    #: The number of sliding intervals to use in the window.
    duration: int
    #: The boolean Timestream to use as a predicate for the window.
    #: Each time the predicate evaluates to true the window starts a new interval.
    #:
    #: The predicate may be a callable which returns the boolean Timestream, in
    #: which case it is applied to the Timestream being aggregated.
    predicate: Timestream | Callable[..., Timestream] | bool

    def __post_init__(self):
        """Validate the window parameters."""
        if self.duration <= 0:
            raise ValueError("duration must be positive")

    @staticmethod
    def minutely(duration: int) -> Sliding:
        """Return a sliding window containing `duration` minutes.

        Args:
            duration: The number of minutes to use in the window.
        """
        return Sliding(
            duration=duration,
            predicate=lambda domain: Timestream._call("minutely", domain),
        )

    @staticmethod
    def hourly(duration: int) -> Sliding:
        """Return a sliding window containing `duration` hours.

        Args:
            duration: The number of hours to use in the window.
        """
        return Sliding(
            duration=duration,
            predicate=lambda domain: Timestream._call("hourly", domain),
        )

    @staticmethod
    def daily(duration: int) -> Sliding:
        """Return a sliding window containing `duration` daily.

        Args:
            duration: The number of days to use in the window.
        """
        return Sliding(
            duration=duration,
            predicate=lambda domain: Timestream._call("daily", domain),
        )

    @staticmethod
    def monthly(duration: int) -> Sliding:
        """Return a sliding window containing `duration` months.

        Args:
            duration: The number of months to use in the window.
        """
        return Sliding(
            duration=duration,
            predicate=lambda domain: Timestream._call("monthly", domain),
        )

    @staticmethod
    def yearly(duration: int) -> Sliding:
        """Return a sliding window containing `duration` years.

        Args:
            duration: The number of years to use in the window.
        """
        return Sliding(
            duration=duration,
            predicate=lambda domain: Timestream._call("yearly", domain),
        )


@dataclass(frozen=True)
class Trailing(Window):
    """Window the last `duration` time period.

    Args:
        duration: The duration of the window.
    """

    #: The duration of the window.
    duration: timedelta

    def __post_init__(self):
        """Validate the window parameters."""
        if self.duration <= timedelta(0):
            raise ValueError("duration must be positive")
