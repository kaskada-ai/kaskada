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
    """Window producing cumulative values since a predicate.

    "Since" windows are a series of non-overlapping windows producing cumulative values
    since each time the predicate evaluates to true. Each window is exclusive of the time
    it starts at and inclusive of the end time.

    Args:
        predicate: the condition used to determine when the window resets.
    """

    #: The boolean Timestream to use as predicate for the window.
    #: Each time the predicate evaluates to true the window will be cleared.
    #:
    #: The predicate may be a callable which returns the boolean Timestream, in
    #: which case it is applied to the Timestream being aggregated.
    predicate: Timestream | Callable[..., Timestream] | bool

    @staticmethod
    def minutely() -> Since:
        """Return a window since the start of each calendar minute.

        Returns:
            Window for aggregating cumulative values since the predicate.
        """
        return Since(predicate=lambda domain: Timestream._call("minutely", domain))

    @staticmethod
    def hourly() -> Since:
        """Return a window since the start of each calendar hour.

        Returns:
            Window for aggregating cumulative values since the predicate.
        """
        return Since(predicate=lambda domain: Timestream._call("hourly", domain))

    @staticmethod
    def daily() -> Since:
        """Return a window since the start of each calendar day.

        Returns:
            Window for aggregating cumulative values since the predicate.
        """
        return Since(predicate=lambda domain: Timestream._call("daily", domain))

    @staticmethod
    def monthly() -> Since:
        """Return a window since the start of each calendar month.

        Returns:
            Window for aggregating cumulative values since the predicate.
        """
        return Since(predicate=lambda domain: Timestream._call("monthly", domain))

    @staticmethod
    def yearly() -> Since:
        """Return a window since the start of each calendar year.

        Returns:
            Window for aggregating cumulative values since the predicate.
        """
        return Since(predicate=lambda domain: Timestream._call("yearly", domain))


@dataclass(frozen=True)
class Tumbling(Window):
    """Window producing a single value at each predicate.

    "Tumbling" windows are a series of non-overlapping windows that produce values
    each time the predicate evaluates to true. Each window is exclusive of the time
    it starts at and inclusive of the end time.

    Args:
        predicate: the condition used to determine when the window resets.

    Notes:
        Like other systems, Kaskada treats tumbling windows as non-overlapping.
        When one window ends the next starts.

        Unlike other systems, tumbling windows do not need to be of a fixed size.
        Instead, they may be determined by a fixed duration, a calendar duration (such as a "month"),
        or even a predicate.
    """

    #: The boolean Timestream to use as predicate for the window.
    #: Each time the predicate evaluates to true the window will be cleared.
    #:
    #: The predicate may be a callable which returns the boolean Timestream, in
    #: which case it is applied to the Timestream being aggregated.
    predicate: Timestream | Callable[..., Timestream] | bool

    @staticmethod
    def minutely() -> Tumbling:
        """Return a tumbling window that resets at the start of each calendar minute.

        Returns:
            Window for aggregating values since the predicate.
        """
        return Tumbling(predicate=lambda domain: Timestream._call("minutely", domain))

    @staticmethod
    def hourly() -> Tumbling:
        """Return a tumbling window that resets at the start of each calendar hour.

        Returns:
            Window for aggregating values since the predicate.
        """
        return Tumbling(predicate=lambda domain: Timestream._call("hourly", domain))

    @staticmethod
    def daily() -> Tumbling:
        """Return a tumbling window that resets at the start of each calendar day.

        Returns:
            Window for aggregating values since the predicate.
        """
        return Tumbling(predicate=lambda domain: Timestream._call("daily", domain))

    @staticmethod
    def monthly() -> Tumbling:
        """Return a tumbling window that resets at the start of each calendar month.

        Returns:
            Window for aggregating values since the predicate.
        """
        return Tumbling(predicate=lambda domain: Timestream._call("monthly", domain))

    @staticmethod
    def yearly() -> Tumbling:
        """Return a tumbling window that resets at the start of each calendar year.

        Returns:
            Window for aggregating values since the predicate.
        """
        return Tumbling(predicate=lambda domain: Timestream._call("yearly", domain))


@dataclass(frozen=True)
class Sliding(Window):
    """Overlapping windows producing the latest value at each predicate.

    "Sliding" windows are a series of overlapping windows that produce values each
    time the predicate evaluates to true. Each window is exclusive of the time
    it starts at and inclusive of the end time.

    Args:
        duration: the number of active windows at any given time.
        predicate: the condition used to determine when the oldest window ends and a new
            window starts.
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
        """Return a sliding window containing `duration` calendar minutes.

        Args:
            duration: The number of minutes to use in the window.

        Returns:
            Overlapping windows for aggregating values.
        """
        return Sliding(
            duration=duration,
            predicate=lambda domain: Timestream._call("minutely", domain),
        )

    @staticmethod
    def hourly(duration: int) -> Sliding:
        """Return a sliding window containing `duration` calendar hours.

        Args:
            duration: The number of hours to use in the window.

        Returns:
            Overlapping windows for aggregating values.
        """
        return Sliding(
            duration=duration,
            predicate=lambda domain: Timestream._call("hourly", domain),
        )

    @staticmethod
    def daily(duration: int) -> Sliding:
        """Return a sliding window containing `duration` calendar days.

        Args:
            duration: The number of days to use in the window.

        Returns:
            Overlapping windows for aggregating values.
        """
        return Sliding(
            duration=duration,
            predicate=lambda domain: Timestream._call("daily", domain),
        )

    @staticmethod
    def monthly(duration: int) -> Sliding:
        """Return a sliding window containing `duration` calendar months.

        Args:
            duration: The number of months to use in the window.

        Returns:
            Overlapping windows for aggregating values.
        """
        return Sliding(
            duration=duration,
            predicate=lambda domain: Timestream._call("monthly", domain),
        )

    @staticmethod
    def yearly(duration: int) -> Sliding:
        """Return a sliding window containing `duration` calendar years.

        Args:
            duration: The number of years to use in the window.

        Returns:
            Overlapping windows for aggregating values.
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
