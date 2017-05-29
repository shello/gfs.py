#!/usr/bin/env python3
"""Grandfather-father-son backup rotation scheme."""

import collections.abc
from bisect import bisect
from datetime import datetime
from typing import Callable, Coroutine, Hashable, Iterable, Mapping, Union


class Cycle:
    """A rotation cycle definition."""

    def __init__(self, name: str, datetime_fmt_key: str):
        """Create a named cycle with a datetime format string used as key."""
        self.name = str(name)
        self.key_fmt = str(datetime_fmt_key)

    def __str__(self) -> str:
        """Return an informal string representation of the cycle."""
        return self.name

    def __repr__(self) -> str:
        """Return a printable representation of the cycle."""
        return f"Cycle({self.name}, {self.key_fmt})"

    def __eq__(self, other) -> bool:
        """Compare this cycle to another."""
        if type(self) is not type(other):
            return False

        return self.name == other.name and self.key_fmt == other.key_fmt

    def __hash__(self) -> int:
        """Return the hash value of the cycle object."""
        return hash((self.name, self.key_fmt))

    def key(self, date: datetime):
        """Return the key value for a given datetime object."""
        return format(date, self.key_fmt)


# Helper cycles
DAILY = Cycle("daily", "%Y-%m-%d")
WEEKLY = Cycle("weekly", "%Y-%W")
MONTHLY = Cycle("monthly", "%Y-%m")
YEARLY = Cycle("yearly", "%Y")


class _GFS():
    """Grandfather-father-son backup rotation scheme."""

    def __init__(self, cycles: Mapping[Cycle, int]):
        """Initialize the GFS class by passing a scheme of policy cycles."""
        self.policies = {}

        for cycle, value in cycles.items():
            if value < 1:
                raise ValueError("Policies only accept a positive value of "
                                 f"cycles, {value} given for {cycle}")

            self.policies[cycle] = value

        if not self.policies:
            raise RuntimeError("GFS with no policy cycles")

    def __eq__(self, other) -> bool:
        """Compare the object with another."""
        if type(self) is not type(other):
            return False

        return self.policies == other.policies

    def __hash__(self) -> int:
        """Return the hash of the GFS object."""
        return hash(self.policies)

    @classmethod
    def _formatted_date_key(cls, fmt) -> Callable[[datetime], str]:
        """Generate a 'key' function for dates, based on a given format."""
        def inner(date: datetime) -> str:
            return format(date, fmt)
        return inner

    def _gfs(self,
             dates: Iterable[datetime]) -> Mapping[Cycle, Iterable[datetime]]:
        """Filter a list of candidate dates, returning the selected ones.

        >>> fmt = '%Y-%m-%dT%H:%M:%S'
        >>> raw = ['2017-05-21T22:00:00',
        ...        '2017-05-21T21:00:00',  # Earlier in the first day
        ...        '2017-05-20T20:00:00',  # Earlier in the first week
        ...        '2017-05-14T19:00:00',
        ...        '2017-05-13T18:00:00',  # Earlier in the second week
        ...        '2017-05-07T17:00:00',  # Earlier in the same month
        ...        '2017-04-30T16:00:00',
        ...        '2017-04-23T15:00:00',
        ...        '2016-12-25T14:00:00',  # Previous year
        ...        '2015-12-30T13:00:00']  # Two years ago
        >>> dates = [datetime.strptime(d, fmt) for d in raw]
        >>> my_gfs = _GFS({YEARLY: 2, MONTHLY: 2, WEEKLY: 2, DAILY: 2})
        >>> final = my_gfs._gfs(dates)
        >>> {str(c): [format(d, fmt) for d in ds] for c, ds in final.items()}
        ...     # doctest: +NORMALIZE_WHITESPACE
        {'yearly': ['2016-12-25T14:00:00', '2017-05-21T22:00:00'],
         'monthly': ['2017-04-30T16:00:00', '2017-05-21T22:00:00'],
         'weekly': ['2017-05-14T19:00:00', '2017-05-21T22:00:00'],
         'daily': ['2017-05-20T20:00:00', '2017-05-21T22:00:00']}
        """
        cycles = {cycle: self._gfs_cycle(cycle) for cycle in self.policies}

        # Prime the coroutine
        for cycle in cycles.values():
            cycle.send(None)

        # Pass all the dates to this policy cycle
        for date in dates:
            for cycle in cycles.values():
                cycle.send(date)

        # Finalise by sending None, retrieving the selected values
        selected = {}
        for name, cycle in cycles.items():
            selected[name] = iter(cycle.send(None))

        return selected

    def gfs_filter(self, dates: Iterable[datetime]) -> Iterable[datetime]:
        """Filter a list of candidate dates, returning the selected ones.

        >>> fmt = '%Y-%m-%dT%H:%M:%S'
        >>> raw = ['2017-05-21T22:00:00',
        ...        '2017-05-21T21:00:00',  # Earlier in the first day
        ...        '2017-05-20T20:00:00',  # Earlier in the first week
        ...        '2017-05-14T19:00:00',
        ...        '2017-05-13T18:00:00',  # Earlier in the second week
        ...        '2017-05-07T17:00:00',  # Earlier in the same month
        ...        '2017-04-30T16:00:00',
        ...        '2017-04-23T15:00:00',
        ...        '2016-12-25T14:00:00',  # Previous year
        ...        '2015-12-30T13:00:00']  # Two years ago
        >>> dates = [datetime.strptime(d, fmt) for d in raw]
        >>> my_gfs = _GFS({YEARLY: 2, MONTHLY: 2, WEEKLY: 2, DAILY: 2})
        >>> final = my_gfs.gfs_filter(dates)
        >>> [format(d, fmt) for d in sorted(final)]
        ...     # doctest: +NORMALIZE_WHITESPACE
        ['2016-12-25T14:00:00', '2017-04-30T16:00:00', '2017-05-14T19:00:00',
         '2017-05-20T20:00:00', '2017-05-21T22:00:00']
        """
        selected = self._gfs(dates)

        filtered = set()  # Type: Set[datetime]
        for elems in selected.values():
            filtered.update(elems)

        return filtered

    # Aaargh, 80 cols!
    _Gfs_cycle_ret = Coroutine[None, Union[datetime, None], Iterable]

    def _gfs_cycle(self, cycle: Cycle) -> _Gfs_cycle_ret:
        """Coroutine that evaluates every candidate sent, under this cycle."""
        cycle_set = SortedLimitedSet(self.policies[cycle], key=cycle.key)

        date = yield
        while date:
            cycle_set.insert(date)
            date = yield

        yield iter(cycle_set)
        return


class GFS(_GFS):
    """Grandfather-father-son backup rotation scheme."""

    def __init__(self, date_format: str,
                 cycles: Union[None, Mapping[Cycle, int]] = None,
                 **kwargs: Mapping[str, int]):
        """Create a Grandfather-father-son backup rotation scheme helper."""
        self.fmt = date_format

        if cycles is None and kwargs:
            cycles = GFS.parse_keyword_cycles(**kwargs)

        super().__init__(cycles=cycles)

    @classmethod
    def parse_keyword_cycles(cls, **kwargs: Mapping[str, int]):
        """Parse keyword arguments and turn them into a cycle->int mapping.

        >>> fmt = '%Y-%m-%dT%H:%M:%S'
        >>> gfs_kw = GFS(fmt, daily=1, weekly=2, monthly=3, yearly=4)
        >>> gfs_map = GFS(fmt, {DAILY: 1, WEEKLY: 2, MONTHLY: 3, YEARLY: 4})
        >>> gfs_kw == gfs_map
        True
        >>> GFS(fmt, daily=1) == GFS(fmt, {DAILY: 2})
        False
        >>> GFS(fmt, daily=1) == GFS(fmt, {WEEKLY: 1})
        False
        """
        cycles = {}

        for kwarg, value in kwargs.items():
            arg = kwarg.lower()

            if arg in ('daily', 'day'):
                cycle = DAILY
            elif arg in ('weekly', 'week'):
                cycle = WEEKLY
            elif arg in ('monthly', 'month'):
                cycle = MONTHLY
            elif arg in ('yearly', 'year'):
                cycle = YEARLY
            else:
                raise PolicyNotImplemented(f"Policy not available: {kwarg}.")

            cycles[cycle] = value
        return cycles

    def _gfs(self,
             dates: Iterable[str]) -> Mapping[Cycle, Iterable[datetime]]:
        """Filter a list of candidate dates, returning the selected ones.

        >>> fmt = '%Y-%m-%dT%H:%M:%S'
        >>> dates = ['2017-05-21T22:00:00',
        ...          '2017-05-21T21:00:00',  # Earlier in the first day
        ...          '2017-05-20T20:00:00',  # Earlier in the first week
        ...          '2017-05-14T19:00:00',
        ...          '2017-05-13T18:00:00',  # Earlier in the second week
        ...          '2017-05-07T17:00:00',  # Earlier in the same month
        ...          '2017-04-30T16:00:00',
        ...          '2017-04-23T15:00:00',
        ...          '2016-12-25T14:00:00',  # Previous year
        ...          '2015-12-30T13:00:00']  # Two years ago
        >>> my_gfs = GFS(fmt, {YEARLY: 2, MONTHLY: 2, WEEKLY: 2, DAILY: 2})
        >>> final = my_gfs._gfs(dates)
        >>> {str(c): ds for c, ds in final.items()}
        ...     # doctest: +NORMALIZE_WHITESPACE
        {'yearly': ['2016-12-25T14:00:00', '2017-05-21T22:00:00'],
         'monthly': ['2017-04-30T16:00:00', '2017-05-21T22:00:00'],
         'weekly': ['2017-05-14T19:00:00', '2017-05-21T22:00:00'],
         'daily': ['2017-05-20T20:00:00', '2017-05-21T22:00:00']}
        """
        result = super()._gfs(self._str_to_date(dates))
        return {c: list(self._date_to_str(s)) for c, s in result.items()}

    def _str_to_date(self, str_dates: Iterable[str]) -> Iterable[datetime]:
        """A generator that yields datetime from strings."""
        yield from map(lambda s: datetime.strptime(s, self.fmt), str_dates)

    def _date_to_str(self, dates: Iterable[datetime]) -> Iterable[str]:
        """A generator that yields strings from datetime."""
        yield from map(lambda d: format(d, self.fmt), dates)


class SortedLimitedList(collections.abc.Iterable):
    """A sorted list of limited size.

    >>> s = SortedLimitedList(3, [20, 10])
    >>> s
    [10, 20]
    >>> s.insert(5)
    >>> s
    [5, 10, 20]
    >>> s.insert(30)
    >>> s
    [10, 20, 30]
    >>> s.insert(0)
    >>> s
    [10, 20, 30]
    >>> s.insert(40)
    >>> s
    [20, 30, 40]


    With a key:

    >>> s = SortedLimitedList(3, [10, 20], key=(lambda v: -v))
    >>> s
    [20, 10]
    >>> s.insert(5)
    >>> s
    [20, 10, 5]
    >>> s.insert(30)
    >>> s
    [20, 10, 5]
    >>> s.insert(0)
    >>> s
    [10, 5, 0]
    >>> s.insert(-10)
    >>> s
    [5, 0, -10]


    Initial iterable with more items than max_size:

    >>> s = SortedLimitedList(3, [10, 20, 30, 40])
    >>> s
    [20, 30, 40]
    """

    def __init__(self, max_size: int, iterable: Iterable[Hashable] = None,
                 key: Union[Callable, None] = None):
        """Initialize the SortedLimitedList."""
        self.max_size = max_size
        self.key = key if callable(key) else (lambda v: v)

        self.list = []
        self.keys = []

        if iterable:
            for elem in iterable:
                self.insert(elem)

    def insert(self, value, *, _key=None, _index: int = None) -> None:
        """Insert a new value into the list."""
        key = _key or self.key(value)
        idx = _index or bisect(self.keys, key)

        # When the list is at maximum size, and this element would be inserted
        # at the head of the list (index = 0), skip adding it altogether.
        if idx != 0 or len(self.keys) < self.max_size:
            self.keys.insert(idx, key)
            self.list.insert(idx, value)

            # Remove the extraneous elements
            if len(self.keys) > self.max_size:
                del self.keys[0]
                del self.list[0]

    def __iter__(self) -> Iterable:
        """Generate an iterable for this list."""
        yield from self.list

    def __repr__(self) -> str:
        """Compute the string representation of this list."""
        return repr(self.list)


class SortedLimitedSet(SortedLimitedList):
    """A Size-limited Sorted Set.

    >>> s = SortedLimitedSet(3, [20, 10])
    >>> s.insert(5)
    >>> s.insert(30)
    >>> s.insert(0)
    >>> s.insert(40)
    >>> s.insert(30)
    >>> s
    [20, 30, 40]


    With a key:

    >>> s = SortedLimitedSet(3, [10, 20], key=(lambda v: -v))
    >>> s.insert(5)
    >>> s.insert(30)
    >>> s.insert(0)
    >>> s.insert(-10)
    >>> s.insert(0)
    >>> s
    [5, 0, -10]


    Initial iterable with more items than max_size:

    >>> s = SortedLimitedSet(3, [10, 20, 30, 40])
    >>> s
    [20, 30, 40]"""

    def insert(self, value) -> None:
        """Insert a new value into the set."""
        key = self.key(value)
        idx = bisect(self.keys, key)

        ex_key = self.keys[idx - 1] if idx > 0 else None
        if ex_key is not None and ex_key == key:
            # Key collision, compare raw values and substitute if greater;
            # ignore value otherwise
            if value >= self.list[idx - 1]:
                self.list[idx - 1] = value
        else:
            super().insert(value, _key=key, _index=idx)


class PolicyNotImplemented(Exception):
    """Raised when a required policy is not implemented."""

    pass
