"""Control which points and entities are output."""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class History:
    """Execution options for queries producing all historic points."""

    #: If set, only returns points after this time.
    #:
    #: Setting this allows incremental execution to use a checkpoint
    #: from a time before the `since` time.
    since: Optional[datetime] = None

    #: Only return points less than or equal to this time.
    #: If not set, the current time will be used.
    until: Optional[datetime] = None


@dataclass
class Snapshot:
    """Execution options for queries producing snapshots at a specific time."""

    #: If set, only includes entities that changed after this time.
    #:
    #: Snapshot queries support incremental execution even when this isn't set.
    #: However, every snapshot will include every entity unless this is set.
    #: When writing results to an external store that already has values
    #: from an earlier snapshot, this can be used to reduce the amount of
    #: data to be written.
    changed_since: Optional[datetime] = None

    #: If set, produces the snapshot at the given time.
    #: If not set, the current time will be used.
    at: Optional[datetime] = None
