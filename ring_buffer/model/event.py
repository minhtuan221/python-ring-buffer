"""Event model for Ring buffer"""
import dataclasses
import typing as t


@dataclasses.dataclass
class Event:
    """Event with bring the data from service to service
    """
    event_type: str
    data: t.Any
    sid: str = ''
