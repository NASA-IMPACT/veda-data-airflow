# Description: Lightweight schema definitions

from datetime import datetime
from typing import List, Union, Any, Dict
from stac_pydantic.collection import Extent, TimeInterval
from pystac.utils import datetime_to_str
from dateutil import parser as date_parser


class DatetimeInterval(TimeInterval):
    # reimplement stac_pydantic's TimeInterval to leverage datetime types
    interval: List[List[Union[datetime, None]]]


class SpatioTemporalExtent(Extent):
    # reimplement stac_pydantic's Extent to leverage datetime types
    temporal: DatetimeInterval


def normalize_datetime_to_iso8601(dt: Any) -> Union[str, None, Any]:
    """
    Normalize a datetime value to ISO 8601 format with T separator and Z for UTC.

    - datetime objects become ISO 8601 string
    - '2024-09-12 00:00:00+00' -> '2024-09-12T00:00:00Z'
    - '2024-09-12T00:00:00+00:00' -> '2024-09-12T00:00:00Z'

    Args:
        dt: datetime object or string

    Returns:
        Normalized ISO 8601 datetime string
    """
    if dt is None:
        return None

    # Convert to datetime object if it's a string
    if isinstance(dt, str):
        dt = date_parser.parse(dt)
    elif not isinstance(dt, datetime):
        return dt

    # Convert to ISO 8601 format
    dt_str = dt.isoformat()

    # Convert UTC timezone to Z: +00:00 -> Z
    if dt_str.endswith('+00:00'):
        dt_str = dt_str[:-6] + 'Z'
    elif dt_str.endswith('-00:00'):
        dt_str = dt_str[:-6] + 'Z'

    return dt_str


def normalize_temporal_extent(collection: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize temporal extent in a STAC collection to ISO 8601 format

    - '2024-09-12 00:00:00+00' -> '2024-09-12T00:00:00Z'
    """
    if not isinstance(collection, dict):
        return collection

    if "extent" in collection and "temporal" in collection["extent"]:
        temporal = collection["extent"]["temporal"]
        if "interval" in temporal and isinstance(temporal["interval"], list):
            collection["extent"]["temporal"]["interval"] = [
                [normalize_datetime_to_iso8601(dt) for dt in interval]
                if isinstance(interval, list)
                else interval
                for interval in temporal["interval"]
            ]

    return collection
