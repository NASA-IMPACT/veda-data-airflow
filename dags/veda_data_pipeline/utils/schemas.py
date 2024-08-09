# Description: Lightweight schema definitions

from datetime import datetime
from typing import List, Union
from stac_pydantic.collection import Extent, TimeInterval


class DatetimeInterval(TimeInterval):
    # reimplement stac_pydantic's TimeInterval to leverage datetime types
    interval: List[List[Union[datetime, None]]]


class SpatioTemporalExtent(Extent):
    # reimplement stac_pydantic's Extent to leverage datetime types
    temporal: DatetimeInterval
