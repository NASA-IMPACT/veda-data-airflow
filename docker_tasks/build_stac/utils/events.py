from datetime import datetime
from typing import Dict, List, Literal, Optional, Tuple, Union

from pydantic import BaseModel, Field
import pystac


INTERVAL = Literal["month", "year", "day"]


class BaseEvent(BaseModel, frozen=True):
    collection: str
    s3_filename: str

    asset_name: Optional[str] = None
    asset_roles: Optional[List[str]] = None
    asset_media_type: Optional[Union[str, pystac.MediaType]] = None


class CmrEvent(BaseEvent):
    granule_id: str


class RegexEvent(BaseEvent):
    filename_regex: Optional[str]

    start_datetime: Optional[datetime] = None
    end_datetime: Optional[datetime] = None
    single_datetime: Optional[datetime] = None

    properties: Optional[Dict] = Field(default_factory=dict)
    datetime_range: Optional[INTERVAL] = None


SupportedEvent = Union[RegexEvent, CmrEvent]
