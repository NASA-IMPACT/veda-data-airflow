from datetime import datetime
from typing import Dict, List, Literal, Optional, Tuple, Union

import pystac
from pydantic import BaseModel, Field

INTERVAL = Literal["month", "year", "day"]


class RegexEvent(BaseModel, frozen=True):
    collection: str
    datetime_group: str
    # [(asset_label, asset_uri), ...]
    asset_list: List[Tuple[str, str]]

    start_datetime: Optional[datetime] = None
    end_datetime: Optional[datetime] = None
    single_datetime: Optional[datetime] = None

    properties: Optional[Dict] = Field(default_factory=dict)
    datetime_range: Optional[INTERVAL] = None
