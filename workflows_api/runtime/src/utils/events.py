from datetime import datetime
from typing import Dict, Literal, Optional

from pydantic import BaseModel, Field

INTERVAL = Literal["month", "year", "day"]


class RegexEvent(BaseModel, frozen=True):
    collection: str
    item_id: str
    assets: Dict

    start_datetime: Optional[datetime] = None
    end_datetime: Optional[datetime] = None
    single_datetime: Optional[datetime] = None

    properties: Optional[Dict] = Field(default_factory=dict)
    datetime_range: Optional[INTERVAL] = None
