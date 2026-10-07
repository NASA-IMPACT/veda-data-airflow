from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

INTERVAL = Literal["month", "year", "day"]


class RegexEvent(BaseModel, frozen=True):
    collection: str
    item_id: str
    assets: dict

    start_datetime: datetime | None = None
    end_datetime: datetime | None = None
    single_datetime: datetime | None = None

    properties: dict | None = Field(default_factory=dict)
    datetime_range: INTERVAL | None = None
