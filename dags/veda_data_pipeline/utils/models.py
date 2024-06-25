from pydantic import BaseModel, Field, constr
from typing import Dict, Optional, Literal
from datetime import datetime

class AssetModel(BaseModel):
    title: str
    description: str
    regex: str

class DiscoveryModel(BaseModel):
    collection: str
    bucket: str
    prefix: str
    filename_regex: str = r'^(.*).tif$'  # Ensure the regex pattern
    id_regex: Optional[str] = Field(default=r'.*_(.*).tif$')
    ## process_from_yyyy_mm_dd: Optional[datetime]  # Optional field, assumes ISO 8601 format for datetime
    id_template: Optional[str] = "example-id-prefix-{}"
    datetime_range: Literal['daily', 'monthly', 'yearly']
    ## TODO last_successful_execution: Optional[datetime] = datetime(2015, 1, 1) # Optional datetime field, default to long ago
    assets: Optional[Dict[str, AssetModel]]  # Optional dictionary of assets
    schedule:Optional[str] = None # Airflow-compatible schedule definition (CRON-like)

class TransferModel(BaseModel):
    origin_bucket: str
    origin_prefix: str
    filename_regex: str
    target_bucket: str
    collection: str
    cogify: bool = False
    dry_run: bool = False
