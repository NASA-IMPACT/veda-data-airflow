import enum
import re
from datetime import datetime
from typing import Dict, List, Literal, Optional
from urllib.parse import urlparse

import src.validators as validators
from src.utils import regex
from pydantic import (
    BaseModel,
    Field,
    root_validator,
    validator,
    Extra,
)
from src.schema_helpers import (
    BboxExtent,
    SpatioTemporalExtent,
    TemporalExtent,
    DiscoveryItemAsset,
)
from stac_pydantic import Collection, Item, shared
from stac_pydantic.links import Link


class AccessibleAsset(shared.Asset):
    @validator("href")
    def is_accessible(cls, href):
        url = urlparse(href)

        if url.scheme in ["https", "http"]:
            validators.url_is_accessible(href)
        elif url.scheme in ["s3"]:
            validators.s3_object_is_accessible(
                bucket=url.hostname, key=url.path.lstrip("/")
            )
        else:
            ValueError(f"Unsupported scheme: {url.scheme}")

        return href


class AccessibleItem(Item):
    assets: Dict[str, AccessibleAsset]

    @validator("collection")
    def exists(cls, collection):
        validators.collection_exists(collection_id=collection)
        return collection


class DashboardCollection(Collection):
    is_periodic: Optional[bool] = Field(default=False, alias="dashboard:is_periodic")
    time_density: Optional[str] = Field(default=None, alias="dashboard:time_density")
    item_assets: Optional[Dict]
    links: Optional[List[Link]]
    assets: Optional[Dict]
    extent: SpatioTemporalExtent
    renders: Optional[Dict]

    class Config:
        allow_population_by_field_name = True
        extra = Extra.allow

class Status(str, enum.Enum):
    @classmethod
    def _missing_(cls, value):
        for member in cls:
            if member.value.lower() == value.lower():
                return member
        return cls.unknown

    started = "started"
    queued = "queued"
    failed = "failed"
    succeeded = "succeeded"
    cancelled = "cancelled"


class AuthResponse(BaseModel):
    AccessToken: str = Field(..., description="Token used to authenticate the user.")
    ExpiresIn: int = Field(
        ..., description="Number of seconds before the AccessToken expires."
    )
    TokenType: str = Field(
        ..., description="Type of token being returned (e.g. 'Bearer')."
    )
    RefreshToken: str = Field(
        ..., description="Token used to refresh the AccessToken when it expires."
    )
    IdToken: str = Field(
        ..., description="Token containing information about the authenticated user."
    )


class WhoAmIResponse(BaseModel):
    sub: str = Field(..., description="A unique identifier for the user")
    # cognito_groups: List[str] = Field(
    #     ..., description="A list of Cognito groups the user belongs to"
    # )
    iss: str = Field(..., description="The issuer of the token")
    client_id: str = Field(..., description="The client ID of the authenticated app")
    origin_jti: str = Field(
        ..., description="A unique identifier for the authentication event"
    )
    event_id: str = Field(..., description="A unique identifier for the event")
    token_use: str = Field(..., description="The intended use of the token")
    scope: str = Field(..., description="The scope of the token")
    auth_time: int = Field(..., description="The time when the user was authenticated")
    exp: int = Field(..., description="The time when the token will expire")
    iat: int = Field(..., description="The time when the token was issued")
    jti: str = Field(..., description="A unique identifier for the token")
    username: str = Field(..., description="The username of the user")
    aud: str = Field(..., description="The audience of the token")


class WorkflowExecutionResponse(BaseModel):
    id: str = Field(
        ..., description="ID of the workflow execution in discover step function."
    )
    status: Status = Field(
        ..., description="Status of the workflow execution in discover step function."
    )


class ExecutionResponse(WorkflowExecutionResponse):
    message: str = Field(..., description="Message returned from the step function.")
    discovered_files: List[str] = Field(..., description="List of discovered files.")

class ListWorkflowsResponse(BaseModel):
    dags: List


class WorkflowInputBase(BaseModel):
    collection: str = ""
    upload: Optional[bool] = False
    cogify: Optional[bool] = False
    dry_run: bool = False

    @validator("collection")
    def exists(cls, collection):
        """
        Validate that the collection exists.

        Parameters:
        - collection (str): Name of the collection to be validated.

        Returns:
        - str: Name of the collection.
        """
        if not re.match(r"[a-z]+(?:-[a-z]+)*", collection):
            try:
                validators.collection_exists(collection_id=collection)
            except ValueError:
                raise ValueError(
                    "Invalid id - id must be all lowercase, with optional '-' delimiters"
                )
        return collection


class S3Input(WorkflowInputBase):
    discovery: Literal["s3"]
    prefix: str
    bucket: str
    filename_regex: str = r"[\s\S]*"  # default to match all files in prefix
    datetime_range: Optional[str]
    start_datetime: Optional[datetime]
    end_datetime: Optional[datetime]
    single_datetime: Optional[datetime]
    id_regex: Optional[str]
    id_template: Optional[str]
    assets: Optional[Dict[str, DiscoveryItemAsset]]
    zarr_store: Optional[str]

    @root_validator
    def object_is_accessible(cls, values):
        bucket = values.get("bucket")
        prefix = values.get("prefix")
        zarr_store = values.get("zarr_store")
        validators.s3_bucket_object_is_accessible(
            bucket=bucket, prefix=prefix, zarr_store=zarr_store
        )
        return values

class Dataset(BaseModel):
    collection: str
    title: str
    description: str
    license: str
    is_periodic: Optional[bool] = Field(default=False, alias='dashboard:is_periodic')
    time_density: Optional[str] = Field(default=None, alias='dashboard:time_density')
    links: Optional[List[Link]] = []
    discovery_items: List[S3Input]

    # collection id must be all lowercase, with optional - delimiter
    @validator("collection")
    def check_id(cls, collection):
        if not re.match(r"[a-z]+(?:-[a-z]+)*", collection):
            # allow collection id to "break the rules" if an already-existing collection matches
            try:
                validators.collection_exists(collection_id=collection)
            except ValueError:
                # overwrite error - the issue isn't the non-existing function, it's the new id
                raise ValueError(
                    "Invalid id - id must be all lowercase, with optional '-' delimiters"
                )
        return collection

    @root_validator
    def check_time_density(cls, values):
        validators.time_density_is_valid(values["is_periodic"], values["time_density"])
        return values

    class Config:
        extra = Extra.allow

class DataType(str, enum.Enum):
    cog = "cog"
    zarr = "zarr"


class COGDataset(Dataset):
    spatial_extent: BboxExtent
    temporal_extent: TemporalExtent
    sample_files: List[str]  # unknown how this will work with CMR
    data_type: Literal[DataType.cog]
    item_assets: Optional[Dict]
    renders: Optional[Dict]
    stac_extensions: Optional[List[str]]

    @root_validator
    def check_sample_files(cls, values):
        # pydantic doesn't stop at the first validation,
        # if the validation for s3 item access fails, "discovery_items" isn't returned
        # this avoids throwing a KeyError
        if not (discovery_items := values.get("discovery_items")):
            return

        invalid_fnames = []
        for fname in values.get("sample_files", []):
            found_match = False
            for item in discovery_items:
                if all(
                    [
                        re.search(item.filename_regex, fname.split("/")[-1]),
                        "/".join(fname.split("/")[3:]).startswith(item.prefix),
                    ]
                ):
                    if item.datetime_range:
                        try:
                            regex.extract_dates(fname, item.datetime_range)
                        except Exception:
                            raise ValueError(
                                f"Invalid sample file - {fname} does not align"
                                "with the provided datetime_range, and a datetime"
                                "could not be extracted."
                            )
                    found_match = True
            if not found_match:
                invalid_fnames.append(fname)
        if invalid_fnames:
            raise ValueError(
                f"Invalid sample files - {invalid_fnames} do not match any"
                "of the provided prefix/filename_regex combinations."
            )
        return values

class ZarrDataset(Dataset):
    xarray_kwargs: Optional[Dict] = dict()
    x_dimension: Optional[str]
    y_dimension: Optional[str]
    temporal_dimension: Optional[str]
    reference_system: Optional[int]
    data_type: Literal[DataType.zarr]

    @validator("discovery_items")
    def only_one_discover_item(cls, discovery_items):
        if len(discovery_items) != 1:
            raise ValueError("Zarr dataset should have exactly one discovery item")
        if not discovery_items[0].zarr_store:
            raise ValueError(
                "Zarr dataset should include zarr_store in its discovery item"
            )
        return discovery_items
