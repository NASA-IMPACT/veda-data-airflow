from dataclasses import dataclass
import json
from urllib.parse import urlparse
import sys

if sys.version_info >= (3, 8):
    from typing import TypedDict
else:
    from typing_extensions import TypedDict
from typing import Any, Dict, Optional, Union

from airflow.models.variable import Variable
import boto3
import requests


class InputBase(TypedDict):
    dry_run: Optional[Any]


class S3LinkInput(InputBase):
    stac_file_url: str


class StacItemInput(InputBase):
    stac_item: Dict[str, Any]


class AppConfig(TypedDict):
    cognito_domain: str
    client_id: str
    client_secret: str
    scope: str


class Creds(TypedDict):
    access_token: str
    expires_in: int
    token_type: str


@dataclass
class IngestionApi:
    base_url: str
    token: str

    @classmethod
    def from_veda_auth_secret(cls, *, secret_id: str, base_url: str) -> "IngestionApi":
        cognito_details = cls._get_cognito_service_details(secret_id)
        credentials = cls._get_app_credentials(**cognito_details)
        return cls(token=credentials["access_token"], base_url=base_url)

    @staticmethod
    def _get_cognito_service_details(secret_id: str) -> AppConfig:
        client = boto3.client("secretsmanager")
        response = client.get_secret_value(SecretId=secret_id)
        return json.loads(response["SecretString"])

    @staticmethod
    def _get_app_credentials(
        cognito_domain: str, client_id: str, client_secret: str, scope: str, **kwargs
    ) -> Creds:
        response = requests.post(
            f"{cognito_domain}/oauth2/token",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
            },
            auth=(client_id, client_secret),
            data={
                "grant_type": "client_credentials",
                # A space-separated list of scopes to request for the generated access token.
                "scope": scope,
            },
        )
        try:
            response.raise_for_status()
        except:
            print(response.text)
            raise
        return response.json()

    def submit(self, stac_item: Dict[str, Any]):
        response = requests.post(
            f"{self.base_url.rstrip('/')}/ingestions",
            json=stac_item,
            headers={"Authorization": f"bearer {self.token}"},
        )

        try:
            response.raise_for_status()
        except Exception as e:
            print(response.text)
            raise e

        return response.json()





def submission_handler(event: Union[S3LinkInput, StacItemInput]) -> None:
    print(f"SUBMISSION EVENT {event}")
    stac_item = event

    if event.get("dry_run"):
        print("Dry run, not inserting, would have inserted:")
        print(json.dumps(stac_item, indent=2))
        return
    ingestor = IngestionApi.from_veda_auth_secret(
        secret_id=Variable.get("COGNITO_APP_SECRET"),
        base_url=Variable.get("STAC_INGESTOR_API_URL"),
    )
    ingestor.submit(stac_item)
    print(f"Successfully submitted STAC item")


if __name__ == "__main__":
    filename = "example.ndjson"
    sample_event = {
        "stac_file_url": "example.ndjson",
        # or
        "stac_item": {},
        "type": "collections",
    }
    submission_handler(sample_event, {})
