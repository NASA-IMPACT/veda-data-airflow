import json
import os
import sys
from dataclasses import dataclass

if sys.version_info >= (3, 8):
    from typing import TypedDict
else:
    from typing_extensions import TypedDict

from typing import Any, Dict, Optional, Union

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
        except Exception as ex:
            print(response.text)
            raise f"Error, {ex}"
        return response.json()

    def submit(self, event: Dict[str, Any], endpoint: str) -> Dict[str, Any]:
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        response = requests.post(
            f"{self.base_url.rstrip('/')}{endpoint}",
            json=event,
            headers=headers,
        )
        try:
            response.raise_for_status()
        except Exception as e:
            print(response.text)
            raise e
        return response.json()


def submission_handler(
    event: Union[S3LinkInput, StacItemInput, Dict[str, Any]],
    endpoint: str = "/ingestions",
    cognito_app_secret=None,
    stac_ingestor_api_url=None,
    context=None,
) -> None | dict:
    if context is None:
        context = {}

    stac_item = event

    if stac_item.get("dry_run"):
        print("Dry run, not inserting, would have inserted:")
        print(json.dumps(stac_item, indent=2))
        return

    cognito_app_secret = cognito_app_secret or os.getenv("COGNITO_APP_SECRET")
    stac_ingestor_api_url = stac_ingestor_api_url or os.getenv("STAC_INGESTOR_API_URL")

    ingestor = IngestionApi.from_veda_auth_secret(
        secret_id=cognito_app_secret,
        base_url=stac_ingestor_api_url,
    )
    return ingestor.submit(event=stac_item, endpoint=endpoint)


if __name__ == "__main__":
    filename = "example.ndjson"
    sample_event = {
        "stac_file_url": "example.ndjson",
        # or
        "stac_item": {},
        "type": "collections",
    }
    submission_handler(sample_event)
