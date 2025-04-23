import json
import logging
import requests
from typing import List, TypedDict
from dataclasses import dataclass

import boto3

logging.basicConfig(level=logging.INFO)

class Creds(TypedDict):
    access_token: str
    expires_in: int
    token_type: str
    scope: str

class Secret(TypedDict):
    userinfo_url: str
    id: str
    secret: str
    auth_url: str
    token_url: str

@dataclass
class TransactionsApi:
    base_url: str
    token: str

    @classmethod
    def from_veda_auth_secret(cls, *, secret_id: str, base_url: str) -> "TransactionsApi":
        secret_details = cls._get_auth_service_details(secret_id)
        credentials = cls._get_app_credentials(**secret_details)
        return cls(token=credentials["access_token"], base_url=base_url)

    @staticmethod
    def _get_auth_service_details(secret_id: str) -> Secret:
        client = boto3.client("secretsmanager")
        response = client.get_secret_value(SecretId=secret_id)
        return json.loads(response["SecretString"])

    @staticmethod
    def _get_app_credentials(
        userinfo_url: str, id: str, secret: str, auth_url: str, token_url: str, **kwargs
    ) -> Creds:
        response = requests.post(
            token_url,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json"
            },
            data={
                "client_id": id,
                "client_secret": secret,
                "grant_type": "client_credentials",
                "scope": "stac:item:create stac:collection:create stac:collection:update stac:item:update"
            },
        )
        try:
            response.raise_for_status()
        except Exception as ex:
            print(response.text)
            raise RuntimeError(f"Error, {ex}")
        return response.json()


    def post_items(self, collection_id: str, items: List[dict]) -> dict:
        """
        Perform a PUT request to update or create a STAC Item in the given collection.

        :param collection_id: The target collection ID.
        :param items: list of STAC items to be submitted.
        :return: The JSON response (as a dict) from the STAC API.
        :raises RuntimeError: If the response is not 200/201.
        """
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        bulk_items = {"items": {item['id']: item for item in items}, "method": "upsert"}
        response = requests.post(
            f"{self.base_url.rstrip('/')}/collections/{collection_id}/bulk_items", 
            headers=headers, 
            json=bulk_items
        )

        if response.status_code not in (200, 201):
            logging.error("Failed PUT request: %s %s", response.status_code, response.text)
            raise RuntimeError(f"PUT request failed: {response.text}")

        return response.json()


def submit_transactions_handler(
        event, 
        cognito_app_secret=None, # unused, but maintains signature compatibility w/ ingest API
        ingest_url=None
    ):
    """
    Handler function that can be integrated in the same way as the existing `submission_handler`,
    but uses the TransactionsApi to perform a PUT request to STAC's Transactions endpoint.

    :param event: A dict containing the data needed for STAC item submission,
                  including collection_id, item_id, and the STAC item body itself.
    :param context: (Optional) context object, for AWS Lambda or similar environments.
    :return: A dict representing the API response.
    """

    collection_id = event[0].get("collection")
    api = TransactionsApi.from_veda_auth_secret(
        secret_id=cognito_app_secret,
        base_url=ingest_url,
    )
    try:
        response = api.post_items(
            collection_id=collection_id, 
            items=event,
        )
        logging.info("STAC Bulk Item POST completed successfully.")
    except RuntimeError as err:
        logging.error("Error while performing POST: %s", str(err))
        raise
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "POST request completed successfully",
            "response": response
        })
    }
