import json
import logging
import requests
from typing import List, TypedDict

import boto3

logging.basicConfig(level=logging.INFO)

class Creds(TypedDict):
    access_token: str
    expires_in: int
    token_type: str

class AppConfig(TypedDict):
    cognito_domain: str
    client_id: str
    client_secret: str
    scope: str

class TransactionsApi:

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

    def __init__(self, stac_ingestor_api_url: str, cognito_app_secret: str = None):
        """
        :param stac_endpoint: Base URL of the STAC API (e.g., 'https://example.com/stac').
        :param token: Optional Bearer token for authenticated STAC APIs.
        """
        self.stac_ingestor_api_url = stac_ingestor_api_url.rstrip('/')
        self.cognito_app_secret = cognito_app_secret

    def post_items(self, collection_id: str, items: List[dict]) -> dict:
        """
        Perform a PUT request to update or create a STAC Item in the given collection.

        :param collection_id: The target collection ID.
        :param item_id: The target item ID.
        :param item_body: The full STAC Item JSON body.
        :return: The JSON response (as a dict) from the STAC API.
        :raises RuntimeError: If the response is not 200/201.
        """
        url = f"{self.base_url.rstrip('/')}{self.stac_ingestor_api_url}/collections/{collection_id}/bulk_items"
        headers = {"Content-Type": "application/json"}

        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        logging.info(f"PUT {url}")
        response = requests.post(url, headers=headers, json=items)

        if response.status_code not in (200, 201):
            logging.error("Failed PUT request: %s %s", response.status_code, response.text)
            raise RuntimeError(f"PUT request failed: {response.text}")

        return response.json()


def submit_transactions_handler(
        event, 
        cognito_app_secret=None,
        stac_ingestor_api_url=None,
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
    api = TransactionsApi(stac_ingestor_api_url, cognito_app_secret)
    try:
        response = api.post_items(collection_id, event)
        logging.info("STAC Item POST completed successfully.")
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
