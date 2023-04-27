import json
import os
import sys

import requests

from utils import get_collections, get_secret


def get_app_credentials(
    cognito_domain: str, client_id: str, client_secret: str, scope: str, **kwargs
):
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
    except Exception:
        print(response.text)
        raise
    return response.json()


def insert_collections(files):
    print("Authenticating")
    cognito_details = get_secret(os.environ.get("COGNITO_APP_SECRET"))
    credentials = get_app_credentials(**cognito_details)
    bearer_token = credentials["access_token"]

    print("Inserting collections:")
    base_url = os.environ.get("STAC_INGESTOR_API_URL")
    with requests.Session() as s:
        for file in files:
            print(file)
            try:
                with open(file) as fd:
                    response = s.post(
                        f"{base_url.rstrip('/')}/collections",
                        json=json.load(fd),
                        headers={"Authorization": f"Bearer {bearer_token}"},
                    )
                    response.raise_for_status()
                    print(response.text)
            except Exception:
                print("Error inserting collection.")
                raise


if __name__ == "__main__":
    collection_regex = sys.argv[1]
    files = get_collections(collection_regex)
    insert_collections(files)
