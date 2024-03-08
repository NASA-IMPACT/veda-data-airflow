import logging

import requests
import src.config as config
from authlib.jose import JsonWebKey, JsonWebToken, JWTClaims, KeySet, errors
from cachetools import TTLCache, cached

from fastapi import Depends, HTTPException, security

logger = logging.getLogger(__name__)

token_scheme = security.HTTPBearer()


def get_settings() -> config.Settings:
    import src.main as main

    return main.settings


def get_jwks_url(settings: config.Settings = Depends(get_settings)) -> str:
    import boto3
    import json
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=settings.workflows_client_secret_id)
    secrets = json.loads(response["SecretString"])

    return f"https://cognito-idp.{secrets['aws_region']}.amazonaws.com/{secrets['userpool_id']}/.well-known/jwks.json"


@cached(TTLCache(maxsize=1, ttl=3600))
def get_jwks(jwks_url: str = Depends(get_jwks_url)) -> KeySet:
    with requests.get(jwks_url) as response:
        response.raise_for_status()
        return JsonWebKey.import_key_set(response.json())


def decode_token(
    token: security.HTTPAuthorizationCredentials = Depends(token_scheme),
    jwks: KeySet = Depends(get_jwks),
) -> JWTClaims:
    """
    Validate & decode JWT
    """
    try:
        claims = JsonWebToken(["RS256"]).decode(
            s=token.credentials,
            key=jwks,
            claims_options={
                # # Example of validating audience to match expected value
                # "aud": {"essential": True, "values": [APP_CLIENT_ID]}
            },
        )

        if "client_id" in claims:
            # Insert Cognito's `client_id` into `aud` claim if `aud` claim is unset
            claims.setdefault("aud", claims["client_id"])

        claims.validate()
        return claims
    except errors.JoseError:  #
        logger.exception("Unable to decode token")
        raise HTTPException(status_code=403, detail="Bad auth token")


def get_username(claims: security.HTTPBasicCredentials = Depends(decode_token)):
    return claims["sub"]


def get_and_validate_token(
    token: security.HTTPAuthorizationCredentials = Depends(token_scheme),
):
    decode_token(token)
    return token
