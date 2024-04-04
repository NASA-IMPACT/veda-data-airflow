import json
import logging

import boto3
import requests
import jwt
from src.config import settings
from cachetools import TTLCache, cached

from fastapi import Depends, HTTPException, security

logger = logging.getLogger(__name__)

oauth2_scheme = security.OAuth2AuthorizationCodeBearer(
    authorizationUrl=settings.authorization_url,
    tokenUrl=settings.token_url,
    refreshUrl=settings.refresh_url,
)

client = boto3.client("secretsmanager")
response = client.get_secret_value(SecretId=settings.workflows_client_secret_id)
secrets = json.loads(response["SecretString"])
jwks_client = jwt.PyJWKClient(f"https://cognito-idp.{secrets['aws_region']}.amazonaws.com/{secrets['userpool_id']}/.well-known/jwks.json")  # Caches JWKS


def validated_token(
    token_str: Annotated[str, Security(oauth2_scheme)],
    required_scopes: security.SecurityScopes,
):
    # Parse & validate token
    try:
        token = jwt.decode(
            token_str,
            jwks_client.get_signing_key_from_jwt(token_str).key,
            algorithms=["RS256"],
        )
    except jwt.exceptions.InvalidTokenError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        ) from e

    # Validate scopes (if required)
    for scope in required_scopes.scopes:
        if scope not in token["scope"]:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Not enough permissions",
                headers={
                    "WWW-Authenticate": f'Bearer scope="{required_scopes.scope_str}"'
                },
            )

    return token
