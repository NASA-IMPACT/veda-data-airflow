import logging

import jwt

from fastapi import Depends, HTTPException, Security, security, status
from typing import Annotated, Any, Dict

from src.config import settings


logger = logging.getLogger(__name__)

oauth2_scheme = security.OAuth2AuthorizationCodeBearer(
    authorizationUrl=settings.cognito_authorization_url,
    tokenUrl=settings.cognito_token_url,
    refreshUrl=settings.cognito_token_url,
)

jwks_client = jwt.PyJWKClient(settings.jwks_url)


def validated_token(
    token_str: Annotated[str, Security(oauth2_scheme)],
    required_scopes: security.SecurityScopes,
) -> Dict[str, Any]:
    """
    Returns an access token payload, see: https://docs.aws.amazon.com/cognito/latest/developerguide/amazon-cognito-user-pools-using-the-access-token.html
    """
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


def get_username(token: Annotated[Dict[Any, Any], Depends(validated_token)]) -> str:
    return token["username"]
