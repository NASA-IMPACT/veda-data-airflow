"""
Custom Airflow API auth backend: Keycloak Bearer tokens.

Configured via `[api] auth_backends = keycloak_bearer_auth` in `airflow.cfg`.

Docs: https://airflow.apache.org/docs/apache-airflow-providers-fab/1.5.4/auth-manager/api-authentication.html#roll-your-own-api-authentication
"""

from __future__ import annotations

import functools
import logging
import os
from typing import Any, Callable

import jwt
import requests
from flask import Response, request
from flask_appbuilder.security.manager import AUTH_DB, AUTH_LDAP, AUTH_OAUTH
from flask_appbuilder.security.sqla.models import User
from flask_login import login_user

from airflow.www.extensions import get_auth_manager

log = logging.getLogger(__name__)


class _AuthError(Exception):
    def __init__(self, response: Response):
        self.response = response


def init_app(app) -> None:
    pass


def requires_authentication(fn: Callable) -> Callable:

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any):
        try:
            user = _auth_current_user()
        except _AuthError as err:
            return err.response

        if user is not None:
            return fn(*args, **kwargs)
        return Response("Unauthorized", 401, {"WWW-Authenticate": "Bearer"})

    return wrapper


def _auth_current_user() -> User | None:
    client_id = os.getenv("KEYCLOAK_CLIENT_ID", "")
    ab_security_manager = get_auth_manager().security_manager
    user = None
    if ab_security_manager.auth_type == AUTH_OAUTH:
        if 'Authorization' not in request.headers or not request.headers['Authorization']:
            return None

        token = str(request.headers['Authorization']).replace("Bearer ", "")
        token_decoded = jwt.decode(token, options={"verify_signature": False, "verify_aud": False})

        try:
            claims = _introspect_token(token)
        except Exception:
            log.exception("Keycloak token introspection failed")
            raise _AuthError(_unauthorized("Token introspection failed"))

        if not claims.get("active"):
            raise _AuthError(_unauthorized("Token is not active"))

        token_roles = claims.get("resource_access", {}).get(client_id, {}).get("roles", [])
        allowed_roles = ["Admin", "User", "Dag_Launcher"]
        if not any(role in token_roles for role in allowed_roles):
            raise _AuthError(_forbidden("Token is valid but missing required role"))

        username = token_decoded.get("preferred_username") or token_decoded.get("username") or token_decoded.get("sub", "")
        userinfo = {
            "username": username,
            "email": token_decoded.get("email"),
            "first_name": token_decoded.get("given_name"),
            "last_name": token_decoded.get("family_name"),
            "role_keys": token_roles,
        }
        user = ab_security_manager.auth_user_oauth(userinfo)
    else:
        auth = request.authorization
        if auth is None or not auth.username or not auth.password:
            return None
        if ab_security_manager.auth_type == AUTH_LDAP:
            user = ab_security_manager.auth_user_ldap(auth.username, auth.password)
        if ab_security_manager.auth_type == AUTH_DB:
            user = ab_security_manager.auth_user_db(auth.username, auth.password)

    log.info("user: %s", user)
    if user is not None:
        login_user(user, remember=False)
    return user


def _unauthorized(message: str) -> Response:
    return Response(
        message,
        status=401,
        headers={"WWW-Authenticate": 'Bearer realm="airflow"'},
        mimetype="text/plain",
    )


def _forbidden(message: str) -> Response:
    return Response(
        message,
        status=403,
        mimetype="text/plain",
    )


def _keycloak_config() -> tuple[str, str, str, str]:
    base_url = os.getenv("KEYCLOAK_BASE_URL", "").rstrip("/")
    realm = os.getenv("KEYCLOAK_REALM", "")
    client_id = os.getenv("KEYCLOAK_CLIENT_ID", "")
    client_secret = os.getenv("KEYCLOAK_CLIENT_SECRET", "")

    if not (base_url and realm and client_id and client_secret):
        raise RuntimeError(
            "Missing Keycloak env vars. Required: KEYCLOAK_BASE_URL, KEYCLOAK_REALM, "
            "KEYCLOAK_CLIENT_ID, KEYCLOAK_CLIENT_SECRET"
        )
    return base_url, realm, client_id, client_secret


def _introspect_token(token: str) -> dict[str, Any]:
    base_url, realm, client_id, client_secret = _keycloak_config()
    url = f"{base_url}/realms/{realm}/protocol/openid-connect/token/introspect"

    resp = requests.post(
        url,
        data={"token": token},
        auth=(client_id, client_secret),
        timeout=10,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"Keycloak introspection failed: {resp.status_code} {resp.text}")
    data = resp.json()
    return data
