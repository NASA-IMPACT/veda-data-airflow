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
from flask import Response, current_app, request
from flask_login import login_user

log = logging.getLogger(__name__)

def init_app(app) -> None:
    pass

def requires_authentication(fn: Callable) -> Callable:

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any):
        token = _extract_bearer_token()
        if not token:
            return _unauthorized("Missing Bearer token")

        try:
            claims = _introspect_token(token)
        except Exception:
            log.exception("Keycloak token introspection failed")
            return _unauthorized("Token introspection failed")

        if not claims.get("active"):
            return _unauthorized("Token is not active")

        token_roles = _extract_roles(token)
        allowed_roles = ["Admin", "User", "Dag_Launcher"]
        if not any(role in token_roles for role in allowed_roles):
            return _forbidden("Token is valid but missing required role")

        user = _get_or_create_fab_user(claims, token_roles)
        if not user:
            return _unauthorized("Could not find or create Airflow user")
        login_user(user, remember=False, force=True)

        return fn(*args, **kwargs)

    return wrapper


def _extract_bearer_token() -> str | None:
    auth = request.headers.get("Authorization", "")
    if not auth:
        return None
    parts = auth.split(None, 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    return parts[1].strip() or None


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


def _get_or_create_fab_user(claims: dict[str, Any], token_roles: list[str]):
    sm = current_app.appbuilder.sm
    username = claims.get("preferred_username") or claims.get("username") or claims.get("sub", "")

    user = sm.find_user(username=username)
    if user is None:
        user = sm.find_user(username=f"keycloak_{username}")

    if user is None:
        fab_roles = []
        for role_name in token_roles:
            role = sm.find_role(role_name)
            if role:
                fab_roles.append(role)
        if not fab_roles:
            fab_roles = [sm.find_role("Viewer")]

        user = sm.add_user(
            username=username,
            first_name=claims.get("given_name", "Service"),
            last_name=claims.get("family_name", "Account"),
            email=claims.get("email", f"{username}@keycloak"),
            role=fab_roles,
        )
        log.info("Auto-provisioned FAB user %r with roles %s", username, [r.name for r in fab_roles])

    return user


def _extract_roles(token: str) -> set[str]:
    client_id = os.getenv("KEYCLOAK_CLIENT_ID", "")
    decoded = jwt.decode(token, options={"verify_signature": False, "verify_aud": False})
    roles = decoded.get("resource_access").get(client_id, {}).get("roles", [])
    return roles
