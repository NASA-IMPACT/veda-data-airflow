#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Keycloak OAuth configuration for the Airflow webserver."""
from __future__ import annotations
from base64 import b64decode

from flask_appbuilder.security.manager import AUTH_OAUTH

from airflow.auth.managers.fab.security_manager.override import (
    FabAirflowSecurityManagerOverride,
)
import logging
from typing import Any, Union
import os
import jwt
from cryptography.hazmat.primitives import serialization
import requests

basedir = os.path.abspath(os.path.dirname(__file__))

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = None

# ----------------------------------------------------
# AUTHENTICATION CONFIG
# ----------------------------------------------------
AUTH_TYPE = AUTH_OAUTH

AUTH_ROLES_SYNC_AT_LOGIN = True  # Checks roles on every login
AUTH_USER_REGISTRATION = (
    True  # allow users who are not already in the FAB DB to register
)

# Map Keycloak roles/groups to Airflow roles
AUTH_ROLES_MAPPING = {
    "Viewer": ["Viewer"],
    "Admin": ["Admin"],
    "Dag_Launcher": ["DAG Launcher"],
}

# Keycloak OAuth Provider Configuration
KEYCLOAK_BASE_URL = os.getenv("KEYCLOAK_BASE_URL")  # e.g., https://keycloak.example.com
KEYCLOAK_REALM = os.getenv("KEYCLOAK_REALM")  # Your realm name
KEYCLOAK_CLIENT_ID = os.getenv("KEYCLOAK_CLIENT_ID")
KEYCLOAK_CLIENT_SECRET = os.getenv("KEYCLOAK_CLIENT_SECRET")

OAUTH_PROVIDERS = [
    {
        "name": "keycloak",
        "icon": "fa-key",
        "token_key": "access_token",
        "remote_app": {
            "client_id": KEYCLOAK_CLIENT_ID,
            "client_secret": KEYCLOAK_CLIENT_SECRET,
            "api_base_url": f"{KEYCLOAK_BASE_URL}/realms/{KEYCLOAK_REALM}/protocol/openid-connect",
            "client_kwargs": {
                "scope": "openid email profile"
            },
            "access_token_url": f"{KEYCLOAK_BASE_URL}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/token",
            "authorize_url": f"{KEYCLOAK_BASE_URL}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/auth",
            "request_token_url": None,
            "jwks_uri": f"{KEYCLOAK_BASE_URL}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/certs",
        },
    },
]

log = logging.getLogger(__name__)
log.setLevel(os.getenv("AIRFLOW__LOGGING__FAB_LOGGING_LEVEL", "INFO"))

FAB_ADMIN_ROLE = "Admin"
FAB_VIEWER_ROLE = "Viewer"
FAB_DAG_LAUNCHER_ROLE = "Dag_Launcher"
FAB_PUBLIC_ROLE = "Public"  # The "Public" role is given no permissions

req = requests.get(f"{KEYCLOAK_BASE_URL}/realms/{KEYCLOAK_REALM}/")
key_der_base64 = req.json()["public_key"]
key_der = b64decode(key_der_base64.encode())
public_key = serialization.load_der_public_key(key_der)


def extract_roles_from_keycloak(userinfo: dict[str, Any], resp: dict[str, Any]) -> list[str]:
    """
    Extract roles from Keycloak token.
    
    Keycloak can provide roles in multiple ways:
    1. realm_access.roles - Realm-level roles
    2. resource_access.{client_id}.roles - Client-specific roles
    3. groups - User groups (if configured in Keycloak)
    
    Adjust this function based on your Keycloak configuration.
    """
    roles = []

    log.info(f"Extracting roles from Keycloak response: {resp}")
    access_token = resp.get("access_token", "")
    
    try:
        decoded = jwt.decode(access_token, public_key, algorithms=["RS256"], options={"verify_signature": False})
        if "resource_access" in decoded and KEYCLOAK_CLIENT_ID in decoded["resource_access"]:
            roles.extend(decoded["resource_access"][KEYCLOAK_CLIENT_ID].get("roles", []))
        log.info(f"Decoded roles from access_token: {roles}")
    except Exception as e:
        log.warning(f"Failed to decode access_token: {e}")
    
    log.info(f"Extracted roles from Keycloak: {roles}")
    return roles


def map_keycloak_roles_to_fab(keycloak_roles: list[str]) -> list[str]:
    """
    Map Keycloak roles to Airflow FAB roles.
    
    Customize this mapping based on your Keycloak role names.
    Example mappings:
    - Keycloak "airflow-admin" or "admin" -> FAB "Admin"
    - Keycloak "airflow-viewer" or "viewer" -> FAB "Viewer"
    - Keycloak "airflow-dag-launcher" or "dag_launcher" -> FAB "Dag_Launcher"
    """
    role_mapping = {
        # Keycloak role name : Airflow FAB role
        "airflow-admin": FAB_ADMIN_ROLE,
        "airflow-viewer": FAB_VIEWER_ROLE,
        "airflow-dag-launcher": FAB_DAG_LAUNCHER_ROLE,
    }
    
    fab_roles = set()
    for kc_role in keycloak_roles:
        fab_role = role_mapping.get(kc_role.lower(), None)
        if fab_role:
            fab_roles.add(fab_role)
    
    # If no roles matched, assign Public role (no permissions)
    if not fab_roles:
        fab_roles.add(FAB_PUBLIC_ROLE)
    
    log.info(f"Mapped Keycloak roles {keycloak_roles} to FAB roles {list(fab_roles)}")
    return list(fab_roles)


class KeycloakAuthorizer(FabAirflowSecurityManagerOverride):
    """
    Custom security manager for Keycloak OAuth integration.
    
    This class handles the OAuth flow with Keycloak and maps
    Keycloak roles/groups to Airflow FAB roles.
    """
    
    def get_oauth_user_info(
        self, provider: str, resp: Any
    ) -> dict[str, Union[str, list[str]]]:
        """
        Get user info from Keycloak OAuth response.
        
        Args:
            provider: OAuth provider name (should be "keycloak")
            resp: OAuth response object
            
        Returns:
            Dictionary containing username and role_keys for FAB
        """
        if provider != "keycloak":
            log.warning(f"Unexpected OAuth provider: {provider}")
            return {"username": "unknown", "role_keys": [FAB_PUBLIC_ROLE]}
        
        remote_app = self.appbuilder.sm.oauth_remotes[provider]
        
        # Get user info from Keycloak userinfo endpoint
        endpoint = "openid-connect/userinfo"
        log.info(f"Fetching user info from Keycloak endpoint: {endpoint}")
        userinfo_response = remote_app.get(endpoint)
        userinfo = userinfo_response.json()
        log.info(f"Raw user info from Keycloak: {userinfo}")
        
        # Extract username (preferred_username is standard in Keycloak)
        username = userinfo.get("preferred_username") or userinfo.get("email") or userinfo.get("sub")
        
        # Extract roles from Keycloak
        keycloak_roles = extract_roles_from_keycloak(userinfo, resp)
        
        # Map Keycloak roles to FAB roles
        fab_roles = map_keycloak_roles_to_fab(keycloak_roles)
        
        log.info(f"User info from Keycloak: username={username}, roles={fab_roles}")
        
        return {
            "username": f"keycloak_{username}",
            "first_name": userinfo.get("given_name", ""),
            "last_name": userinfo.get("family_name", ""),
            "email": userinfo.get("email", ""),
            "role_keys": fab_roles,
        }


SECURITY_MANAGER_CLASS = KeycloakAuthorizer

# ----------------------------------------------------
# Theme CONFIG
# ----------------------------------------------------
# Flask App Builder comes up with a number of predefined themes
# that you can use for Apache Airflow.
# http://flask-appbuilder.readthedocs.io/en/latest/customizing.html#changing-themes
# Please make sure to remove "navbar_color" configuration from airflow.cfg
# in order to fully utilize the theme. (or use that property in conjunction with theme)
# APP_THEME = "bootstrap-theme.css"  # default bootstrap
