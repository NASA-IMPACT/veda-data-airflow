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

from airflow.providers.fab.auth_manager.security_manager.override import (
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

# Make sure you create these role on Keycloak
AUTH_ROLES_MAPPING = {
    "Viewer": ["Viewer"],
    "Admin": ["Admin"],
    "User": ["User"],
    "Public": ["Public"],
    "Op": ["Op"],
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


_keycloak_public_key = None


def _get_keycloak_public_key():
    global _keycloak_public_key
    if _keycloak_public_key is None:
        req = requests.get(f"{KEYCLOAK_BASE_URL}/realms/{KEYCLOAK_REALM}/")
        key_der_base64 = req.json()["public_key"]
        key_der = b64decode(key_der_base64.encode())
        _keycloak_public_key = serialization.load_der_public_key(key_der)
    return _keycloak_public_key


def extract_roles_from_keycloak(userinfo: dict[str, Any], resp: dict[str, Any]) -> list[str]:
    """
    Extract roles from Keycloak token. Possible roles: Admin, Viewer, Public, Dag_Launcher 
    See Airflow default roles: https://airflow.apache.org/docs/apache-airflow-providers-fab/stable/auth-manager/access-control.html#default-roles 
    
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
        decoded = jwt.decode(access_token, _get_keycloak_public_key(), algorithms=["RS256"], options={"verify_signature": False})
        if "resource_access" in decoded and KEYCLOAK_CLIENT_ID in decoded["resource_access"]:
            roles.extend(decoded["resource_access"][KEYCLOAK_CLIENT_ID].get("roles", []))
        log.info(f"Decoded roles from access_token: {roles}")
    except Exception as e:
        log.warning(f"Failed to decode access_token: {e}")
    
    log.info(f"Extracted roles from Keycloak: {roles}")
    return roles


class KeycloakAuthorizer(FabAirflowSecurityManagerOverride):
    """
    Custom security manager for Keycloak OAuth integration.
    
    This class handles the OAuth flow with Keycloak and maps
    Keycloak roles/groups to Airflow FAB roles.

    On initialization, ensures the DAG Launcher role exists with the correct permissions.
    """

    role_name = "DAG Launcher"
    permissions = [
        ("can_read", "My Profile"),
        ("can_create", "DAG Runs"),
        ("can_read", "DAG Runs"),
        ("can_edit", "DAG Runs"),
        ("menu_access", "DAG Runs"),
        ("menu_access", "Browse"),
        ("can_read", "Jobs"),
        ("menu_access", "Jobs"),
        ("can_read", "Task Instances"),
        ("menu_access", "Task Instances"),
        ("can_read", "XComs"),
        ("menu_access", "DAGs"),
        ("menu_access", "Documentation"),
        ("menu_access", "Docs"),
        ("can_read", "DAG Dependencies"),
        ("can_read", "Task Logs"),
        ("can_read", "Website"),
        ("can_edit", "DAG:veda_discover"),
        ("can_read", "DAG:veda_discover"),
        ("can_edit", "DAG:veda_dataset_pipeline"),
        ("can_read", "DAG:veda_dataset_pipeline"),
        ("can_edit", "DAG:veda_collection_pipeline"),
        ("can_read", "DAG:veda_collection_pipeline"),
    ]

    def __init__(self, appbuilder):
        super().__init__(appbuilder)
        
        role = self.find_role(self.role_name)
        if not role:
            role = self.add_role(self.role_name)

        for perm_name, view_menu_name in self.permissions:
            self.add_permissions_menu(view_menu_name)
            permission = self.get_permission(perm_name, view_menu_name)
            if permission and permission not in role.permissions:
                self.add_permission_to_role(role, permission)

        log.info(f"Role '{self.role_name}' created with specified permissions.")


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
            return {"username": "unknown", "role_keys": ["Public"]}
        
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
         
        log.info(f"User info from Keycloak: username={username}, roles={keycloak_roles}")
        
        return {
            "username": f"keycloak_{username}",
            "first_name": userinfo.get("given_name", ""),
            "last_name": userinfo.get("family_name", ""),
            "email": userinfo.get("email", ""),
            "role_keys": keycloak_roles,
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
