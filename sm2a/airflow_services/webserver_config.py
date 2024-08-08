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
"""Default configuration for the Airflow webserver."""
from __future__ import annotations


from flask_appbuilder.security.manager import AUTH_OAUTH

from airflow.auth.managers.fab.security_manager.override import (
    FabAirflowSecurityManagerOverride,
)
import logging
from typing import Any, Union
import os

basedir = os.path.abspath(os.path.dirname(__file__))

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = None

# ----------------------------------------------------
# AUTHENTICATION CONFIG
# ----------------------------------------------------
# For details on how to set up each of the following authentication, see
# http://flask-appbuilder.readthedocs.io/en/latest/security.html# authentication-methods
# for details.

# The authentication type
# AUTH_OID : Is for OpenID
# AUTH_DB : Is for database
# AUTH_LDAP : Is for LDAP
# AUTH_REMOTE_USER : Is for using REMOTE_USER from web server
# AUTH_OAUTH : Is for OAuth
AUTH_TYPE = AUTH_OAUTH

# Uncomment to setup Full admin role name
# AUTH_ROLE_ADMIN = 'Admin'

# Uncomment and set to desired role to enable access without authentication
# AUTH_ROLE_PUBLIC = 'Viewer'

# Will allow user self registration
# AUTH_USER_REGISTRATION = True

# The recaptcha it's automatically enabled for user self registration is active and the keys are necessary
# RECAPTCHA_PRIVATE_KEY = PRIVATE_KEY
# RECAPTCHA_PUBLIC_KEY = PUBLIC_KEY

# Config for Flask-Mail necessary for user self registration
# MAIL_SERVER = 'smtp.gmail.com'
# MAIL_USE_TLS = True
# MAIL_USERNAME = 'yourappemail@gmail.com'
# MAIL_PASSWORD = 'passwordformail'
# MAIL_DEFAULT_SENDER = 'sender@gmail.com'

AUTH_ROLES_SYNC_AT_LOGIN = True  # Checks roles on every login
AUTH_USER_REGISTRATION = (
    True  # allow users who are not already in the FAB DB to register
)
# Make sure to replace this with the path to your security manager class
AUTH_ROLES_MAPPING = {
    "Viewer": ["Viewer"],
    "Admin": ["Admin"],
}
# If you wish, you can add multiple OAuth providers.
OAUTH_PROVIDERS = [
    {
        "name": "github",
        "icon": "fa-github",
        "token_key": "access_token",
        "remote_app": {
            "client_id": os.getenv("GH_CLIENT_ID"),
            "client_secret": os.getenv("GH_CLIENT_SECRET"),
            "api_base_url": "https://api.github.com",
            "client_kwargs": {"scope": "read:user, read:org"},
            "access_token_url": "https://github.com/login/oauth/access_token",
            "authorize_url": "https://github.com/login/oauth/authorize",
            "request_token_url": None,
        },
    },
]


log = logging.getLogger(__name__)
log.setLevel(os.getenv("AIRFLOW__LOGGING__FAB_LOGGING_LEVEL", "INFO"))

FAB_ADMIN_ROLE = "Admin"
FAB_VIEWER_ROLE = "Viewer"
FAB_PUBLIC_ROLE = "Public"  # The "Public" role is given no permissions
TEAM_ID_A_FROM_GITHUB = os.getenv("GH_ADMIN_TEAM_ID")
TEAM_ID_B_FROM_GITHUB = os.getenv("GH_USER_TEAM_ID")


def team_parser(team_payload: dict[str, Any]) -> list[int]:
    # Parse the team payload from GitHub however you want here.
    return [team["name"] for team in team_payload]


def map_roles(team_list: list[int]) -> list[str]:
    # Associate the team IDs with Roles here.
    # The expected output is a list of roles that FAB will use to Authorize the user.

    team_role_map = {
        TEAM_ID_A_FROM_GITHUB: FAB_ADMIN_ROLE,
        TEAM_ID_B_FROM_GITHUB: FAB_VIEWER_ROLE,
    }
    return list(set(team_role_map.get(team, FAB_PUBLIC_ROLE) for team in team_list))


class GithubTeamAuthorizer(FabAirflowSecurityManagerOverride):
    # In this example, the oauth provider == 'github'.
    # If you ever want to support other providers, see how it is done here:
    # https://github.com/dpgaspar/Flask-AppBuilder/blob/master/flask_appbuilder/security/manager.py#L550
    def get_oauth_user_info(
        self, provider: str, resp: Any
    ) -> dict[str, Union[str, list[str]]]:
        # Creates the user info payload from Github.
        # The user previously allowed your app to act on their behalf,
        #   so now we can query the user and teams endpoints for their data.
        # Username and team membership are added to the payload and returned to FAB.

        remote_app = self.appbuilder.sm.oauth_remotes[provider]
        me = remote_app.get("user")
        user_data = me.json()
        team_data = remote_app.get("user/teams")
        teams = team_parser(team_data.json())
        roles = map_roles(teams)
        log.debug(f"User info from Github: {user_data}\nTeam info from Github: {teams}")
        print(f"User info from Github: {user_data}\nTeam info from Github: {teams}")
        return {"username": "github_" + user_data.get("login"), "role_keys": roles}


SECURITY_MANAGER_CLASS = GithubTeamAuthorizer
# The default user self registration role
# AUTH_USER_REGISTRATION_ROLE = "Public"

# When using OAuth Auth, uncomment to setup provider(s) info
# Google OAuth example:
# OAUTH_PROVIDERS = [{
#   'name':'google',
#     'token_key':'access_token',
#     'icon':'fa-google',
#         'remote_app': {
#             'api_base_url':'https://www.googleapis.com/oauth2/v2/',
#             'client_kwargs':{
#                 'scope': 'email profile'
#             },
#             'access_token_url':'https://accounts.google.com/o/oauth2/token',
#             'authorize_url':'https://accounts.google.com/o/oauth2/auth',
#             'request_token_url': None,
#             'client_id': GOOGLE_KEY,
#             'client_secret': GOOGLE_SECRET_KEY,
#         }
# }]

# When using LDAP Auth, setup the ldap server
# AUTH_LDAP_SERVER = "ldap://ldapserver.new"

# When using OpenID Auth, uncomment to setup OpenID providers.
# example for OpenID authentication
# OPENID_PROVIDERS = [
#    { 'name': 'Yahoo', 'url': 'https://me.yahoo.com' },
#    { 'name': 'AOL', 'url': 'http://openid.aol.com/<username>' },
#    { 'name': 'Flickr', 'url': 'http://www.flickr.com/<username>' },
#    { 'name': 'MyOpenID', 'url': 'https://www.myopenid.com' }]

# ----------------------------------------------------
# Theme CONFIG
# ----------------------------------------------------
# Flask App Builder comes up with a number of predefined themes
# that you can use for Apache Airflow.
# http://flask-appbuilder.readthedocs.io/en/latest/customizing.html#changing-themes
# Please make sure to remove "navbar_color" configuration from airflow.cfg
# in order to fully utilize the theme. (or use that property in conjunction with theme)
# APP_THEME = "bootstrap-theme.css"  # default bootstrap
# APP_THEME = "amelia.css"
# APP_THEME = "cerulean.css"
# APP_THEME = "cosmo.css"
# APP_THEME = "cyborg.css"
# APP_THEME = "darkly.css"
# APP_THEME = "flatly.css"
# APP_THEME = "journal.css"
# APP_THEME = "lumen.css"
# APP_THEME = "paper.css"
# APP_THEME = "readable.css"
# APP_THEME = "sandstone.css"
# APP_THEME = "simplex.css"
# APP_THEME = "slate.css"
# APP_THEME = "solar.css"
# APP_THEME = "spacelab.css"
# APP_THEME = "superhero.css"
# APP_THEME = "united.css"
# APP_THEME = "yeti.css"
