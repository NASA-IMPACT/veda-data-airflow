import logging

import boto3
import src.auth as auth
import src.config as config
import src.services as services

from fastapi import Depends, HTTPException, security

logger = logging.getLogger(__name__)

token_scheme = security.HTTPBearer()
