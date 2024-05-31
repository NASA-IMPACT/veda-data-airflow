from pydantic import AnyHttpUrl, BaseSettings, Field, constr
from typing import Optional

AwsArn = constr(regex=r"^arn:aws:iam::\d{12}:role/.+")
AwsStepArn = constr(regex=r"^arn:aws:states:.+:\d{12}:stateMachine:.+")


class Settings(BaseSettings):
    cognito_domain: AnyHttpUrl = Field(
        description="The base url of the Cognito domain for authorization and token urls"
    )
    client_id: str = Field(description="The Cognito APP client ID")
    data_access_role_arn: Optional[AwsArn, str] = Field(  # type: ignore
        # Note: terraform sends the env. var as an empty string if it's not provided, so we need str type as well.
        description="ARN of AWS Role used to validate access to S3 data"
    )
    jwks_url: Optional[AnyHttpUrl] = Field(
        description="URL of JWKS, e.g. https://cognito-idp.{region}.amazonaws.com/{userpool_id}/.well-known/jwks.json"
    )

    workflows_client_secret_id: str = Field(
        description="The Cognito APP Secret that contains cognito creds"
    )
    stage: str = Field(description="API stage")
    workflow_root_path: str = Field(description="Root path of API")
    ingest_url: str = Field(description="URL of ingest API")
    raster_url: str = Field(description="URL of raster API")
    stac_url: str = Field(description="URL of STAC API")
    mwaa_env: str = Field(description="MWAA URL")

    @property
    def cognito_authorization_url(self) -> AnyHttpUrl:
        """Cognito user pool authorization url"""
        return f"{self.cognito_domain}/oauth2/authorize"

    @property
    def cognito_token_url(self) -> AnyHttpUrl:
        """Cognito user pool token and refresh url"""
        return f"{self.cognito_domain}/oauth2/token"

    class Config:
        env_file = ".env"
