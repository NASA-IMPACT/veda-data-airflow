from pydantic import AnyHttpUrl, BaseSettings, Field, constr

AwsArn = constr(regex=r"^arn:aws:iam::\d{12}:role/.+")
AwsStepArn = constr(regex=r"^arn:aws:states:.+:\d{12}:stateMachine:.+")


class Settings(BaseSettings):
    data_access_role_arn: AwsArn = Field(  # type: ignore
        description="ARN of AWS Role used to validate access to S3 data"
    )

    cognito_app_secret: str = Field(description="The Cognito APP Secret that contains cognito creds")
    stage: str = Field(description="API stage")
    workflow_root_path: str = Field(description="Root path of API")
    ingest_url: str = Field(description="URL of ingest API")
    raster_url: str = Field(description="URL of raster API")
    stac_url: str = Field(description="URL of STAC API")
    mwaa_env: str = Field(description="MWAA URL")

    class Config:
        env_file = ".env"
