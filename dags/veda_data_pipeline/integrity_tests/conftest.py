import os

"""
Define any env variables expected in test files.
Airflow variables are in format AIRFLOW_VAR_XXX
"""
TEST_ENV_VARS = {
    'AIRFLOW_VAR_MWAA_STACK_CONF': "{\"foo\": \"bar\"}",
    'AIRFLOW_VAR_VECTOR_SECRET_NAME': "{\"foo\": \"bar\"}"
}


def pytest_configure(config):
    """Configure and init envvars for airflow."""
    print("loading PYTEST CONF")
    config.old_env = {}
    for key, value in TEST_ENV_VARS.items():
        config.old_env[key] = os.getenv(key)
        os.environ[key] = value
        print('updating: ', key, value)


def pytest_unconfigure(config):
    """Restore envvars to old values."""
    for key, value in config.old_env.items():
        if value is None:
            del os.environ[key]
        else:
            os.environ[key] = value