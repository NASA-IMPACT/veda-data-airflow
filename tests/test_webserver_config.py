"""This test guards against any oauth url changes become tuples"""

import ast
from pathlib import Path

CONFIG = (
    Path(__file__).resolve().parents[1] / "airflow_services" / "webserver_config.py"
)
URL_KEYS = {"api_base_url", "access_token_url", "authorize_url", "jwks_uri"}


def test_keycloak_oauth_urls_are_not_tuples():
    tree = ast.parse(CONFIG.read_text())
    bad_urls = [
        key.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Dict)
        for key, value in zip(node.keys, node.values, strict=False)
        if isinstance(key, ast.Constant)
        and key.value in URL_KEYS
        and isinstance(value, ast.Tuple)
    ]
    assert not bad_urls, (
        f"{bad_urls} - A tuple has been found! "
        "Remove the trailing comma inside parentheses to fix it"
    )
