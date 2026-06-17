import os
from flask import Blueprint
from airflow.plugins_manager import AirflowPlugin

bp = Blueprint("display_version_bp", __name__)


@bp.app_context_processor
def _deployment_context():
    veda_airflow_version = os.getenv("VEDA_AIRFLOW_VERSION", "")
    git_sha = os.getenv("GIT_SHA", "")
    return {
        "veda_airflow_version": veda_airflow_version if veda_airflow_version else "unknown",
        "deployment_git_sha": git_sha[:7] if git_sha else "unknown",
    }


class DisplayVersionPlugin(AirflowPlugin):
    name = "display_version"
    flask_blueprints = [bp]
