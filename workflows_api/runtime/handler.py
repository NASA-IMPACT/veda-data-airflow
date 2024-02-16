"""
Entrypoint for Lambda execution.
"""

from mangum import Mangum
from src.main import workflows_app

handler = Mangum(workflows_app, lifespan="off")
