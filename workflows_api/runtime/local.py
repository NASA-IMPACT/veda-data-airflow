""" Script used to run the API locally"""

import uvicorn
from src import main

if __name__ == "__main__":
    uvicorn.run(main.workflows_app, host="0.0.0.0", port=8000)
