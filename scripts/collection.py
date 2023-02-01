import os
import sys

from pypgstac.load import Loader, Methods
from pypgstac.db import PgstacDB

from utils import args_handler, data_files, DATA_PATH, get_secret


collections_path = os.path.join(DATA_PATH, "collections")


def get_dsn_string(secret: dict, localhost: bool = False) -> str:
    """Form database connection string from a dictionary of connection secrets

    Args:
        secret (dict): dictionary containing connection secrets including username, database name, host, and password

    Returns:
        dsn (str): full database data source name
    """
    if localhost:
        host = "localhost"
        port = 9999
    else:
        host = secret["host"]
        port = secret["port"]

    return f"postgres://{secret['username']}:{secret['password']}@{host}:{port}/{secret.get('dbname', 'postgis')}"


def insert_collection(collection_ndjson):
    secret_name = os.environ.get("SECRET_NAME")
    con_secrets = get_secret(secret_name)
    dsn = get_dsn_string(con_secrets)

    with PgstacDB(dsn=dsn, debug=False) as db:
        loader = Loader(db=db)
        loader.load_collections(collection_ndjson, Methods.upsert)


def insert_collections(files):
    print("Inserting collections:")
    for file in files:
        print(file)
        try:
            insert_collection(file)
            print("Inserted")
        except:
            print("Error inserting collection.")
            raise


if __name__ == "__main__":
    collection_regex = sys.argv[1]
    files = data_files(collection_regex, collections_path)
    insert_collections(files)
