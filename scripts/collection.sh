#! /bin/bash
# Check .env file


DOT_ENV=$1

if [ -f $DOT_ENV ]
then
  set -a; source $DOT_ENV; set +a
  shift
else
  echo "Run: ./scripts/item.sh <.env_file>"
  echo "Please create $DOT_ENV file first and try again"
  exit 1
fi

if [ $STAGE == "staging" ]
then
    export SECRET_NAME=delta-backend-stagingv2/pgstac/5d4eb447
else
    export SECRET_NAME=veda-backend-uah-dev/pgstac/621feede
fi

python scripts/collection.py "$@"
