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

python scripts/item.py "$@"
