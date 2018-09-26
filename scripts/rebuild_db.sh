#!/usr/bin/env bash

echo "`date` COMMAND:" $(basename $(readlink -f $0)) "$@"
scriptdir="$(dirname $(readlink -f $0))"

cd $scriptdir/..
source venv/bin/activate

python cm/database.py --drop --force --debug=1
python cm/database.py --create --debug=1
