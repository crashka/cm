#!/usr/bin/env bash

echo "`date` COMMAND:" $(basename $(readlink -f $0)) "$@"
scriptdir="$(dirname $(readlink -f $0))"

cd $scriptdir/..
source venv/bin/activate

python -m cm.database --drop --force --debug=1
python -m cm.database --create --debug=1
