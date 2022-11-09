#!/usr/bin/env bash

echo "`date` COMMAND:" $(basename $(readlink -f $0)) "$@"
scriptdir="$(dirname $(readlink -f $0))"

cd $scriptdir/..
source venv/bin/activate

python -m cm.playlist "$@"
