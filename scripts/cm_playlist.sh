#! /bin/bash    

scriptdir="$(dirname $(readlink -f $0))"

cd $scriptdir/..
source venv/bin/activate

python cm/playlist.py "$@"
