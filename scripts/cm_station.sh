#! /bin/bash    

dir="$(dirname $(readlink -f $0))"

cd $dir/..
source venv/bin/activate

cd cm
python station.py "$@"
