#!/bin/bash

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
VERSIONFILE=$SCRIPT_DIR"/gis_fillers/_version.py"

echo "Old version:"
cat $VERSIONFILE
echo $(cat $VERSIONFILE | awk -F. '/[0-9]\./{$NF++;print}' OFS=.)"'" > $VERSIONFILE

echo "New version:"
cat $VERSIONFILE

git commit -am 'bumping version number'
git push
