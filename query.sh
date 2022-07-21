#!/bin/sh
# exit when any command fails
set -e

echo "${1}" | python jina_app.py -t query -k 10000
#echo "A data supplier (or data vendor or data provider) is an organization or business that provides data for use of consumption by third parties" | python app.py -t query