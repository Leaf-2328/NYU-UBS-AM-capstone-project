#!/bin/sh
# exit when any command fails
set -e

# Chek if docker is running
#docker run -d -p 80:80 docker/getting-started

pip install -r requirements.txt
python jina_app.py -t index -n 20000
echo "data supplier provider vendor" | python jina_app.py -t query
#echo "A data supplier (or data vendor or data provider) is an organization or business that provides data for use of consumption by third parties" | python app.py -t query -k 15000