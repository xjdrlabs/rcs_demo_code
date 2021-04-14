#!/usr/bin/env bash

while getopts f:p:v: flag
do
    case "${flag}" in
        f) function=${OPTARG};;
        p) project=${OPTARG};;
        v) vpc_connector=${OPTARG};;
    esac
done

set -eu

echo "Deploying"
echo "Cloud Function: $function";
echo "Project: $project";
echo "VPC Connector: $vpc_connector";

# VPC Connector
#projects/<project_id>/locations/us-central1/connectors/us-central1-connector

function deploy_cloud_function() {
  pipenv install
  pipenv lock -r > cloud_functions/frontdoor/requirements.txt
  pushd cloud_functions/frontdoor
  gcloud functions deploy $function \
    --project $project \
    --runtime python37 \
    --trigger-http \
    --region us-central1 \
    --source $PWD \
    --vpc-connector $vpc_connector \
    --set-env-vars REDISHOST=10.58.90.20,REDISPORT=6379
  popd
  cloud_functions/frontdoor/requirements.txt
}

deploy_cloud_function
