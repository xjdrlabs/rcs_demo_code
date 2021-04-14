#!/usr/bin/env bash

while getopts f:p:v:t: flag
do
    case "${flag}" in
        f) function=${OPTARG};;
        p) project=${OPTARG};;
        v) vpc_connector=${OPTARG};;
        t) topic=${OPTARG};;
    esac
done
echo "Deploying Pub/Sub triggered cloud function"
echo "Function: $function";
echo "Project: $project";
echo "VPC Connector: $vpc_connector";
echo "Trigger Topic: $topic"

# VPC Connector
#projects/<project_id>/locations/us-central1/connectors/us-central1-connector

function deploy_cloud_function() {
  pipenv install
  pipenv lock -r > cloud_functions/on_event/requirements.txt
  pushd cloud_functions/on_event
  gcloud functions deploy $function \
    --project $project \
    --runtime python37 \
    --trigger-topic $topic \
    --region us-central1 \
    --vpc-connector $vpc_connector \
    --set-env-vars REDISHOST=10.58.90.20,REDISPORT=6379
  popd
  cloud_functions/on_event/requirements.txt
}

deploy_cloud_function
