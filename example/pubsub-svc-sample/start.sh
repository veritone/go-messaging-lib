#!/bin/sh

$(gcloud beta emulators pubsub env-init)
export LOGGER=dev
SERVICE_NAME="GRAPHQL" ./main -p 8090

