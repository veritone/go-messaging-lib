#!/bin/sh
$(gcloud beta emulators pubsub env-init)
export LOGGER=dev
SERVICE_NAME="TRAINING_SERVICE" ./main \
    -ct topic2 \
    -cs topic2a_sub \
    -pt topic3 \
    -id 3 \
    -d 8