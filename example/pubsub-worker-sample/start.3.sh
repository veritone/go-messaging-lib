#!/bin/sh
$(gcloud beta emulators pubsub env-init)
export LOGGER=dev
SERVICE_NAME="EMAIL_SERVICE" ./main \
    -ct topic3 \
    -cs topic3_sub \
    -pt topic4 \
    -id 4 \
    -d 1