#!/bin/sh
$(gcloud beta emulators pubsub env-init)
export LOGGER=dev
SERVICE_NAME="WEBHOOK_SERVICE" ./main \
    -ct topic2 \
    -cs topic2b_sub \
    -pt topic5 \
    -id 2 \
    -d 1