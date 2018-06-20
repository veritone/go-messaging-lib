#!/bin/sh
$(gcloud beta emulators pubsub env-init)
export LOGGER=dev
SERVICE_NAME="JOB_SCHEDULER_SERVICE" ./main \
    -ct topic1 \
    -cs topic1_sub \
    -pt topic2 \
    -id 1  \
    -d 5