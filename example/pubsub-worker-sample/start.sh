#!/bin/sh

$(gcloud beta emulators pubsub env-init)

export LOGGER=dev

#go build -o main

SERVICE_NAME="JOB_SCHEDULER_SERVICE" ./main \
    -ct topic1 \
    -cs topic1_sub \
    -pt topic2 \
    -id 1  \
    -d 5 & 

SERVICE_NAME="TRAINING_SERVICE" ./main \
    -ct topic2 \
    -cs topic2a_sub \
    -pt topic3 \
    -id 3 \
    -d 8 & 

SERVICE_NAME="EMAIL_SERVICE" ./main \
    -ct topic3 \
    -cs topic3_sub \
    -pt topic4 \
    -id 4 \
    -d 1 & 

SERVICE_NAME="WEBHOOK_SERVICE" ./main \
    -ct topic2 \
    -cs topic2b_sub \
    -pt topic5 \
    -id 2 \
    -d 1 & 
