#!/usr/bin/env bash

REMOTE_MACHINE=$1
NUM_PAIRS=$2
OUTPUT_FILE=$3

ssh -n $REMOTE_MACHINE nohup cd ece419/ece419_m2 && java -jar $PWD/perfclient.jar $NUM_PAIRS $OUTPUT_FILE &
