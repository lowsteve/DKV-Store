#!/usr/bin/env bash

# Before using:
# 1) ssh-keygen -> don't add a password and use the default name
# 2) ssh-copy-id utorID@ugXXX
# 3) enter your password
# 4) ssh-add
# You should now be able to turn this script without a password

SERVER_HOST=$1
SERVER_PORT=$2    #args[0]
CACHE_SIZE=$3     #args[1]
CACHE_STRAT=$4    #args[2]
SERVER_NAME=$5    #args[3]
ZK_HOST_NAME=$6   #args[4]
ZK_PORT=$7        #args[5]
LEVEL=$8          #args[6]


ssh -n $USER@$SERVER_HOST nohup java -jar $PWD/m2-server.jar $SERVER_PORT $CACHE_SIZE $CACHE_STRAT $SERVER_NAME $ZK_HOST_NAME $ZK_PORT $LEVEL &

