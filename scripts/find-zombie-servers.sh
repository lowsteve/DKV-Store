#!/usr/bin/env bash

# use 'find-zombie-servers.sh' to list any running kvServers
# use 'find-zombie-servers.sh -kill' to kill them if any 

if [ $# = 0 ] ; then
  ps aux | grep $USER | grep -F "m2-server" | grep -v "ssh" | grep -v "csh" |grep -v "grep" | grep -v "idea"
fi

if [ $# -gt 0 ] ; then
  if [ $1 = "-kill" ] ; then
    OUT="$(ps aux | grep $USER | grep -F "m2-server" | grep -v "ssh" | grep -v "csh" |grep -v "grep" | grep -v "idea" | awk '{print $2}')"
    PIDs=$(echo $OUT | tr " " "\n")
    for i in "${PIDs[@]}" ; do
      kill $i
    done
  fi
fi

