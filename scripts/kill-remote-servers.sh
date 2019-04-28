#!/usr/bin/env bash

for i in $(seq 132 180); do
  echo "Killing servers on ug$i"
  ssh -n ug$i nohup bash /nfs/ug/homes-5/l/lowsteve/ece419/ece419_m2/scripts/find-zombie-servers.sh -kill
done
echo "Killing servers on ug51"
ssh -n ug51 nohup bash /nfs/ug/homes-5/l/lowsteve/ece419/ece419_m2/scripts/find-zombie-servers.sh -kill
