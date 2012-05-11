#!/bin/bash

echo "start"

cat $hostname

sleep 20

python async-centralized.py --type=REMD --configfile=remd.conf
