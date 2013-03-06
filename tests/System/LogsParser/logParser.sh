#!/bin/bash

# ubeda@cern.ch
#-------------------------------------------------------------------------------

dirac-proxy-info
if [ ! $? -eq 0 ]
then
  echo Please get a valid proxy
  dirac-proxy-init
#  exit 1
fi

matches=$( ls /opt/dirac/startup | awk -F _ '{print $1}' | sort -u )

timestamp=$(date +%s)
mkdir -p /tmp/finder/$timestamp

echo 'Scanning...'

for i in $matches
do

  python componentsScript.py -m $i -g --lastRelease> /tmp/finder/$timestamp/$i.txt
  echo "  $i"

done

echo find logs on /tmp/finder/$timestamp 

#-------------------------------------------------------------------------------
#EOF