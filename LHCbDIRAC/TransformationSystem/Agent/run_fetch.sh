#!/bin/bash
### 
### script that setups proper environment and downloads events from request
###

infile=$1
outfile=$2
LBVERSION=LBSCRIPTS_v7r8p1
BASEDIR=`dirname $0`
source /afs/cern.ch/lhcb/software/releases/LBSCRIPTS/$LBVERSION/InstallArea/scripts/LbLogin.sh
source /afs/cern.ch/lhcb/software/releases/LBSCRIPTS/$LBVERSION/InstallArea/scripts/SetupProject.sh Panoramix v22r0
#/var/www/grid-collector/fetch_event.py /var/eventindex/requests/req.running /var/eventindex/download_cache/x1.root
$BASEDIR/fetch_event.py $infile $outfile
