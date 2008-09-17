#!/bin/bash
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Environments/GenericPilotEnv.sh,v 1.5 2008/09/17 18:01:03 paterson Exp $
# File :   GenericPilotEnv.sh
# Author : Ricardo Graciani
# Usage : ./GenericPilotEnv.sh
########################################################################
__RCSID__="$Id: GenericPilotEnv.sh,v 1.5 2008/09/17 18:01:03 paterson Exp $"
__VERSION__="$Revision: 1.5 $"
export DIRACROOT=`dirname $0`
export DIRACROOT=`cd $DIRACROOT ; pwd`
source $DIRACROOT/diracEnv.sh GenericPilot
bash -p