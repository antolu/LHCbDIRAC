#!/bin/bash
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Environments/Attic/OldGenericPilotEnv.sh,v 1.1 2008/09/23 06:20:32 joel Exp $
# File :   GenericPilotEnv.sh
# Author : Ricardo Graciani
# Usage : ./GenericPilotEnv.sh
########################################################################
__RCSID__="$Id: OldGenericPilotEnv.sh,v 1.1 2008/09/23 06:20:32 joel Exp $"
__VERSION__="$Revision: 1.1 $"
export DIRACROOT=`dirname $0`
export DIRACROOT=`cd $DIRACROOT ; pwd`
source $DIRACROOT/diracEnv.sh GenericPilot
bash -p