#!/bin/bash
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Environments/GenericPilotEnv.sh,v 1.7 2008/10/27 16:31:52 atsareg Exp $
# File :   GenericPilotEnv.sh
# Author : Ricardo Graciani
# Usage : ./GenericPilotEnv.sh
########################################################################
__RCSID__="$Id: GenericPilotEnv.sh,v 1.7 2008/10/27 16:31:52 atsareg Exp $"
__VERSION__="$Revision: 1.7 $"
export DIRACROOT=`dirname $0`
export DIRACROOT=`cd $DIRACROOT ; pwd`
source $DIRACROOT/diracEnv.sh GenericPilot
bash -p