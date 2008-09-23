#!/bin/bash
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Environments/Attic/OldProductionEnv.sh,v 1.1 2008/09/23 06:20:48 joel Exp $
# File :   ProductionEnv.sh
# Author : Ricardo Graciani
# Usage : ./ProductionEnv.sh
########################################################################
__RCSID__="$Id: OldProductionEnv.sh,v 1.1 2008/09/23 06:20:48 joel Exp $"
__VERSION__="$Revision: 1.1 $"
export DIRACROOT=`dirname $0`
export DIRACROOT=`cd $DIRACROOT ; pwd`
source $DIRACROOT/diracEnv.sh production
bash -p
