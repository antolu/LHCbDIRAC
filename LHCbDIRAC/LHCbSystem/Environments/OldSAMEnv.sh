#!/bin/bash
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Environments/Attic/OldSAMEnv.sh,v 1.1 2008/09/23 06:21:00 joel Exp $
# File :   SamEnv.sh
# Author : Ricardo Graciani
# Usage : ./SamEnv.sh
########################################################################
__RCSID__="$Id: OldSAMEnv.sh,v 1.1 2008/09/23 06:21:00 joel Exp $"
__VERSION__="$Revision: 1.1 $"
export DIRACROOT=`dirname $0`
export DIRACROOT=`cd $DIRACROOT ; pwd`
source $DIRACROOT/diracEnv.sh sam
bash -p