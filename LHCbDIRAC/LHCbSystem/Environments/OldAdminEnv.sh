#!/bin/bash
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Environments/Attic/OldAdminEnv.sh,v 1.1 2008/09/23 06:20:18 joel Exp $
# File :   AdminEnv.sh
# Author : Ricardo Graciani
# Usage : ./AdminEnv.sh
########################################################################
__RCSID__="$Id: OldAdminEnv.sh,v 1.1 2008/09/23 06:20:18 joel Exp $"
__VERSION__="$Revision: 1.1 $"
export DIRACROOT=`dirname $0`
export DIRACROOT=`cd $DIRACROOT ; pwd`
source $DIRACROOT/diracEnv.sh admin
bash -p
