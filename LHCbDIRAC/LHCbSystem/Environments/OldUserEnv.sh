#!/bin/bash
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Environments/Attic/OldUserEnv.sh,v 1.1 2008/09/23 06:21:13 joel Exp $
# File :   UserEnv.sh
# Author : Ricardo Graciani
# Usage : ./UserEnv.sh
########################################################################
__RCSID__="$Id: OldUserEnv.sh,v 1.1 2008/09/23 06:21:13 joel Exp $"
__VERSION__="$Revision: 1.1 $"
export DIRACROOT=`dirname $0`
export DIRACROOT=`cd $DIRACROOT ; pwd`
source $DIRACROOT/diracEnv.sh user
bash -p