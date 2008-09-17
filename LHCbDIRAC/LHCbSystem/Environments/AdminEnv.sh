#!/bin/bash
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Environments/AdminEnv.sh,v 1.5 2008/09/17 18:00:28 paterson Exp $
# File :   AdminEnv.sh
# Author : Ricardo Graciani
# Usage : ./AdminEnv.sh
########################################################################
__RCSID__="$Id: AdminEnv.sh,v 1.5 2008/09/17 18:00:28 paterson Exp $"
__VERSION__="$Revision: 1.5 $"
export DIRACROOT=`dirname $0`
export DIRACROOT=`cd $DIRACROOT ; pwd`
source $DIRACROOT/diracEnv.sh admin
bash -p
