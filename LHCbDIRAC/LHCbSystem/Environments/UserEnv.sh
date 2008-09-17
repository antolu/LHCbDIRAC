#!/bin/bash
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Environments/UserEnv.sh,v 1.4 2008/09/17 18:02:19 paterson Exp $
# File :   UserEnv.sh
# Author : Ricardo Graciani
# Usage : ./UserEnv.sh
########################################################################
__RCSID__="$Id: UserEnv.sh,v 1.4 2008/09/17 18:02:19 paterson Exp $"
__VERSION__="$Revision: 1.4 $"
export DIRACROOT=`dirname $0`
export DIRACROOT=`cd $DIRACROOT ; pwd`
source $DIRACROOT/diracEnv.sh user
bash -p