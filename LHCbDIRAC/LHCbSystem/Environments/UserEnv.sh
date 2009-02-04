#!/bin/bash
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Environments/UserEnv.sh,v 1.9 2009/02/04 09:22:52 atsareg Exp $
# File :   UserEnv.sh
# Author : Ricardo Graciani
# Usage : ./UserEnv.sh
########################################################################
__RCSID__="$Id: UserEnv.sh,v 1.9 2009/02/04 09:22:52 atsareg Exp $"
__VERSION__="$Revision: 1.9 $"
export DIRACROOT=`dirname $0`
export DIRACROOT=`cd $DIRACROOT/.. ; pwd`
source $DIRACROOT/scripts/diracEnv.sh user
$DIRACROOT/scripts/dirac-bash