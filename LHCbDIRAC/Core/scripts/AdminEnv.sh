#!/bin/bash
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Environments/AdminEnv.sh,v 1.10 2009/02/04 09:22:52 atsareg Exp $
# File :   AdminEnv.sh
# Author : Ricardo Graciani
# Usage : ./AdminEnv.sh
########################################################################
__RCSID__="$Id: AdminEnv.sh,v 1.10 2009/02/04 09:22:52 atsareg Exp $"
__VERSION__="$Revision: 1.10 $"
export DIRACROOT=`dirname $0`
export DIRACROOT=`cd $DIRACROOT/.. ; pwd`
source $DIRACROOT/scripts/diracEnv.sh admin
$DIRACROOT/scripts/dirac-bash