#!/bin/bash
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Environments/ProductionEnv.sh,v 1.10 2009/02/04 09:22:52 atsareg Exp $
# File :   ProductionEnv.sh
# Author : Ricardo Graciani
# Usage : ./ProductionEnv.sh
########################################################################
__RCSID__="$Id: ProductionEnv.sh,v 1.10 2009/02/04 09:22:52 atsareg Exp $"
__VERSION__="$Revision: 1.10 $"
export DIRACROOT=`dirname $0`
export DIRACROOT=`cd $DIRACROOT/.. ; pwd`
source $DIRACROOT/scripts/diracEnv.sh production
$DIRACROOT/scripts/dirac-bash