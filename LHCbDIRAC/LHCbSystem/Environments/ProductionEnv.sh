#!/bin/bash
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Environments/ProductionEnv.sh,v 1.5 2008/09/17 18:01:24 paterson Exp $
# File :   ProductionEnv.sh
# Author : Ricardo Graciani
# Usage : ./ProductionEnv.sh
########################################################################
__RCSID__="$Id: ProductionEnv.sh,v 1.5 2008/09/17 18:01:24 paterson Exp $"
__VERSION__="$Revision: 1.5 $"
export DIRACROOT=`dirname $0`
export DIRACROOT=`cd $DIRACROOT ; pwd`
source $DIRACROOT/diracEnv.sh production
bash -p
