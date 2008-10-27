#!/bin/bash
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Environments/ProductionEnv.sh,v 1.7 2008/10/27 16:35:19 atsareg Exp $
# File :   ProductionEnv.sh
# Author : Ricardo Graciani
# Usage : ./ProductionEnv.sh
########################################################################
__RCSID__="$Id: ProductionEnv.sh,v 1.7 2008/10/27 16:35:19 atsareg Exp $"
__VERSION__="$Revision: 1.7 $"
export DIRACROOT=`dirname $0`
export DIRACROOT=`cd $DIRACROOT ; pwd`
source $DIRACROOT/diracEnv.sh production
bash -p
