########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Environments/ProductionEnv.sh,v 1.8 2008/10/29 11:31:20 rgracian Exp $
# File :   ProductionEnv.sh
# Author : Ricardo Graciani
# Usage : ./ProductionEnv.sh
########################################################################
__RCSID__="$Id: ProductionEnv.sh,v 1.8 2008/10/29 11:31:20 rgracian Exp $"
__VERSION__="$Revision: 1.8 $"
export DIRACROOT=`dirname $0`
export DIRACROOT=`cd $DIRACROOT ; pwd`
source $DIRACROOT/diracEnv.sh production