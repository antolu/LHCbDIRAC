########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Environments/AdminEnv.sh,v 1.9 2009/02/04 08:10:50 atsareg Exp $
# File :   AdminEnv.sh
# Author : Ricardo Graciani
# Usage : ./AdminEnv.sh
########################################################################
__RCSID__="$Id: AdminEnv.sh,v 1.9 2009/02/04 08:10:50 atsareg Exp $"
__VERSION__="$Revision: 1.9 $"
export DIRACROOT=`dirname $0`
export DIRACROOT=`cd $DIRACROOT/.. ; pwd`
source $DIRACROOT/scripts/diracEnv.sh admin
$DIRACROOT/scripts/dirac-bash