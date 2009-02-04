########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Environments/SAMEnv.sh,v 1.9 2009/02/04 08:10:50 atsareg Exp $
# File :   SamEnv.sh
# Author : Ricardo Graciani
# Usage : ./SamEnv.sh
########################################################################
__RCSID__="$Id: SAMEnv.sh,v 1.9 2009/02/04 08:10:50 atsareg Exp $"
__VERSION__="$Revision: 1.9 $"
export DIRACROOT=`dirname $0`
export DIRACROOT=`cd $DIRACROOT/.. ; pwd`
source $DIRACROOT/scripts/diracEnv.sh sam
$DIRACROOT/scripts/dirac-bash