########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Environments/GenericPilotEnv.sh,v 1.9 2009/02/04 08:10:50 atsareg Exp $
# File :   GenericPilotEnv.sh
# Author : Ricardo Graciani
# Usage : ./GenericPilotEnv.sh
########################################################################
__RCSID__="$Id: GenericPilotEnv.sh,v 1.9 2009/02/04 08:10:50 atsareg Exp $"
__VERSION__="$Revision: 1.9 $"
export DIRACROOT=`dirname $0`
export DIRACROOT=`cd $DIRACROOT/.. ; pwd`
source $DIRACROOT/scripts/diracEnv.sh GenericPilot
$DIRACROOT/scripts/dirac-bash