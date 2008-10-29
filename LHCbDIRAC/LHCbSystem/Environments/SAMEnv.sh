########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Environments/SAMEnv.sh,v 1.8 2008/10/29 11:31:20 rgracian Exp $
# File :   SamEnv.sh
# Author : Ricardo Graciani
# Usage : ./SamEnv.sh
########################################################################
__RCSID__="$Id: SAMEnv.sh,v 1.8 2008/10/29 11:31:20 rgracian Exp $"
__VERSION__="$Revision: 1.8 $"
export DIRACROOT=`dirname $0`
export DIRACROOT=`cd $DIRACROOT ; pwd`
source $DIRACROOT/diracEnv.sh sam