########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Environments/AdminEnv.sh,v 1.8 2008/10/29 11:31:20 rgracian Exp $
# File :   AdminEnv.sh
# Author : Ricardo Graciani
# Usage : ./AdminEnv.sh
########################################################################
__RCSID__="$Id: AdminEnv.sh,v 1.8 2008/10/29 11:31:20 rgracian Exp $"
__VERSION__="$Revision: 1.8 $"
export DIRACROOT=`dirname $0`
export DIRACROOT=`cd $DIRACROOT ; pwd`
source $DIRACROOT/diracEnv.sh admin
