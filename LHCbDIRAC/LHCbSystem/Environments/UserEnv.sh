########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Environments/UserEnv.sh,v 1.7 2008/10/29 11:31:20 rgracian Exp $
# File :   UserEnv.sh
# Author : Ricardo Graciani
# Usage : ./UserEnv.sh
########################################################################
__RCSID__="$Id: UserEnv.sh,v 1.7 2008/10/29 11:31:20 rgracian Exp $"
__VERSION__="$Revision: 1.7 $"
export DIRACROOT=`dirname $0`
export DIRACROOT=`cd $DIRACROOT ; pwd`
source $DIRACROOT/diracEnv.sh user