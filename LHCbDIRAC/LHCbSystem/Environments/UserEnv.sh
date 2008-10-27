#!/bin/bash
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Environments/UserEnv.sh,v 1.6 2008/10/27 16:35:19 atsareg Exp $
# File :   UserEnv.sh
# Author : Ricardo Graciani
# Usage : ./UserEnv.sh
########################################################################
__RCSID__="$Id: UserEnv.sh,v 1.6 2008/10/27 16:35:19 atsareg Exp $"
__VERSION__="$Revision: 1.6 $"
export DIRACROOT=`dirname $0`
export DIRACROOT=`cd $DIRACROOT ; pwd`
source $DIRACROOT/diracEnv.sh user
bash -p