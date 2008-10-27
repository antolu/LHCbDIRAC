#!/bin/bash
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Environments/AdminEnv.sh,v 1.7 2008/10/27 16:35:19 atsareg Exp $
# File :   AdminEnv.sh
# Author : Ricardo Graciani
# Usage : ./AdminEnv.sh
########################################################################
__RCSID__="$Id: AdminEnv.sh,v 1.7 2008/10/27 16:35:19 atsareg Exp $"
__VERSION__="$Revision: 1.7 $"
export DIRACROOT=`dirname $0`
export DIRACROOT=`cd $DIRACROOT ; pwd`
source $DIRACROOT/diracEnv.sh admin
bash -p
