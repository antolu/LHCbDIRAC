#!/bin/bash
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Environments/SAMEnv.sh,v 1.7 2008/10/27 16:35:19 atsareg Exp $
# File :   SamEnv.sh
# Author : Ricardo Graciani
# Usage : ./SamEnv.sh
########################################################################
__RCSID__="$Id: SAMEnv.sh,v 1.7 2008/10/27 16:35:19 atsareg Exp $"
__VERSION__="$Revision: 1.7 $"
export DIRACROOT=`dirname $0`
export DIRACROOT=`cd $DIRACROOT ; pwd`
source $DIRACROOT/diracEnv.sh sam
bash -p