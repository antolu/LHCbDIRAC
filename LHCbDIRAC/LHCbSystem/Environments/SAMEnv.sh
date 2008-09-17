#!/bin/bash
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Environments/SAMEnv.sh,v 1.5 2008/09/17 18:01:57 paterson Exp $
# File :   SamEnv.sh
# Author : Ricardo Graciani
# Usage : ./SamEnv.sh
########################################################################
__RCSID__="$Id: SAMEnv.sh,v 1.5 2008/09/17 18:01:57 paterson Exp $"
__VERSION__="$Revision: 1.5 $"
export DIRACROOT=`dirname $0`
export DIRACROOT=`cd $DIRACROOT ; pwd`
source $DIRACROOT/diracEnv.sh sam
bash -p