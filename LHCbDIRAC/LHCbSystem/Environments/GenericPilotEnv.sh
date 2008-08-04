#!/bin/bash
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Environments/GenericPilotEnv.sh,v 1.2 2008/08/04 18:13:57 rgracian Exp $
# File :   GenericPilotEnv.sh
# Author : Ricardo Graciani
########################################################################
__RCSID__="$Id: GenericPilotEnv.sh,v 1.2 2008/08/04 18:13:57 rgracian Exp $"
__VERSION__="$Revision: 1.2 $"

export DIRACROOT=`dirname $0`
export DIRACROOT=`cd $DIRACROOT ; pwd`

echo
echo Running DIRAC GenericPilotEnv.sh version $__VERSION__ with DIRACROOT=$DIRACROOT
echo

hostname | grep -q ".cern.ch" && source /afs/cern.ch/lhcb/scripts/GridEnv.sh

if [ ! -e ~/.lcgpasswd ] ; then
  echo -n "Certificate password: "
  stty -echo
  read userPasswd
  stty echo
else
  userPasswd=`cat ~/.lcgpasswd`
fi

group=lhcb_pilot

export DPLAT=`$DIRACROOT/scripts/platform.py`
export PATH=$DIRACROOT/$DPLAT/bin:$DIRACROOT/scripts:$PATH
export LD_LIBRARY_PATH=$DIRACROOT/$DPLAT/lib:$LD_LIBRARY_PATH
export PYTHONPATH=$DIRACROOT

if ! echo $userPasswd | lhcb-proxy-init -d -g $group --pwstdin; then
  echo "You aren't allowed in the DIRAC $group group!"
  exit 1
fi

export PS1="(\[\e[1;34m\]DIRAC3-Pilot\[\e[0m\])[\u@\h \w]\$ "
bash -p
