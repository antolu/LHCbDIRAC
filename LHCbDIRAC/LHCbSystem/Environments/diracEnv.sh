#!/bin/bash
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Environments/diracEnv.sh,v 1.4 2009/02/04 08:10:50 atsareg Exp $
# File :   diracEnv.sh
# Author : Ricardo Graciani
########################################################################
__RCSID__="$Id: diracEnv.sh,v 1.4 2009/02/04 08:10:50 atsareg Exp $"
__VERSION__="$Revision: 1.4 $"

if ! [ $# = 1 ] ;then
  echo "usage : . diracEnv <role>"
  echo "   "
  echo " role is : production, admin, user, sgm, sam, GenericPilot"
  exit 0
fi

role=$1

export DIRACROOT=`dirname $0`
export DIRACROOT=`cd $DIRACROOT/.. ; pwd`

echo
echo Running DIRAC diracEnv.sh version $__VERSION__ with DIRACROOT=$DIRACROOT
echo


case $role in
    production)
           group=lhcb_prod
           ;;
    sam)
           group=lhcb_admin
           ;;
    sgm)
           group=lhcb_admin
           ;;
    admin)
           group=diracAdmin
           ;;
    GenericPilot)
           group=lhcb_pilot
           ;;
    user)
           group=lhcb_user
           ;;
   *)
     echo "This role does not exist " $role
     echo "set the default role as user"
     group=lhcb_user
     role=user
     ;;
esac

hostname | grep -q ".cern.ch" && source /afs/cern.ch/lhcb/scripts/GridEnv.sh

if [ ! -e ~/.lcgpasswd ] ; then
  echo -n "Certificate password: "
  stty -echo
  read userPasswd
  stty echo
else
  userPasswd=`cat ~/.lcgpasswd`
fi


export DIRACPLAT=`$DIRACROOT/scripts/platform.py`
export PATH=$DIRACROOT/scripts:$PATH

if ! echo $userPasswd | lhcb-proxy-init -d -g $group --pwstdin; then
  echo "You aren't allowed in the DIRAC $group group!"
  exit 1
fi

export PS1="(\[\e[1;31m\]DIRAC3-"$role"\[\e[0m\])[\u@\h \w]\$ "

