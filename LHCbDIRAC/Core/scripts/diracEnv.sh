#!/bin/bash
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Environments/diracEnv.sh,v 1.8 2009/08/16 16:45:12 rgracian Exp $
# File :   diracEnv.sh
# Author : Ricardo Graciani
########################################################################
__RCSID__="$Id: diracEnv.sh,v 1.8 2009/08/16 16:45:12 rgracian Exp $"
__VERSION__="$Revision: 1.8 $"

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

hostname | grep -q ".cern.ch" && source `which GridEnv.sh`

if [ ! -e ~/.lcgpasswd ] ; then
  echo -n "Certificate password: "
  stty -echo
  read userPasswd
  stty echo
else
  userPasswd=`cat ~/.lcgpasswd`
fi

[ -z "$DIRACPLAT" ]        && export DIRACPLAT=`$DIRACROOT/scripts/dirac-platform`
# If not defined it will prevent grid commands in lcg tar to find CAs
[ -z "$X509_CERT_DIR" ]    && export X509_CERT_DIR="$DIRACROOT/etc/grid-security/certificates"
# If not defined it will prevent voms commands DIRAC to determine voms servers
[ -z "$DIRAC_VOMSES" ]     && export DIRAC_VOMSES="$DIRACROOT/etc/grid-security/vomses"
# If not defined it will prevent voms commands in lcg tar to check voms server credentials
[ -z "$X509_VOMS_DIR" ]    && export X509_VOMS_DIR="$DIRACROOT/etc/grid-security/vomsdir"
# If not defined it will prevent gsissh command in lcg tar to work
[ -z "$GLOBUS_LOCATION" ]  && export GLOBUS_LOCATION="$DIRACROOT/$DIRACPLAT"

export PATH=$DIRACROOT/$DIRACPLAT/bin:$DIRACROOT/scripts:$PATH
export DIRACPYTHON=`which python`
if ! echo $userPasswd | lhcb-proxy-init -d -g $group --pwstdin; then
  echo "You aren't allowed in the DIRAC $group group!"
  exit 1
fi

export PS1="(\[\e[1;31m\]DIRAC3-"$role"\[\e[0m\])[\u@\h \w]\$ "

