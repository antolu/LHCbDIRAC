#!/bin/csh
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Environments/diracEnv.csh,v 1.5 2008/10/17 12:58:12 rgracian Exp $
# File :   diracEnv.csh
# Author : Joel Closier
# usage : source diracEnv.csh <role>
########################################################################
set __RCSID__='$Id: diracEnv.csh,v 1.5 2008/10/17 12:58:12 rgracian Exp $'
set __VERSION__='$Revision: 1.5 $'

if ($#argv != 1) then
  echo "usage : source diracEnv.csh <role>"
  echo "   "
  echo " role is : production, admin, user, sgm, sam, GenericPilot"
  exit 0
endif

setenv DIRACROOT `dirname $0`
setenv DIRACROOT `cd $DIRACROOT ; pwd`

set role=$1

switch ( $role )
  case production:
                  set group=lhcb_prod
                  breaksw
  case admin:
             set group=diracAdmin
             breaksw
  case sam:
           set group=lhcb_admin
           breaksw
  case sgm:
           set group=lhcb_admin
           breaksw
  case user:
            set group=lhcb_user
            breaksw
  case GenericPilot:
                    set group=lhcb_pilot
                    breaksw
  default:
           echo "This role does not exist " $role
           echo "set the default role as user"
           set role = user
           set group=lhcb_user
           breaksw

endsw

echo
echo Running DIRAC diracEnv.csh version $__VERSION__ with DIRACROOT=$DIRACROOT
echo


hostname | grep -q ".cern.ch"
if $status == 0 then
   source /afs/cern.ch/lhcb/scripts/GridEnv.csh
endif

if ! ( -e ~/.lcgpasswd ) then
  echo -n "Certificate password: "
  stty -echo
  set userPasswd=$<
  stty echo
else
  set userPasswd=`cat ~/.lcgpasswd`
endif


setenv DIRACPLAT `$DIRACROOT/scripts/platform.py`
setenv PATH $DIRACROOT/scripts:$PATH

echo $userPasswd | lhcb-proxy-init -d -g $group --pwstdin
if $status == 1 then
  echo "You aren't allowed in the DIRAC $group group!"
  exit 1
endif

set prompt = '(DIRAC3-'$role') %B%n@%m %~\[\!]\>%b  '
