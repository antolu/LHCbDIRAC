#!/bin/csh
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Environments/diracEnv.csh,v 1.12 2009/08/16 16:45:25 rgracian Exp $
# File :   diracEnv.csh
# Author : Joel Closier
# usage : source diracEnv.csh <role>
########################################################################
set __RCSID__='$Id: diracEnv.csh,v 1.12 2009/08/16 16:45:25 rgracian Exp $'
set __VERSION__='$Revision: 1.12 $'

if ($#argv != 1) then
  echo "usage : source diracEnv.csh <role>"
  echo "   "
  echo " role is : production, admin, user, sgm, sam, GenericPilot"
  exit 0
endif

setenv DIRACROOT `dirname $0`
setenv DIRACROOT `cd $DIRACROOT/.. ; pwd`

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

[ -z "$DIRACPLAT" ]        && setenv DIRACPLAT        `$DIRACROOT/scripts/platform.py`
# If not defined it will prevent grid commands in lcg tar to find CAs
[ -z "$X509_CERT_DIR" ]    && setenv X509_CERT_DIR    "$DIRACROOT/etc/grid-security/certificates"
# If not defined it will prevent voms commands DIRAC to determine voms servers
[ -z "$DIRAC_VOMSES" ]     && setenv DIRAC_VOMSES     "$DIRACROOT/etc/vomses"
# If not defined it will prevent voms commands in lcg tar to check voms server credentials
[ -z "$X509_VOMS_DIR" ]    && setenv X509_VOMS_DIR    "$DIRACROOT/etc/grid-security/vomsdir"
# If not defined it will prevent gsissh command in lcg tar to work
[ -z "$GLOBUS_LOCATION" ]  && setenv GLOBUS_LOCATION  "$DIRACROOT/$DIRACPLAT"

setenv PATH $DIRACROOT/$DIRACPLAT/bin:$DIRACROOT/scripts:$PATH
setenv DIRACPYTHON `which python`
echo $userPasswd | lhcb-proxy-init -d -g $group --pwstdin
if $status == 1 then
  echo "You aren't allowed in the DIRAC $group group!"
  exit 1
endif

set prompt = '(DIRAC3-'$role') %B%n@%m %~\[\!]\>%b  '

setenv PS1 "(\[\e[1;31m\]DIRAC3-$role\[\e[0m\])[\u@\h \w]"'$'" "
