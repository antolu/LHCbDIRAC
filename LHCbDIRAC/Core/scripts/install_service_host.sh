#!/bin/bash
########################################################################
# File :   install_base.sh
########################################################################
#
# User that is allow to execute the script
DIRACUSER=dirac
#
# Host where it es allow to run the script
DIRACHOST=volhcb19.cern.ch
#
# Location of the installation
DESTDIR=/opt/dirac
#
SiteName=VOLHCB19.CERN.CH
DIRACSETUP=LHCb-NewProduction
DIRACVERSION=v5r0p0-pre22
DIRACARCH=Linux_x86_64_glibc-2.5
DIRACPYTHON=25
DIRACDIRS="startup runit control work data requestDB"
DIRACDBs=""
LCGVERSION=2009-08-13

export DIRACINSTANCE=NewProduction
export LOGLEVEL=INFO
#
# Uncomment to install from CVS (default install from TAR)
# it implies -b (build from sources)
#DIRACCVS=yes
#
# check if we are called in the rigth host
if [ "`hostname`" != "$DIRACHOST" ] ; then
  echo $0 should be run at $DIRACHOST
fi
# check if we are the right user
if [ $USER != $DIRACUSER ] ; then
  echo $0 should be run by $DIRACUSER
  exit
fi
# make sure $DESTDIR is available
mkdir -p $DESTDIR || exit 1

CURDIR=`dirname $0`
CURDIR=`cd $CURDIR; pwd -P`

ROOT=`dirname $DESTDIR`/dirac

echo
echo "Installing under $ROOT"
echo
[ -d $ROOT ] || exit

if [ ! -d $DESTDIR/etc ]; then
  mkdir -p $DESTDIR/etc || exit 1
fi
if [ ! -e $DESTDIR/etc/dirac.cfg ] ; then
  cat >> $DESTDIR/etc/dirac.cfg << EOF || exit
LocalSite
{
  EnableAgentMonitoring = yes
}
DIRAC
{
  Setup = $DIRACSETUP
  Configuration
  {
    Servers = dips://lhcbprod.pic.es:9135/Configuration/Server
    Servers += dips://lhcb-conf-dirac.cern.ch:9135/Configuration/Server
    Name = LHCb-Prod
  }
  Security
  {
    CertFile = $DESTDIR/etc/grid-security/hostcert.pem
    KeyFile = $DESTDIR/etc/grid-security/hostkey.pem
  }
}
Systems
{
  Databases
  {
    User = Dirac
    Password = To be Defined
  }
}
EOF

fi

for dir in $DIRACDIRS ; do
  if [ ! -d $DESTDIR/$dir ]; then
    mkdir -p $DESTDIR/$dir || exit 1
  fi
done

# give an unique name to dest directory
# VERDIR
VERDIR=$DESTDIR/versions/${DIRACVERSION}-`date +"%s"`
mkdir -p $VERDIR   || exit 1

echo python dirac-install.py -t server -P $VERDIR -r $DIRACVERSION -g $LCGVERSION -p $DIRACARCH -i $DIRACPYTHON -e LHCbDIRAC || exit 1
     python dirac-install.py -t server -P $VERDIR -r $DIRACVERSION -g $LCGVERSION -p $DIRACARCH -i $DIRACPYTHON -e LHCbDIRAC || exit 1

echo 

for dir in etc $DIRACDIRS ; do
  ln -s ../../$dir $VERDIR   || exit 1
done

$VERDIR/scripts/dirac-configure -n $SiteName --UseServerCertificate -o /LocalSite/Root=$ROOT/pro -V lhcb --SkipCAChecks || exit 1

echo
#
# Create pro and old links
old=$DESTDIR/old
pro=$DESTDIR/pro
[ -L $old ] && rm $old; [ -e $old ] && exit 1; [ -L $pro ] && mv $pro $old; [ -e $pro ] && exit 1; ln -s $VERDIR $pro || exit 1

#
# Create bin link
ln -sf pro/$DIRACARCH/bin $DESTDIR/bin
##
## Compile all python files .py -> .pyc, .pyo
##
cmd="from compileall import compile_dir ; compile_dir('"$DESTDIR/pro"', force=1, quiet=True )"
$DESTDIR/pro/$DIRACARCH/bin/python -c "$cmd" 1> /dev/null || exit 1
$DESTDIR/pro/$DIRACARCH/bin/python -O -c "$cmd" 1> /dev/null  || exit 1

$DESTDIR/pro/scripts/install_bashrc.sh    $DESTDIR $DIRACVERSION $DIRACARCH python$DIRACPYTHON || exit 1

#
# fix user .bashrc
#
grep -q "source $DESTDIR/bashrc" $HOME/.bashrc || \
  echo "source $DESTDIR/bashrc" >> $HOME/.bashrc

#
# Configure MySQL if not yet done
#
if [ ! -z "$DIRACDBs" ] ; then
  source $DESTDIR/pro/scripts/install_mysql.sh $DIRACHOST
  $DESTDIR/pro/mysql/share/mysql/mysql.server start

  #
  # Check necessary DBs are there
  #
  $DESTDIR/pro/scripts/install_mysql_db.sh $DIRACDBs
fi

##############################################################
# INSTALL SERVICES
# (modify SystemName and ServiceName)
#
$DESTDIR/pro/scripts/install_oracle-client.sh

$DESTDIR/pro/scripts/install_service.sh Configuration        Server

$DESTDIR/pro/scripts/install_service.sh Bookkeeping          BookkeepingManager

$DESTDIR/pro/scripts/install_service.sh DataManagement       DataIntegrity
$DESTDIR/pro/scripts/install_service.sh DataManagement       RAWIntegrity
$DESTDIR/pro/scripts/install_service.sh DataManagement       ReplicationPlacement
$DESTDIR/pro/scripts/install_service.sh DataManagement       StorageUsage
$DESTDIR/pro/scripts/install_service.sh DataManagement       TransferDBMonitoring
$DESTDIR/pro/scripts/install_service.sh DataManagement       DataLogging
$DESTDIR/pro/scripts/install_service.sh DataManagement       LcgFileCatalogProxy

$DESTDIR/pro/scripts/install_service.sh Framework            UserProfileManager
$DESTDIR/pro/scripts/install_service.sh Framework            Notification
$DESTDIR/pro/scripts/install_service.sh Framework            Plotting
$DESTDIR/pro/scripts/install_service.sh Framework            SystemLogging
$DESTDIR/pro/scripts/install_service.sh Framework            SystemLoggingReport

$DESTDIR/pro/scripts/install_service.sh ProductionManagement ProductionManager
$DESTDIR/pro/scripts/install_service.sh ProductionManagement ProductionRequest

$DESTDIR/pro/scripts/install_service.sh RequestManagement    RequestManager

$DESTDIR/pro/scripts/install_service.sh ResourceStatus       ResourceStatus

$DESTDIR/pro/scripts/install_service.sh Stager               Stager

$DESTDIR/pro/scripts/install_service.sh WorkloadManagement   JobManager
$DESTDIR/pro/scripts/install_service.sh WorkloadManagement   JobMonitoring
$DESTDIR/pro/scripts/install_service.sh WorkloadManagement   JobStateUpdate
$DESTDIR/pro/scripts/install_service.sh WorkloadManagement   JobStateUpdate
$DESTDIR/pro/scripts/install_service.sh WorkloadManagement   Matcher
$DESTDIR/pro/scripts/install_service.sh WorkloadManagement   WMSAdministrator


# If any special CS entried required modify and uncomment the following:

#cat > $DESTDIR/etc/SystemName_ServiceName.cfg <<EOF
#Systems
#{
#  SystemName
#  {
#    $DIRACINSTANCE
#    {
#      Services
#      {
#        ServiceName
#        {
#          Option = Value
#        }
#      }
#    }
#  }
#}
#EOF

##############################################################
# INSTALL AGENTS
# (modify SystemName and AgentName)
#

# $DESTDIR/pro/scripts/install_agent.sh Configuration UsersAndGroups

# If any special CS entried required modify and uncomment the following:
#cat > $DESTDIR/etc/SystemName_AgentName.cfg <<EOF
#Systems
#{
#  SystemName
#  {
#    $DIRACINSTANCE
#    {
#      Agents
#      {
#        AgentName
#        {
#          Option = Value
#        }
#      }
#    }
#  }
#}
#EOF

######################################################################

if [ ! -z "$DIRACCVS" ] ; then


        cd `dirname $DESTDIR`
        mv DIRAC3/DIRAC DIRAC3/DIRAC.save

echo
echo
echo   To get a CVS installation:
echo
echo   "   login with your own user"
echo   "   start a bash shell"
echo   "   execute"
echo
echo
cat << EOF
umask 0002
export CVSROOT=:kserver:isscvs.cern.ch:/local/reps/dirac
cd `dirname $DESTDIR`
cvs -Q co -r $DIRACVERSION DIRAC3/DIRAC DIRAC3/LHCbSystem
cvs update -A DIRAC3/DIRAC DIRAC3/LHCbSystem
cd DIRAC3/DIRAC
ln -s ../LHCbSystem .
EOF

fi

exit