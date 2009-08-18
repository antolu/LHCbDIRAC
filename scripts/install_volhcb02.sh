#!/bin/bash
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/scripts/install_volhcb02.sh,v 1.19 2009/08/18 16:32:17 rgracian Exp $
# File :   install_volhcb01.sh
# Author : Ricardo Graciani
########################################################################
#
# Preliminary commets:
# - volhcb02 has 180 GB of free space under /home.
# - link from /opt/dirac -> /home/dirac/opt/dirac created to hold the installation
#
#
#
#
# User that is allow to execute the script
DIRACUSER=dirac
#
# Host where it es allow to run the script
DIRACHOST=volhcb02.cern.ch
#
# Location of the installation
DESTDIR=/opt/dirac
#
SiteName=VOLHCB02.CERN.CH
DIRACSETUP=LHCb-Development
DIRACVERSION=HEAD
EXTVERSION=HEAD
DIRACARCH=Linux_x86_64_glibc-2.3.4
DIRACPYTHON=25
DIRACDIRS="startup runit data work control requestDB"

export DIRACINSTANCE=Development
export LOGLEVEL=VERBOSE
#
# Uncomment to install from CVS (default install from TAR)
# it implies -b (build from sources)
DIRACCVS=yes
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
# check if the mask is properly set
if [ "`umask`" != "0002" ] ; then
  echo umask should be set to 0002 at system level for users
  exit
fi
#
# make sure $DESTDIR is available
mkdir -p $DESTDIR || exit 1

CURDIR=`dirname $0`
CURDIR=`cd $CURDIR; pwd -P`

ROOT=`dirname $DESTDIR`/DIRAC3

echo
echo "Installing under $ROOT"
echo
[ -L $ROOT ] || ln -sf $DESTDIR/pro $ROOT || exit

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
    Servers += dips://lhcb-wms-dirac.cern.ch:9135/Configuration/Server
    Name = LHCb-Prod
  }
  Security
  {
    CertFile = $DESTDIR/etc/grid-security/hostcert.pem
    KeyFile = $DESTDIR/etc/grid-security/hostkey.pem
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
for dir in etc $DIRACDIRS ; do
  ln -s ../../$dir $VERDIR   || exit 1
done

# to make sure we do not use DIRAC python
dir=`echo $DESTDIR/pro/$DIRACARCH/bin | sed 's/\//\\\\\//g'`
PATH=`echo $PATH | sed "s/$dir://"`

echo $CURDIR/dirac-install -S -P $VERDIR -v $DIRACVERSION -e $EXTVERSION -p $DIRACARCH -i $DIRACPYTHON -o /LocalSite/Root=$ROOT -o /LocalSite/Site=$SiteName 2>/dev/null || exit 1
     $CURDIR/dirac-install -S -P $VERDIR -v $DIRACVERSION -e $EXTVERSION -p $DIRACARCH -i $DIRACPYTHON -o /LocalSite/Root=$ROOT -o /LocalSite/Site=$SiteName || exit 1

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

chmod +x $DESTDIR/pro/scripts/install_bashrc.sh
$DESTDIR/pro/scripts/install_bashrc.sh    $DESTDIR $DIRACVERSION $DIRACARCH python$DIRACPYTHON || exit 1

#
# fix user .bashrc
#
grep -q "export CVSROOT=:pserver:anonymous@isscvs.cern.ch:/local/reps/dirac" $HOME/.bashrc || \
  echo "export CVSROOT=:pserver:anonymous@isscvs.cern.ch:/local/reps/dirac" >>  $HOME/.bashrc
grep -q "source $DESTDIR/bashrc" $HOME/.bashrc || \
  echo "source $DESTDIR/bashrc" >> $HOME/.bashrc
chmod +x $DESTDIR/pro/scripts/install_service.sh
cp $CURDIR/dirac-install $DESTDIR/pro/scripts

#
# Configure MySQL if not yet done
#

$CURDIR/install_mysql.sh $DIRACHOST
sed -i 's/^log-bin=/# log-bin=/' $DESTDIR/pro/mysql/etc/my.cnf
/opt/dirac/pro/mysql/share/mysql/mysql.server start

$DESTDIR/pro/scripts/install_service.sh Configuration Server

$DESTDIR/pro/scripts/install_service.sh WorkloadManagement JobManager
$DESTDIR/pro/scripts/install_service.sh WorkloadManagement JobMonitoring
$DESTDIR/pro/scripts/install_service.sh WorkloadManagement InputSandbox
$DESTDIR/pro/scripts/install_service.sh WorkloadManagement OutputSandbox
$DESTDIR/pro/scripts/install_service.sh WorkloadManagement JobStateUpdate
$DESTDIR/pro/scripts/install_service.sh WorkloadManagement Matcher
$DESTDIR/pro/scripts/install_service.sh WorkloadManagement WMSAdministrator
$DESTDIR/pro/scripts/install_agent.sh   WorkloadManagement JobHistoryAgent
# Missing in CS
$DESTDIR/pro/scripts/install_agent.sh   WorkloadManagement StatesAccountingAgent
$DESTDIR/pro/scripts/install_agent.sh   WorkloadManagement InputDataAgent
# $DESTDIR/pro/scripts/install_agent.sh   WorkloadManagement JobPathAgent
# $DESTDIR/pro/scripts/install_agent.sh   WorkloadManagement JobPolicyAgent
# $DESTDIR/pro/scripts/install_agent.sh   WorkloadManagement JobSanityAgent
# $DESTDIR/pro/scripts/install_agent.sh   WorkloadManagement JobSchedulingAgent
# $DESTDIR/pro/scripts/install_agent.sh   WorkloadManagement ProcessingDBAgent
# $DESTDIR/pro/scripts/install_agent.sh   WorkloadManagement TaskQueueAgent
$DESTDIR/pro/scripts/install_agent.sh   WorkloadManagement MightyOptimizer
$DESTDIR/pro/scripts/install_agent.sh   WorkloadManagement StalledJobAgent
$DESTDIR/pro/scripts/install_agent.sh   WorkloadManagement JobCleaningAgent
$DESTDIR/pro/scripts/install_agent.sh   WorkloadManagement Director
$DESTDIR/pro/scripts/install_agent.sh   WorkloadManagement PilotStatusAgent
$DESTDIR/pro/scripts/install_agent.sh   WorkloadManagement PilotMonitor

$DESTDIR/pro/scripts/install_service.sh Monitoring SiteMapping
$DESTDIR/pro/scripts/install_agent.sh   Monitoring SiteMappingAgent

$DESTDIR/pro/scripts/install_service.sh ProductionManagement ProductionManager
$DESTDIR/pro/scripts/install_agent.sh   ProductionManagement ProductionJobAgent
$DESTDIR/pro/scripts/install_agent.sh   ProductionManagement ProductionUpdateAgent
$DESTDIR/pro/scripts/install_agent.sh   ProductionManagement TransformationAgent

$DESTDIR/pro/scripts/install_service.sh RequestManagement    RequestManager
$DESTDIR/pro/scripts/install_agent.sh   RequestManagement    ZuziaAgent

$DESTDIR/pro/scripts/install_service.sh Stager Stager
$DESTDIR/pro/scripts/install_agent.sh   Stager StagerMonitorAgent
$DESTDIR/pro/scripts/install_agent.sh   Stager StagerMonitorWMSAgent
$DESTDIR/pro/scripts/install_agent.sh   Stager StagerAgent

$DESTDIR/pro/scripts/install_service.sh Framework ProxyManager
$DESTDIR/pro/scripts/install_service.sh Framework Notification
$DESTDIR/pro/scripts/install_agent.sh   Framework MyProxyRenewalAgent

$DESTDIR/pro/scripts/install_service.sh Logging SystemLogging
$DESTDIR/pro/scripts/install_service.sh Logging SystemLoggingReport
$DESTDIR/pro/scripts/install_agent.sh Logging SystemLoggingDBCleaner
$DESTDIR/pro/scripts/install_agent.sh Logging ErrorMessageMonitor

$DESTDIR/pro/scripts/install_agent.sh LHCb CondDBAgent
$DESTDIR/pro/scripts/install_agent.sh LHCb AncestorFilesAgent

$DESTDIR/pro/scripts/install_service.sh Bookkeeping BookkeepingManager

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
# export CVSROOT=:kserver:isscvs.cern.ch:/local/reps/dirac
export CVSROOT=:ext:isscvs.cern.ch:/local/reps/dirac
cd `dirname $DESTDIR`
cvs -Q co -r $DIRACVERSION DIRAC3/DIRAC DIRAC3/LHCbSystem
cvs update -A DIRAC3/DIRAC DIRAC3/LHCbSystem
cd DIRAC3/DIRAC
ln -s ../LHCbSystem .

EOF

fi

exit
