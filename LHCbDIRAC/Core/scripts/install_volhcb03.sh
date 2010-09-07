#!/bin/bash
########################################################################
# File :   install_base.sh
########################################################################
#
# User that is allow to execute the script
DIRACUSER=dirac
#
# Host where it es allow to run the script
DIRACHOST=volhcb03.cern.ch
#
# Location of the installation
DESTDIR=/opt/dirac
#
SiteName=VOLHCB03.CERN.CH
DIRACSETUP=LHCb-Certification
DIRACVERSION=v5r0p0-pre11
DIRACARCH=Linux_x86_64_glibc-2.5
DIRACPYTHON=25
DIRACDIRS="startup runit data work control requestDB"
DIRACDBs=""
LCGVERSION=2009-08-13

export DIRACINSTANCE=Certification
export LOGLEVEL=VERBOSE
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

echo python dirac-install.py -t server -P $VERDIR -r $DIRACVERSION -g $LCGVERSION -p $DIRACARCH -i $DIRACPYTHON -e LHCbDIRAC || exit 1
     python dirac-install.py -t server -P $VERDIR -r $DIRACVERSION -g $LCGVERSION -p $DIRACARCH -i $DIRACPYTHON -e LHCbDIRAC || exit 1

for dir in etc $DIRACDIRS ; do
  ln -s ../../$dir $VERDIR   || exit 1
done

echo 
     $VERDIR/scripts/dirac-configure -n $SiteName --UseServerCertificate -o /LocalSite/Root=$ROOT/pro -V lhcb --SkipCADownload || exit 1

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

$DESTDIR/pro/scripts/install_service.sh Framework BundleDelivery
$DESTDIR/pro/scripts/install_service.sh Framework Monitoring
$DESTDIR/pro/scripts/install_service.sh Framework Notification
$DESTDIR/pro/scripts/install_service.sh Framework Plotting
$DESTDIR/pro/scripts/install_service.sh Framework ProxyManager
$DESTDIR/pro/scripts/install_service.sh Framework SecurityLogging
$DESTDIR/pro/scripts/install_service.sh Framework SystemLogging
$DESTDIR/pro/scripts/install_service.sh Framework SystemLoggingReport
$DESTDIR/pro/scripts/install_service.sh Framework UserProfileManager

$DESTDIR/pro/scripts/install_service.sh WorkloadManagement JobManager
$DESTDIR/pro/scripts/install_service.sh WorkloadManagement JobMonitoring
$DESTDIR/pro/scripts/install_service.sh WorkloadManagement JobStateUpdate
$DESTDIR/pro/scripts/install_service.sh WorkloadManagement JobStateUpdate
$DESTDIR/pro/scripts/install_service.sh WorkloadManagement Matcher
$DESTDIR/pro/scripts/install_service.sh WorkloadManagement WMSAdministrator
$DESTDIR/pro/scripts/install_service.sh WorkloadManagement SandboxStore

$DESTDIR/pro/scripts/install_service.sh Stager Stager

# LHCb specific services
$DESTDIR/pro/scripts/install_service.sh ProductionManagement ProductionManager
$DESTDIR/pro/scripts/install_service.sh ProductionManagement ProductionRequest

$DESTDIR/pro/scripts/install_service.sh Bookkeeping BookkeepingManager
$DESTDIR/pro/scripts/install_oracle-client.sh

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

$DESTDIR/pro/scripts/install_agent.sh WorkloadManagement StatesAccountingAgent
$DESTDIR/pro/scripts/install_agent.sh WorkloadManagement InputDataAgent
#$DESTDIR/pro/scripts/install_agent.sh WorkloadManagement MightyOptimizer
$DESTDIR/pro/scripts/install_agent.sh WorkloadManagement ThreadedMightyOptimizer
$DESTDIR/pro/scripts/install_agent.sh WorkloadManagement JobHistoryAgent
$DESTDIR/pro/scripts/install_agent.sh WorkloadManagement JobCleaningAgent
$DESTDIR/pro/scripts/install_agent.sh WorkloadManagement StalledJobAgent
$DESTDIR/pro/scripts/install_agent.sh WorkloadManagement PilotStatusAgent
$DESTDIR/pro/scripts/install_agent.sh WorkloadManagement PilotMonitorAgent
$DESTDIR/pro/scripts/install_agent.sh WorkloadManagement TaskQueueDirector

# LHCb specific agents
$DESTDIR/pro/scripts/install_agent.sh WorkloadManagement BKInputDataAgent
$DESTDIR/pro/scripts/install_agent.sh WorkloadManagement AncestorFilesAgent
$DESTDIR/pro/scripts/install_agent.sh WorkloadManagement CondDBAgent
$DESTDIR/pro/scripts/install_agent.sh WorkloadManagement JobLogUploadAgent
$DESTDIR/pro/scripts/install_agent.sh WorkloadManagement SiteAvailabilityAgent

$DESTDIR/pro/scripts/install_agent.sh ProductionManagement BookkeepingWatchAgent
$DESTDIR/pro/scripts/install_agent.sh ProductionManagement DataRecoveryAgent
$DESTDIR/pro/scripts/install_agent.sh ProductionManagement ProductionCleaningAgent
$DESTDIR/pro/scripts/install_agent.sh ProductionManagement ProductionJobAgent
$DESTDIR/pro/scripts/install_agent.sh ProductionManagement ProductionStatusAgent
$DESTDIR/pro/scripts/install_agent.sh ProductionManagement ProductionUpdateAgent
$DESTDIR/pro/scripts/install_agent.sh ProductionManagement ReplicationSubmissionAgent
$DESTDIR/pro/scripts/install_agent.sh ProductionManagement RequestTrackingAgent
$DESTDIR/pro/scripts/install_agent.sh ProductionManagement TransformationAgent
$DESTDIR/pro/scripts/install_agent.sh ProductionManagement ValidateOutputDataAgent

$DESTDIR/pro/scripts/install_agent.sh Stager StagerAgent
$DESTDIR/pro/scripts/install_agent.sh Stager StagerMonitorAgent
$DESTDIR/pro/scripts/install_agent.sh Stager StagerMonitorWMSAgent

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
