#!/bin/bash
########################################################################
# File :   install_base.sh
########################################################################
#
# User that is allow to execute the script
DIRACUSER=dirac
#
# Host where it es allow to run the script
DIRACHOST=volhcb05.cern.ch
#
# Location of the installation
DESTDIR=/opt/dirac
#
SiteName=VOLHCB05.CERN.CH
DIRACSETUP=LHCb-Certification
DIRACVERSION=v4r18p2
EXTVERSION=v4r0
DIRACARCH=Linux_x86_64_glibc-2.3.4
DIRACPYTHON=24
DIRACDIRS="startup runit data work control requestDB"

export DIRACINSTANCE=Certification
export LOGLEVEL=VERBOSE
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

ROOT=`dirname $DESTDIR`/DIRAC

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
Resources
{
  FileCatalogs
  {
    LcgFileCatalogCombined
    {
      ReadOnlyHosts = lfc-lhcb-ro.cern.ch
    }
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
cp $DESTDIR/pro/scripts/dirac-update $DESTDIR/pro/scripts/dirac-install

#
# Configure MySQL if not yet done
#

$CURDIR/install_mysql.sh $DIRACHOST
/opt/dirac/pro/mysql/share/mysql/mysql.server start

##############################################################
# INSTALL SERVICES
# (modify SystemName and ServiceName)
#

# Workloadmanagement
$DESTDIR/pro/scripts/install_service.sh WorkloadManagement JobManager
$DESTDIR/pro/scripts/install_service.sh WorkloadManagement JobMonitoring
$DESTDIR/pro/scripts/install_service.sh WorkloadManagement JobStateUpdate
# $DESTDIR/pro/scripts/install_service.sh WorkloadManagement InputSandbox 
# $DESTDIR/pro/scripts/install_service.sh WorkloadManagement OutputSandbox 
$DESTDIR/pro/scripts/install_service.sh WorkloadManagement JobStateUpdate
$DESTDIR/pro/scripts/install_service.sh WorkloadManagement Matcher
$DESTDIR/pro/scripts/install_service.sh WorkloadManagement WMSAdministrator
$DESTDIR/pro/scripts/install_service.sh WorkloadManagement SandboxStore


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

# Workloadmanagement
$DESTDIR/pro/scripts/install_agent.sh   WorkloadManagement StatesAccountingAgent
$DESTDIR/pro/scripts/install_agent.sh   WorkloadManagement InputDataAgent
$DESTDIR/pro/scripts/install_agent.sh   WorkloadManagement MightyOptimizer

$DESTDIR/pro/scripts/install_agent.sh   WorkloadManagement JobHistoryAgent
$DESTDIR/pro/scripts/install_agent.sh   WorkloadManagement JobCleaningAgent
$DESTDIR/pro/scripts/install_agent.sh   WorkloadManagement StalledJobAgent
$DESTDIR/pro/scripts/install_agent.sh   WorkloadManagement PilotStatusAgent
$DESTDIR/pro/scripts/install_agent.sh   WorkloadManagement PilotMonitor
$DESTDIR/pro/scripts/install_agent.sh   WorkloadManagement TaskQueueDirector

# LHCb
$DESTDIR/pro/scripts/install_agent.sh   LHCb   AncestorFilesAgent
$DESTDIR/pro/scripts/install_agent.sh   LHCb   BKInputDataAgent


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

exit