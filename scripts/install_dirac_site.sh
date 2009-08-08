#!/bin/bash
########################################################################
# File :   install_base.sh
########################################################################
#
# User that is allowed to execute the script
DIRACUSER=dirac
#
# Host where it is allowed to run the script
DIRACHOST=YOUR_HOSTNAME
#
# Location of the DIRAC site installation (default: /opt/dirac)
DESTDIR=/opt/dirac
#
# Your queue for DIRAC jobs (batch is the default queue of Torque)
Queue=batch
# The name of your site given to you by the DIRAC admins
SiteName=YOUR_SITE_NAME
# The path to your shared area (default: /opt/shared)
SharedArea=/opt/shared
# Dirac Setup (e.g. LHCb-Production or LHCb-Development)
DIRACSETUP=LHCb-Development
# Dirac version to install (default: HEAD)
DIRACVERSION=HEAD
# Version of the external applications to install (default: HEAD)
EXTVERSION=HEAD
# Dirac Architecture as determindes by the platform.py script
DIRACARCH=Linux_x86_64_glibc-2.3.4
# Python Version to use
DIRACPYTHON=24
# Directories to create at DESTDIR
DIRACDIRS="startup runit data work control requestDB"

export DIRACINSTANCE=Development
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
# check if the mask is properly set
if [ "`umask`" != "0002" ] && [ "`umask`" != "0022" ]; then
  echo umask should be set to 0022 or 0002 at system level for users
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
DIRAC
{
  Setup = $DIRACSETUP
  Setups
  {
    LHCb-Development
    {
      WorkloadManagement = Development
      LHCb = Development
    }
  }
  Configuration
  {
    Servers = dips://volhcb04.cern.ch:9135/Configuration/Server
    #Servers = dips://lhcbprod.pic.es:9135/Configuration/Server
    #Servers += dips://lhcb-wms-dirac.cern.ch:9135/Configuration/Server
    Name = LHCb-Devel
  }
  Security
  {
    CertFile = $DESTDIR/etc/grid-security/hostcert.pem
    KeyFile = $DESTDIR/etc/grid-security/hostkey.pem
  }
}
LocalSite
{
  Queue = $Queue
  SharedArea = $SharedArea 
  WaitingToRunningRatio = 0.5
  MaxWaitingJobs = 50
  MaxNumberJobs = 10000
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
grep -q "source $DESTDIR/bashrc" $HOME/.bashrc || \
  echo "source $DESTDIR/bashrc" >> $HOME/.bashrc
chmod +x $DESTDIR/pro/scripts/install_service.sh
cp $CURDIR/dirac-install $DESTDIR/pro/scripts

##############################################################
# INSTALL SERVICES
# (modify SystemName and ServiceName)
#

#$DESTDIR/pro/scripts/install_service.sh SystemName ServiceName

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
#

$DESTDIR/pro/scripts/install_agent.sh WorkloadManagement TaskQueueDirector

cat > $DESTDIR/etc/WorkloadManagement_TaskQueueDirector.cfg <<EOF
Systems
{
  WorkloadManagement
  {
    Development
    {
      Agents
      {
        TaskQueueDirector
        {
          SubmitPools = DIRAC
          ComputingElements = Torque
        }
      }
    }
  }
}
EOF

######################################################################
