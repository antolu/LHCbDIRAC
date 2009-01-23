#!/bin/bash
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/scripts/install_gw02.sh,v 1.7 2009/01/23 09:38:23 acsmith Exp $
# File :   install_gw02.sh
# Author : Andrew C. Smith
########################################################################
#
# User that is allow to execute the script
DIRACUSER=lhcbprod
#
# Host where it es allow to run the script
DIRACHOST=gw02.lbdaq.cern.ch
#
# Location of the installation
DESTDIR=/sw/dirac
#
SiteName=DIRAC.ONLINE.ch
DIRACSETUP=LHCb-Production
DIRACVERSION=v4r4
EXTVERSION=v4r4
DIRACARCH=Linux_x86_64_glibc-2.3.4
DIRACPYTHON=24
DIRACDIRS="startup runit requestDB"
export LOGLEVEL=VERBOSE
export DIRACINSTANCE=Production

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
#if [ "`umask`" != "0002" ] ; then
#  echo umask should be set to 0002 at system level for users
#  exit
#fi

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
  EnableAgentMonitoring = no
  Architecture = $DIRACARCH
  CPUScalingFactor = 0.0
  Root = $DESTDIR
  Site = $SiteName
}
DIRAC
{
  Setup = $DIRACSETUP
  Configuration
  {
    Name = Online
    Master = no
  }
  Security
  {
    CertFile = $DESTDIR/etc/grid-security/hostcert.pem
    KeyFile = $DESTDIR/etc/grid-security/hostkey.pem
    UseServerCertificate = true
  }
  Setups
  {
    $DIRACSETUP
    {
      Configuration = $DIRACINSTANCE
      WorkloadManagement = $DIRACINSTANCE
      ProductionManagement = $DIRACINSTANCE
      Logging = $DIRACINSTANCE
      DataManagement = $DIRACINSTANCE
      RequestManagement = $DIRACINSTANCE
      Monitoring = $DIRACINSTANCE
      Bookkeeping = $DIRACINSTANCE
    }
  }
}
Systems
{
  Bookkeeping
  {
    $DIRACINSTANCE
    {
      URLs
      {
        BookkeepingManager = dips://lhcb-bk-dirac.cern.ch:9202/Bookkeeping/BookkeepingManager
      }
    }
  }
  DataManagement
  {
    $DIRACINSTANCE
    {
      URLs
      {
        RAWIntegrity = dips://lhcb-dms-dirac.cern.ch:9190/DataManagement/RAWIntegrity
        DataLogging = dips://lhcb-dms-dirac.ch:9146/DataManagement/DataLogging
      }
    }
  }
  RequestManagement
  {
    $DIRACINSTANCE
    {
      URLs
      {
        localURL = dip://store02.lbdaq.cern.ch:9199/RequestManagement/RequestManager
      }
    }
  }
}
Resources
{
  FileCatalogs
  {
    RAWIntegrity
    {
      AccessType = Read-Write
      Status = Active
    }
  }
  StorageElements
  {
    OnlineRunDB
    {
      StorageBackend = RunDB
      AccessProtocol.1
      {
        ProtocolName = LHCbOnline
        Access = local
        Protocol = http
        Host = rundb02.lbdaq.cern.ch
        Port = 8080
      }
    }
    CERN-RAW
    {
      StorageBackend = Castor
      AccessProtocol.1
      {
        ProtocolName = RFIO
        Access = local
        Protocol = rfio
        Host = castorlhcb
        Port = 9002
        Path = /castor/cern.ch/grid
        SpaceToken = lhcbraw
      }
    }
  }
  Sites
  {
    DIRAC
    {
      DIRAC.ONLINE.ch
      {
        SE = CERN-RAW
        SE += OnlineRunDB
      }
    }
  }
  SiteLocalSEMapping
  {
    DIRAC.ONLINE.ch = CERN-RAW
    DIRAC.ONLINE.ch += OnlineRunDB
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

$CURDIR/dirac-install -S -P $VERDIR -v $DIRACVERSION -e $EXTVERSION -p $DIRACARCH -i $DIRACPYTHON -o /LocalSite/Root=$ROOT -o /LocalSite/Site=$SiteName -o /DIRAC/Configuration/Servers= 2>/dev/null || exit 1

# Create pro and old links
old=$DESTDIR/old
pro=$DESTDIR/pro
[ -L $old ] && rm $old; [ -e $old ] && exit 1; [ -L $pro ] && mv $pro $old; [ -e $pro ] && exit 1; ln -s $VERDIR $pro || exit 1

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

# fix user .bashrc
grep -q "export CVSROOT=:pserver:anonymous@isscvs.cern.ch:/local/reps/dirac" $HOME/.bashrc || \
  echo "export CVSROOT=:pserver:anonymous@isscvs.cern.ch:/local/reps/dirac" >>  $HOME/.bashrc
grep -q "source $DESTDIR/bashrc" $HOME/.bashrc || \
  echo "source $DESTDIR/bashrc" >> $HOME/.bashrc
chmod +x $DESTDIR/pro/scripts/install_service.sh
cp $CURDIR/dirac-install $DESTDIR/pro/scripts

##############################################################
# INSTALL SERVICES

$DESTDIR/pro/scripts/install_service.sh RequestManagement RequestManager
cat > $DESTDIR/etc/RequestManagement_RequestManager.cfg <<EOF
Systems
{
  RequestManagement
  {
    $DIRACINSTANCE
    {
      Services
      {
        RequestManager
        {
          LogLevel = INFO
          HandlerPath = DIRAC/RequestManagementSystem/Service/RequestManagerHandler.py
          Port = 9199
          Protocol = dip
          Backend = file
          Path = $DESTDIR/requestDB
          Authorization
          {
            Default = all
          }
        }
      }
    }
  }
}
EOF

##############################################################
# INSTALL AGENTS

$DESTDIR/pro/scripts/install_agent.sh   DataManagement    TransferAgent
cat > $DESTDIR/etc/DataManagement_TransferAgent.cfg <<EOF
Systems
{
  DataManagement
  {
    $DIRACINSTANCE
    {
      Agents
      {
        TransferAgent
        {
          LogLevel = INFO
          LogOutputs = stdout
          PollingTime = 10
          Status = Active
          ControlDirectory = $DESTDIR/runit/DataManagement/TransferAgent
          NumberOfThreads = 4
          ThreadPoolDepth = 0
          UseProxies = False
        }
      }
    }
  }
}
EOF

$DESTDIR/pro/scripts/install_agent.sh   DataManagement    RemovalAgent
cat > $DESTDIR/etc/DataManagement_RemovalAgent.cfg <<EOF
Systems
{
  DataManagement
  {
    $DIRACINSTANCE
    {
      Agents
      {
        RemovalAgent
        {
          LogLevel = INFO
          LogOutputs = stdout
          PollingTime = 10
          Status = Active
          ControlDirectory = $DESTDIR/runit/DataManagement/RemovalAgent
          NumberOfThreads = 4
          ThreadPoolDepth = 0
          UseProxies = False
        }
      }
    }
  }
}
EOF

$DESTDIR/pro/scripts/install_agent.sh   DataManagement    RegistrationAgent
cat > $DESTDIR/etc/DataManagement_RegistrationAgent.cfg <<EOF
Systems
{
  DataManagement
  {
    $DIRACINSTANCE
    {
      Agents
      {
        RegistrationAgent
        {
          LogLevel = INFO
          LogOutputs = stdout
          PollingTime = 10
          Status = Active
          ControlDirectory = $DESTDIR/runit/DataManagement/RegistrationAgent
          NumberOfThreads = 4
          ThreadPoolDepth = 0
          UseProxies = False
        }
      }
    }
  }
}
EOF

exit
