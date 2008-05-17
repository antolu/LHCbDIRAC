#!/bin/bash
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/scripts/install_store02.sh,v 1.5 2008/05/17 14:01:44 rgracian Exp $
# File :   install_store02.sh
# Author : Ricardo Graciani
########################################################################
#
# User that is allow to execute the script
DIRACUSER=lhcbprod
#
# Host where it es allow to run the script
DIRACHOST=store02.lbdaq.cern.ch
#
# Location of the installtion
DESTDIR=/opt/dirac
#
SiteName=DIRAC.ONLINE.ch
DIRACSETUP=LHCb-Development
DIRACVERSION=HEAD
DIRACARCH=Linux_i686_glibc-2.3.4
DIRACPYTHON=24
DIRACDIRS="startup runit data requestDB"
#
# Uncomment to install from CVS (default install from TAR)
# it imples -b (build from sources)
# DIRACCVS=-C
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
# # check if the mask is properly set
# if [ "`umask`" != "0002" ] ; then
#   echo umask should be set to 0002 at system level for users
#   exit
# fi
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
  Site = DIRAC.ONLINE.ch
}
DIRAC
{
  Site = DIRAC.ONLINE.ch
  Setup = LHCb-Development
  Security
  {
    CertFile = /opt/dirac/etc/grid-security/hostcert.pem
    KeyFile = /opt/dirac/etc/grid-security/hostkey.pem
  }
  Configuration
  {
    Name = Online
    Master = no
  }
  Setups
  {
    LHCb-Development
    {
      Configuration = Development
      WorkloadManagement = Development
      ProductionManagement = Development
      Logging = Development
      DataManagement = Development
      RequestManagement = Development
      Monitoring = Development
    }
  }
}

Systems
{

  Monitoring
  {
     Development
     {
       URLs
       {
         Server = dips://volhcb03.cern.ch:9142/Monitoring/Server
       }
     }
  }
  DataManagement
  {
    Development
    {
      URLs
      {
        RAWIntegrity = dips://volhcb03.cern.ch:9198/DataManagement/RAWIntegrity
        DataLogging = dips://volhcb03.cern.ch:9146/DataManagement/DataLogging
      }
      Agents
      {
        RemovalAgent
        {
          LogLevel = INFO
          LogOutputs = stdout
          PollingTime = 10
          Status = Active
          ControlDirectory = /opt/dirac/runit/RemovalAgent
          NumberOfThreads = 1
          ThreadPoolDepth = 0
          UseProxies = False
        }
        TransferAgent
        {
          LogLevel = INFO
          LogOutputs = stdout
          PollingTime = 10
          Status = Active
          ControlDirectory = /opt/dirac/runit/TransferAgent
          NumberOfThreads = 4
          ThreadPoolDepth = 4
          UseProxies = False
        }
      }
    }
  }
  RequestManagement
  {
    Development
    {
      URLs
      {
        localURL = dip://store02.lbdaq.cern.ch:9199/RequestManagement/RequestManager
      }
      Databases
      {
      }
      Services
      {
        RequestManager
        {
          LogLevel = DEBUG
          HandlerPath = DIRAC/RequestManagementSystem/Service/RequestManagerHandler.py
          Port = 9199
          Protocol = dip
          Backend = file
          Path = /opt/dirac/requestDB
          Authorization
          {
            Default = all
          }
        }
      }
    }
  }
  WorkloadManagement
  {
    Development
    {
      URLs
      {
        WMSAdministrator = dips://volhcb03.cern.ch:9145/WorkloadManagement/WMSAdministrator
      }
    }
  }
}
Resources
{
  FileCatalogs
  {
    LcgFileCatalogCombined
    {
      AccessType = Read-Write
      Status = Active
      LcgGfalInfosys = lcg-bdii.cern.ch:2170
      MasterHost = lfc-lhcb.cern.ch
      ReadOnlyHosts = lfc-lhcb-ro.cern.ch
    }
    PlacementDB
    {
      AccessType = Write
      Status = Active
    }
    BookkeepingDB
    {
      AccessType = Write
      Status = Stopped
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
        Path =
        SpaceToken =
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
  SiteLocalSEMapping
  {
    DIRAC.ONLINE.ch = CERN-RAW,OnlineRunDB
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

$CURDIR/dirac-install -S -P $VERDIR -v $DIRACVERSION -p $DIRACARCH -i $DIRACPYTHON -o /LocalSite/Root=$ROOT -o /LocalSite/Site=$SiteName 2>/dev/null || exit 1

#
# Create pro and old links
old=$DESTDIR/old
pro=$DESTDIR/pro
[ -L $old ] && rm $old; [ -e $old ] && exit 1; [ -L $pro ] && mv $pro $old; [ -e $pro ] && exit 1; ln -s $VERDIR $pro || exit 1
#
# Create bin link
ln -sf pro/$DIRACARCH/bin $DESTDIR/bin
##
## Compile all python files .py -> .pyc
##
cmd="from compileall import compile_dir ; compile_dir('"$DESTDIR/pro"', force=1, quiet=True )"
$DESTDIR/pro/$DIRACARCH/bin/python -c "$cmd" 1> /dev/null || exit 1
$DESTDIR/pro/$DIRACARCH/bin/python -O -c "$cmd" 1> /dev/null  || exit 1

chmod +x $DESTDIR/pro/scripts/install_bashrc.sh
$DESTDIR/pro/scripts/install_bashrc.sh    $DESTDIR $DIRACVERSION $DIRACARCH python$DIRACPYTHON || exit 1

#
# fix user .bashrc
#
# grep -q "export CVSROOT=:pserver:anonymous@isscvs.cern.ch:/local/reps/dirac" $HOME/.bashrc || \
#   echo "export CVSROOT=:pserver:anonymous@isscvs.cern.ch:/local/reps/dirac" >>  $HOME/.bashrc
grep -q "source $DESTDIR/bashrc" $HOME/.bashrc || \
  echo "source $DESTDIR/bashrc" >> $HOME/.bashrc
chmod +x $DESTDIR/pro/scripts/install_service.sh

$DESTDIR/pro/scripts/install_service.sh RequestManagement RequestManager no
$DESTDIR/pro/scripts/install_agent.sh   DataManagement    TransferAgent  no
$DESTDIR/pro/scripts/install_agent.sh   DataManagement    RemovalAgent   no

exit
