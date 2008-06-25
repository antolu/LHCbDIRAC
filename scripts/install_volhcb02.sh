#!/bin/bash
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/scripts/install_volhcb02.sh,v 1.6 2008/06/25 17:18:06 atsareg Exp $
# File :   install_volhcb01.sh
# Author : Ricardo Graciani
########################################################################
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
DIRACSETUP=LHCb-Production
DIRACVERSION=v0r2p2
EXTVERSION=v0r2p0
DIRACARCH=Linux_i686_glibc-2.3.4
DIRACPYTHON=24
DIRACDIRS="startup runit data work requestDB"

export LOGLEVEL=INFO
#
# Uncomment to install from CVS (default install from TAR)
# it implies -b (build from sources)
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
    Servers += dips://volhcb03.cern.ch:9135/Configuration/Server
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
  WorkloadManagement
  {
    Production
    {
      Databases
      {
        JobDB
        {
          LogLevel = INFO
          User = Dirac
          Host = volhcb01.cern.ch
          Password = lhcbMySQL
          DBName = JobDB
        }
        JobLoggingDB
        {
          LogLevel = INFO
          User = Dirac
          Host = volhcb01.cern.ch
          Password = lhcbMySQL
          DBName = JobLoggingDB
        }
        ProxyRepositoryDB
        {
          LogLevel = INFO
          User = Dirac
          Host = volhcb01.cern.ch
          Password = lhcbMySQL
          DBName = ProxyRepositoryDB
        }
        PilotAgentsDB
        {
          LogLevel = INFO
          User = Dirac
          Host = volhcb01.cern.ch
          Password = lhcbMySQL
          DBName = PilotAgentsDB
        }
    }
    }
  }
}
EOF
fi
#
# Special CS file for the pilotAgent
cat > $DESTDIR/etc/WorkloadManagement_PilotAgent.cfg <<EOF
Systems
{
  WorkloadManagement
  {
    Development
    {
      Agents
      {
        PilotAgent
        {
          Middleware = gLite
        }
      }
      PilotAgent
      {
        gLitePilotDirector
        {
          ResourceBrokers = wms101.cern.ch,wms106.cern.ch
        }
      }
    }
  }
}
EOF
#

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
$CURDIR/dirac-install -S -P $VERDIR -v $DIRACVERSION -e $EXTVERSION -p $DIRACARCH -i $DIRACPYTHON -o /LocalSite/Root=$ROOT -o /LocalSite/Site=$SiteName 2>/dev/null || exit 1

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

#$CURDIR/install_mysql.sh $DIRACHOST
#/opt/dirac/pro/mysql/share/mysql/mysql.server start

$DESTDIR/pro/scripts/install_service.sh RequestManagement RequestManager
$DESTDIR/pro/scripts/install_agent.sh  WorkloadManagement PilotStatusAgent
$DESTDIR/pro/scripts/install_agent.sh  WorkloadManagement PilotAgent
$DESTDIR/pro/scripts/install_agent.sh  WorkloadManagement PilotMonitor

exit