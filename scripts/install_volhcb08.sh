#!/bin/bash
########################################################################
# File :   install_volhcb08.sh
########################################################################
#
# User that is allow to execute the script
DIRACUSER=dirac
#
# Host where it es allow to run the script
DIRACHOST=volhcb08.cern.ch
#
# Location of the installation
DESTDIR=/opt/dirac
#
SiteName=VOLHCB08.CERN.CH
DIRACSETUP=LHCb-Development
DIRACVERSION=v0r3p2
EXTVERSION=v0r3p0
DIRACARCH=Linux_x86_64_glibc-2.3.4
DIRACPYTHON=24
DIRACDIRS="startup runit control"

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
    Servers += dips://volhcb01.cern.ch:9135/Configuration/Server
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
/opt/dirac/pro/mysql/share/mysql/mysql.server start

# The request db
$DESTDIR/pro/scripts/install_service.sh RequestManagement    RequestManager
cat > $DESTDIR/etc/RequestManagement_RequestManager.cfg <<EOF
Systems
{
  RequestManagement
  {
    Development
    {
      URLs
      {
        localURL = dips://localhost:9143/RequestManagement/RequestManager
      }
      Services
      {
        RequestManager
        {
          Backend = mysql
        }
      }
    }
  }
}
EOF

# The standard data management agents
$DESTDIR/pro/scripts/install_agent.sh   DataManagement     RegistrationAgent
$DESTDIR/pro/scripts/install_agent.sh   DataManagement     RemovalAgent
$DESTDIR/pro/scripts/install_agent.sh   DataManagement     TransferAgent

# Components for RAW integrity
$DESTDIR/pro/scripts/install_agent.sh   DataManagement     RAWIntegrityAgent
$DESTDIR/pro/scripts/install_service.sh DataManagement     RAWIntegrity

# Data management transformation components
$DESTDIR/pro/scripts/install_service.sh DataManagement     ReplicationPlacement
$DESTDIR/pro/scripts/install_agent.sh   DataManagement     ReplicationPlacementAgent

# Bulk transfer components
$DESTDIR/pro/scripts/install_service.sh DataManagement     TransferDBMonitoring
$DESTDIR/pro/scripts/install_agent.sh   DataManagement     ReplicationScheduler
$DESTDIR/pro/scripts/install_agent.sh   DataManagement     FTSMonitor
$DESTDIR/pro/scripts/install_agent.sh   DataManagement     FTSRegister
$DESTDIR/pro/scripts/install_agent.sh   DataManagement     FTSSubmit

# Data integrity components
$DESTDIR/pro/scripts/install_service.sh DataManagement     DataIntegrity
$DESTDIR/pro/scripts/install_agent.sh   DataManagement     SEvsLFCAgent
$DESTDIR/pro/scripts/install_agent.sh   DataManagement     LFCvsSEAgent

# The data logging service
$DESTDIR/pro/scripts/install_service.sh DataManagement     DataLogging

# The storage usage components
$DESTDIR/pro/scripts/install_service.sh DataManagement     StorageUsage
$DESTDIR/pro/scripts/install_agent.sh   DataManagement     StorageUsage

if [ ! -z DIRACCVS ] ; then


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

EOF
fi

exit
