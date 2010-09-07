#!/bin/bash
########################################################################
# $Header: /local/reps/dirac/DIRAC3/scripts/install_vobox.sh,v 1.12 2009/07/21 08:22:45 rgracian Exp $
# File :   install_vobox.sh
# Author : Ricardo Graciani
########################################################################
#
if [ $# -ne 2 ] ; then
  echo Usage: $0 DiracSiteName DiracVersion
  exit
fi
SiteName=$1
DiracVersion=$2
#
DESTDIR=/opt/vobox/lhcb/dirac
DIRACSETUP=LHCb-Production
DIRACVERSION=$DiracVersion
DIRACARCH=Linux_x86_64_glibc-2.5
# DIRACARCH=Linux_i686_glibc-2.3.4
DIRACPYTHON=25
DIRACDIRS="startup runit data work requestDB"
LCGVERSION=2009-08-13

export LOGLEVEL=INFO
# Can not be done !!!!
# export FROMCVS="yes"
#
# make sure $DESTDIR is available
#
# umask 0002
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
DIRAC
{
  Setup = $DIRACSETUP
  Configuration
  {
    Servers  =  dips://lhcb-sec-dirac.cern.ch:9135/Configuration/Server
    Servers += dips://lhcb-serv1-dirac.cern.ch:9135/Configuration/Server
    Servers += dips://lhcbprod.pic.es:9135/Configuration/Server
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
  RequestManagement
  {
    Production
    {
      URLs
      {
        localURL = dips://localhost:9143/RequestManagement/RequestManager
      }
      Services
      {
        RequestManager
        {
          Backend = file
          Path = $DESTDIR/requestDB
        }
      }
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

wget -q -O dirac-install.py http://svnweb.cern.ch/guest/dirac/DIRAC/trunk/DIRAC/Core/scripts/dirac-install.py

echo python dirac-install.py -t server -P $VERDIR -r $DIRACVERSION -g $LCGVERSION -p $DIRACARCH -i $DIRACPYTHON -e LHCbDIRAC || exit 1
     python dirac-install.py -t server -P $VERDIR -r $DIRACVERSION -g $LCGVERSION -p $DIRACARCH -i $DIRACPYTHON -e LHCbDIRAC || exit 1

echo 

for dir in etc $DIRACDIRS ; do
  ln -s ../../$dir $VERDIR   || exit 1
done

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

chmod +x $DESTDIR/pro/scripts/install_bashrc.sh
$DESTDIR/pro/scripts/install_bashrc.sh    $DESTDIR $DIRACVERSION $DIRACARCH python$DIRACPYTHON || exit 1
sed -i 's:.*X509.*::' $DESTDIR/bashrc

#
# fix user .bashrc
#
# grep -q "export CVSROOT=:pserver:anonymous@isscvs.cern.ch:/local/reps/dirac" $HOME/.bashrc || \
#   echo "export CVSROOT=:pserver:anonymous@isscvs.cern.ch:/local/reps/dirac" >>  $HOME/.bashrc
grep -q "source $DESTDIR/bashrc" $HOME/.bashrc || \
  echo "source $DESTDIR/bashrc" >> $HOME/.bashrc
chmod +x $DESTDIR/pro/scripts/install_service.sh
# Hack until permission are fix on cvs for install_service.sh
$DESTDIR/pro/scripts/install_service.sh Configuration Server
$DESTDIR/pro/scripts/install_service.sh RequestManagement RequestManager
$DESTDIR/pro/scripts/install_service.sh Framework SystemAdministrator
$DESTDIR/pro/scripts/install_agent.sh   RequestManagement ZuziaAgent

# startup script
STARTDIR=`dirname $DESTDIR`/start
[ ! -d $STARTDIR ] && rm -rf $STARTDIR && mkdir $STARTDIR
cat > $STARTDIR/runsvdir-start << EOF
#!/bin/bash
exec 2>&1
exec 1>/dev/null
source $DESTDIR/bashrc
runsvdir -P $DESTDIR/startup 'log:  DIRAC runsv' &
EOF
chmod +x $STARTDIR/runsvdir-start

# stop script
STOPDIR=`dirname $DESTDIR`/stop
[ ! -d $STOPDIR ] && rm -rf $STOPDIR && mkdir $STOPDIR
cat > $STOPDIR/runsvdir-stop << EOF
#!/bin/bash
killall -9 runsvdir
killall -9 runsv
killall -9 dirac-agent
killall -9 dirac-service
killall -9 python
killall -9 svlogd
EOF
chmod +x $STOPDIR/runsvdir-stop

exit

