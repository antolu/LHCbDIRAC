#!/bin/bash
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/scripts/install_vobox.sh,v 1.3 2008/05/10 11:17:41 rgracian Exp $
# File :   install_vobox.sh
# Author : Ricardo Graciani
########################################################################
#
if [ $# -ne 1 ] ; then
  echo Usage: $0 DiracSiteName
  exit
fi
SiteName=$1
#
DESTDIR=/opt/vobox/lhcb/dirac
VERSION=CCRC08-v1
ARCH=slc4_amd64_gcc34
ARCH=Linux_i686_glibc-2.3.4
PYTHON=24
DIRACDIRS="startup runit data requestDB"
# Can not be done !!!!
# export FROMCVS="yes"
#
# make sure $DESTDIR is available
#
# umask 0002
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
  Setup = LHCb-Production
  Configuration
  {
    Servers =  dips://volhcb01.cern.ch:9135/Configuration/Server
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
VERDIR=$DESTDIR/versions/${VERSION}-`date +"%s"`
mkdir -p $VERDIR   || exit 1
for dir in etc data runit startup ; do
  ln -s ../../$dir $VERDIR   || exit 1
done

$CURDIR/dirac-install -S -P $VERDIR -v $VERSION -p $ARCH -i $PYTHON -o /LocalSite/Root=$ROOT -o /LocalSite/Site=$SiteName || exit 1

old=$DESTDIR/old
pro=$DESTDIR/pro
[ -L $old ] && rm $old; [ -e $old ] && exit 1; [ -L $pro ] && mv $pro $old; [ -e $pro ] && exit 1; ln -s $VERDIR $pro || exit 1

##
## Compile all python files .py -> .pyc
##
cmd="from compileall import compile_dir ; compile_dir('"$DESTDIR/pro"', force=1, quiet=True )"
$DESTDIR/pro/$ARCH/bin/python -c "$cmd" 1> /dev/null || exit 1
$DESTDIR/pro/$ARCH/bin/python -O -c "$cmd" 1> /dev/null  || exit 1

chmod +x $DESTDIR/pro/scripts/install_bashrc.sh
$DESTDIR/pro/scripts/install_bashrc.sh    $DESTDIR $VERSION $ARCH python$PYTHON || exit 1

# Hack until permission are fix on cvs for install_service.sh
bash $DESTDIR/pro/scripts/install_service.sh Configuration Server
bash $DESTDIR/pro/scripts/install_service.sh RequestManagement RequestManager
$DESTDIR/pro/scripts/install_agent.sh   DataManagement TransferAgent

# startup script
STARTDIR=`dirname $DESTDIR`/start
[ ! -d $STARTDIR ] && rm -rf $STARTDIR && mkdir $STARTDIR || exit 1
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
[ ! -d $STOPDIR ] && rm -rf $STOPDIR && mkdir $STOPDIR || exit 1
cat > $STOPDIR/runsvdir-stop << EOF
#!/bin/bash
killall -9 runsvdir
killall -9 runsv
killall -9 dirac-agent
killall -9 dirac-service
killall -9 svlogd
EOF
chmod +x $STOPDIR/runsvdir-stop

exit

