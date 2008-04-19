#!/bin/bash
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/scripts/install_vobox.sh,v 1.1 2008/04/19 10:45:33 rgracian Exp $
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
VERSION=HEAD
ARCH=slc4_amd64_gcc34
ARCH=slc4_ia32_gcc34
PYTHON=25
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
  Setup = LHCb-Development
  Configuration
  {
    Servers =  dips://volhcb03.cern.ch:9135/Configuration/Server
    Name = LHCb-Devel
  }
  Security
  {
    CertFile = $DESTDIR/etc/grid-security/hostcert.pem
    KeyFile = $DESTDIR/etc/grid-security/hostkey.pem
  }

}
EOF
fi

for dir in startup runit data ; do
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

$CURDIR/dirac-update -S -P $VERDIR -v $VERSION -p $ARCH -i $PYTHON -o /LocalSite/Root=$ROOT -o /LocalSite/Site=$SiteName || exit 1

old=$DESTDIR/old
pro=$DESTDIR/pro
[ -L $old ] && rm $old; [ -e $old ] && exit 1; [ -L $pro ] && mv $pro $old; [ -e $pro ] && exit 1; ln -s $VERDIR $pro || exit 1

##
## Compile all python files .py -> .pyc
##
cmd="from compileall import compile_dir ; compile_dir('"$DESTDIR/pro"', force=1, quiet=True )"
$DESTDIR/pro/$ARCH/bin/python -c "$cmd" 1> /dev/null || exit 1
$DESTDIR/pro/$ARCH/bin/python -O -c "$cmd" 1> /dev/null  || exit 1

$DESTDIR/pro/scripts/install_bashrc.sh    $DESTDIR $VERSION $ARCH python$PYTHON || exit 1

$DESTDIR/pro/scripts/install_service.sh Configuration Server

exit
