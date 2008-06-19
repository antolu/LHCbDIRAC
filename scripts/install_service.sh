#!/bin/bash
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/scripts/install_service.sh,v 1.8 2008/06/19 11:25:13 rgracian Exp $
# File :   install_service.sh
# Author : Ricardo Graciani
########################################################################
#
DESTDIR=`dirname $0`
DESTDIR=`cd $DESTDIR/../..; pwd`
[ -z "$LOGLEVEL" ] && LOGLEVEL=INFO
#
source $DESTDIR/bashrc
System=$1
Service=$2
[ -z "$Service" ] && exit 1
echo ${System}/${Service} ..
#
ServiceDir=$DESTDIR/runit/${System}/${Service}
if [ -d  $ServiceDir ] && [ ! -z "$3" ] ; then
  # Create a new installation or Replace existing on if required
  rm -rf $ServiceDir
  NewInstall=1
fi
mkdir -p $ServiceDir/log
#
cat > $ServiceDir/log/config << EOF
s10000000
n100
EOF
cat > $ServiceDir/log/run << EOF
#!/bin/bash
#
source $DESTDIR/bashrc
#
exec svlogd .
EOF
cat > $ServiceDir/run << EOF
#!/bin/bash
#
source $DESTDIR/bashrc
#
exec 2>&1
#
exec dirac-service $System/$Service \$DIRAC/etc/${System}_${Service}.cfg -o LogLevel=$LOGLEVEL
EOF
chmod +x $ServiceDir/log/run $ServiceDir/run

touch $DIRAC/etc/${System}_${Service}.cfg
cd $ServiceDir

# If the installation is not new do not try to restart the service
[ -z "$NewInstall" ] && exit 1

runsv . &
id=$!
sleep 2
echo d > supervise/control
sleep 1
kill  $id

exit 0
