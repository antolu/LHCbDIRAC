#!/bin/bash
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/scripts/install_service.sh,v 1.3 2008/05/06 09:24:50 rgracian Exp $
# File :   install_service.sh
# Author : Ricardo Graciani
########################################################################
#
DESTDIR=`dirname $0`
DESTDIR=`cd $DESTDIR/../..; pwd`
#
source $DESTDIR/bashrc
System=$1
Service=$2
[ -z "$Service" ] && exit 1
echo ${System}/${Service} ..
#
ServiceDir=$DESTDIR/runit/${System}/${Service}Service
if [ -z "$3" ] || [ -d  $ServiceDir ] ; then
  # Create a new installation or Replace existing on if required
  rm -rf $ServiceDir
  mkdir -p $ServiceDir/log
fi
#
cat >> $ServiceDir/log/config << EOF
s10000000
n100
EOF
cat >> $ServiceDir/log/run << EOF
#!/bin/bash
#
source $DESTDIR/bashrc
#
svlogd .
EOF
cat >> $ServiceDir/run << EOF
#!/bin/bash
#
source $DESTDIR/bashrc
#
exec 2>&1
#
dirac-service -o LogLevel=VERBOSE $System/$Service \$DIRAC/etc/${System}_${Service}.cfg
EOF
chmod +x $ServiceDir/log/run $ServiceDir/run

touch $DIRAC/etc/${System}_${Service}.cfg
cd $ServiceDir

runsv . &
id=$!
sleep 2
echo d > supervise/control
sleep 1
kill  $id

exit 0
