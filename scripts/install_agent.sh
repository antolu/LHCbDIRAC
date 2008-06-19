#!/bin/bash
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/scripts/install_agent.sh,v 1.7 2008/06/19 15:16:54 rgracian Exp $
# File :   install_agent.sh
# Author : Ricardo Graciani
########################################################################
#
DESTDIR=`dirname $0`
DESTDIR=`cd $DESTDIR/../..; pwd`
#
source $DESTDIR/bashrc
System=$1
Agent=$2
[ -z "$Agent" ] && exit 1
echo ${System}/${Agent} ..
#
AgentDir=$DESTDIR/runit/${System}/${Agent}Agent
if [ -d  $AgentDir ] && [ ! -z "$3" ] ; then
  # Create a new installation or Replace existing on if required
  rm -rf $AgentDir
  NewInstall=1
elif [ ! -d $AgentDir ] ; then
  NewInstall=1
fi
mkdir -p $AgentDir/log
#
cat >> $AgentDir/log/config << EOF
s10000000
n100
EOF
cat >> $AgentDir/log/run << EOF
#!/bin/bash
#
source $DESTDIR/bashrc
#
exec svlogd .
EOF
cat >> $AgentDir/run << EOF
#!/bin/bash
#
source $DESTDIR/bashrc
#
exec 2>&1
#
exec dirac-agent -o LogLevel=VERBOSE $System/$Agent \$DIRAC/etc/${System}_${Agent}.cfg
EOF
chmod +x $AgentDir/log/run $AgentDir/run

touch $DIRAC/etc/${System}_${Agent}.cfg
cd $AgentDir

# If the installation is not new do not try to restart the agent
[ -z "$NewInstall" ] && exit 1

runsv . &
id=$!
sleep 5
echo d > supervise/control
sleep 1
kill  $id

exit 0
