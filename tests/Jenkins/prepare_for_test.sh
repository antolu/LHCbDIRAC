#!/bin/sh 
#-------------------------------------------------------------------------------
# prepare_for_test
#  
# : prepares installs DIRAC, installs MySQL
# : assumes ./project.version exists
# : assumes ./bashrc exists
# : assumes ~/host{cert,key}.pem  
#  
# ubeda@cern.ch  
# 26/VI/2013
#-------------------------------------------------------------------------------

set -o errexit

#-------------------------------------------------------------------------------
# findRelease:
#
#   If any parameter is passed, we assume we are on pre-release mode, otherwise, 
#   we assume production. It reads from releases.cfg and picks the latest version
#   which is written to {project,dirac,lhcbdirac}.version
#
#-------------------------------------------------------------------------------
findRelease(){

  [ $1 ] && PRE='-pre' || PRE='p[[:digit:]]*'  
  
  tmp_dir=`mktemp -d -q`
  
  cd $tmp_dir

  wget http://svn.cern.ch/guest/dirac/LHCbDIRAC/trunk/LHCbDIRAC/releases.cfg --quiet

  project=`cat releases.cfg | grep [^:]v[[:digit:]]r[[:digit:]]*$PRE | head -1 | sed 's/ //g'`

  s=`cat releases.cfg | grep -n $project | cut -d ':' -f 1 | head -1`  
  s=$(($s+2))
  e=$(($s+3))
  versions=`sed -n "$s,$e p" releases.cfg`

  dirac=`echo $versions | tr ' ' '\n' | grep ^DIRAC:v*[^,] | sed 's/,//g' | cut -d ':' -f2`
  lhcbdirac=`echo $versions | tr ' ' '\n' | grep ^LHCbDIRAC:v* | sed 's/,//g' | cut -d ':' -f2`
  
  cd - >> /dev/null 
  rm -r $tmp_dir

  echo PROJECT:$project && echo $project > project.version
  echo DIRAC:$dirac && echo $dirac > dirac.version
  echo LHCbDIRAC:$lhcbdirac && echo $lhcbdirac > lhcbdirac.version

}

#-------------------------------------------------------------------------------
# findDatabases:
#
#   gets all database names from *DIRAC code and writes them to a file
#   named databases.
#
#-------------------------------------------------------------------------------

findDatabases(){

  find *DIRAC -name *DB.sql | uniq | awk -F "/" '{print $4}' | cut -d '.' -f1 > databases

  echo found `wc -l databases`

}

#-------------------------------------------------------------------------------
# diracInstall:
#
#   gets `project.version` code from the repository and copies certificates
#
#-------------------------------------------------------------------------------

diracInstall(){

  wget --no-check-certificate -O dirac-install 'https://github.com/DIRACGrid/DIRAC/raw/integration/Core/scripts/dirac-install.py' --quiet
  chmod +x dirac-install
  ./dirac-install -l LHCb -r `cat project.version` -e LHCb -t server

  mkdir -p etc/grid-security
  cp ~/host{cert,key}.pem etc/grid-security

}

#-------------------------------------------------------------------------------
# diracConfigure:
# 
#   writes dirac.cfg file 
#
#   o /DIRAC/Security/UseServerCertificate
#   o /DIRAC/Security/CertFile
#   o /DIRAC/Security/KeyFile
#   o /LocalSite/Architecture
#   o /LocalInstallation/Database/RootPwd
#   o /LocalInstallation/Database/Password
#   o /LocalInstallation/Database/Host
#-------------------------------------------------------------------------------

diracConfigure(){

  certFile='/DIRAC/Security/CertFile='$WORKSPACE/etc/grid-security/hostcert.pem
  keyFile='/DIRAC/Security/KeyFile='$WORKSPACE/etc/grid-security/hostkey.pem
  arch=`dirac-architecture`
  # Randomly generated
  rootPass=/LocalInstallation/Database/RootPwd=`tr -cd '[:alnum:]' < /dev/urandom | fold -w20 | head -n1`
  # Randomly generated
  userPass=/LocalInstallation/Database/Password=`tr -cd '[:alnum:]' < /dev/urandom | fold -w20 | head -n1`
  hostPath=/LocalInstallation/Database/Host='localhost'

  echo '/LocalSite/Architecture:' $arch
  echo $certFile
  echo $keyFile
  echo $rootPass
  echo $userPass
  echo $hostPath

  dirac-configure -o $certFile -o $keyFile -A $arch -o $rootPass -o $userPass -o $hostPath -S Jenkins
  
}  

#-------------------------------------------------------------------------------
# diracMySQL:
#
#   installs MySQL. If it was running before, it returns an error.
#-------------------------------------------------------------------------------

diracMySQL(){
  
  echo 1
  
  mysqlRunning=`ps | grep mysql | grep -v grep`
  
  echo $mysqlRunning
  
  if [ "$mysqlRunning" ]
  then
    echo MySQL is running, being killed.
    killall mysqld
    exit 1
  fi  
  
  dirac-install-mysql
  
}  

#-------------------------------------------------------------------------------
#EOF