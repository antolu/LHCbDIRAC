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

diracMysql(){
  
  [ -e mysql/db/`hostname`.pid ] && echo 'MySQL is running (PID found)' && dirac-stop-mysql && exit 1
  
  dirac-install-mysql
  
}  

#-------------------------------------------------------------------------------

set +o errexit

#EOF