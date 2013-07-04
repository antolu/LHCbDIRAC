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

  find *DIRAC -name *DB.sql | awk -F "/" '{print $2,$4}' | sort | uniq > databases

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
  ln -s ~/host{cert,key}.pem etc/grid-security
  /etc/init.d/cvmfs probe
  ln -s /cvmfs/grid.cern.ch/etc/grid-security/certificates/ etc/grid-security/certificates

}

#-------------------------------------------------------------------------------
# diracConfigure:
# 
#   writes dirac.cfg file 
#
#   o /LocalSite/Architecture
#   o /LocalInstallation/Database/RootPwd
#   o /LocalInstallation/Database/Password
#-------------------------------------------------------------------------------

diracConfigure(){

  arch=`dirac-architecture`
  # Randomly generated
  randomRoot=`tr -cd '[:alnum:]' < /dev/urandom | fold -w20 | head -n1`
  rootPass=/LocalInstallation/Database/RootPwd=$randomRoot
  # Randomly generated
  randomUser=`tr -cd '[:alnum:]' < /dev/urandom | fold -w20 | head -n1`
  userPass=/LocalInstallation/Database/Password=$randomUser
  # Setups
  setups=`cat databases | cut -d ' ' -f 1 | uniq | sed 's/^/-o \/DIRAC\/Setups\/Jenkins\//' | sed 's/$/=Jenkins/' | sed 's/System=/=/'` 
  # Databases
  dbs=`cat databases | cut -d ' ' -f 2 | uniq | grep -v TransferDB | cut -d '.' -f 1 | tr '\n' ','`
  databases=/LocalInstallation/Databases=$dbs
 
  echo $randomRoot > rootMySQL
  echo $randomUser > userMySQL

  ln -s $WORKSPACE/LHCbTestDirac/Jenkins/install.cfg etc/install.cfg

  dirac-configure etc/install.cfg -A $arch -o $rootPass -o $userPass -o $setups -o $databases -d 
  dirac-setup-site -d
  
  # Do not use Server Certificate
  sed -i '87i\    UseServerCertificate=yes' etc/dirac.cfg
  
}  

#-------------------------------------------------------------------------------
# diracStopMySQL:
#
#   if MySQL is running, it stops it. If not running, returns exit code 1
#
#-------------------------------------------------------------------------------

diracStopMySQL(){

  # It happens that if ps does not find anything, spits a return code 1 !
  set +o errexit
  mysqlRunning=`ps aux | grep mysql | grep -v grep`
  set -o errexit
   
  if [ ! -z "$mysqlRunning" ]
  then
    killall mysqld
  fi   
   
}

#-------------------------------------------------------------------------------
# diracStopRunit:
#
#   stops scripts running on startup
#
#-------------------------------------------------------------------------------

diracStopRunit(){

  set +o errexit
  runsvdirRunning=`ps aux | grep 'runsvdir ' | grep -v 'grep'`
  set -o errexit

  # It happens that if ps does not find anything, spits a return code 1 !  
  if [ ! -z "$runsvdirRunning" ]
  then
    killall runsvdir
  fi   

  set +o errexit
  runsvRunning=`ps aux | grep 'runsv ' | grep -v 'grep'`
  set -o errexit

  # It happens that if ps does not find anything, spits a return code 1 !  
  if [ ! -z "$runsvRunning" ]
  then
    killall runsv
  fi   
   
}

#-------------------------------------------------------------------------------
# diracMySQL:
#
#   installs MySQL. If it was running before, it returns an error.
#-------------------------------------------------------------------------------

diracMySQL(){
  
  diracStopMySQL    
  dirac-install-mysql
  
}  

#-------------------------------------------------------------------------------
# finalCleanup:
#
#   remove symlinks, remove cached info
#-------------------------------------------------------------------------------

finalCleanup(){
  
  rm etc/grid-security/certificates
  rm etc/grid-security/host*.pem
  rm -r .installCache

} 

#-------------------------------------------------------------------------------
# diracDBs:
#
#   installs all databases on the file databases
#
#-------------------------------------------------------------------------------

diracDBs(){

  cat databases | grep -v TransferDB.sql | cut -d ' ' -f 2 | cut -d '.' -f 1 | xargs dirac-install-db

}

#-------------------------------------------------------------------------------
#EOF