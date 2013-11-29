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

#set -o errexit

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

  find *DIRAC -name *DB.sql | grep -v TransferDB.sql | awk -F "/" '{print $2,$4}' | sort | uniq > databases

  echo found `wc -l databases`

}

#-------------------------------------------------------------------------------
# findServices:
#
#   gets all service names from *DIRAC code and writes them to a file
#   named services.
#
#-------------------------------------------------------------------------------

findServices(){

  find *DIRAC/*/Service/ -name *Handler.py | grep -v test | awk -F "/" '{print $2,$4}' | sort | uniq > services

  echo found `wc -l services`

}

#-------------------------------------------------------------------------------
# findSystems:
#
#   gets all system names from *DIRAC code and writes them to a file
#   named systems.
#
#-------------------------------------------------------------------------------

findSystems(){

  find *DIRAC/ -name *System  | cut -d '/' -f 2 | sort | uniq > systems

  echo found `wc -l systems`

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
  ./dirac-install -l LHCb -r `cat project.version` -e LHCb -t server $DEBUG

  mkdir -p etc/grid-security/certificates
  cd etc/grid-security
  openssl genrsa -out hostkey.pem 2048
  cp $WORKSPACE/LHCbTestDirac/Jenkins/openssl_config openssl_config
  fqdn=`hostname --fqdn`
  sed -i "s/#hostname#/$fqdn/g" openssl_config
  openssl req -new -x509 -key hostkey.pem -out hostcert.pem -days 1 -config openssl_config
  
  cp host{cert,key}.pem certificates/ 
  #/etc/init.d/cvmfs probe
  #ln -s /cvmfs/grid.cern.ch/etc/grid-security/certificates/ etc/grid-security/certificates
  cd -

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
  setups=`cat systems | sed 's/System//' | sed 's/^/-o \/DIRAC\/Setups\/Jenkins\//' | sed 's/$/=Jenkins/'` 
 
  echo $randomRoot > rootMySQL
  echo $randomUser > userMySQL

  cp -s $WORKSPACE/LHCbTestDirac/Jenkins/install.cfg etc/install.cfg
  hostdn=`openssl x509 -noout -in etc/grid-security/hostcert.pem -subject | sed 's/subject= //g'`
  #
  # TRICK ALERT: we are using colons instead of forward slashes
  # otherwise, we cannot scape the / in the DN 
  #
  sed -i "s:#hostdn#:$hostdn:g" etc/install.cfg
  
  fqdn=`hostname --fqdn`
  sed -i "s/#hostname#/$fqdn/g" etc/install.cfg

  dirac-configure etc/install.cfg -A $arch -o $rootPass -o $userPass $setups $DEBUG
  echo "=======================================================================" 
  dirac-setup-site $DEBUG
  
  # Do not use Server Certificate
  #sed -i '107i\    UseServerCertificate = yes' etc/dirac.cfg
  #sed -i '125i\    UseServerCertificate = yes' etc/dirac.cfg
  
}  

#-------------------------------------------------------------------------------
# diracKillMySQL:
#
#   if MySQL is running, it stops it. If not running, returns exit code 1
#
#-------------------------------------------------------------------------------

diracKillMySQL(){

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
# diracKillRunit:
#
#   stops scripts running on startup
#
#-------------------------------------------------------------------------------

diracKillRunit(){

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
# diracCredentials:
#
#   hacks CS service to create a first dirac_admin proxy that will be used
#   to install and play around
#-------------------------------------------------------------------------------

diracCredentials(){
  
  sed -i 's/commitNewData = CSAdministrator/commitNewData = authenticated/g' etc/Configuration_Server.cfg
  dirac-proxy-init -g dirac_admin $DEBUG
  sed -i 's/commitNewData = authenticated/commitNewData = CSAdministrator/g' etc/Configuration_Server.cfg
  
}


#-------------------------------------------------------------------------------
# diracMySQL:
#
#   installs MySQL. If it was running before, it returns an error.
#-------------------------------------------------------------------------------

diracMySQL(){
  
  diracKillMySQL    
  dirac-install-mysql $DEBUG
  
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

  dbs=`cat databases | cut -d ' ' -f 2 | grep -v ^RequestDB | cut -d '.' -f 1`
  for db in $dbs
  do
    dirac-install-db $db $DEBUG
  done

}

#-------------------------------------------------------------------------------
# diracServices:
#
#   installs all services on the file services
#
#-------------------------------------------------------------------------------

diracServices(){

  services=`cat services | cut -d '.' -f 1 | grep -v ^ConfigurationSystem | grep -v SystemAdministrator | grep -v RAWIntegrity | grep -v RunDBInterface | grep -v ProductionRequest | grep -v MigrationMonitoring | grep -v Future | grep -v Bookkeeping | sed 's/System / /g' | sed 's/Handler//g' | sed 's/ /\//g'`
  for serv in $services
  do
    dirac-install-service $serv $DEBUG
  done

}

#-------------------------------------------------------------------------------
# dumpDBs:
#
#   removes all the DBs from the MySQL server to start with a fresh installation
#
#-------------------------------------------------------------------------------

dumpDBs(){

  rootPass=`cat rootMySQL`
  sqlStatements=`mysql -u root -p$rootPass -e "show databases" | grep -v mysql | grep -v Database | grep -v information_schema | grep -v test`
  echo "$sqlStatements" | gawk '{print "drop database " $1 ";select sleep(0.1);"}' | mysql -u root -p$rootPass 

}

#-------------------------------------------------------------------------------
#EOF