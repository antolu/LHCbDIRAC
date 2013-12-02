#!/bin/sh 
#-------------------------------------------------------------------------------
# lhcb_ci
#  
# : prepares installs DIRAC, installs MySQL
# : assumes ./project.version exists
# : assumes ./bashrc exists
# : assumes ~/host{cert,key}.pem  
#  
# ubeda@cern.ch  
# 26/VI/2013
#-------------------------------------------------------------------------------


# Exit on error. If something goes wrong, we terminate execution
set -o errexit

# URL where to get dirac-install script
DIRAC_INSTALL='https://github.com/DIRACGrid/DIRAC/raw/integration/Core/scripts/dirac-install.py'

# Path to lhcb_ci config files
LHCb_CI_CONFIG=$WORKSPACE/LHCbTestDirac/Jenkins/config/lhcb_ci


#-------------------------------------------------------------------------------
# Finders... functions devoted to find DBs, Services, versions, etc..
#-------------------------------------------------------------------------------


  #.............................................................................
  #
  # findRelease:
  #
  #   If the environment variable "PRERELEASE" exists, we use a LHCb prerelease
  #   instead of a regular release ( production-like ).
  #   If any parameter is passed, we assume we are on pre-release mode, otherwise, 
  #   we assume production. It reads from releases.cfg and picks the latest version
  #   which is written to {project,dirac,lhcbdirac}.version
  #
  #.............................................................................
  
  function findRelease(){
    echo '[findRelease]'

    cd $WORKSPACE

    PRE='p[[:digit:]]*'

    if [ ! -z "$PRERELEASE" ]
    then
      echo 'Running on PRERELEASE mode'
      PRE='-pre'
    else
      echo 'Running on REGULAR mode'
    fi  
  
    # Create temporary directory where to store releases.cfg ( will be deleted at
    # the end of the function )
    tmp_dir=`mktemp -d -q`
    cd $tmp_dir
    wget http://svn.cern.ch/guest/dirac/LHCbDIRAC/trunk/LHCbDIRAC/releases.cfg --quiet

    # Match project ( LHCbDIRAC, soon BeautyDirac ) version from releases.cfg
    # Example releases.cfg
    # v7r15-pre2
    # {
    #   Modules = LHCbDIRAC:v7r15-pre2, LHCbWebDIRAC:v3r3p5
    #   Depends = DIRAC:v6r10-pre12
    #   LcgVer = 2013-09-24
    # }
    
    # projectVersion := v7r15-pre2 ( if we are in PRERELEASE mode )
    projectVersion=`cat releases.cfg | grep [^:]v[[:digit:]]r[[:digit:]]*$PRE | head -1 | sed 's/ //g'`

    # projectVersionLine : line number where v7r15-pre2 is
    projectVersionLine=`cat releases.cfg | grep -n $projectVersion | cut -d ':' -f 1 | head -1`
    # start := line number after "{"  
    start=$(($projectVersionLine+2))
    # end   := line number after "}"
    end=$(($start+2))
    # versions :=
    #   Modules = LHCbDIRAC:v7r15-pre2, LHCbWebDIRAC:v3r3p5
    #   Depends = DIRAC:v6r10-pre12
    #   LcgVer = 2013-09-24    
    versions=`sed -n "$start,$end p" releases.cfg`

    # Extract DIRAC version
    diracVersion=`echo $versions | tr ' ' '\n' | grep ^DIRAC:v*[^,] | sed 's/,//g' | cut -d ':' -f2`
    # Extract LHCbDIRAC version
    lhcbdiracVersion=`echo $versions | tr ' ' '\n' | grep ^LHCbDIRAC:v* | sed 's/,//g' | cut -d ':' -f2`
  
    # Back to $WORKSPACE and clean tmp_dir
    cd $WORKSPACE
    rm -r $tmp_dir
    
    # PrintOuts
    echo PROJECT:$projectVersion     && echo $projectVersion   > project.version
    echo DIRAC:$diracVersion         && echo $diracVersion     > dirac.version
    echo LHCbDIRAC:$lhcbdiracVersion && echo $lhcbdiracVersion > lhcbdirac.version

  }


  #.............................................................................
  #
  # findSystems:
  #
  #   gets all system names from *DIRAC code and writes them to a file
  #   named systems.
  #
  #.............................................................................
  
  function findSystems(){
    echo '[findSystems]'
   
    cd $WORKSPACE
    find *DIRAC/ -name *System  | cut -d '/' -f 2 | sort | uniq > systems

    echo found `wc -l systems`

  }


  #.............................................................................
  #
  # findDatabases:
  #
  #   gets all database names from *DIRAC code and writes them to a file
  #   named databases.
  #
  #.............................................................................

  function findDatabases(){
    echo '[findDatabases]'
    
    cd $WORKSPACE
    #
    # HACK ALERT:
    #
    #   We are avoiding TransferDB, which will be deprecated soon.. 
    #
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
# OPEN SSL... let's create a fake CA and certificates
#-------------------------------------------------------------------------------

  
  #.............................................................................
  #
  # function generateCertificates
  #
  #   This function generates a random host certificate ( certificate and key ), 
  #   which will be stored on etc/grid-security. As we need a CA to validate it,
  #   we simply copy it to the directory where the CA certificates are supposed
  #   to be stored etc/grid-security/certificates. In real, we'd copy them from 
  #   CVMFS:
  #     /cvmfs/grid.cern.ch/etc/grid-security/certificates    
  #
  #   Additional info:
  #     http://www.openssl.org/docs/apps/req.html
  #
  #.............................................................................
  
  function generateCertificates(){
    echo '[generateCertificates]'

    mkdir -p $WORKSPACE/etc/grid-security/certificates
    cd $WORKSPACE/etc/grid-security
    
    # Generate private RSA key
    openssl genrsa -out hostkey.pem 2048 2&>1 /dev/null
    
    # Prepare OpenSSL config file, it contains extensions to put into place,
    # DN configuration, etc..
    cp $LHCb_CI_CONFIG/openssl_config openssl_config
    fqdn=`hostname --fqdn`
    sed -i "s/#hostname#/$fqdn/g" openssl_config
   
    # Generate X509 Certificate based on the private key and the OpenSSL configuration
    # file, valid for one day.
    openssl req -new -x509 -key hostkey.pem -out hostcert.pem -days 1 -config openssl_config
  
    # Copy hostcert, hostkey to certificates ( CA dir )
    cp host{cert,key}.pem certificates/
  
  }

  
  #.............................................................................
  #
  # generateUserCredentials:
  #
  #   Given we know the "CA" certificates, we can use them to sign a randomly 
  #   generated key / host certificate. This function is very similar to
  #   generateCertificates. User credentials will be stored at:
  #     $WORKSPACE/user 
  #   The user will be called "lhcbciuser". Do not confuse with the admin user,
  #   which is "lhcbci".
  #
  #   Additional info:
  #     http://acs.lbl.gov/~boverhof/openssl_certs.html
  #
  #.............................................................................

  function generateUserCredentials(){
    echo '[generateUserCreedentials]'
  
    # Generate directory where to store credentials
    mkdir $WORKSPACE/user
    
    cd $WORKSPACE/user    
    
    cp $LHCb_CI_CONFIG/openssl_config openssl_config
    sed -i 's/#hostname#/lhcbciuser/g' openssl_config
    openssl genrsa -out client.key 1024 2&>1 /dev/null
    openssl req -key client.key -new -out client.req -config openssl_config
    # This is a little hack to make OpenSSL happy...
    echo 00 > file.srl
    
    CA=$WORKSPACE/etc/grid-security/certificates
    
    openssl x509 -req -in client.req -CA $CA/hostcert.pem -CAkey $CA/hostkey.pem -CAserial file.srl -out client.pem 2&>1 /dev/null
  
  }


#-------------------------------------------------------------------------------
# DIRAC scripts... well, not really - built on top of DIRAC scripts.
#-------------------------------------------------------------------------------


  #.............................................................................
  #
  # diracInstall:
  #
  #   This function gets the DIRAC install script defined on $DIRAC_INSTAll and
  #   runs it with some hardcoded options. The only option that varies is the 
  #   project version, in this case LHCb project version, obtained from the file
  #   'project.version'.
  #
  #.............................................................................

  function diracInstall(){
    echo '[diracInstall]'

    cd $WORKSPACE

    wget --no-check-certificate -O dirac-install $DIRAC_INSTALL --quiet
    chmod +x dirac-install
    ./dirac-install -l LHCb -r `cat project.version` -e LHCb -t server $DEBUG

  }


  #.............................................................................
  #
  # diracConfigure:
  # 
  #   in short, it writes dirac.cfg file. More on detail, it sets: 
  #
  #   o /LocalSite/Architecture
  #   o /LocalInstallation/Database/RootPwd
  #   o /LocalInstallation/Database/Password
  #   o /LocalInstallation/HostDN
  #   o /LocalInstallation/Host
  #
  #.............................................................................

  function diracConfigure(){
    echo '[diracConfigure]'
    
    cd $WORKSPACE

    # Find architecture platform
    arch=`dirac-architecture`
    
    # Randomly generated passwords
    # DB root
    randomRoot=`tr -cd '[:alnum:]' < /dev/urandom | fold -w20 | head -n1`
    rootPass=/LocalInstallation/Database/RootPwd=$randomRoot
    # DB user
    randomUser=`tr -cd '[:alnum:]' < /dev/urandom | fold -w20 | head -n1`
    userPass=/LocalInstallation/Database/Password=$randomUser
    
    # Stores passwords on files for latter usage
    echo $randomRoot > rootMySQL
    echo $randomUser > userMySQL
    
    # Sets all systems on Jenkins setup
    setups=`cat systems | sed 's/System//' | sed 's/^/-o \/DIRAC\/Setups\/Jenkins\//' | sed 's/$/=Jenkins/'` 

    cp $LHCb_CI_CONFIG/install.cfg etc/install.cfg
    
    # Set HostDN in install.cfg
    # We use colons instead of forward slashes in sed, otherwise we cannot scape
    # the '/' characters in the DN
    hostdn=`openssl x509 -noout -in etc/grid-security/hostcert.pem -subject | sed 's/subject= //g'` 
    sed -i "s:#hostdn#:$hostdn:g" etc/install.cfg
  
    # Set FQDN in install.cfg
    fqdn=`hostname --fqdn`
    sed -i "s/#hostname#/$fqdn/g" etc/install.cfg

    dirac-configure etc/install.cfg -A $arch -o $rootPass -o $userPass $setups $DEBUG
    dirac-setup-site $DEBUG
  
  }  


  #.............................................................................
  #
  # diracCredentials:
  #
  #   hacks CS service to create a first dirac_admin proxy that will be used
  #   to install the components and run the test ( some of them ).
  #
  #.............................................................................

  function diracCredentials(){
  
    cd $WORKSPACE
  
    sed -i 's/commitNewData = CSAdministrator/commitNewData = authenticated/g' etc/Configuration_Server.cfg
    dirac-proxy-init -g dirac_admin -C $WORKSPACE/user/client.pem -K $WORKSPACE/user/client.key $DEBUG
    sed -i 's/commitNewData = authenticated/commitNewData = CSAdministrator/g' etc/Configuration_Server.cfg
  
  }


  #.............................................................................
  #
  # diracMySQL:
  #
  #   installs MySQL. If it was running before, kills it before proceeding with
  #   the installation. It seems the installation procedure is not complete,
  #   which means we have to introduce a little hack on mysql.server file such
  #   that the basedir points to our Linux external binaries.
  #
  #.............................................................................

  function diracMySQL(){
  
    # Kills MySQL daemon if running
    killMySQL
    
    # Hacks a bit mysql.server file
    linuxDir=`ls $WORKSPACE | grep Linux`
    basedir=$WORKSPACE/$linuxDir
    sed -i "s:basedir=:basedir=$basedir:g" $WORKSPACE/mysql/share/mysql/mysql.server
  
    # Install MySQL using DIRAC scripts
    dirac-install-mysql $DEBUG
    dirac-fix-mysql-script $DEBUG
  
  }


#-------------------------------------------------------------------------------
# Kill scripts. Used to clean environment before getting into trouble
#-------------------------------------------------------------------------------


  #.............................................................................
  #
  # killRunsv:
  #
  #   it makes sure there are no runsv processes running. If it finds any, it
  #   terminates it. This means, no more than one Job running this kind of test
  #   on the same machine at the same time ( executors =< 1 ). Indeed, it cleans
  #   two particular processes, 'runsvdir' and 'runsv'. 
  #
  #.............................................................................

  function killRunsv(){
    echo '[killRunsv]'

    # Bear in mind that we run with 'errexit' mode. This call, if finds nothing
    # will return an error, which will make the whole script exit. However, if 
    # finds nothing we are good, it means there are not leftover processes from
    # other runs. So, we disable 'errexit' mode for this call.
    
    set +o errexit
    runsvdir=`ps aux | grep 'runsvdir ' | grep -v 'grep'`
    set -o errexit
  
    if [ ! -z "$runsvdir" ]
    then
      killall runsvdir
    fi   

    # Same as before
    set +o errexit
    runsv=`ps aux | grep 'runsv ' | grep -v 'grep'`
    set -o errexit
  
    if [ ! -z "$runsv" ]
    then
      killall runsv
    fi   
   
  }


  #.............................................................................
  #
  # killMySQL:
  #
  #   if MySQL is running, it stops it.
  #
  #.............................................................................

  function killMySQL(){

    # Bear in mind that we run with 'errexit' mode. This call, if finds nothing
    # will return an error, which will make the whole script exit. However, if 
    # finds nothing we are good, it means there are not leftover processes from
    # other runs. So, we disable 'errexit' mode for this call.
    
    set +o errexit
    mysql=`ps aux | grep mysql | grep -v grep`
    set -o errexit
   
    if [ ! -z "$mysql" ]
    then
      killall mysqld
    fi   
   
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


#
#-------------------------------------------------------------------------------
# Here are where the real functions start
#-------------------------------------------------------------------------------
#


function prepareTest(){
  
  [ "$DEBUG" ] && 'Running in DEBUG mode' && DEBUG='-ddd'
  
  killRunsv
  
  findRelease
  
  diracInstall
  generateCertificates
  
  findSystems
  findDatabases

  source $WORKSPACE/bashrc

  diracConfigure
  
  generateUserCredentials
  diracCredentials

  diracMySQL

}


#-------------------------------------------------------------------------------
#EOF