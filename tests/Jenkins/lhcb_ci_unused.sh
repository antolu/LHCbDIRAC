

# ##### Older functions
#
# o prepareDIRAC
# o prepareTestExternals
# o runTest
# o mergeTests


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
  # killMySQL:
  #
  #   if MySQL is running, it stops it.
  #
  #.............................................................................

  function killMySQL(){
    echo '[killMySQL]'

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

  #.............................................................................
  #
  # stopMySQL:
  #
  #   if MySQL is running, it stops it.
  #
  #.............................................................................

  function stopMySQL(){
    echo '[stopMySQL]'

    # Let's try to be a bit more delicated than the function above

    $WORKSPACE/mysql/share/mysql/mysql.server stop
    
    # If does not work, we kill it
    killMySQL
        
  }


  #.............................................................................
  #
  # startMySQL:
  #
  #   if MySQL is not running, it starts it.
  #
  #.............................................................................

  function startMySQL(){
    echo '[startMySQL]'

    $WORKSPACE/mysql/share/mysql/mysql.server start
        
  }


#...............................................................................
#
# prepareDIRAC:
#
#   installs DIRAC, MySQL, (DIRAC) Externals, creates credentials, etc...
#   IT is THE function. 
#
#...............................................................................


function prepareDIRAC(){
  
	if [ ! -z "$DEBUG" ]
	then
		echo 'Running in DEBUG mode'
		export DEBUG='-ddd'
	fi  
  
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
  
	dirac-install-db ProxyDB $DEBUG
	dirac-install-service Framework/ProxyManager $DEBUG
	ln -s $WORKSPACE/runit/Framework/ProxyManager $WORKSPACE/startup/Framework_ProxyManager
  
	# Give runit 10 secs to pick it up
	sleep 10
	diracProxies
}


#...............................................................................
#
# prepareTestExternals:
#
#   installs all externals we need to run the test:
#   o nose 
#
#...............................................................................


function prepareTestExternals(){

	source $WORKSPACE/bashrc
	python `which easy_install` nose

}

#...............................................................................
#
# runTest:
#
#   given a TEST_MODE ( configure, install ) runs all tests tagged with the 
#   TEST_MODE and stores their results together.  
#
#...............................................................................

function runTest(){

  set +o errexit

  if [ -z "$TEST_MODE" ]
  then
    exit "TEST_MODE not found"
  fi
  
  mkdir -p $WORKSPACE/lhcb_ci

  source $WORKSPACE/bashrc

  export PYTHONPATH=$PYTHONPATH:$WORKSPACE/LHCbTestDirac/Jenkins
  export LHCB_CI_DEBUG=$WORKSPACE/lhcb_ci/${TEST_MODE}.log

  echo "########################################################"
  echo "$TEST_MODE TESTS"
  echo "log file: $LHCB_CI_DEBUG"
  echo "########################################################"

  nosetests -a $TEST_MODE --with-xunit LHCbTestDirac/Jenkins/lhcb_ci/test -v --xunit-file=nosetests_${TEST_MODE}.xml --with-coverage --cover-package=DIRAC,LHCbDIRAC
  mv .coverage .coverage.${TEST_MODE}

  set -o errexit

}


#...............................................................................
#
# mergeTests:
#
#   merges the individual results of all the tests run. 
#     
#
#...............................................................................

function mergeTests(){

  source $WORKSPACE/bashrc

  coverage combine
  coverage xml --include="*DIRAC/*"

}


#-------------------------------------------------------------------------------
# dumpDBs:
#
#   removes all the DBs from the MySQL server to start with a fresh installation
#
#-------------------------------------------------------------------------------

function dumpDBs(){
	echo '[dumpDBs]'

	rootPass=`cat rootMySQL`
	sqlStatements=`mysql -u root -p$rootPass -e "show databases" | grep -v mysql | grep -v Database | grep -v information_schema | grep -v test`
	echo "$sqlStatements" | gawk '{print "drop database " $1 ";select sleep(0.1);"}' | mysql -u root -p$rootPass 

}



function integrationTest(){
	echo '[integrationTest]'
	
	nosetests --with-xunit $WORKSPACE/$1/Integration/$2 -v --xunit-file=nosetests_$1_$2.xml --with-coverage --cover-package=DIRAC,LHCbDIRAC
	mv .coverage .coverage._$1_$2
	
}
