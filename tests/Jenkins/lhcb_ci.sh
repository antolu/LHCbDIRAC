#!/bin/sh
#-------------------------------------------------------------------------------
# lhcb_ci
#
# Collection of utilities and functions for automated tests (using Jenkins)
#
# Requires dirac_ci.sh where most generic utility functions are
#-------------------------------------------------------------------------------


# Exit on error. If something goes wrong, we terminate execution
#TODO: fix
#set -o errexit

# first first: sourcing dirac_ci file # the location from where this script is sourced is critical
source TestCode/DIRAC/tests/Jenkins/dirac_ci.sh

#install file
INSTALL_CFG_FILE='$TESTCODE/LHCbDIRAC/tests/Jenkins/install.cfg'


#.............................................................................
#
# findRelease for LHCbDIRAC:
#
#   If the environment variable "LHCBDIRAC_RELEASE" exists, and set, we use the specified release.
#   If the environment variable "LHCBDIRACBRANCH" exists, and set, we use the specified "branch", otherwise we take the last one.
#
#   It reads from releases.cfg and picks the latest version
#   which is written to {project,dirac,lhcbdirac}.version
#
#.............................................................................

function findRelease(){
  echo '[findRelease]'

  # store the current branch
  currentBranch=`git --git-dir=$TESTCODE/LHCbDIRAC/.git rev-parse --abbrev-ref HEAD`

  if [ $currentBranch == 'devel' ]
  then
    echo 'we were already on devel, no need to change'
    # get the releases.cfg file
    cp $TESTCODE/LHCbDIRAC/LHCbDIRAC/releases.cfg $TESTCODE/
  else
    git --git-dir=$TESTCODE/LHCbDIRAC/.git checkout devel
    # get the releases.cfg file
    cp $TESTCODE/LHCbDIRAC/LHCbDIRAC/releases.cfg $TESTCODE/
    # reset the branch
    git --git-dir=$TESTCODE/LHCbDIRAC/.git checkout $currentBranch
  fi

  # Match project ( LHCbDIRAC ) version from releases.cfg
  # Example releases.cfg
  # v7r15-pre2
  # {
  #   Modules = LHCbDIRAC:v7r15-pre2, LHCbWebDIRAC:v3r3p5
  #   Depends = DIRAC:v6r10-pre12
  #   LcgVer = 2013-09-24
  # }

  if [ ! -z "$LHCBDIRAC_RELEASE" ]
  then
    echo '==> Specified release'
    echo $LHCBDIRAC_RELEASE
    projectVersion=$LHCBDIRAC_RELEASE
  else
    if [ ! -z "$LHCBDIRACBRANCH" ]
    then
      echo '==> Looking for LHCBDIRAC branch ' $LHCBDIRACBRANCH
    else
      echo '==> Running on last one'
    fi

    # If I don't specify a LHCBDIRACBRANCH, it will get the latest "production" release
    # First, try to find if we are on a production tag
    if [ ! -z "$LHCBDIRACBRANCH" ]
    then
      projectVersion=`cat $TESTCODE/releases.cfg | grep '[^:]v[[:digit:]]*r[[:digit:]]*p[[:digit:]]*' | grep $LHCBDIRACBRANCH | head -1 | sed 's/ //g'`
    else
      projectVersion=`cat $TESTCODE/releases.cfg | grep '[^:]v[[:digit:]]*r[[:digit:]]*p[[:digit:]]*' | head -1 | sed 's/ //g'`
    fi

    # The special case is when there's no 'p'... (e.g. version v8r3)
    if [ ! "$projectVersion" ]
    then
      if [ ! -z "$LHCBDIRACBRANCH" ]
      then
        projectVersion=`cat $TESTCODE/releases.cfg | grep '[^:]v[[:digit:]]*r[[:digit:]]' | grep $LHCBDIRACBRANCH | head -1 | sed 's/ //g'`
      else
        projectVersion=`cat $TESTCODE/releases.cfg | grep '[^:]v[[:digit:]]*r[[:digit:]]' | head -1 | sed 's/ //g'`
      fi
    fi

    # In case there are no production tags for the branch, look for pre-releases in that branch
    if [ ! "$projectVersion" ]
    then
      if [ ! -z "$LHCBDIRACBRANCH" ]
      then
        projectVersion=`cat $TESTCODE/releases.cfg | grep '[^:]v[[:digit:]]*r[[:digit:]]*'-pre'' | grep $LHCBDIRACBRANCH | head -1 | sed 's/ //g'`
      else
        projectVersion=`cat $TESTCODE/releases.cfg | grep '[^:]v[[:digit:]]*r[[:digit:]]*'-pre'' | head -1 | sed 's/ //g'`
      fi
    fi

  fi



  echo PROJECT:$projectVersion && echo $projectVersion > project.version

  # projectVersionLine : line number where v7r15-pre2 is
  projectVersionLine=`cat $TESTCODE/releases.cfg | grep -n $projectVersion | cut -d ':' -f 1 | head -1`
  # start := line number after "{"
  start=$(($projectVersionLine+2))
  # end   := line number after "}"
  end=$(($start+2))
  # versions :=
  #   Modules = LHCbDIRAC:v7r15-pre2, LHCbWebDIRAC:v3r3p5
  #   Depends = DIRAC:v6r10-pre12
  #   LcgVer = 2013-09-24
  versions=`sed -n "$start,$end p" $TESTCODE/releases.cfg`

  # Extract DIRAC version
  diracVersion=`echo $versions | tr ' ' '\n' | grep ^DIRAC:v*[^,] | sed 's/,//g' | cut -d ':' -f2`
  # Extract LHCbDIRAC version
  lhcbdiracVersion=`echo $versions | tr ' ' '\n' | grep ^LHCbDIRAC:v* | sed 's/,//g' | cut -d ':' -f2`
  # Extract LCG version
  lcgVersion=`echo $versions | sed s/' = '/'='/g | tr ' ' '\n' | grep LcgVer | cut -d '=' -f2`

  # PrintOuts
  echo '==> ' DIRAC:$diracVersion && echo $diracVersion > dirac.version
  echo '==> ' LHCbDIRAC:$lhcbdiracVersion && echo $lhcbdiracVersion > lhcbdirac.version
  echo '==> ' LCG:$lcgVersion && echo $lcgVersion > lcg.version

}



#-------------------------------------------------------------------------------
# diracServices:
#
#   specialized, for fixing BKK DB
#
#-------------------------------------------------------------------------------

diracServices(){
  echo '==> [diracServices]'

  services=`cat services | cut -d '.' -f 1 | grep -v IRODSStorageElementHandler | grep -v ^ConfigurationSystem | grep -v Plotting | grep -v RAWIntegrity | grep -v RunDBInterface | grep -v ComponentMonitoring | grep -v WMSSecureGW | sed 's/System / /g' | sed 's/Handler//g' | sed 's/ /\//g'`

  for serv in $services
  do

    if [ "$serv" == "Bookkeeping/BookkeepingManager" ]
    then
      # start BKK DB setup
      setupBKKDB
      wget http://lhcb-portal-dirac.cern.ch/defaults/cx_Oracle-5.1.tar.gz -O cx_Oracle-5.1.tar.gz
      source /afs/cern.ch/project/oracle/script/setoraenv.sh
      # -s 11203
      python `which easy_install` cx_Oracle-5.1.tar.gz
      # end BKK DB setup
    fi

    echo '==> calling dirac-install-component' $serv $DEBUG
    dirac-install-component $serv $DEBUG
  done

}


#-------------------------------------------------------------------------------
# diracAgents:
#
#   specialized, just adding some more agents to exclude
#
#-------------------------------------------------------------------------------

diracAgents(){
  echo '==> [diracAgents]'

  agents=`cat agents | cut -d '.' -f 1 | grep -v LFC | grep -v MyProxy | grep -v CAUpdate | grep -v CE2CSAgent.py | grep -v GOCDB2CS | grep -v Bdii2CS | grep -v CacheFeeder | grep -v NetworkAgent | grep -v FrameworkSystem | grep -v DiracSiteAgent | grep -v StatesMonitoringAgent | grep -v DataProcessingProgressAgent | grep -v RAWIntegrityAgent  | grep -v GridSiteWMSMonitoringAgent | grep -v HCAgent | grep -v GridCollectorAgent | grep -v HCProxyAgent | grep -v Nagios | grep -v AncestorFiles | grep -v BKInputData | grep -v LHCbPRProxyAgent | grep -v StorageUsageAgent | grep -v PopularityAnalysisAgent | grep -v SEUsageAgent | sed 's/System / /g' | sed 's/ /\//g'`

  for agent in $agents
  do
    if [[ $agent == *" JobAgent"* ]]
    then
      echo '==> '
    else
      echo '==> calling dirac-cfg-add-option agent' $agent
      python $TESTCODE/DIRAC/tests/Jenkins/dirac-cfg-add-option.py agent $agent
      echo '==> calling dirac-agent' $agent -o MaxCycles=1 $DEBUG
      dirac-agent $agent  -o MaxCycles=1 $DEBUG
    fi
  done

}



#.............................................................................
#
# diracInstallCommand:
#
#   Specialized command:
#  LHCb project version, obtained from the file 'project.version'.
#
#.............................................................................

function diracInstallCommand(){
  $SERVERINSTALLDIR/dirac-install -l LHCb -r `cat project.version` -e LHCb -t server $DEBUG
}

# Getting a CFG file for the installation: Specialized command
function getCFGFile(){
  echo '==> [getCFGFile]'

  cp $TESTCODE/LHCbDIRAC/tests/Jenkins/install.cfg $SERVERINSTALLDIR/
  sed -i s/VAR_Release/$projectVersion/g $SERVERINSTALLDIR/install.cfg
}




#-------------------------------------------------------------------------------
# Here is where the real functions start
#-------------------------------------------------------------------------------

#...............................................................................
#
# LHCbDIRACPilotInstall:
#
#   This function uses the pilot code to make a DIRAC pilot installation
#   The JobAgent is not run here
#
#...............................................................................

function LHCbDIRACPilotInstall(){

  echo '==> Starting LHCbDIRACPilotInstall'

  prepareForPilot
  default

  cp $TESTCODE/LHCbDIRAC/LHCbDIRAC/WorkloadManagementSystem/PilotAgent/LHCbPilotCommands.py $PILOTINSTALLDIR/

  if [ ! -z "$LHCBDIRAC_RELEASE" ]
  then
    echo '==> Specified release'
    echo $LHCBDIRAC_RELEASE
    installVersion='-r'
    installVersion+=" $LHCBDIRAC_RELEASE"
  else
    installVersion=''
  fi

  #run the dirac-pilot script, only for installing, do not run the JobAgent here
  cwd=$PWD
  cd $PILOTINSTALLDIR
  if [ $? -ne 0 ]
  then
    echo 'ERROR: cannot change to ' $PILOTINSTALLDIR
    return
  fi

  commandList="LHCbGetPilotVersion,CheckWorkerNode,LHCbInstallDIRAC,LHCbConfigureBasics,CheckCECapabilities,CheckWNCapabilities,LHCbConfigureSite,LHCbConfigureArchitecture,LHCbConfigureCPURequirements"
  options="-S $DIRACSETUP -l LHCb $installVersion -g $lcgVersion -C $CSURL -N $JENKINS_CE -Q $JENKINS_QUEUE -n $JENKINS_SITE --cert --certLocation=/home/dirac/certs/ -E LHCbPilot"

  if [ "$customCommands" ]
  then
    echo 'Using custom command list'
    commandList=$customCommands
  fi

  if [ "$customOptions" ] #like "devLbLogin"
  then
    echo 'Using custom options'
    options="$options -o $customOptions"
  fi

  echo $( eval echo Executing python dirac-pilot.py $options -X $commandList $DEBUG)
  python dirac-pilot.py $options -X $commandList $DEBUG

  cd $cwd
  if [ $? -ne 0 ]
  then
    echo 'ERROR: cannot change to ' $cwd
    return
  fi

  echo '==> Done LHCbDIRACPilotInstall'
}


function fullLHCbPilot(){

  # This supposes that the version to install is got already

  #first simply install via the pilot
  LHCbDIRACPilotInstall

  #this should have been created, we source it so that we can continue (otherwise the dirac commands below are not found)
  echo '==> sourcing environmentLHCbDirac'
  source $PILOTINSTALLDIR/environmentLHCbDirac

  echo -e '\n----PATH:'$PATH'\n----' | tr ":" "\n"
  echo -e '\n----LD_LIBRARY_PATH:'$LD_LIBRARY_PATH'\n----' | tr ":" "\n"
  echo -e '\n----DYLD_LIBRARY_PATH:'$DYLD_LIBRARY_PATH'\n----' | tr ":" "\n"
  echo -e '\n----PYTHONPATH:'$PYTHONPATH'\n----' | tr ":" "\n"

  echo -e '\n----python'
  echo $(python -V)
  echo $(which python)

  echo '==> Adding the LocalSE, for the subsequent tests'
  dirac-configure -FDMH --UseServerCertificate -L CERN-SWTEST -O $PILOTINSTALLDIR/$PILOTCFG $PILOTINSTALLDIR/$PILOTCFG $DEBUG

  #be sure we only have pilot.cfg
  mv $PILOTINSTALLDIR/etc/dirac.cfg $PILOTINSTALLDIR/etc/dirac.cfg-not-here

  getUserProxy

  echo '==> Set not to use the server certificate for running the jobs'
  dirac-configure -FDMH -o /DIRAC/Security/UseServerCertificate=False -O $PILOTINSTALLDIR/$PILOTCFG $PILOTINSTALLDIR/$PILOTCFG $DEBUG
}

function getUserProxy(){

  echo '==> Started getUserProxy'

  touch $PILOTINSTALLDIR/$PILOTCFG
  #Configure for CPUTimeLeft
  python $TESTCODE/DIRAC/tests/Jenkins/dirac-cfg-update.py $PILOTINSTALLDIR/$PILOTCFG -F $PILOTINSTALLDIR/$PILOTCFG -S $DIRACSETUP -o /DIRAC/Security/UseServerCertificate=True -o /DIRAC/Security/CertFile=/home/dirac/certs/hostcert.pem -o /DIRAC/Security/KeyFile=/home/dirac/certs/hostkey.pem $DEBUG
  #Getting a user proxy, so that we can run jobs
  downloadProxy

  echo '==> Done getUserProxy'
}

function submitAndMatch(){

  installLHCbDIRAC
  submitJob

  #Run the full pilot, including the JobAgent
  cd $PILOTINSTALLDIR
  if [ $? -ne 0 ]
  then
    echo 'ERROR: cannot change to ' $PILOTINSTALLDIR
    return
  fi
  prepareForPilot
  default
  cp $TESTCODE/LHCbDIRAC/LHCbDIRAC/WorkloadManagementSystem/PilotAgent/LHCbPilotCommands.py $PILOTINSTALLDIR/LHCbPilotCommands.py

  if [ ! -z "$PILOT_VERSION" ]
  then
    echo -e "==> Running python dirac-pilot.py -S $DIRACSETUP -l LHCb -r $PILOT_VERSION -g $lcgVersion -C $CSURL -N $JENKINS_CE -Q $JENKINS_QUEUE -n $JENKINS_SITE --cert --certLocation=/home/dirac/certs/ -M 4 -E LHCbPilot -X LHCbGetPilotVersion,CheckWorkerNode,LHCbInstallDIRAC,LHCbConfigureBasics,CheckCECapabilities,CheckWNCapabilities,LHCbConfigureSite,LHCbConfigureArchitecture,LHCbConfigureCPURequirements,LaunchAgent $DEBUG"
    python dirac-pilot.py -S $DIRACSETUP -l LHCb -r $PILOT_VERSION -g $lcgVersion -C $CSURL -N $JENKINS_CE -Q $JENKINS_QUEUE -n $JENKINS_SITE --cert --certLocation=/home/dirac/certs/ -M 4 -E LHCbPilot -X LHCbGetPilotVersion,CheckWorkerNode,LHCbInstallDIRAC,LHCbConfigureBasics,CheckCECapabilities,CheckWNCapabilities,LHCbConfigureSite,LHCbConfigureArchitecture,LHCbConfigureCPURequirements,LaunchAgent $DEBUG
  else
    echo -e "==> Running python dirac-pilot.py -S $DIRACSETUP -l LHCb -g $lcgVersion -C $CSURL -N $JENKINS_CE -Q $JENKINS_QUEUE -n $JENKINS_SITE --cert --certLocation=/home/dirac/certs/ -M 4 -E LHCbPilot -X LHCbGetPilotVersion,CheckWorkerNode,LHCbInstallDIRAC,LHCbConfigureBasics,CheckCECapabilities,CheckWNCapabilities,LHCbConfigureSite,LHCbConfigureArchitecture,LHCbConfigureCPURequirements,LaunchAgent $DEBUG"
    python dirac-pilot.py -S $DIRACSETUP -l LHCb -g $lcgVersion -C $CSURL -N $JENKINS_CE -Q $JENKINS_QUEUE -n $JENKINS_SITE --cert --certLocation=/home/dirac/certs/ -M 4 -E LHCbPilot -X LHCbGetPilotVersion,CheckWorkerNode,LHCbInstallDIRAC,LHCbConfigureBasics,CheckCECapabilities,CheckWNCapabilities,LHCbConfigureSite,LHCbConfigureArchitecture,LHCbConfigureCPURequirements,LaunchAgent $DEBUG
  fi
}

function installLHCbDIRAC(){

  if [ ! "$LBRUNRELEASE" ]
  findRelease
  then
    echo '==> Installing client with dirac-install'
    installLHCbDIRACClient
  else
    echo '==> Installing client with lb-run'
    setupLHCbDIRAC
  fi

}

function installLHCbDIRACClient(){

  echo '==> Installing LHCbDIRAC client'

  cp $TESTCODE/DIRAC/Core/scripts/dirac-install.py $CLIENTINSTALLDIR/dirac-install
  chmod +x $CLIENTINSTALLDIR/dirac-install
  cd $CLIENTINSTALLDIR
  if [ $? -ne 0 ]
  then
    echo 'ERROR: cannot change to ' $CLIENTINSTALLDIR
    return
  fi
  ./dirac-install -l LHCb -r `cat $WORKSPACE/project.version` -e LHCb -t client $DEBUG

  mkdir $CLIENTINSTALLDIR/etc
  ln -s /cvmfs/lhcb.cern.ch/lib/lhcb/DIRAC/etc/dirac.cfg $CLIENTINSTALLDIR/etc/dirac.cfg

  source bashrc

}

function setupLHCbDIRAC(){

  echo -e "==> Invoking LbLogin.sh"
  . /cvmfs/lhcb.cern.ch/lib/lhcb/LBSCRIPTS/LBSCRIPTS_v8r6p5/InstallArea/scripts/LbLogin.sh

  local version=`cat project.version`
  echo -e "==> Invoking lb-run LHCbDirac/$version bash -norc"
  lb-run LHCbDirac/$version bash -norc
  local status=$?
  if [ $status -ne 0 ]
  then
    echo -e "==> lb-run NOT successful: going to install client with dirac-install"
    installLHCbDIRACClient
  else
    export PYTHONPATH=$PYTHONPATH:$CLIENTINSTALLDIR/
  fi
}


function submitJob(){

  echo -e "==> Submitting a simple job"

  #This is is executed from the $CLIENTINSTALLDIR

  export PYTHONPATH=$TESTCODE:$PYTHONPATH
  #Get a proxy and submit the job: this job will go to the certification setup, so we suppose the JobManager there is accepting jobs
  getUserProxy #this won't really download the proxy, so that's why the next command is needed
  cp $TESTCODE/DIRAC/tests/Jenkins/dirac-proxy-download.py .
  python dirac-proxy-download.py $DIRACUSERDN -R $DIRACUSERROLE -o /DIRAC/Security/UseServerCertificate=True -o /DIRAC/Security/CertFile=/home/dirac/certs/hostcert.pem -o /DIRAC/Security/KeyFile=/home/dirac/certs/hostkey.pem -o /DIRAC/Setup=LHCb-Certification -ddd
  cp $TESTCODE/LHCbDIRAC/tests/Jenkins/dirac-test-job.py .
  python dirac-test-job.py -o /DIRAC/Setup=LHCb-Certification $DEBUG

  rm $PILOTINSTALLDIR/$PILOTCFG
}


function sourcingEnv(){

  echo -e "==> Sourcing the environment (inlcuding LbLogin env)"
  source $PILOTINSTALLDIR/environmentLHCbDirac
}

function setupBKKDB(){
  echo -e "==> Setting up the Bookkeeping Database"
  python $TESTCODE/LHCbDIRAC/tests/Jenkins/dirac-bkk-cfg-update.py -p $ORACLEDB_PASSWORD $DEBUG
}

#-------------------------------------------------------------------------------
#EOF
