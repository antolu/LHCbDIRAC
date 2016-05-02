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
#   If the environment variable "PRERELEASE" exists, we use a LHCb prerelease
#   instead of a regular release ( production-like ).
#   If any parameter is passed, we assume we are on pre-release mode, otherwise,
#   we assume production. It reads from releases.cfg and picks the latest version
#   which is written to {project,dirac,lhcbdirac}.version
#
#.............................................................................

function findRelease(){
	echo '[findRelease]'

	# store the current branch
	currentBranch=`git --git-dir=$TESTCODE/LHCbDIRAC/.git rev-parse --abbrev-ref HEAD`
	echo 'we were on ' $currentBranch
	# get the releases.cfg file
	git --git-dir=$TESTCODE/LHCbDIRAC/.git checkout devel
	cp $TESTCODE/LHCbDIRAC/LHCbDIRAC/releases.cfg $TESTCODE/
	# reset the branch
	git --git-dir=$TESTCODE/LHCbDIRAC/.git checkout $currentBranch

  # Match project ( LHCbDIRAC ) version from releases.cfg
  # Example releases.cfg
  # v7r15-pre2
  # {
  #   Modules = LHCbDIRAC:v7r15-pre2, LHCbWebDIRAC:v3r3p5
  #   Depends = DIRAC:v6r10-pre12
  #   LcgVer = 2013-09-24
  # }

	PRE='p[[:digit:]]*'

	if [ ! -z "$LHCBDIRAC_RELEASE" ]
	then
		echo '==> Specified release'
		echo $LHCBDIRAC_RELEASE
		projectVersion=$LHCBDIRAC_RELEASE
	else
		if [ ! -z "$PRERELEASE" ]
		then
			echo '==> Running on PRERELEASE mode'
			PRE='-pre'
		else
			echo '==> Running on REGULAR mode'
		fi
		# projectVersion := v7r15-pre2 ( if we are in PRERELEASE mode )
		projectVersion=`cat $TESTCODE/releases.cfg | grep [^:]v[[:digit:]]r[[:digit:]]*$PRE | head -1 | sed 's/ //g'`
	fi

	echo PROJECT:$projectVersion && echo $projectVersion > project.version

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
  # Extract LCG version
	lcgVersion=`echo $versions | sed s/' = '/'='/g | tr ' ' '\n' | grep LcgVer | cut -d '=' -f2`

  # PrintOuts
  echo '==> ' DIRAC:$diracVersion && echo $diracVersion > dirac.version
  echo '==> ' LHCbDIRAC:$lhcbdiracVersion && echo $lhcbdiracVersion > lhcbdirac.version
  echo '==> ' LCG:$lcgVersion && echo $lcgVersion > lcg.version

}


#.............................................................................
#
# diracInstallCommand:
#
#   Specialized command:
#	LHCb project version, obtained from the file 'project.version'.
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
	python $PILOTINSTALLDIR/dirac-pilot.py -S $DIRACSETUP -l LHCb $installVersion -C $CSURL -N $JENKINS_CE -Q $JENKINS_QUEUE -n $JENKINS_SITE --cert --certLocation=/home/dirac/certs/ -E LHCbPilot -X LHCbGetPilotVersion,CheckWorkerNode,LHCbInstallDIRAC,LHCbConfigureBasics,CheckCECapabilities,CheckWNCapabilities,LHCbConfigureSite,LHCbConfigureArchitecture,LHCbConfigureCPURequirements $DEBUG

	echo '==> Done LHCbDIRACPilotInstall'
}


function fullLHCbPilot(){

	# This supposes that the version to install is got already

	#first simply install via the pilot
	LHCbDIRACPilotInstall

	#this should have been created, we source it so that we can continue
	echo '==> sourcing bashrc'
	source $PILOTINSTALLDIR/bashrc

	echo '==> Adding the LocalSE and the CPUTimeLeft, for the subsequent tests'
	dirac-configure -FDMH --UseServerCertificate -L CERN-SWTEST -O $PILOTCFG $PILOTCFG $DEBUG

	#be sure we only have pilot.cfg
	mv $PILOTINSTALLDIR/etc/dirac.cfg $PILOTINSTALLDIR/etc/dirac.cfg-not-here

	getUserProxy

	echo '==> Set not to use the server certificate for running the jobs'
	dirac-configure -FDMH -o /DIRAC/Security/UseServerCertificate=False -O $PILOTCFG $PILOTCFG $DEBUG
}

function getUserProxy(){

	echo '==> Started getUserProxy'

	touch $PILOTINSTALLDIR/$PILOTCFG
	#Configure for CPUTimeLeft
	python $TESTCODE/DIRAC/tests/Jenkins/dirac-cfg-update.py -F $PILOTINSTALLDIR/$PILOTCFG -S $DIRACSETUP -o /DIRAC/Security/UseServerCertificate=True -o /DIRAC/Security/CertFile=/home/dirac/certs/hostcert.pem -o /DIRAC/Security/KeyFile=/home/dirac/certs/hostkey.pem $DEBUG
	#Getting a user proxy, so that we can run jobs
	downloadProxy

	echo '==> Done getUserProxy'
}

function submitAndMatch(){

	# I execute in a subshell
	(
		installLHCbDIRAC
		submitJob
	)

	#Run the full pilot, including the JobAgent
	prepareForPilot
	default
	cp $TESTCODE/LHCbDIRAC/LHCbDIRAC/WorkloadManagementSystem/PilotAgent/LHCbPilotCommands.py $TESTCODE/LHCbPilotCommands.py

	if [ ! -z "$PILOT_VERSION" ]
	then
		echo "==> Running python dirac-pilot.py -S $DIRACSETUP -l LHCb -r $PILOT_VERSION -C $CSURL -N $JENKINS_CE -Q $JENKINS_QUEUE -n $JENKINS_SITE --cert --certLocation=/home/dirac/certs/ -M 2 -E LHCbPilot -X LHCbGetPilotVersion,CheckWorkerNode,LHCbInstallDIRAC,LHCbConfigureBasics,LHCbConfigureSite,LHCbConfigureArchitecture,LHCbConfigureCPURequirements,LaunchAgent $DEBUG"
		python dirac-pilot.py -S $DIRACSETUP -l LHCb -r $PILOT_VERSION -C $CSURL -N $JENKINS_CE -Q $JENKINS_QUEUE -n $JENKINS_SITE --cert --certLocation=/home/dirac/certs/ -M 2 -E LHCbPilot -X LHCbGetPilotVersion,CheckWorkerNode,LHCbInstallDIRAC,LHCbConfigureBasics,LHCbConfigureSite,LHCbConfigureArchitecture,LHCbConfigureCPURequirements,LaunchAgent $DEBUG
	else
		echo "==> Running python dirac-pilot.py -S $DIRACSETUP -l LHCb -C $CSURL -N $JENKINS_CE -Q $JENKINS_QUEUE -n $JENKINS_SITE --cert --certLocation=/home/dirac/certs/ -M 2 -E LHCbPilot -X LHCbGetPilotVersion,CheckWorkerNode,LHCbInstallDIRAC,LHCbConfigureBasics,LHCbConfigureSite,LHCbConfigureArchitecture,LHCbConfigureCPURequirements,LaunchAgent $DEBUG"
		python dirac-pilot.py -S $DIRACSETUP -l LHCb -C $CSURL -N $JENKINS_CE -Q $JENKINS_QUEUE -n $JENKINS_SITE --cert --certLocation=/home/dirac/certs/ -M 2 -E LHCbPilot -X LHCbGetPilotVersion,CheckWorkerNode,LHCbInstallDIRAC,LHCbConfigureBasics,LHCbConfigureSite,LHCbConfigureArchitecture,LHCbConfigureCPURequirements,LaunchAgent $DEBUG
	fi

	#try running the job agent. The job should be matched and everything should be "ok"
	#dirac-agent WorkloadManagement/JobAgent -o MaxCycles=1 -s /Resources/Computing/CEDefaults -o WorkingDirectory=$PWD -o TotalCPUs=1 -o MaxCPUTime=47520 -o CPUTime=47520 -o MaxRunningJobs=1 -o MaxTotalJobs=10 -o /LocalSite/InstancePath=$PWD -o /AgentJobRequirements/ExtraOptions=$PILOTCFG $PILOTCFG $DEBUG
}

function installLHCbDIRAC(){

	if [ ! -z "$PRERELEASECLIENT" ]
	then
		echo '==> Installing PRERELEASE client'
		findRelease
		installLHCbDIRACClient
	else
		#Setup Release (saving what's there before - most probably PRERELEASE=True)
		export PRERELEASEVALUE=$PRERELEASE
		export PRERELEASE=''
		findRelease
		setupLHCbDIRAC
		#reset
		export PRERELEASE=$PRERELEASEVALUE
	fi

}

function installLHCbDIRACClient(){

	echo '==> Installing LHCbDIRAC client'

	cp $TESTCODE/DIRAC/Core/scripts/dirac-install.py $CLIENTINSTALLDIR/dirac-install
	chmod +x $CLIENTINSTALLDIR/dirac-install
	$CLIENTINSTALLDIR/dirac-install -l LHCb -r `cat project.version` -e LHCb -t client $DEBUG

	mkdir $CLIENTINSTALLDIR/etc
	ln -s /cvmfs/lhcb.cern.ch/lib/lhcb/DIRAC/etc/dirac.cfg $CLIENTINSTALLDIR/etc/dirac.cfg

	source bashrc

}

function setupLHCbDIRAC(){

	echo '==> Invoking SetupProject LHCbDIRAC'

	. /cvmfs/lhcb.cern.ch/lib/lhcb/LBSCRIPTS/LBSCRIPTS_v8r3p1/InstallArea/scripts/LbLogin.sh
	. /cvmfs/lhcb.cern.ch/lib/lhcb/LBSCRIPTS/LBSCRIPTS_v8r3p1/InstallArea/scripts/SetupProject.sh LHCbDIRAC `cat project.version`
	local status=$?
	if [ $status -ne 0 ]
	then
		echo "==> Going to install client with dirac-install"
		installLHCbDIRACClient
	else
		export PYTHONPATH=$PYTHONPATH:$CLIENTINSTALLDIR/
	fi
}


function submitJob(){

	#Here, since I have CVMFS only, I can't use the "latest" pre-release, because won't be on CVMFS

	#Get a proxy and submit the job: this job will go to the certification setup, so we suppose the JobManager there is accepting jobs
	getUserProxy #this won't really download the proxy, so that's why the next command is needed
	python $TESTCODE/DIRAC/test/Jenkins/dirac-proxy-download.py $DIRACUSERDN -R $DIRACUSERROLE -o /DIRAC/Security/UseServerCertificate=True -o /DIRAC/Security/CertFile=/home/dirac/certs/hostcert.pem -o /DIRAC/Security/KeyFile=/home/dirac/certs/hostkey.pem -o /DIRAC/Setup=LHCb-Certification $PILOTCFG -ddd
	python $TESTCODE/LHCbDIRAC/tests/Jenkins/dirac-test-job.py -o /DIRAC/Setup=LHCb-Certification $DEBUG

	rm $PILOTCFG
}



#-------------------------------------------------------------------------------
#EOF
