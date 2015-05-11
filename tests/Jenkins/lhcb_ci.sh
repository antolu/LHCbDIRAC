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

# first first: sourcing dirac_ci file
source $WORKSPACE/TestDIRAC/Jenkins/dirac_ci.sh

# URLs where to get scripts
LHCbDIRAC_PILOT_COMMANDS='http://svn.cern.ch/guest/dirac/LHCbDIRAC/trunk/LHCbDIRAC/WorkloadManagementSystem/PilotAgent/LHCbPilotCommands.py'

#install file
INSTALL_CFG_FILE='$WORKSPACE/LHCbTestDirac/Jenkins/install.cfg'


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

	cd $WORKSPACE

    PRE='p[[:digit:]]*'

  
    # Create temporary directory where to store releases.cfg ( will be deleted at
    # the end of the function )
    #tmp_dir=`mktemp -d -q`
    #echo 'Moving to'
    #echo $tmp_dir
    #cd $tmp_dir
    wget http://svn.cern.ch/guest/dirac/LHCbDIRAC/trunk/LHCbDIRAC/releases.cfg

    # Match project ( LHCbDIRAC, soon BeautyDirac ) version from releases.cfg
    # Example releases.cfg
    # v7r15-pre2
    # {
    #   Modules = LHCbDIRAC:v7r15-pre2, LHCbWebDIRAC:v3r3p5
    #   Depends = DIRAC:v6r10-pre12
    #   LcgVer = 2013-09-24
    # }
    
	if [ ! -z "$LHCBDIRAC_RELEASE" ]
	then
		echo 'Specified release'
		echo $LHCBDIRAC_RELEASE
		projectVersion=$LHCBDIRAC_RELEASE
	else
		if [ ! -z "$PRERELEASE" ]
		then
			echo 'Running on PRERELEASE mode'
			PRE='-pre'
		else
			echo 'Running on REGULAR mode'
		fi
		# projectVersion := v7r15-pre2 ( if we are in PRERELEASE mode )
		projectVersion=`cat releases.cfg | grep [^:]v[[:digit:]]r[[:digit:]]*$PRE | head -1 | sed 's/ //g'`
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
  
    # Back to $WORKSPACE and clean tmp_dir
    cd $WORKSPACE
    #rm -r $tmp_dir
    
    # PrintOuts
    echo DIRAC:$diracVersion && echo $diracVersion > dirac.version
    echo LHCbDIRAC:$lhcbdiracVersion && echo $lhcbdiracVersion > lhcbdirac.version
    echo LCG:$lcgVersion && echo $lcgVersion > lcg.version

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
	./dirac-install -l LHCb -r `cat project.version` -e LHCb -t server $DEBUG
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
	
	prepareForPilot
	
	wget --no-check-certificate -O LHCbPilotCommands.py $LHCbDIRAC_PILOT_COMMANDS

	if [ ! -z "$LHCBDIRAC_RELEASE" ]
	then
		echo 'Specified release'
		echo $LHCBDIRAC_RELEASE
		installVersion='-r'
		installVersion+=" $LHCBDIRAC_RELEASE"
	else
		installVersion=''
	fi

	#run the dirac-pilot script, only for installing, do not run the JobAgent here
	python dirac-pilot.py -S $DIRACSETUP -l LHCb $installVersion -C $CSURL -N jenkins.cern.ch -Q jenkins-queue_not_important -n DIRAC.Jenkins.ch --cert --certLocation=/home/dirac/certs/ -E LHCbPilot -X LHCbGetPilotVersion,CheckWorkerNode,LHCbInstallDIRAC,LHCbConfigureBasics,LHCbConfigureSite,LHCbConfigureArchitecture,LHCbConfigureCPURequirements $DEBUG
}


function fullLHCbPilot(){

	if [ ! -z "$DEBUG" ]
	then
		echo 'Running in DEBUG mode'
		export DEBUG='-ddd'
	fi  

	#first simply install via the pilot
	LHCbDIRACPilotInstall

	#this should have been created, we source it so that we can continue
	source bashrc
	
	#Adding the LocalSE and the CPUTimeLeft, for the subsequent tests
	dirac-configure -FDMH --UseServerCertificate -L CERN-SWTEST -O $PILOTCFG $PILOTCFG $DEBUG
	
	#be sure we only have pilot.cfg
	mv $WORKSPACE/etc/dirac.cfg $WORKSPACE/etc/dirac.cfg-not-here
	
	getUserProxy

	#Set not to use the server certificate for running the jobs 
	dirac-configure -FDMH -o /DIRAC/Security/UseServerCertificate=False -O $PILOTCFG $PILOTCFG $DEBUG
}

function getUserProxy(){

	touch $PILOTCFG
	#Configure for CPUTimeLeft
	python $WORKSPACE/TestDIRAC/Jenkins/dirac-cfg-update.py -F $PILOTCFG -S $DIRACSETUP -o /DIRAC/Security/UseServerCertificate=True -o /DIRAC/Security/CertFile=/home/dirac/certs/hostcert.pem -o /DIRAC/Security/KeyFile=/home/dirac/certs/hostkey.pem $DEBUG
	#Getting a user proxy, so that we can run jobs
	downloadProxy

}

function submitAndMatch(){
	
	if [ ! -z "$DEBUG" ]
	then
		echo 'Running in DEBUG mode'
		export DEBUG='-ddd'
	fi  

	# I execute in a subshell 
	(
		submitJob
	)

	#Run the full pilot, including the JobAgent
	prepareForPilot
	wget --no-check-certificate -O LHCbPilotCommands.py $LHCbDIRAC_PILOT_COMMANDS
	python dirac-pilot.py -S $DIRACSETUP -l LHCb $installVersion -C $CSURL -N jenkins.cern.ch -Q jenkins-queue_not_important -n DIRAC.Jenkins.ch --cert --certLocation=/home/dirac/certs/ -M 1 -E LHCbPilot -X LHCbGetPilotVersion,CheckWorkerNode,LHCbInstallDIRAC,LHCbConfigureBasics,LHCbConfigureSite,LHCbConfigureArchitecture,LHCbConfigureCPURequirements,LaunchAgent $DEBUG

	#try running the job agent. The job should be matched and everything should be "ok"
	#dirac-agent WorkloadManagement/JobAgent -o MaxCycles=1 -s /Resources/Computing/CEDefaults -o WorkingDirectory=$PWD -o TotalCPUs=1 -o MaxCPUTime=47520 -o CPUTime=47520 -o MaxRunningJobs=1 -o MaxTotalJobs=10 -o /LocalSite/InstancePath=$PWD -o /AgentJobRequirements/ExtraOptions=$PILOTCFG $PILOTCFG $DEBUG
}

function submitJob(){

	#Here, since I have CVMFS only, I can't use the "latest" pre-release, because won't be on CVMFS
	if [ ! -z "$DEBUG" ]
	then
		echo 'Running in DEBUG mode'
		export DEBUG='-ddd'
	fi  
	
	#Setup Release (saving what's there before - most probably PRERELEASE=True)
	export PRERELEASEVALUE=$PRERELEASE
	export PRERELEASE=''
	findRelease
	
	. /cvmfs/lhcb.cern.ch/lib/lhcb/LBSCRIPTS/LBSCRIPTS_v8r3p1/InstallArea/scripts/SetupProject.sh
	. /cvmfs/lhcb.cern.ch/lib/lhcb/LBSCRIPTS/LBSCRIPTS_v8r3p1/InstallArea/scripts/SetupProject.sh LHCbDIRAC `cat project.version`
	export PYTHONPATH=$PYTHONPATH:$WORKSPACE
	
	#Get a proxy and submit the job: this job will go to the certification setup, so we suppose the JobManager there is accepting jobs
	getUserProxy #this won't really download the proxy, so that's why the next command is needed
	python $WORKSPACE/TestDIRAC/Jenkins/dirac-proxy-download.py $DIRACUSERDN -R $DIRACUSERROLE -o /DIRAC/Security/UseServerCertificate=True -o /DIRAC/Security/CertFile=/home/dirac/certs/hostcert.pem -o /DIRAC/Security/KeyFile=/home/dirac/certs/hostkey.pem -o /DIRAC/Setup=LHCb-Certification $PILOTCFG -ddd
	python $WORKSPACE/LHCbTestDirac/Jenkins/dirac-test-job.py -o /DIRAC/Setup=LHCb-Certification $DEBUG
	
	#reset
	export PRERELEASE=$PRERELEASEVALUE
	
	rm $PILOTCFG
}


#-------------------------------------------------------------------------------
#EOF
