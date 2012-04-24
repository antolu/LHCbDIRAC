#!/usr/bin/env python
__RCSID__ = "$Id$"

import string, os, shutil

import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch( "p:", "Project=", "Project Name e.g. Gauss, Boole, Brunel or DaVinci" )
Script.registerSwitch( "v:", "Version=", "Project Version e.g. vXrY" )
Script.registerSwitch( "f:", "OptionsFile=", "Optional: path to python options file (otherwise options are obtained from site shared area)" )
Script.registerSwitch( "e:", "Events=", "Optional: number of events to process (default is 2)" )
Script.registerSwitch( "y:", "SystemConfig=", "Optional: job system configuration (default is slc4_ia32_gcc34)" )
Script.registerSwitch( "t:", "CPUTime=", "Optional: CPU time requirement (default is 3600)" )
Script.registerSwitch( "l:", "JobLogLevel=", "Optional: job log level (default is info)" )
Script.registerSwitch( "m:", "Mode=", "Optional: submission mode is either: 'wms', 'local' or 'agent' (set to local by default)" )
Script.registerSwitch( "n:", "JobName=", "Optional: job name (default is 'TestJob')" )
Script.registerSwitch( "i:", "InputData=", "Optional: only for jobs requiring input data, colon seperated list e.g. <LFN>;<LFN>" )
Script.registerSwitch( "d:", "TestDir=", "Optional: path to a directory to execute the test (default is '<CWD>/<Project>_<Version>_<Mode>')" )
Script.registerSwitch( "g", "Generate", "Optional: specify this switch to only generate the API script and retrieve the project options file (disabled by default)" )
Script.parseCommandLine( ignoreErrors = True )

from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC import gLogger, S_OK

args = Script.getPositionalArgs()

from LHCbDIRAC.Core.Utilities.CombinedSoftwareInstallation  import SharedArea

#Constants (some can be overidden, some are messy)

events = 2
systemConfig = 'x86_64-slc5-gcc43-opt'
cpuTime = 3600
logLevel = 'info'
submissionMode = 'local'
jobName = 'TestJob'
lhcbConvention = {'Gauss':'Sim', 'Boole':'Digi', 'Brunel':'Rec', 'DaVinci':'Phys'}
sharedArea = SharedArea()

#Variables to be assigned or dynamically evaluated

generate = 0
testDir = ''
projectName = ''
projectVersion = ''
optionsFile = ''
inputDatasets = []
exitCode = 0

#Methods to help with the script execution

def usage():
  print 'Usage: %s [Try -h,--help for more information]' % ( Script.scriptName )
  DIRAC.exit( 2 )

def getOptionsFile( projectName, projectVersion, sharedArea, lhcbConvention, events ):
  """Simple function to retrieve project python options file from specified sharedArea
     if not specified by the user.
  """
  optionsFiles = ['lhcb-2008.py', '%s-2008.py' % projectName, '%s.py' % projectName]
  projectUpper = projectName.upper()
  optionsPath = '%s/lhcb/%s/%s_%s/%s/%s/options' % ( sharedArea, projectUpper, projectUpper, projectVersion, lhcbConvention[projectName], projectName )
  if not os.path.isdir( optionsPath ):
    gLogger.verbose( 'Assume %s %s uses old package structure' % ( projectName, projectVersion ) )
    optionsPath = '%s/lhcb/%s/%s_%s/%s/%s/%s/options' % ( sharedArea, projectUpper, projectUpper, projectVersion, lhcbConvention[projectName], projectName, projectVersion )
  gLogger.info( 'Searching for %s %s options in path:\n%s' % ( projectName, projectVersion, optionsPath ) )
  newPath = '%s/Opts%s%s.py' % ( os.getcwd(), projectName, projectVersion )
  for opts in optionsFiles:
    optionsFile = '%s/%s' % ( optionsPath, opts )
    gLogger.verbose( 'Searching for %s %s options file:\n%s' % ( projectName, projectVersion, optionsFile ) )
    if not os.path.exists( optionsFile ):
      continue
    gLogger.info( 'Found options file %s' % optionsFile )
    shutil.copy( optionsFile, newPath )
    fopen = open( newPath, 'a' )
    fopen.write( 'ApplicationMgr( EvtMax = %s )\n' % events )
    fopen.close()
    gLogger.info( '%s %s options written to %s' % ( projectName, projectVersion, newPath ) )
    return newPath
  else:
    return None

def runJob( projectName, projectVersion, optionsFile, systemConfig, submissionMode, cpuTime, logLevel, jobName, inputDatasets, generate ):
  """Local submission of a dynamically created job.
  """
  scriptName = '%s/DiracAPI_%s_%s_%s.py' % ( os.getcwd(), projectName, projectVersion, submissionMode.lower().capitalize() )
  fopen = open( scriptName, 'w' )
  fopen.write( '# Example DIRAC API script written for %s %s in %s mode using:\n# %s\n\n' % ( projectName, projectVersion, submissionMode, __RCSID__ ) )
  fopen.write( 'from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob\nfrom DIRAC.Interfaces.API.Dirac import Dirac\nj=LHCbJob()\n' )
  j = LHCbJob()
  fopen.write( 'j.setCPUTime("%s")\n' % cpuTime )
  j.setCPUTime( cpuTime )
  fopen.write( 'j.setSystemConfig("%s")\n' % systemConfig )
  j.setSystemConfig( systemConfig )
  if not inputDatasets:
    fopen.write( 'j.setApplication("%s","%s","%s")\n' % ( projectName, projectVersion, optionsFile ) )
    j.setApplication( projectName, projectVersion, optionsFile )
  else:
    fopen.write( 'j.setApplication("%s","%s","%s",inputData=["%s"])\n' % ( projectName, projectVersion, optionsFile, string.join( inputDatasets, '","' ) ) )
    j.setApplication( projectName, projectVersion, optionsFile, inputData = inputDatasets )
  fopen.write( 'j.setName("%s")\n' % jobName )
  j.setName( jobName )
  fopen.write( 'j.setLogLevel("%s")\n' % logLevel )
  j.setLogLevel( logLevel )
  fopen.write( 'dirac=Dirac()\nprint dirac.submit(j,mode="%s")\n' % submissionMode )
  fopen.close()
  gLogger.info( 'DIRAC API script written to %s for future use' % scriptName )
  if submissionMode.lower() == 'wms':
    submissionMode = None
  if not generate:
    dirac = Dirac()
    return dirac.submit( j, mode = submissionMode )
  else:
    gLogger.info( 'Generate flag is enabled, only creating API script and options file for %s %s' % ( projectName, projectVersion ) )
    return S_OK()

#Start the script and perform checks
if args or not Script.getUnprocessedSwitches():
  usage()

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() in ( 'p', 'project' ):
    projectName = switch[1]
  elif switch[0].lower() in ( 'v', 'version' ):
    projectVersion = switch[1]
  elif switch[0].lower() in ( 'f', 'optionsfile' ):
    optionsFile = switch[1]
  elif switch[0].lower() in ( 'y', 'systemconfig' ):
    systemConfig = switch[1]
  elif switch[0].lower() in ( 't', 'cputime' ):
    cpuTime = int( switch[1] )
  elif switch[0].lower() in ( 'l', 'jobloglevel' ):
    logLevel = switch[1]
  elif switch[0].lower() in ( 'm', 'mode' ):
    submissionMode = switch[1].lower()
  elif switch[0].lower() in ( 'n', 'jobname' ):
    jobName = switch[1]
  elif switch[0].lower() in ( 'e', 'events' ):
    events = int( switch[1] )
  elif switch[0].lower() in ( 'd', 'testdir' ):
    testDir = switch[1]
  elif switch[0].lower() in ( 'g', 'generate' ):
    generate = True
  elif switch[0].lower() in ( 'i', 'inputdata' ):
    id = str( switch[1] ).split( ';' )
    for i in id:
      if i: inputDatasets.append( i )

if not projectName in lhcbConvention.keys():
  exitCode = 2
  print 'ERROR: Project name must be one of %s not %s' % ( string.join( lhcbConvention.keys(), ',' ), projectName )

if not submissionMode.lower() in ['local', 'agent', 'wms']:
  exitCode = 2
  print 'ERROR: Submission mode must be "local", "agent" or "wms" not %s' % submissionMode
  DIRAC.exit( exitCode )

if not sharedArea or not os.path.exists( sharedArea ):
  print 'ERROR: Could not determine site shared area'
  exitCode = 2
  DIRAC.exit( exitCode )

start = os.getcwd()
if not testDir:
  testDir = '%s/%s_%s_%s' % ( os.getcwd(), projectName, projectVersion, submissionMode.lower().capitalize() )
  gLogger.verbose( 'Using default test directory: %s' % testDir )

if not os.path.exists( testDir ):
  gLogger.verbose( 'Creating specified test directory %s' % testDir )
  try:
    os.mkdir( testDir )
  except Exception, x:
    print 'ERROR: Could not create test directory %s' % testDir
    exitCode = 2
else:
  gLogger.info( 'Specified test directory %s already exists' % testDir )
  exitCode = 2
  print 'ERROR: Please remove test directory %s or specify another directory with the --TestDir=<PATH> option' % testDir
  DIRAC.exit( exitCode )

if not exitCode:
  os.chdir( testDir )
  if optionsFile:
    if not os.path.exists( optionsFile ):
      if os.path.exists( '%s/%s' % ( start, optionsFile ) ):
        shutil.copy( '%s/%s' % ( start, optionsFile ), '%s/%s' % ( os.getcwd(), optionsFile ) )
        optionsFile = '%s/%s' % ( os.getcwd(), optionsFile )
        gLogger.verbose( 'Options file found in original directory %s' % ( optionsFile ) )
      else:
        exitCode = 2
        print 'ERROR: Options file must exist locally, %s was not found' % optionsFile
  else:
    optionsFile = getOptionsFile( projectName, projectVersion, sharedArea, lhcbConvention, events )
    if not optionsFile:
      exitCode = 2
      print 'ERROR: Could not retrieve options file for %s %s from shared area %s' % ( projectName, projectVersion, sharedArea )

if exitCode:
  os.chdir( start )
  gLogger.info( 'Cleaning up test directory %s' % testDir )
  os.rmdir( testDir )
  DIRAC.exit( exitCode )


result = runJob( projectName, projectVersion, optionsFile, systemConfig, submissionMode, cpuTime, logLevel, jobName, inputDatasets, generate )
gLogger.verbose( result )
if not result['OK']:
  exitCode = 2
  print 'ERROR: %s %s execution completed with errors' % ( projectName, projectVersion )

if generate:
  gLogger.info( 'Script and options for %s %s are in %s' % ( projectName, projectVersion, testDir ) )
elif submissionMode.lower() == 'wms':
  gLogger.info( 'Submitted job with ID %s to WMS' % result['Value'] )
  gLogger.info( '%s %s script and options file are in %s' % ( projectName, projectVersion, testDir ) )
else:
  gLogger.info( 'Outputs from %s %s are in %s' % ( projectName, projectVersion, testDir ) )

os.chdir( start )
DIRAC.exit( exitCode )
