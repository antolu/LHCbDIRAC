#!/usr/bin/env python
"""
  dirac-lhcb-sam-submit

    This script merges all sam scripts used to submit jobs into one.

    Usage:
      dirac-procuction-job
        --ce                  Computing Element to submit to or `all`
        --removeLock          Force lock removal at site ( False by default )
        --deleteSharedArea    Force deletion of the shared area at site ( False by default )
        --enable              Global enable flag, set to False for debugging ( True by default )
        --softwareEnable      False for safe mode, disables SW module and SW removal ( True by default )
        --reportEnable        Set to False to disable reportSoftware module ( True by default )
        --logUpload           Log file upload flag ( True by default )
        --mode                Job submission mode (set to local for debugging)
        --install_project     Optional install_project URL [Experts only]
        --script              Optional path to python script to execute in SAM jobs [Experts only]
        --number              number of SAM Jobs to be submitted [Experts only]        

    Verbosity:
        -o LogLevel=LEVEL     NOTICE by default, levels available: INFO, DEBUG, VERBOSE...

"""
from DIRAC           import gLogger, exit as DIRACExit
from DIRAC.Core.Base import Script 

from LHCbDIRAC.SAMSystem.Client.DiracSAM import DiracSAM

__RCSID__ = '$Id$'

# Module variables used along the functions
diracSAM   = None
subLogger  = None
switchDict = {}

def registerSwitches():
  '''
    Registers all switches that can be used while calling the script from the
    command line interface.
  '''
  
  switches = (
    ( 'ce=', 'Computing Element to submit to (must be in DIRAC CS) or all' ),
    ( 'removeLock=', 'Force lock removal at site <bool> ( False by default )' ),
    ( 'deleteSharedArea=', 'Force deletion of the shared area at site <bool> ( False by default )' ),
    ( 'enable=', 'Global enable flag, set to False for debugging <bool> ( True by default )' ),
    ( 'softwareEnable=', 'False for safe mode, disables SW module and SW removal <bool> ( True by default )' ),
    ( 'reportEnable=', 'Set to False to disable reportSoftware module <bool> ( True by default )' ),
    ( 'logUpload=', 'Log file upload flag <bool> ( True by default )' ),
    ( 'mode=', 'Job submission mode (set to local for debugging)' ),
    ( 'install_project=', 'Optional install_project URL [Experts only]' ),
    ( 'script=', 'Optional path to python script to execute in SAM jobs [Experts only]' ),
    ( 'number=', 'number of SAM Jobs to be submitted [Experts only]' )
              )
  for switch in switches:
    Script.registerSwitch( '', switch[ 0 ], switch[ 1 ] )
  Script.setUsageMessage( __doc__ )

def parseSwitches():
  '''
    Parses the arguments passed by the user
  '''
  
  Script.parseCommandLine( ignoreErrors = True )
  args = Script.getPositionalArgs()
  if args:
    subLogger.error( "Found the following positional args '%s', but we only accept switches" % args )
    subLogger.error( "Please, check documentation below" )
    Script.showHelp()
    DIRACExit( 1 )
  
  switches = dict( Script.getUnprocessedSwitches() )  
  
  # Default values
  switches.setdefault( 'ce',    'all' )
  switches.setdefault( 'number', 1 )  
  
  switches = getBooleans( switches )

  subLogger.debug( "The switches used are:" )
  map( subLogger.debug, switches.iteritems() )
      
  return switches

def getBooleans( switches ):
  '''
    Method that sanitizes the boolean flags ( got them as strings on the command
    line ). 
  '''
  
  keys = ( 'removeLock', 'deleteSharedArea', 'enable', 'softwareEnable', 'reportEnable', 'logUpload' )
  for key in keys:
    if not key in switches:
      continue
    
    if not switches[ key ] in ( 'True', 'False' ):
      subLogger.debug( 'Error, got %s instead of True/False' % switches[ key ] ) 
      DIRACExit( 2 )
    
    switches[ key ] = ( switches[ key ] == 'True' )          

  return switches

################################################################################

def getCEs():
  '''
    Gets all CEs of the non-banned sites if the user sets the ce as "all".
  '''

  ces = switchDict[ 'ce' ]
  
  if ces == 'all':

    userReply = diracSAM._promptUser( 'Are you sure you want to submit SAM jobs for all CEs known to DIRAC?' )
    if not userReply[ 'OK' ]:
      subLogger.info( 'Action cancelled.' )
      DIRACExit( 2 )

    ces = diracSAM.getSuitableCEs()
    if not ces[ 'OK' ]:
      subLogger.error( ces[ 'Message' ] )
      DIRACExit( 2 )
    ces = ces[ 'Value' ]
  else:
    ces = [ ces ]  

  return ces
  
def getNumber():
  '''
    Checks whether the user knows he is sending a number of jobs different that
    1 to the system.
  '''
  
  number = switchDict[ 'number' ]
  try:
    number = int( number )
  except ValueError, x:
    subLogger.error( x )
    DIRACExit( 2 )
  
  if number != 1:
    userReply = diracSAM._promptUser( 'Are you sure you want to submit %d jobs to DIRAC?' % number )
    if not userReply[ 'OK' ]:
      subLogger.info( 'Buddy, You are wise !' )
      DIRACExit( 2 )    
  
  return number  
      
def submit( ces, number ):
  '''
    Submit <number> of jobs to each ce in <ces>. Also passing switches as 
    keyword arguments to diracSAM.
  '''
  
  #FIXME: this needs to be updated to use the new API     
      
      
  subLogger.info( 'Submitting %d jobs to each ce' % number )    
      
  for ce in ces:
    
    subLogger.verbose( 'Submitting job(s) to %s CE' % ce )
    
    # Copy switchDict and pass it to the submit function, with few changes to
    # fit the interface
    
    args = switchDict.copy()
    args[ 'ce' ] = ce
    del args[ 'number' ]    
    subLogger.debug( args )    
    
    # Submit a number of times the SAM Job to the same CE    
    for _j in xrange( 0, number ):
    
      result = diracSAM.submitSAMJob( **args )
      if not result[ 'OK' ]:
        subLogger.error( 'Submission of SAM job to CE %s failed' % ce )
        subLogger.verbose( 'with message:\n%s' % result[ 'Message' ] )
        break  
      
      subLogger.verbose( '  JobID: %s' % result[ 'Value' ] )

def run():
  '''
    Gets the eligible Computing Elements, the number of submissions per CE and
    submits the SAM jobs.
  '''

  jobsNumber = getNumber()
  subLogger.debug( jobsNumber )
  
  ces = getCEs()
  subLogger.debug( ces )
    
  submit( ces, jobsNumber )
  
  subLogger.info( 'done' ) 
    
################################################################################    
    
if __name__ == "__main__":

  # Script initialization
  registerSwitches()
  subLogger  = gLogger.getSubLogger( __file__ )
  switchDict = parseSwitches() 
  diracSAM   = DiracSAM()
  
  # Run the script
  run()
    
  # Bye
  DIRACExit( 0 )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF