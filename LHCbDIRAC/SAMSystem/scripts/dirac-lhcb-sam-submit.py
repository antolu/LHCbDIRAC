#!/usr/bin/env python
"""
  dirac-lhcb-sam-submit

    This script merges all sam scripts used to submit jobs into one.

    Usage:
      dirac-lhcb-sam-submit
        --ce                  Computing Element to submit to or `all`
        --number              number of SAM Jobs to be submitted [Experts only]

    Verbosity:
        -o LogLevel=LEVEL     NOTICE by default, levels available: INFO, DEBUG, VERBOSE...

"""

__RCSID__ = '$Id$'

# Module variables used along the functions
diracSAM = None
subLogger = None
switchDict = {}

def registerSwitches():
  '''
    Registers all switches that can be used while calling the script from the
    command line interface.
  '''

  switches = ( 
    ( 'ce=', 'Computing Element to submit to (must be in DIRAC CS) or all' ),
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
  switches.setdefault( 'ce', 'all' )
  switches.setdefault( 'number', 1 )

  subLogger.debug( "The switches used are:" )
  map( subLogger.debug, switches.iteritems() )

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

  # FIXME: this needs to be updated to use the new API


  subLogger.info( 'Submitting %d jobs to each ce' % number )

  for ce in ces:

    subLogger.verbose( 'Submitting job(s) to %s CE' % ce )

    # Submit a number of times the SAM Job to the same CE
    for _j in xrange( 0, number ):

      result = diracSAM.submitNewSAMJob( ce )
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
  from DIRAC.Core.Base import Script
  from DIRAC           import gLogger, exit as DIRACExit
  subLogger = gLogger.getSubLogger( __file__ )

  registerSwitches()
  switchDict = parseSwitches()

  from LHCbDIRAC.SAMSystem.Client.DiracSAM import DiracSAM

  diracSAM = DiracSAM()

  # Run the script
  run()

  # Bye
  DIRACExit( 0 )

################################################################################
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
