"""
  dirac-production-job

    This script merges the former scripts dirac-production-job-lfn-check,
    dirac-production-job-lfn and dirac-production-job-select-check.

    Usage:
      dirac-procuction-job
        --jobid               Job id(s) on csv format e.g. "1123,111233,1232"
        --status              Job major status e.g. "Active"
        --minorStatus         Job minor status e.g. "Input Data Resolution"
        --applicationStatus   Job application status e.g. "unknown"
        --site                Site where the job runs e.g. "LCG.CERN.ch"
        --owner               DIRAC username that owns the job
        --jobGroup            JobGroup the job belongs to e.g. "SAM"
        --date                Date on yyyy-mm-dd format e.g. "2012-12-21"

    Verbosity:
        -o LogLevel=LEVEL     NOTICE by default, levels available: INFO, DEBUG, VERBOSE...

"""

from DIRAC.Core.Base import Script
from DIRAC           import gLogger, exit as DiracExit


__RCSID__ = '$Id$'


# Place holders for lazy imports................................................
dirac          = None
jobInfoFromXML = None
transClient    = None

subLogger      = None
switchDict     = {}


def registerSwitches():
  """
    Registers all switches that can be used while calling the script from the
    command line interface.
  """

  switches = ( 
     ( 'jobid=', 'Job id(s) on csv format' ),
     ( 'status=', 'Job major status' ),
     ( 'minorStatus=', 'Job minor status' ),
     ( 'applicationStatus=', 'Job application status' ),
     ( 'site=', 'Site where the job runs' ),
     ( 'owner=', 'DIRAC username that owns the job' ),
     ( 'jobGroup=', 'JobGroup the job belongs to' ),
     ( 'date=', 'Date on yyyy-mm-dd format' )
              )
  for switch in switches:
    Script.registerSwitch( '', switch[0], switch[1] )

  Script.setUsageMessage( __doc__ )


def parseSwitches():
  """
    Parses the arguments passed by the user
  """

  Script.parseCommandLine( ignoreErrors = True )
  args = Script.getPositionalArgs()

  if args:
    subLogger.error( "Found the following positional args '%s', but we only accept switches" % args )
    subLogger.error( "Please, check documentation below" )
    Script.showHelp()
    DiracExit( 1 )

  switchDict = dict( Script.getUnprocessedSwitches() )

  subLogger.debug( "The switches used are:" )
  map( subLogger.debug, switchDict.iteritems() )

  return switchDict


#...............................................................................

def getJobs():
  """
    Given a dictionary with the switches, either gets the jobs directly from the
    switch jobid, or gets them with the help of the other switches.
  """

  if 'jobid' in switchDict:
    subLogger.verbose( "Found jobid key, ignoring the rest of the switches", switchDict )
    try:
      jobs = map( int, switchDict[ 'jobid' ].split( ',' ) )
    except ValueError, e:
      subLogger.error( e )
      Script.showHelp()
      DiracExit( 1 )
  else:
    subLogger.verbose( "No jobid provided, extracting jobs from switches", switchDict )

    condDict = { 'status'           : switchDict.get( 'status', None ),
                 'minorStatus'      : switchDict.get( 'minorStatus', None ),
                 'applicationStatus': switchDict.get( 'applicationStatus', None ),
                 'site'             : switchDict.get( 'site', None ),
                 'owner'            : switchDict.get( 'owner', None ),
                 'jobGroup'         : switchDict.get( 'jobGroup', None ),
                 'date'             : switchDict.get( 'date', None ),
                 }

    subLogger.debug( '\n'.join( [ str( c ) for c in condDict.items()] ), switchDict )

    jobs = dirac.selectJobs( **condDict )
    if not jobs[ 'OK' ]:
      subLogger.error( jobs[ 'Message' ] )
      DiracExit( 1 )

    jobs = jobs[ 'Value' ]

  subLogger.debug( "Processing %d jobs" % len( jobs ), switchDict )

  return jobs


def processJobs( jobs ):
  """
    Iterates over the jobs, getting the output LFNS and the input LFNS.

    For the output, it checks the DataManager, to see that all LFNS are
    Suscesfully replicated. With Bookkeeping checks that files are properly
    registered.

    For the input, it checks ProductionDB.

    Depending on the logger, more or less information is printed.
  """

  for job in jobs:

    jobinfo = jobInfoFromXML( job )
    result  = jobinfo.valid()
    if not result[ 'OK' ]:
      subLogger.error( '%s %s' % ( job, result[ 'Message' ] ) )
      continue

    subLogger.info( 'JobID : %s' % job )
    

    processInput( jobinfo )

    output = jobinfo.getOutputLFN()
    if not output[ 'OK' ]:
      subLogger.error( output[ 'Message' ] )
      continue
    subLogger.info( '\noutput' )
    map( subLogger.info, output[ 'Value' ] )
    

def processInput( jobinfo ):
  
  jInput = jobinfo.getInputLFN()
  if not jInput[ 'OK' ]:
    subLogger.error( jInput[ 'Message' ] )
    return
  
  subLogger.info( '\ninput' )
  if jInput[ 'Value' ]:
    inputFiles = jInput[ 'Value' ][ 0 ].split( ';' )
  
    fileStatus = transClient.getFileSummary( inputFiles )
    if not fileStatus[ 'OK' ]:
      subLogger.error( fileStatus[ 'Message' ] )
    
    def processFileSummary( status ):
    
      subLogger.info( '%s files' % status )
      for fileName, fileDict in fileStatus[ 'Value' ][ status ].iteritems():
        summary = ''            
        for value in fileDict.values():
          summary += '%(Status)s ( %(TransformationID)s ),' % value
        subLogger.info( '%s ::: %s' % ( fileName, summary ) )   

    processFileSummary( 'Successful' )
    processFileSummary( 'Failed' )


def run():
  """
    Main script method
  """

  subLogger.verbose( "Running main method" )

  jobs = getJobs()
  processJobs( jobs )


#...............................................................................

def lazyImports():

  global dirac
  from DIRAC.Interfaces.API.Dirac import Dirac
  dirac = Dirac()

  global jobInfoFromXML
  from LHCbDIRAC.Core.Utilities.JobInfoFromXML import JobInfoFromXML
  jobInfoFromXML = JobInfoFromXML
  
  global transClient
  from DIRAC.Core.DISET.RPCClient import RPCClient
  transClient = RPCClient( 'Transformation/TransformationManager')


#...............................................................................

if __name__ == "__main__":

  # Script initialization
  registerSwitches()
  subLogger = gLogger.getSubLogger( __file__ )
  switchDict = parseSwitches()
  
  # lazy Imports
  lazyImports()

  # Script execution  
  run()

  # Bye my friend  
  DiracExit( 0 )

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
