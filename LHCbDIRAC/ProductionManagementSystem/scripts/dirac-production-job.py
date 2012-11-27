# $HeadURL$
"""
  dirac-production-job
  
    This script merges the former scripts `dirac-production-job-lfn-check`,
    `dirac-production-job-lfn` and `dirac-production-job-select-check`.
    
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
from DIRAC                      import gLogger, exit as DiracExit, S_OK
from DIRAC.Core.Base            import Script
from DIRAC.Interfaces.API.Dirac import Dirac

from LHCbDIRAC.Core.Utilities.JobInfoFromXML import JobInfoFromXML

__RCSID__ = '$Id$'

subLogger  = None
switchDict = {}

def registerSwitches():
  '''
    Registers all switches that can be used while calling the script from the
    command line interface.
  '''
 
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
  '''
    Parses the arguments passed by the user
  '''
  
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

################################################################################
# Cosmetic stuff, to be replaced by pprint

def printDict( dictionary ):
  """ Dictionary pretty printing
  """
  key_max = 0
  value_max = 0
  for key, value in dictionary.items():
    if len( key ) > key_max:
      key_max = len( key )
    if len( str( value ) ) > value_max:
      value_max = len( str( value ) )
  for key, value in dictionary.items():
    subLogger.verbose( key.rjust( key_max ), ' : ', str( value ).ljust( value_max ) )

################################################################################

def getJobs():
  '''
    Given a dictionary with the switches, either gets the jobs directly from the
    switch jobid, or gets them with the help of the other switches. 
  '''
  
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
    
    subLogger.debug( '\n'.join( [ str(c) for c in condDict.items()] ), switchDict )
    
    dirac = Dirac()
    jobs = dirac.selectJobs( **condDict )
    if not jobs[ 'OK' ]:
      subLogger.error( jobs[ 'Message' ] )
      DiracExit( 1 )
      
    jobs = jobs[ 'Value' ]
    
  subLogger.debug( "Processing %d jobs" %len( jobs ), switchDict )  
  
  return jobs
  
def processJobs( jobs ):
  '''
    Iterates over the jobs, getting the output LFNS and the input LFNS.
    
    For the output, it checks the ReplicaManager, to see that all LFNS are
    Suscesfully replicated. With Bookkeeping checks that files are properly
    registered.
    
    For the input, it checks ProductionDB.
    
    Depending on the logger, more or less information is printed. 
  '''
  
  rManager = ReplicaManager()
  bClient  = BookkeepingClient()
  pClient  = RPCClient( 'Transformation/TransformationManager' )
  
  for job in jobs:
    
    jobinfo = JobInfoFromXML( job )
    result  = jobinfo.valid()
    if not result[ 'OK' ]:
      subLogger.error( '%s %s' % ( job, result[ 'Message' ] ) )
      continue
    subLogger.debug( job )   
    
    outputFlags = processOutput( jobinfo, rManager, bClient, switchDict )
    if not outputFlags[ 'OK' ]:
      subLogger.error( outputFlags[ 'Message' ] )
      replicaFlag, bkkFlag = False, False
    else:
      replicaFlag, bkkFlag = outputFlags[ 'Value' ]    

def processOutput( job, replicaManger, bookkeepingClient ):
  '''
    Given a Job and two clients, gets the flags for the replica and the bookkeeping.
  '''
    
  lfns = job.getOutputLFN()
  if not lfns[ 'OK' ]:
    return lfns
  lfns = lfns[ 'Value' ]

  replicaFlag = getReplicas( lfns, replicaManager )
  if not replicaFlag[ 'OK' ]:
    subLogger.error( replicaFlag[ 'Message' ] )
    replicaFlag = False
  else:  
    replicaFlag = replicaFlag[ 'Value' ]  
  
  bookkeepingFlag = getBookkeeping( lfns, bookkeepingClient )
  if not bookkeepingFlag[ 'OK' ]:
    subLogger.error( bookkeepingFlag[ 'Message' ] )
    bookkeepingFlag = False
  else:
    bookkeepingFlag = bookkeepingFlag[ 'Value' ]
    
  return S_OK( replicaFlag, bookkeepingFlag )

def getReplicas( lfns, replicaManager ):
  '''
    Gets replicas of the given lfns.
  '''

  replicaFlag = False

  replicas = replicaManger.getReplicas( lfns )
  if not replicas[ 'OK' ]:
    return replicas
  replicas = replicas[ 'Value' ]
   
  successful = value.get( 'Successful', [] )
  failed     = value.get( 'Failed', [] )

  if len( successful ) == len( lfns ):
    replicaFlag = True
     
  subLogger.verbose( "LFC replicas:" )
  subLogger.verbose( "Successful:" )
  printDict( successful )  
  subLogger.verbose( "Failed:" )
  printDict( failed )
    
  return S_OK( replicaFlag )  

def getBookkeeping( lfns, bookkeepingClient ):
  '''
    Gets files registered for the given lfns
  '''  
    
  bookkeepingFlag = False
  
  lfnsBKK = bookkeepingClient.exists( lfns )
  if not lfnsBKK[ 'OK' ]:
    return lfnsBKK
  lfnsBKK = lfnsBKK[ 'Value' ]
  
  subLogger.verbose( "Bookkeping:" )
  printDict( lfnsBKK )
  
  bkkFound = len( [ value for value in lfnsBKK.itervalues() if value ] )
      
  if bkkFound == len( lfns ):
    bookkeepingFlag = True  
    
  return S_OK( bookkeepingFlag )

def processInput( job, productionClient ):

  lfns = job.getInputLFN()
  if not lfns[ 'OK' ]:
    return lfns
  lfns = lfns[ 'Value' ]  
  
  prodID = int( job.prodid )
  subLogger.verbose( "ProductionDB for production %d" % prodID )
  
  fileSummary = productionClient.getFileSummary( lfns, prodID )
  if not fileSummary[ 'OK' ]:
    return fileSummary
  fileSummary = fileSummary[ 'Value' ]

# TODO: finish it !  
#  #okPROD = True
#  if len( lfns ):
#    
#      for lfn in lfns:
#        try:
#          status = fs['Value']['Successful'][lfn][prodid]
#          if not status['FileStatus'].count( 'Processed' ):
#            okPROD = False
#          if verbose:
#            print status
#      
#        except:
#          okPROD = False
    
def run():
  '''
    Main script method
  '''

  subLogger.verbose( "Running main method" )

  jobs = getJobs()
  processJobs( jobs )

################################################################################
   
if __name__ == "__main__":
  
  # Script initialization
  registerSwitches()
  subLogger  = gLogger.getSubLogger( __file__ )
  switchDict = parseSwitches()  
  
  # Script execution  
  run() 
    
  # Bye my friend  
  DiracExit( 0 )
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF