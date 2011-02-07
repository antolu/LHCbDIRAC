########################################################################
# $Id$
########################################################################
""" Utility to construct production LFNs from workflow parameters
    according to LHCb conventions.
    
    The methods here are mostly from ancient history and need to be 
    reviewed, these methods were grouped together as they form the
    "interface" for production clients and workflow modules to create LFNs.
    
"""

__RCSID__ = "$Id$"

import string, re, os, types, datetime, copy

from DIRAC import S_OK, S_ERROR, gLogger, gConfig

gLogger = gLogger.getSubLogger( 'ProductionData' )

#############################################################################
def constructProductionLFNs( paramDict ):
  """ Used for local testing of a workflow, a temporary measure until
      LFN construction is tidied.  This works using the workflow commons for
      on the fly construction.
  """
  keys = ['PRODUCTION_ID', 'JOB_ID', 'dataType', 'configVersion', 'JobType', 'outputList', 'configName', 'outputDataFileMask']
  for k in keys:
    if not paramDict.has_key( k ):
      return S_ERROR( '%s not defined' % k )

  productionID = paramDict['PRODUCTION_ID']
  jobID = paramDict['JOB_ID']
#  wfMode = paramDict['dataType']
  wfConfigName = paramDict['configName']
  wfConfigVersion = paramDict['configVersion']
  wfMask = paramDict['outputDataFileMask']
  if not type( wfMask ) == type( [] ):
    wfMask = [i.lower().strip() for i in wfMask.split( ';' )]
  wfType = paramDict['JobType']
  outputList = paramDict['outputList']
  inputData = ''
  if paramDict.has_key( 'InputData' ):
    inputData = paramDict['InputData']

  fileTupleList = []
  gLogger.verbose( 'wfConfigName = %s, wfConfigVersion = %s, wfMask = %s, wfType=%s' % ( wfConfigName, wfConfigVersion,
                                                                                         wfMask, wfType ) )
  for info in outputList:
    #Nasty check on whether the created code parameters were not updated e.g. when changing defaults in a workflow
    fileName = info['outputDataName'].split( '_' )
    index = 0
    if not re.search( '^\d', fileName[index] ):
      index += 1
    if not fileName[index] == str( productionID ).zfill( 8 ):
      fileName[index] = str( productionID ).zfill( 8 )
    if not fileName[index + 1] == str( jobID ).zfill( 8 ):
      fileName[index + 1] = str( jobID ).zfill( 8 )
    fileTupleList.append( ( string.join( fileName, '_' ), info['outputDataType'] ) )

  #Strip output data according to file mask
  fileTupleListMasked = _applyMask( wfMask, fileTupleList )

  lfnRoot = ''
  debugRoot = ''
  if inputData:
    gLogger.verbose( 'Making LFN_ROOT for job with inputdata: %s' % ( inputData ) )
    lfnRoot = _getLFNRoot( inputData, wfConfigName )
    debugRoot = _getLFNRoot( '', 'debug', wfConfigVersion )
  else:
    lfnRoot = _getLFNRoot( '', wfConfigName, wfConfigVersion )
    gLogger.verbose( 'LFN_ROOT is: %s' % ( lfnRoot ) )
    debugRoot = _getLFNRoot( '', 'debug', wfConfigVersion )

  gLogger.verbose( 'LFN_ROOT is: %s' % ( lfnRoot ) )
  if not lfnRoot:
    return S_ERROR( 'LFN root could not be constructed' )

  #Get all LFN(s) to both output data and BK lists at this point (fine for BK)
  outputData = []
  bkLFNs = []
  debugLFNs = []

  #outputData is masked
  for fileTuple in fileTupleListMasked:
    lfn = _makeProductionLFN( str( jobID ).zfill( 8 ), lfnRoot, fileTuple, wfConfigName, str( productionID ).zfill( 8 ) )
    outputData.append( lfn )

  #BKLFNs and debugLFNs are not masked
  for fileTuple in fileTupleList:
    lfn = _makeProductionLFN( str( jobID ).zfill( 8 ), lfnRoot, fileTuple, wfConfigName, str( productionID ).zfill( 8 ) )
    bkLFNs.append( lfn )
    if debugRoot:
      debugLFNs.append( _makeProductionLFN( str( jobID ).zfill( 8 ), debugRoot, fileTuple, wfConfigName, str( productionID ).zfill( 8 ) ) )

  if debugRoot:
    debugLFNs.append( _makeProductionLFN( str( jobID ).zfill( 8 ), debugRoot, ( '%s_core' % str( jobID ).zfill( 8 ) , 'core' ), wfConfigName, str( productionID ).zfill( 8 ) ) )

  #Get log file path - unique for all modules
  logPath = _makeProductionPath( str( jobID ).zfill( 8 ), lfnRoot, 'LOG', wfConfigName, str( productionID ).zfill( 8 ), log = True )
  logFilePath = ['%s/%s' % ( logPath, str( jobID ).zfill( 8 ) )]
  logTargetPath = ['%s/%s_%s.tar' % ( logPath, str( productionID ).zfill( 8 ), str( jobID ).zfill( 8 ) )]
  #[ aside, why does makeProductionPath not append the jobID itself ????
  #  this is really only used in one place since the logTargetPath is just written to a text file (should be reviewed)... ]

  if not outputData:
    gLogger.info( 'No output data LFN(s) constructed' )
  else:
    gLogger.verbose( 'Created the following output data LFN(s):\n%s' % ( string.join( outputData, '\n' ) ) )
  gLogger.verbose( 'Log file path is:\n%s' % logFilePath[0] )
  gLogger.verbose( 'Log target path is:\n%s' % logTargetPath[0] )
  if bkLFNs:
    gLogger.verbose( 'BookkeepingLFN(s) are:\n%s' % ( string.join( bkLFNs, '\n' ) ) )
  if debugLFNs:
    gLogger.verbose( 'DebugLFN(s) are:\n%s' % ( string.join( debugLFNs, '\n' ) ) )
  jobOutputs = {'ProductionOutputData':outputData, 'LogFilePath':logFilePath, 'LogTargetPath':logTargetPath, 'BookkeepingLFNs':bkLFNs, 'DebugLFNs':debugLFNs}
  return S_OK( jobOutputs )

#############################################################################

def _applyMask( mask, dataTuplesList ):

  maskedData = copy.deepcopy( dataTuplesList )

  if type( mask ) != type( [] ):
    mask = [mask]

  if mask != ['']:
    maskLower = [x.lower() for x in mask]

    for dt in dataTuplesList:
      if dt[1].lower() not in maskLower:
        maskedData.remove( dt )

  return maskedData

#############################################################################


def getLogPath( paramDict ):
  """ Can construct log file paths even if job fails e.g. no output files available.
  """
  keys = ['PRODUCTION_ID', 'JOB_ID', 'dataType', 'configVersion', 'JobType']
  for k in keys:
    if not paramDict.has_key( k ):
      return S_ERROR( '%s not defined' % k )

  productionID = paramDict['PRODUCTION_ID']
  jobID = paramDict['JOB_ID']
  wfConfigName = paramDict['dataType']
  wfConfigVersion = paramDict['configVersion']
  wfType = paramDict['JobType']
  inputData = ''
  if paramDict.has_key( 'InputData' ):
    inputData = paramDict['InputData']

  gLogger.verbose( 'wfConfigName = %s, wfConfigVersion = %s, wfType=%s' % ( wfConfigName, wfConfigVersion, wfType ) )
  lfnRoot = ''
  if inputData:
    lfnRoot = _getLFNRoot( inputData, wfType )
  else:
    lfnRoot = _getLFNRoot( '', wfConfigName, wfConfigVersion )

  #Get log file path - unique for all modules
  logPath = _makeProductionPath( str( jobID ).zfill( 8 ), lfnRoot, 'LOG', wfConfigName, str( productionID ).zfill( 8 ), log = True )
  logFilePath = ['%s/%s' % ( logPath, str( jobID ).zfill( 8 ) )]
  logTargetPath = ['%s/%s_%s.tar' % ( logPath, str( productionID ).zfill( 8 ), str( jobID ).zfill( 8 ) )]

  gLogger.verbose( 'Log file path is:\n%s' % logFilePath )
  gLogger.verbose( 'Log target path is:\n%s' % logTargetPath )
  jobOutputs = {'LogFilePath':logFilePath, 'LogTargetPath':logTargetPath}
  return S_OK( jobOutputs )

#############################################################################
def constructUserLFNs( jobID, owner, outputFiles, outputPath ):
  """ This method is used to supplant the standard job wrapper output data policy
      for LHCb.  The initial convention adopted for user output files is the following:
      
      /lhcb/user/<initial e.g. p>/<owner e.g. paterson>/<outputPath>/<yearMonth e.g. 2010_02>/<subdir>/<fileName>
  """
  initial = owner[:1]
  subdir = str( jobID / 1000 )
  timeTup = datetime.date.today().timetuple()
  yearMonth = '%s_%s' % ( timeTup[0], string.zfill( str( timeTup[1] ), 2 ) )
  outputLFNs = {}

  #Strip out any leading or trailing slashes but allow fine structure
  if outputPath:
    outputPathList = string.split( outputPath, os.sep )
    newPath = []
    for i in outputPathList:
      if i:
        newPath.append( i )
    outputPath = string.join( newPath, os.sep )

  if not type( outputFiles ) == types.ListType:
    outputFiles = [outputFiles]

  for outputFile in outputFiles:
    #strip out any fine structure in the output file specified by the user, restrict to output file names
    #the output path field can be used to describe this    
    outputFile = outputFile.replace( 'LFN:', '' )
    lfn = os.sep + os.path.join( 'lhcb', 'user', initial, owner, outputPath, yearMonth, subdir, str( jobID ) ) + os.sep + os.path.basename( outputFile )
    outputLFNs[outputFile] = lfn

  outputData = outputLFNs.values()
  if outputData:
    gLogger.info( 'Created the following output data LFN(s):\n%s' % ( string.join( outputData, '\n' ) ) )
  else:
    gLogger.info( 'No output LFN(s) constructed' )

  return S_OK( outputData )

#############################################################################
def preSubmissionLFNs( jobCommons, jobCode, productionID = '1', jobID = '2', inputData = None ):
  """ Constructs LFNs to be added to the job description prior to submission
      or simply for visual inspection. 
      
      This is a wrapper around constructProductionLFNs used by the production
      clients.
  """
  outputList = []
  for line in jobCode.split( "\n" ):
    if line.count( "listoutput" ):
      outputList += eval( line.split( "#" )[0].split( "=" )[-1] )

  jobCommons['outputList'] = outputList
  jobCommons['PRODUCTION_ID'] = productionID
  jobCommons['JOB_ID'] = jobID
  if inputData:
    jobCommons['InputData'] = inputData

  gLogger.debug( jobCommons )
  result = constructProductionLFNs( jobCommons )
  if not result['OK']:
    gLogger.error( result )
  return result

#############################################################################
def _makeProductionPath( JOB_ID, LFN_ROOT, typeName, mode, prodstring, log = False ):
  """ Constructs the path in the logical name space where the output
      data for the given production will go. In
  """
  result = LFN_ROOT + '/' + typeName.upper() + '/' + prodstring + '/'
  if log:
    try:
      jobid = int( JOB_ID )
      jobindex = string.zfill( jobid / 10000, 4 )
    except:
      jobindex = '0000'
    result += jobindex

  return result

#############################################################################
def _makeProductionLFN( JOB_ID, LFN_ROOT, filetuple, mode, prodstring ):
  """ Constructs the logical file name according to LHCb conventions.
      Returns the lfn without 'lfn:' prepended.
  """
  gLogger.debug( 'Making production LFN for JOB_ID %s, LFN_ROOT %s, mode %s, prodstring %s for\n%s' % ( JOB_ID, LFN_ROOT, mode, prodstring, str( filetuple ) ) )
  try:
    jobid = int( JOB_ID )
    jobindex = string.zfill( jobid / 10000, 4 )
  except:
    jobindex = '0000'

  fname = filetuple[0].lower()
  if re.search( 'lfn:', fname ) or re.search( 'LFN:', fname ):
    return fname.replace( 'lfn:', '' ).replace( 'LFN:', '' )

  return LFN_ROOT + '/' + filetuple[1].upper() + '/' + prodstring + '/' + jobindex + '/' + fname

#############################################################################
def _getLFNRoot( lfn, namespace = '', configVersion = 0 ):
  """
  return the root path of a given lfn

  eg : /lhcb/data/CCRC08/00009909 = getLFNRoot(/lhcb/data/CCRC08/00009909/DST/0000/00009909_00003456_2.dst)
  eg : /lhcb/MC/<year>/  = getLFNRoot(None)
  """
  dataTypes = gConfig.getValue( '/Operations/Bookkeeping/FileTypes', [] )
  gLogger.verbose( 'DataTypes retrieved from /Operations/Bookkeeping/FileTypes are:\n%s' % ( string.join( dataTypes, ', ' ) ) )
  LFN_ROOT = ''
  gLogger.verbose( 'wf lfn: %s, namespace: %s, configVersion: %s' % ( lfn, namespace, configVersion ) )
  if not lfn:
    LFN_ROOT = '/lhcb/%s/%s' % ( namespace, configVersion )
    gLogger.verbose( 'LFN_ROOT will be %s' % ( LFN_ROOT ) )
    return LFN_ROOT

  lfn = [fname.replace( ' ', '' ).replace( 'LFN:', '' ) for fname in lfn.split( ';' )]
  lfnroot = lfn[0].split( '/' )
  for part in lfnroot:
    if not part in dataTypes:
      LFN_ROOT += '/%s' % ( part )
    else:
      break

  if re.search( '//', LFN_ROOT ):
    LFN_ROOT = LFN_ROOT.replace( '//', '/' )

  if namespace.lower() in ( 'test', 'debug' ):
    tmpLfnRoot = LFN_ROOT.split( os.path.sep )
    if len( tmpLfnRoot ) > 2:
      tmpLfnRoot[2] = namespace
    else:
      tmpLfnRoot[-1] = namespace

    LFN_ROOT = string.join( tmpLfnRoot, os.path.sep )

  return LFN_ROOT

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
