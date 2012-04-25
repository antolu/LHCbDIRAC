""" Utility to construct production LFNs from workflow parameters
    according to LHCb conventions.

    The methods here are mostly from ancient history and need to be
    reviewed, these methods were grouped together as they form the
    "interface" for production clients and workflow modules to create LFNs.

"""

__RCSID__ = "$Id$"

import re, os, types, datetime, copy

from DIRAC import S_OK, S_ERROR, gLogger

gLogger = gLogger.getSubLogger( 'ProductionData' )

#############################################################################

def constructProductionLFNs( paramDict, bkClient = None ):
  """ Used for local testing of a workflow, a temporary measure until
      LFN construction is tidied.  This works using the workflow commons for
      on the fly construction.
  """
  try:

    keys = ['PRODUCTION_ID', 'JOB_ID', 'configVersion', 'outputList', 'configName', 'outputDataFileMask']
    for k in keys:
      if not paramDict.has_key( k ):
        return S_ERROR( '%s not defined' % k )

    productionID = paramDict['PRODUCTION_ID']
    jobID = paramDict['JOB_ID']
    wfConfigName = paramDict['configName']
    wfConfigVersion = paramDict['configVersion']
    wfMask = paramDict['outputDataFileMask']
    if not type( wfMask ) == type( [] ):
      wfMask = [i.lower().strip() for i in wfMask.split( ';' )]
    outputList = paramDict['outputList']

    fileTupleList = []
    gLogger.verbose( 'wfConfigName = %s, wfConfigVersion = %s, wfMask = %s' % ( wfConfigName, wfConfigVersion, wfMask ) )
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
      fileTupleList.append( ( str.join( fileName, '_' ), info['outputDataType'] ) )

    #Strip output data according to file mask
    fileTupleListMasked = _applyMask( wfMask, fileTupleList )

  #  lfnRoot = ''
  #  debugRoot = ''
  #  if inputData:
  #    gLogger.verbose( 'Making LFN_ROOT for job with inputdata: %s' % ( inputData ) )
  #    lfnRoot = _getLFNRoot( inputData, wfConfigName )
  #    debugRoot = _getLFNRoot( '', 'debug', wfConfigVersion )
  #  else:
  #    lfnRoot = _getLFNRoot( '', wfConfigName, wfConfigVersion )
  #    gLogger.verbose( 'LFN_ROOT is: %s' % ( lfnRoot ) )
  #    debugRoot = _getLFNRoot( '', 'debug', wfConfigVersion )
    lfnRoot = _getLFNRoot( '', wfConfigName, wfConfigVersion, bkClient )
    gLogger.verbose( 'LFN_ROOT is: %s' % ( lfnRoot ) )
    debugRoot = _getLFNRoot( '', 'debug', wfConfigVersion, bkClient )

    gLogger.verbose( 'LFN_ROOT is: %s' % ( lfnRoot ) )
    if not lfnRoot:
      return S_ERROR( 'LFN root could not be constructed' )

    #Get all LFN(s) to both output data and BK lists at this point (fine for BK)
    outputData = []
    bkLFNs = []
    debugLFNs = []

    #outputData is masked
    for fileTuple in fileTupleListMasked:
      lfn = _makeProductionLFN( str( jobID ).zfill( 8 ), lfnRoot, fileTuple, str( productionID ).zfill( 8 ) )
      outputData.append( lfn )

    #BKLFNs and debugLFNs are not masked
    for fileTuple in fileTupleList:
      lfn = _makeProductionLFN( str( jobID ).zfill( 8 ), lfnRoot, fileTuple, str( productionID ).zfill( 8 ) )
      bkLFNs.append( lfn )
      if debugRoot:
        debugLFNs.append( _makeProductionLFN( str( jobID ).zfill( 8 ),
                                              debugRoot,
                                              fileTuple,
                                              str( productionID ).zfill( 8 ) ) )

    if debugRoot:
      debugLFNs.append( _makeProductionLFN( str( jobID ).zfill( 8 ),
                                            debugRoot,
                                            ( '%s_core' % str( jobID ).zfill( 8 ) , 'core' ),
                                            str( productionID ).zfill( 8 ) ) )

    #Get log file path - unique for all modules
    logPath = _makeProductionPath( str( jobID ).zfill( 8 ), lfnRoot, 'LOG', wfConfigName, str( productionID ).zfill( 8 ), log = True )
    logFilePath = ['%s/%s' % ( logPath, str( jobID ).zfill( 8 ) )]
    logTargetPath = ['%s/%s_%s.tar' % ( logPath, str( productionID ).zfill( 8 ), str( jobID ).zfill( 8 ) )]
    #[ aside, why does makeProductionPath not append the jobID itself ????
    #  this is really only used in one place since the logTargetPath is just written to a text file (should be reviewed)... ]

    if not outputData:
      gLogger.info( 'No output data LFN(s) constructed' )
    else:
      gLogger.verbose( 'Created the following output data LFN(s):\n%s' % ( str.join( outputData, '\n' ) ) )
    gLogger.verbose( 'Log file path is:\n%s' % logFilePath[0] )
    gLogger.verbose( 'Log target path is:\n%s' % logTargetPath[0] )
    if bkLFNs:
      gLogger.verbose( 'BookkeepingLFN(s) are:\n%s' % ( str.join( bkLFNs, '\n' ) ) )
    if debugLFNs:
      gLogger.verbose( 'DebugLFN(s) are:\n%s' % ( str.join( debugLFNs, '\n' ) ) )
    jobOutputs = {'ProductionOutputData':outputData, 'LogFilePath':logFilePath, 'LogTargetPath':logTargetPath, 'BookkeepingLFNs':bkLFNs, 'DebugLFNs':debugLFNs}
    return S_OK( jobOutputs )

  except Exception, e:
    return S_ERROR( e )

#############################################################################

def _applyMask( mask, dataTuplesList ):
  """ apply the MASK to the dataset"""

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

def getLogPath( paramDict, bkClient = None ):
  """ Can construct log file paths even if job fails e.g. no output files available.
  """
  try:
    keys = ['PRODUCTION_ID', 'JOB_ID', 'configName', 'configVersion']
    for k in keys:
      if not paramDict.has_key( k ):
        return S_ERROR( '%s not defined' % k )

    productionID = paramDict['PRODUCTION_ID']
    jobID = paramDict['JOB_ID']
    wfConfigName = paramDict['configName']
    wfConfigVersion = paramDict['configVersion']

    gLogger.verbose( 'wfConfigName = %s, wfConfigVersion = %s' % ( wfConfigName, wfConfigVersion ) )
  #  lfnRoot = ''
  #  if inputData:
  #    lfnRoot = _getLFNRoot( inputData, wfType )
  #  else:
  #    lfnRoot = _getLFNRoot( '', wfConfigName, wfConfigVersion )
    lfnRoot = _getLFNRoot( '', wfConfigName, wfConfigVersion, bkClient )

    #Get log file path - unique for all modules
    logPath = _makeProductionPath( str( jobID ).zfill( 8 ), lfnRoot, 'LOG', wfConfigName, str( productionID ).zfill( 8 ), log = True )
    logFilePath = ['%s/%s' % ( logPath, str( jobID ).zfill( 8 ) )]
    logTargetPath = ['%s/%s_%s.tar' % ( logPath, str( productionID ).zfill( 8 ), str( jobID ).zfill( 8 ) )]

    gLogger.verbose( 'Log file path is:\n%s' % logFilePath )
    gLogger.verbose( 'Log target path is:\n%s' % logTargetPath )
    jobOutputs = {'LogFilePath':logFilePath, 'LogTargetPath':logTargetPath}
    return S_OK( jobOutputs )
  except Exception, e:
    return S_ERROR( e )

#############################################################################

def constructUserLFNs( jobID, owner, outputFiles, outputPath ):
  """ This method is used to supplant the standard job wrapper output data policy
      for LHCb.  The initial convention adopted for user output files is the following:

      /lhcb/user/<initial e.g. p>/<owner e.g. paterson>/<outputPath>/<yearMonth e.g. 2010_02>/<subdir>/<fileName>
  """
  try:
    initial = owner[:1]
    subdir = str( jobID / 1000 )
    timeTup = datetime.date.today().timetuple()
    yearMonth = '%s_%s' % ( timeTup[0], str.zfill( str( timeTup[1] ), 2 ) )
    outputLFNs = {}

    #Strip out any leading or trailing slashes but allow fine structure
    if outputPath:
      outputPathList = str.split( outputPath, os.sep )
      newPath = []
      for i in outputPathList:
        if i:
          newPath.append( i )
      outputPath = str.join( newPath, os.sep )

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
      gLogger.info( 'Created the following output data LFN(s):\n%s' % ( str.join( outputData, '\n' ) ) )
    else:
      gLogger.info( 'No output LFN(s) constructed' )

    return S_OK( outputData )
  except Exception, e:
    return S_ERROR( e )

#############################################################################

def preSubmissionLFNs( jobCommons, jobCode, productionID = '1', jobID = '2' ):
  """ Constructs LFNs to be added to the job description prior to submission
      or simply for visual inspection.

      This is a wrapper around constructProductionLFNs used by the production
      clients.
  """
  try:
    outputList = []
    for line in jobCode.split( "\n" ):
      if line.count( "listoutput" ):
        outputList += eval( line.split( "#" )[0].split( "=" )[-1] )

    jobCommons['outputList'] = outputList
    jobCommons['PRODUCTION_ID'] = productionID
    jobCommons['JOB_ID'] = jobID

    gLogger.debug( jobCommons )
    result = constructProductionLFNs( jobCommons )
    if not result['OK']:
      gLogger.error( result )
    return result
  except Exception, e:
    return S_ERROR( e )

#############################################################################

def _makeProductionPath( JOB_ID, LFN_ROOT, typeName, mode, prodstring, log = False ):
  """ Constructs the path in the logical name space where the output
      data for the given production will go. In
  """
  result = LFN_ROOT + '/' + typeName.upper() + '/' + prodstring + '/'
  if log:
    try:
      jobid = int( JOB_ID )
      jobindex = str.zfill( jobid / 10000, 4 )
    except Exception:
      jobindex = '0000'
    result += jobindex

  return result

#############################################################################

def _makeProductionLFN( JOB_ID, LFN_ROOT, filetuple, prodstring ):
  """ Constructs the logical file name according to LHCb conventions.
      Returns the lfn without 'lfn:' prepended.
  """
  gLogger.debug( 'Making production LFN for JOB_ID %s, LFN_ROOT %s, prodstring %s for\n%s' % ( JOB_ID, LFN_ROOT,
                                                                                               prodstring,
                                                                                               str( filetuple ) ) )
  try:
    jobid = int( JOB_ID )
    jobindex = str.zfill( jobid / 10000, 4 )
  except Exception:
    jobindex = '0000'

  fname = filetuple[0]
  if re.search( 'lfn:', fname ) or re.search( 'LFN:', fname ):
    return fname.replace( 'lfn:', '' ).replace( 'LFN:', '' )

  return LFN_ROOT + '/' + filetuple[1].upper() + '/' + prodstring + '/' + jobindex + '/' + fname

#############################################################################

def _getLFNRoot( lfn, namespace = '', configVersion = 0, bkClient = None ):
  """
  return the root path of a given lfn

  eg : /lhcb/data/CCRC08/00009909 = getLFNRoot(/lhcb/data/CCRC08/00009909/DST/0000/00009909_00003456_2.dst)
  eg : /lhcb/MC/<year>/  = getLFNRoot(None)
  """
  if not bkClient:
    from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
    bkClient = BookkeepingClient()

  dataTypes = bkClient.getFileTypes( {} )
  if not dataTypes['OK']:
    raise Exception, dataTypes['Message']
  dataTypes = [x[0] for x in dataTypes['Value']['Records']]
  gLogger.verbose( 'DataTypes retrieved from the BKK are:\n%s' % ( str.join( dataTypes, ', ' ) ) )
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

    LFN_ROOT = str.join( tmpLfnRoot, os.path.sep )

  return LFN_ROOT

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
