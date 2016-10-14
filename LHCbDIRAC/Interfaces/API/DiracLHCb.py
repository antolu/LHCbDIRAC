"""
   LHCb API Class

   The LHCb API exposes LHCb specific functionality in addition to the standard DIRAC API.

"""

__RCSID__ = "$Id$"

import os
import glob
import fnmatch
import time

from DIRAC                                               import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.SiteSEMapping                  import getSEsForSite, getSitesForSE
from DIRAC.Interfaces.API.Dirac                          import Dirac
from DIRAC.Interfaces.API.DiracAdmin                     import DiracAdmin
from DIRAC.ResourceStatusSystem.Client.ResourceStatus    import ResourceStatus

from LHCbDIRAC.Core.Utilities.File                        import makeGuid
from LHCbDIRAC.Core.Utilities.ClientTools                 import mergeRootFiles
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

COMPONENT_NAME = 'DiracLHCb'

def getSiteForSE( se ):
  """ Get site name for the given SE
  """
  result = getSitesForSE( se )
  if not result['OK']:
    return result
  if result['Value']:
    return S_OK( result['Value'][0] )
  return S_OK( '' )

class DiracLHCb( Dirac ):

  #############################################################################
  def __init__( self, withRepo = False, repoLocation = '', operationsHelperIn = None ):
    """Internal initialization of the DIRAC API.
    """

    super( DiracLHCb, self ).__init__( withRepo = withRepo, repoLocation = repoLocation )
    self.tier1s = []

    if not operationsHelperIn:
      self.opsH = Operations()
    else:
      self.opsH = operationsHelperIn

    self.bkQueryTemplate = { 'SimulationConditions'     : 'All',
                             'DataTakingConditions'     : 'All',
                             'ProcessingPass'           : 'All',
                             'FileType'                 : 'All',
                             'EventType'                : 'All',
                             'ConfigName'               : 'All',
                             'ConfigVersion'            : 'All',
                             'Production'               :     0,
                             'StartRun'                 :     0,
                             'EndRun'                   :     0,
                             'DataQuality'              : 'All',
                             'Visible'                  : 'Yes'}
    self.bk = BookkeepingClient()  # to expose all BK client methods indirectly
    self.resourceStatus = ResourceStatus()

  #############################################################################
  def addRootFile( self, lfn, fullPath, diracSE, printOutput = False ):
    """ Add a Root file to Grid storage, an attempt is made to retrieve the
        POOL GUID of the file prior to upload.

       Example Usage:

       >>> print dirac.addFile('/lhcb/user/p/paterson/myRootFile.tar.gz','myFile.tar.gz','CERN-USER')
       {'OK': True, 'Value':{'Failed': {},
        'Successful': {'/lhcb/user/p/paterson/test/myRootFile.tar.gz': {'put': 64.246301889419556,
                                                                    'register': 1.1102778911590576}}}}

       @param lfn: Logical File Name (LFN)
       @type lfn: string
       @param diracSE: DIRAC SE name e.g. CERN-USER
       @type diracSE: strin
       @param printOutput: Optional flag to print result
       @type printOutput: boolean
       @return: S_OK,S_ERROR
    """
    return super( DiracLHCb, self ).addFile( lfn, fullPath, diracSE,
                                             fileGuid = makeGuid( fullPath )[fullPath],
                                             printOutput = printOutput )

  def addFile( self, lfn, fullPath, diracSE, printOutput = False ):
    """ Copy of addRootFile
    """
    return super( DiracLHCb, self ).addFile( lfn, fullPath, diracSE,
                                             fileGuid = makeGuid( fullPath )[fullPath],
                                             printOutput = printOutput )

  #############################################################################
  def rootMergeRepository( self, outputFileName, inputFileMask = '*.root',
                           location = 'Sandbox', requestedStates = ['Done'] ):
    """ Create a merged ROOT file using root files retrived in the sandbox or output data

       Example Usage:

       >>> print dirac.rootMergeRepository('MyMergedRootFile.root',
       inputFileMask='DVHistos.root',location='Sandbox', requestedStates = ['Done'])
       {'OK': True, 'Value': ''}

       @param outputFileName: The target merged file
       @type outputFileName: string
       @param inputFileMask: Mask to be used when locating input files. Can support wildcards like 'Tuple*.root'
       @type inputFileMask: string
       @param location: The input files present either in the 'Sandbox' (retrieved with getOutputSandbox)
        or 'OutputFiles' (getJobOutputData)
       @type location: string
       @param requestedStates: List of jobs states to be considered
       @type requestedStates: list of strings
       @return: S_OK,S_ERROR
    """
    if not self.jobRepo:
      self.log.warn( "No repository is initialised" )
      return S_OK()

    # Get the input files to be used
    jobs = self.jobRepo.readRepository()['Value']
    inputFiles = []
    for jobID in sorted( jobs ):
      jobDict = jobs[jobID]
      if jobDict.get( 'State' ) in requestedStates:
        if location == 'OutputFiles':
          jobFiles = eval( jobDict[location] )
          for jobFile in jobFiles:
            fileName = os.path.basename( jobFile )
            if fnmatch.fnmatch( fileName, inputFileMask ):
              if os.path.exists( jobFile ):
                inputFiles.append( jobFile )
              else:
                self.log.warn( "Repository output file does not exist locally", jobFile )
        elif location == 'Sandbox':
          globStr = "%s/%s" % ( jobDict[location], inputFileMask )
          print glob.glob( globStr )
          inputFiles.extend( glob.glob( globStr ) )
        else:
          return self._errorReport( "Location of .root should be 'Sandbox' or 'OutputFiles'." )

    # Perform the root merger
    res = mergeRootFiles( outputFileName, inputFiles, daVinciVersion = '' )
    if not res['OK']:
      return self._errorReport( res['Message'], "Failed to perform final ROOT merger" )
    return S_OK()

  #############################################################################

  def getBKAncestors( self, lfns, depth = 1, replica = True ):
    """ This function allows to retrieve ancestor files from the Bookkeeping.

        Example Usage:

        >>> dirac.getBKAncestors('/lhcb/data/2009/DST/00005727/0000/00005727_00000042_1.dst',2)
        {'OK': True, 'Value': ['/lhcb/data/2009/DST/00005727/0000/00005727_00000042_1.dst',
        '/lhcb/data/2009/RAW/FULL/LHCb/COLLISION09/63807/063807_0000000004.raw']}

       @param lfn: Logical File Name (LFN)
       @type lfn: string or list
       @param depth: Ancestor depth
       @type depth: integer
    """

    result = self.bk.getFileAncestors( lfns, depth, replica = replica )
    if not result['OK']:
      return S_ERROR( 'Could not get ancestors: ' + result['Message'] )
    ancestors = [x[0]['FileName'] for x in result['Value']['Successful'].values()]

    return S_OK( lfns + ancestors )

  #############################################################################

  def __translateBKPath( self, bkPath, procPassID = 3 ):
    bk = filter( None, bkPath.split( '/' ) )
    if procPassID < 0:
      return bk
    try:
      bkNodes = bk[0:procPassID]
      bkNodes.append( '/' + '/'.join( bk[procPassID:-2] ) )
      bkNodes.append( bk[-2] )
      bkNodes.append( bk[-1] )
    except:
      print "Incorrect BKQuery...\n"
      bkNodes = None
    return bkNodes

  #############################################################################
  def bkQueryRunsByDate( self, bkPath, startDate, endDate, dqFlag = 'All', selection = 'Runs' ):
    """ This function allows to create and perform a BK query given a supplied
        BK path. The following BK path convention is expected:

        /<ConfigurationName>/<Configuration Version>/<Processing Pass>/<Event Type>/<File Type>

        so an example for 2010 collisions data would be:

        /LHCb/Collision09/Real Data + RecoToDST-07/90000000/DST

        The startDate and endDate must be specified as yyyy-mm-dd.

        Runs can be selected based on their status e.g. the selection parameter
        has the following possible attributes:
         - Runs - data for all runs in the range are queried (default)
         - ProcessedRuns - data is retrieved for runs that are processed
         - NotProcessed - data is retrieved for runs that are not yet processed.

       Example Usage:

       >>> dirac.bkQueryRunsByDate('/LHCb/Collision10/Real Data/90000000/RAW','2010-05-18','2010-05-20',dqFlag='EXPRESS_OK',selection='ProcessedRuns')
       {'OK': True, 'Value': [<LFN1>,<LFN2>]}

       @param bkPath: BK path as described above
       @type bkPath: string
       @param dqFlag: Optional Data Quality flag
       @type dqFlag: string
       @param startDate: Start date  yyyy-mm-dd
       @param startDate: string
       @param endDate: End date  yyyy-mm-dd
       @param endDate: string
       @param selection: Either Runs, ProcessedRuns or NotProcessed
       @param selection: string
       @return: S_OK,S_ERROR
    """
    runSelection = ['Runs', 'ProcessedRuns', 'NotProcessed']
    if not selection in runSelection:
      return S_ERROR( 'Expected one of %s not "%s" for selection' % ( ', '.join( runSelection ), selection ) )

    if not isinstance( bkPath, str ):
      return S_ERROR( 'Expected string for bkPath' )

    # remove any double slashes, spaces must be preserved
    # remove any empty components from leading and trailing slashes
    bkPath = self.__translateBKPath( bkPath, procPassID = 2 )
    if not len( bkPath ) == 5:
      return S_ERROR( 'Expected 5 components to the BK path: /<ConfigurationName>/<Configuration Version>/<Processing Pass>/<Event Type>/<File Type>' )

    if not startDate or not endDate:
      return S_ERROR( 'Expected both start and end dates to be defined in format: yyyy-mm-dd' )

    if not isinstance( startDate, str ) or not isinstance( endDate, str ):
      return S_ERROR( 'Expected yyyy-mm-dd string for start and end dates' )

    if not len( startDate.split( '-' ) ) == 3 or not len( endDate.split( '-' ) ) == 3:
      return S_ERROR( 'Expected yyyy-mm-dd string for start and end dates' )

    start = time.time()
    result = self.bk.getRunsForAGivenPeriod( {'StartDate':startDate, 'EndDate':endDate} )
    rtime = time.time() - start
    self.log.info( 'BK query time: %.2f sec' % rtime )
    if not result['OK']:
      self.log.info( 'Could not get runs with given dates from BK with result: "%s"' % result )
      return result

    if not result['Value']:
      self.log.info( 'No runs selected from BK for specified dates' )
      return result

    if selection not in result['Value']:
      return S_ERROR( 'No %s runs for specified dates' % ( selection ) )

    runs = result['Value'][selection]
    self.log.info( 'Found the following %s runs:\n%s' % ( len( runs ), ', '.join( [str( i ) for i in runs] ) ) )
    # temporary until we can query for a discrete list of runs

    selectedData = []
    for run in runs:
      query = self.bkQueryTemplate.copy()
      query['StartRun'] = run
      query['EndRun'] = run
      query['ConfigName'] = bkPath[0]
      query['ConfigVersion'] = bkPath[1]
      query['ProcessingPass'] = bkPath[2]
      query['EventType'] = bkPath[3]
      query['FileType'] = bkPath[4]
      if dqFlag:
        check = self.__checkDQFlags( dqFlag )
        if not check['OK']:
          return check
        dqFlag = check['Value']
        query['DataQuality'] = dqFlag
      result = self.bkQuery( query )
      self.log.verbose( result )
      if not result['OK']:
        return result
      self.log.info( 'Selected %s files for run %s' % ( len( result['Value'] ), run ) )
      selectedData += result['Value']

    self.log.info( 'Total files selected = %s' % ( len( selectedData ) ) )
    return S_OK( selectedData )

  #############################################################################
  def bkQueryRun( self, bkPath, dqFlag = 'All' ):
    """ This function allows to create and perform a BK query given a supplied
        BK path. The following BK path convention is expected:

        /<Run Number>/<Processing Pass>/<Event Type>/<File Type>

        so an example for 2009 collisions data would be:

       /63566/Real Data + RecoToDST-07/90000000/DST

       In addition users can specify a range of runs using the following convention:

       /<Run Number 1> - <Run Number 2>/<Processing Pass>/<Event Type>/<File Type>

       so extending the above example this would look like:

       /63566-63600/Real Data + RecoToDST-07/90000000/DST

       Example Usage:

       >>> dirac.bkQueryRun('/63566/Real Data/RecoToDST-07/90000000/DST')
       {'OK':True,'Value': ['/lhcb/data/2009/DST/00005842/0000/00005842_00000008_1.dst']}

       @param bkPath: BK path as described above
       @type bkPath: string
       @param dqFlag: Optional Data Quality flag
       @type dqFlag: string
       @return: S_OK,S_ERROR
    """
    if not isinstance( bkPath, str ):
      return S_ERROR( 'Expected string for bkPath' )

    # remove any double slashes, spaces must be preserved
    # remove any empty components from leading and trailing slashes
    bkPath = self.__translateBKPath( bkPath, procPassID = 1 )
    if not len( bkPath ) == 4:
      return S_ERROR( 'Expected 4 components to the BK path: /<Run Number>/<Processing Pass>/<Event Type>/<File Type>' )

    runNumberString = bkPath[0].replace( '--', '-' ).replace( ' ', '' )
    startRun = 0
    endRun = 0
    if '-' in runNumberString:
      runs = runNumberString.split( '-' )
      if len( runs ) != 2:
        return S_ERROR( 'Could not determine run range from "%s", try "<Run 1> - <Run2>"' % ( runNumberString ) )
      try:
        start = int( runs[0] )
        end = int( runs[1] )
      except:
        return S_ERROR( 'Invalid run range: %s' % runNumberString )
      startRun = min( start, end )
      endRun = max( start, end )
    else:
      try:
        startRun = int( runNumberString )
        endRun = startRun
      except:
        return S_ERROR( 'Invalid run number: %s' % runNumberString )

    query = self.bkQueryTemplate.copy()
    query['StartRun'] = startRun
    query['EndRun'] = endRun
    query['ProcessingPass'] = bkPath[1]
    query['EventType'] = bkPath[2]
    query['FileType'] = bkPath[3]

    if dqFlag:
      check = self.__checkDQFlags( dqFlag )
      if not check['OK']:
        return check
      dqFlag = check['Value']
      query['DataQuality'] = dqFlag

    result = self.bkQuery( query )
    self.log.verbose( result )
    return result

  #############################################################################
  def bkQueryProduction( self, bkPath, dqFlag = 'All' ):
    """ This function allows to create and perform a BK query given a supplied
        BK path. The following BK path convention is expected:

        /<ProductionID>/[<Processing Pass>/<Event Type>/]<File Type>

        so an example for 2009 collisions data would be:

       /5842/Real Data + RecoToDST-07/90000000/DST

       Note that neither the processing pass nor the event type should be necessary. So either of them can be omitted

       a data quality flag can also optionally be provided, the full list of these is available
       via the getAllDQFlags() method.

       Example Usage:

       >>> dirac.bkQueryProduction('/5842/Real Data/RecoToDST-07/90000000/DST')
       {'OK': True, 'Value': [<LFN1>,<LFN2>]}

       @param bkPath: BK path as described above
       @type bkPath: string
       @param dqFlag: Optional Data Quality flag
       @type dqFlag: string
       @return: S_OK,S_ERROR
    """
    if not isinstance( bkPath, str ):
      return S_ERROR( 'Expected string for bkPath' )

    # remove any double slashes, spaces must be preserved
    # remove any empty components from leading and trailing slashes
    bkPath = self.__translateBKPath( bkPath, procPassID = 1 )
    if len( bkPath ) < 2:
      return S_ERROR( 'Invalid bkPath: should at least contain /ProductionID/FileType' )
    query = self.bkQueryTemplate.copy()
    try:
      query['Production'] = int( bkPath[0] )
    except:
      return S_ERROR( 'Invalid production ID' )
    query['FileType'] = bkPath[-1]

    if dqFlag:
      check = self.__checkDQFlags( dqFlag )
      if not check['OK']:
        return check
      dqFlag = check['Value']
      query['DataQuality'] = dqFlag

    for key, val in query.items():
      if isinstance( val, basestring ) and val.lower() == 'all':
        query.pop( key )
    result = self.bkQuery( query )
    self.log.verbose( result )
    return result

  #############################################################################
  def bkQueryPath( self, bkPath, dqFlag = 'All' ):
    """ This function allows to create and perform a BK query given a supplied
        BK path. The following BK path convention is expected:

       /<ConfigurationName>/<Configuration Version>/<Sim or Data Taking Condition>/<Processing Pass>/<Event Type>/<File Type>

       so an example for 2009 collsions data would be:

       /LHCb/Collision09/Beam450GeV-VeloOpen-MagDown/Real Data + RecoToDST-07/90000000/DST

       or for MC09 simulated data:

       /MC/2010/Beam3500GeV-VeloClosed-MagDown-Nu1/2010-Sim01Reco01-withTruth/27163001/DST

       a data quality flag can also optionally be provided, the full list of these is available
       via the getAllDQFlags() method.

       Example Usage:

       >>> dirac.bkQueryPath('/MC/2010/Beam3500GeV-VeloClosed-MagDown-Nu1/Sim07/Reco06-withTruth/10012004/DST')
       {'OK': True, 'Value': [<LFN1>,<LFN2>]}

       @param bkPath: BK path as described above
       @type bkPath: string
       @param dqFlag: Optional Data Quality flag
       @type dqFlag: string
       @return: S_OK,S_ERROR
    """
    if not isinstance( bkPath, str ):
      return S_ERROR( 'Expected string for bkPath' )

    # remove any double slashes, spaces must be preserved
    # remove any empty components from leading and trailing slashes
    bkPath = self.__translateBKPath( bkPath, procPassID = 3 )
    if not len( bkPath ) == 6:
      return S_ERROR( 'Expected 6 components to the BK path: \
      /<ConfigurationName>/<Configuration Version>/<Sim or Data Taking Condition>/<Processing Pass>/<Event Type>/<File Type>' )

    query = self.bkQueryTemplate.copy()
    query['ConfigName'] = bkPath[0]
    query['ConfigVersion'] = bkPath[1]
    query['ProcessingPass'] = bkPath[3]
    query['EventType'] = bkPath[4]
    query['FileType'] = bkPath[5]

    if dqFlag:
      check = self.__checkDQFlags( dqFlag )
      if not check['OK']:
        return check
      dqFlag = check['Value']
      query['DataQuality'] = dqFlag

    # The problem here is that we don't know if it's a sim or data taking condition,
    # assume that if configName=MC this is simulation
    if bkPath[0].lower() == 'mc':
      query['SimulationConditions'] = bkPath[2]
    else:
      query['DataTakingConditions'] = bkPath[2]

    result = self.bkQuery( query )
    self.log.verbose( result )
    return result

  #############################################################################
  def bookkeepingQuery( self, SimulationConditions = 'All', DataTakingConditions = 'All',
                        ProcessingPass = 'All', FileType = 'All', EventType = 'All', ConfigName = 'All',
                        ConfigVersion = 'All', ProductionID = 0, DataQuality = 'ALL' ):
    """ This function will create and perform a BK query using the supplied arguments
        and return a list of LFNs.

        Example Usage:

        >>> dirac.bookkeepingQuery(ConfigName='LHCb',ConfigVersion='Collision09',
        EventType='90000000',ProcessingPass='Real Data',DataTakingConditions='Beam450GeV-VeloOpen-MagDown')
        {'OK':True,'Value':<files>}

       @param  ConfigName: BK ConfigName
       @type ConfigName: string
       @param  EventType: BK EventType
       @type EventType: string
       @param  FileType: BK FileType
       @type FileType: string
       @param  ProcessingPass: BK ProcessingPass
       @type ProcessingPass: string
       @param  ProductionID: BK ProductionID
       @type ProductionID: integer
       @param  DataQuality: BK DataQuality
       @type DataQuality: string
       @param  ConfigVersion: BK ConfigVersion
       @type ConfigVersion: string
       @param  DataTakingConditions: BK DataTakingConditions
       @type DataTakingConditions: string
       @param  SimulationConditions: BK SimulationConditions
       @type SimulationConditions: string
       @return: S_OK,S_ERROR
    """
    query = self.bkQueryTemplate.copy()
    query['SimulationConditions'] = SimulationConditions
    query['DataTakingConditions'] = DataTakingConditions
    query['ProcessingPass'] = ProcessingPass
    query['FileType'] = FileType
    query['EventType'] = EventType
    query['ConfigName'] = ConfigName
    query['ConfigVersion'] = ConfigVersion
    query['Production'] = ProductionID
    query['DataQuality'] = DataQuality
    return self.bkQuery( query )

  #############################################################################
  def bkQuery( self, bkQueryDict ):
    """ Developer function. Perform a query to the LHCb Bookkeeping to return
        a list of LFN(s). This method takes a BK query dictionary.

        Example Usage:

        >>> print dirac.bkQuery(query)
        {'OK':True,'Value':<files>}

       @param bkQueryDict: BK query
       @type bkQueryDict: dictionary (see bookkeepingQuery() for keys)
       @return: S_OK,S_ERROR
    """
    problematicFields = []
    # Remove the Visible flag as anyway the method is for visible files ;-)
    # bkQueryDict.setdefault( 'Visible', 'Yes' )
    for name, value in bkQueryDict.items():
      if name not in self.bkQueryTemplate:
        problematicFields.append( name )

    if problematicFields:
      msg = 'The following fields are not valid for a BK query: %s\nValid fields include: %s' % ( ', '.join( problematicFields ),
                                                                                                  ', '.join( self.bkQueryTemplate.keys() ) )
      return S_ERROR( msg )

    for name, value in bkQueryDict.items():
      if name == "Production" or name == "EventType" or name == "StartRun" or name == "EndRun":
        if value == 0:
          del bkQueryDict[name]
        else:
          bkQueryDict[name] = str( value )
      elif name == "FileType":
        if value.lower() == "all":
          bkQueryDict[name] = 'ALL'
      else:
        if str( value ).lower() == "all":
          del bkQueryDict[name]

    if 'Production' in bkQueryDict or 'StartRun' in bkQueryDict or 'EndRun' in bkQueryDict:
      self.log.verbose( 'Found a specific query so loosening some restrictions to prevent BK overloading' )
    else:
      if 'SimulationConditions' not in bkQueryDict and 'DataTakingConditions' not in bkQueryDict:
        return S_ERROR( 'A Simulation or DataTaking Condition must be specified for a BK query.' )
      if 'EventType' not in bkQueryDict and 'ConfigName' not in bkQueryDict and 'ConfigVersion' not in bkQueryDict:
        return S_ERROR( 'The minimal set of BK fields for a query is: EventType, ConfigName and ConfigVersion in addition to a Simulation or DataTaking Condition' )

    self.log.verbose( 'Final BK query dictionary is:' )
    for n, v in bkQueryDict.items():
      self.log.verbose( '%s : %s' % ( n, v ) )

    start = time.time()
    result = self.bk.getVisibleFilesWithMetadata( bkQueryDict )
#    result = bk.getFilesWithGivenDataSets(bkQueryDict)
    rtime = time.time() - start
    self.log.info( 'BK query time: %.2f sec' % rtime )

    if not result['OK']:
      return S_ERROR( 'BK query returned an error: "%s"' % ( result['Message'] ) )

    if not result['Value']:
      return self._errorReport( 'No BK files selected' )

    returnedFiles = len( result['Value'] )
    self.log.verbose( '%s files selected from the BK' % ( returnedFiles ) )
    return result

  #############################################################################
  def __checkDQFlags( self, flags ):
    """ Internal function.  Checks the provided flags against the list of
        possible DQ flag statuses from the Bookkeeping.
    """
    dqFlags = []
    if isinstance( flags, list ):
      dqFlags = flags
    else:
      dqFlags = [flags]

    bkFlags = self.getAllDQFlags()
    if not bkFlags['OK']:
      return bkFlags

    final = []
    for flag in dqFlags:
      if flag.lower() == 'all':
        final.append( flag.upper() )
      else:
        flag = flag.upper()
        if not flag in bkFlags['Value']:
          msg = 'Specified DQ flag "%s" is not in allowed list: %s' % ( flag, ', '.join( bkFlags['Value'] ) )
          self.log.error( msg )
          return S_ERROR( msg )
        else:
          final.append( flag )

    # when first coding this it was not possible to use a list ;)
    if len( final ) == 1:
      final = final[0]

    return S_OK( final )

  #############################################################################
  def getAllDQFlags( self, printOutput = False ):
    """ Helper function.  Returns the list of possible DQ flag statuses
        from the Bookkeeping.

        Example Usage:

        >>> print dirac.getAllDQFlags()
        {'OK':True,'Value':<flags>}

       @param printOutput: Optional flag to print result
       @type printOutput: boolean
       @return: S_OK,S_ERROR
    """
    result = self.bk.getAvailableDataQuality()
    if not result['OK']:
      self.log.error( 'Could not obtain possible DQ flags from BK with result:\n%s' % ( result ) )
      return result

    if printOutput:
      flags = result['Value']
      self.log.info( 'Possible DQ flags from BK are: %s' % ( ', '.join( flags ) ) )

    return result

  #############################################################################
  def getDataByRun( self, lfns, printOutput = False ):
    """Sort the supplied lfn list by run. An S_OK object will be returned
       containing a dictionary of runs and the corresponding list of LFN(s)
       associated with them.

       Example usage:

       >>> print dirac.getDataByRun(lfns)
       {'OK': True, 'Value': {<RUN>:['<LFN>','<LFN>',...], <RUN>:['<LFN>',..]}}


       @param lfns: Logical File Name(s)
       @type lfns: list
       @param printOutput: Optional flag to print result
       @type printOutput: boolean
       @return: S_OK,S_ERROR
    """
    if isinstance( lfns, str ):
      lfns = [lfns.replace( 'LFN:', '' )]
    elif isinstance( lfns, list ):
      try:
        lfns = [str( lfn.replace( 'LFN:', '' ) ) for lfn in lfns]
      except Exception as x:
        return self._errorReport( str( x ), 'Expected strings for LFNs' )
    else:
      return self._errorReport( 'Expected single string or list of strings for LFN(s)' )

    runDict = {}
    start = time.time()
    result = self.bk.getFileMetadata( lfns )
    self.log.verbose( "Obtained BK file metadata in %.2f seconds" % ( time.time() - start ) )
    if not result['OK']:
      self.log.error( 'Failed to get bookkeeping metadata with result "%s"' % ( result['Message'] ) )
      return result

    for lfn, metadata in result['Value']['Successful'].items():
      if 'RunNumber' in metadata:
        runNumber = metadata['RunNumber']
        runDict.setdefault( runNumber, [] ).append( lfn )
      else:
        self.log.warn( 'Could not find run number from BK for %s' % ( lfn ) )

    if printOutput:
      print self.pPrint.pformat( runDict )

    return S_OK( runDict )

  #############################################################################
  def bkMetadata( self, lfns, printOutput = False ):
    """Return metadata for the supplied lfn list. An S_OK object will be returned
       containing a dictionary of LFN(s) and the corresponding metadata associated
       with them.

       Example usage:

       >>> print dirac.bkMetadata(lfns)
       {'OK': True, 'Value': {<LFN>:{'<Name>':'<Value>',...},...}}

       @param lfns: Logical File Name(s)
       @type lfns: list
       @param printOutput: Optional flag to print result
       @type printOutput: boolean
       @return: S_OK,S_ERROR
    """
    if isinstance( lfns, str ):
      lfns = [lfns.replace( 'LFN:', '' )]
    elif isinstance( lfns, list ):
      try:
        lfns = [str( lfn.replace( 'LFN:', '' ) ) for lfn in lfns]
      except Exception as x:
        return self._errorReport( str( x ), 'Expected strings for LFNs' )
    else:
      return self._errorReport( 'Expected single string or list of strings for LFN(s)' )

    start = time.time()
    result = self.bk.getFileMetadata( lfns )
    self.log.verbose( "Obtained BK file metadata in %.2f seconds" % ( time.time() - start ) )
    if not result['OK']:
      self.log.error( 'Failed to get bookkeeping metadata with result "%s"' % ( result['Message'] ) )
      return result

    if printOutput:
      print self.pPrint.pformat( result['Value'] )

    return result

  #############################################################################

  def lhcbProxyInit( self, *args ):
    """ just calling the dirac-proxy-init script
    """
    os.system( "dirac-proxy-init -o LogLevel=NOTICE -t %s" % "' '".join( args ) )

  #############################################################################

  def lhcbProxyInfo( self, *args ):
    """ just calling the dirac-proxy-info script
    """
    os.system( "dirac-proxy-info -o LogLevel=NOTICE %s" % "' '".join( args ) )

  #############################################################################
  def gridWeather( self, printOutput = False ):
    """This method gives a snapshot of the current Grid weather from the perspective
       of the DIRAC site and SE masks.  Tier-1 sites are returned with more detailed
       information.

       Example usage:

       >>> print dirac.gridWeather()
       {'OK': True, 'Value': {{'Sites':<siteInfo>,'SEs':<seInfo>,'Tier-1s':<tierInfo>}}

       @param printOutput: Optional flag to print result
       @type printOutput: boolean
       @return: S_OK,S_ERROR
    """

    lcgSites = gConfig.getSections( '/Resources/Sites/LCG' )
    if not lcgSites[ 'OK' ]:
      return lcgSites

    for lcgSite in lcgSites[ 'Value' ]:

      tier = gConfig.getValue( '/Resources/Sites/LCG/%s/MoUTierLevel' % lcgSite, 2 )
      if tier in ( 0, 1 ):
        self.tier1s.append( lcgSite )

    siteInfo = self.checkSites()
    if not siteInfo['OK']:
      return siteInfo
    siteInfo = siteInfo['Value']

    seInfo = self.checkSEs()
    if not seInfo['OK']:
      return seInfo
    seInfo = seInfo['Value']

    tierSEs = {}
    for site in self.tier1s:
      tierSEs[site] = getSEsForSite( site )['Value']

    tierInfo = {}
    for site, seList in tierSEs.items():
      tierInfo[site] = {}
      for se in seList:
        if se in seInfo:
          tierSEInfo = seInfo[se]
          tierInfo[site][se] = tierSEInfo
      if site in siteInfo['AllowedSites']:
        tierInfo[site]['MaskStatus'] = 'Allowed'
      else:
        tierInfo[site]['MaskStatus'] = 'Banned'

    if printOutput:
      print '========> Tier-1 status in DIRAC site and SE masks'
      for site in sorted( self.tier1s ):
        print '\n====> %s is %s in site mask\n' % ( site, tierInfo[site]['MaskStatus'] )
        print '%s %s %s' % ( 'Storage Element'.ljust( 25 ), 'Read Status'.rjust( 15 ), 'Write Status'.rjust( 15 ) )
        for se in sorted( tierSEs[site] ):
          if se in tierInfo[site]:
            print '%s %s %s' % ( se.ljust( 25 ), tierInfo[site][se]['ReadStatus'].rjust( 15 ), tierInfo[site][se]['WriteStatus'].rjust( 15 ) )

      print '\n========> Tier-2 status in DIRAC site mask\n'
      allowedSites = siteInfo['AllowedSites']
      bannedSites = siteInfo['BannedSites']
      for site in self.tier1s:
        if site in allowedSites:
          allowedSites.remove( site )
        if site in bannedSites:
          bannedSites.remove( site )
      print ' %s sites are in the site mask, %s are banned.\n' % ( len( allowedSites ), len( bannedSites ) )

    summary = {'Sites':siteInfo, 'SEs':seInfo, 'Tier-1s':tierInfo}
    return S_OK( summary )

  #############################################################################
  def checkSites( self, gridType = 'LCG', printOutput = False ):
    """Return the list of sites in the DIRAC site mask and those which are banned.

       Example usage:

       >>> print dirac.checkSites()
       {'OK': True, 'Value': {'AllowedSites':['<Site>',...],'BannedSites':[]}

       @param printOutput: Optional flag to print result
       @type printOutput: boolean
       @return: S_OK,S_ERROR
    """
    siteMask = DiracAdmin().getSiteMask()
    if not siteMask['OK']:
      return siteMask

    totalList = gConfig.getSections( '/Resources/Sites/%s' % gridType )

    if not totalList['OK']:
      return S_ERROR( 'Could not get list of sites from CS' )
    totalList = totalList['Value']
    sites = siteMask['Value']
    bannedSites = []
    for site in totalList:
      if not site in sites:
        bannedSites.append( site )

    if printOutput:
      print '\n========> Allowed Sites\n'
      print '\n'.join( sites )
      print '\n========> Banned Sites\n'
      print '\n'.join( bannedSites )
      print '\nThere is a total of %s allowed sites and %s banned sites in the system.' % ( len( sites ),
                                                                                            len( bannedSites ) )

    return S_OK( {'AllowedSites':sites, 'BannedSites':bannedSites} )

  #############################################################################
  def checkSEs( self, printOutput = False ):
    """Check the status of read and write operations in the DIRAC SE mask.

       Example usage:

       >>> print dirac.checkSEs()
       {'OK': True, 'Value': {<LFN>:{'<Name>':'<Value>',...},...}}

       @param printOutput: Optional flag to print result
       @type printOutput: boolean
       @return: S_OK,S_ERROR
    """
    storageCFGBase = '/Resources/StorageElements'
    res = gConfig.getSections( storageCFGBase, True )

    if not res['OK']:
      return S_ERROR( 'Failed to get storage element information' )

    if printOutput:
      print '%s %s %s' % ( 'Storage Element'.ljust( 25 ), 'Read Status'.rjust( 15 ), 'Write Status'.rjust( 15 ) )

    seList = sorted( res['Value'] )
    res = self.resourceStatus.getStorageElementStatus( seList )
    if not res[ 'OK' ]:
      gLogger.error( "Failed to get StorageElement status for %s" % str( seList ) )

    result = {}
    for k, v in res[ 'Value' ].items():
      readState = v.get( 'ReadAccess' , 'Active' )
      writeState = v.get( 'WriteAccess', 'Active' )

      result[ k ] = { 'ReadStatus' : readState, 'WriteStatus' : writeState }
      if printOutput:
        print '%s %s %s' % ( k.ljust( 25 ), readState.rjust( 15 ), writeState.rjust( 15 ) )

    return S_OK( result )

  def splitInputDataBySize( self, lfns, maxSizePerJob = 20, printOutput = False ):
    """Split the supplied lfn list by the replicas present at the possible
       destination sites, based on a maximum size.
       An S_OK object will be returned containing a list of
       lists in order to create the jobs.

       Example usage:

       >>> d.splitInputDataBySize(lfns,10)
       {'OK': True, 'Value': [['<LFN>'], ['<LFN>']]}


       @param lfns: Logical File Name(s) to split
       @type lfns: list
       @param maxSizePerJob: Maximum size (in GB) per bunch
       @type maxSizePerJob: integer
       @param printOutput: Optional flag to print result
       @type printOutput: boolean
       @return: S_OK,S_ERROR
    """
    sitesForSE = {}
    if isinstance( lfns, str ):
      lfns = [lfns.replace( 'LFN:', '' )]
    elif isinstance( lfns, list ):
      try:
        lfns = [str( lfn.replace( 'LFN:', '' ) ) for lfn in lfns]
      except Exception as x:
        return self._errorReport( str( x ), 'Expected strings for LFNs' )
    else:
      return self._errorReport( 'Expected single string or list of strings for LFN(s)' )

    if not isinstance( maxSizePerJob, int ):
      try:
        maxSizePerJob = int( maxSizePerJob )
      except Exception as x:
        return self._errorReport( str( x ), 'Expected integer for maxSizePerJob' )
    maxSizePerJob *= 1000 * 1000 * 1000

    replicaDict = self.getReplicas( lfns )
    if not replicaDict['OK']:
      return replicaDict
    replicas = replicaDict['Value']['Successful']
    if not replicas:
      return self._errorReport( replicaDict['Value']['Failed'].items()[0], 'Failed to get replica information' )
    siteLfns = {}
    for lfn, reps in replicas.items():
      possibleSites = set( [site for se in reps for site in sitesForSE.setdefault( se, getSitesForSE( se ).get( 'Value', [] ) )] )
      siteLfns.setdefault( ','.join( sorted( possibleSites ) ), [] ).append( lfn )

    if '' in siteLfns:
      # Some files don't have active replicas
      return self._errorReport( 'No active replica found for', str( siteLfns[''] ) )
    # Get size of files
    metadataDict = self.getMetadata( lfns, printOutput )
    if not metadataDict['OK']:
      return metadataDict
    fileSizes = dict( [( lfn, metadataDict['Value']['Successful'].get( lfn, {} ).get( 'Size', maxSizePerJob ) ) for lfn in lfns] )

    lfnGroups = []
    # maxSize is in GB
    for files in siteLfns.values():
      # Now get bunches of files,
      # Sort in decreasing size
      files.sort( cmp = ( lambda f1, f2: fileSizes[f2] - fileSizes[f1] ) )
      while( files ):
        # print [( lfn, fileSizes[lfn] ) for lfn in files]
        group = []
        sizeTot = 0
        for lfn in list( files ):
          size = fileSizes[lfn]
          if size >= maxSizePerJob:
            lfnGroups.append( [lfn] )
          elif sizeTot + size < maxSizePerJob:
            sizeTot += size
            group.append( lfn )
            files.remove( lfn )
        if group:
          lfnGroups.append( group )

    if printOutput:
      print self.pPrint.pformat( lfnGroups )
    return S_OK( lfnGroups )

