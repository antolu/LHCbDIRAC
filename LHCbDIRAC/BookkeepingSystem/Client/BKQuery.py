"""
BKQuery is a class that decodes BK paths, queries the BK at a high level
"""

__RCSID__ = "$Id$"

import os, sys
from DIRAC import gLogger
from DIRAC.Core.Utilities.List import sortList
from DIRAC.Core.DISET.RPCClient import RPCClient
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

class BKQuery():
  """
  It used to build a dictionary using a given Bookkeeping path
  which is used to query the Bookkeeping database.
  """
  def __init__( self, bkQuery = None, prods = None, runs = None, fileTypes = None, visible = True ):
    prods = prods if prods is not None else []
    runs = runs if runs is not None else []
    fileTypes = fileTypes if fileTypes is not None else []

    self.extraBKitems = ( "StartRun", "EndRun", "Production", "RunNumber" )
    self.__bkClient = BookkeepingClient()
    bkPath = ''
    bkQueryDict = {}
    self.__bkFileTypes = set()
    self.__exceptFileTypes = set()
    self.__fakeAllDST = 'ZZZZZZZZALL.DST'
    if isinstance( bkQuery, BKQuery ):
      bkQueryDict = bkQuery.getQueryDict().copy()
    elif type( bkQuery ) == type( {} ):
      bkQueryDict = bkQuery.copy()
    elif type( bkQuery ) == type( '' ):
      bkPath = bkQuery
    bkQueryDict = self.buildBKQuery( bkPath, bkQueryDict = bkQueryDict,
                                     prods = prods, runs = runs,
                                     fileTypes = fileTypes,
                                     visible = visible )
    self.__bkPath = bkPath
    self.__bkQueryDict = bkQueryDict
    if not bkQueryDict.get( 'Visible' ):
      self.setVisible( visible )

  def __str__( self ):
    return str( self.__bkQueryDict )

  def buildBKQuery( self, bkPath = '', bkQueryDict = None, prods = None, runs = None, fileTypes = None, visible = True ):
    """ it builds a dictionary using a path
    """
    bkQueryDict = bkQueryDict if bkQueryDict is not None else {}
    prods = prods if prods is not None else []
    runs = runs if runs is not None else []
    fileTypes = fileTypes if fileTypes is not None else []

    gLogger.verbose( "BKQUERY.buildBKQuery: Path %s, Dict %s, Prods %s, Runs %s, FileTypes %s, Visible %s" % ( bkPath,
                                                                                         str( bkQueryDict ),
                                                                                         str( prods ),
                                                                                         str( runs ),
                                                                                         str( fileTypes ),
                                                                                         visible ) )
    self.__bkQueryDict = {}
    if not bkPath and not prods and not runs and not bkQueryDict and not fileTypes:
      return {}
    if bkQueryDict:
      bkQuery = bkQueryDict.copy()
    else:
      bkQuery = {}

    ###### Query given as a path /ConfigName/ConfigVersion/ConditionDescription/ProcessingPass/EventType/FileType ######
    # or if prefixed with evt: /ConfigName/ConfigVersion/EventType/ConditionDescription/ProcessingPass/FileType
    if bkPath:
      self.__getAllBKFileTypes()
      bkFields = ( "ConfigName", "ConfigVersion", "ConditionDescription", "ProcessingPass", "EventType", "FileType" )
      url = bkPath.split( ':', 1 )
      if len( url ) == 1:
        bkPath = url[0]
      else:
        if url[0] == 'evt':
          bkFields = ( "ConfigName", "ConfigVersion",
                       "EventType", "ConditionDescription",
                       "ProcessingPass", "FileType" )
        elif url[0] == 'pp':
          bkFields = ( "ProcessingPass", "EventType", "FileType" )
        elif url[0] == 'prod':
          bkFields = ( "Production", "ProcessingPass", "EventType", "FileType" )
        elif url[0] == 'runs':
          bkFields = ( "Runs", "ProcessingPass", "EventType", "FileType" )
        elif url[0] not in ( 'sim', 'daq', 'cond' ):
          gLogger.error( 'Invalid BK path:%s' % bkPath )
          return self.__bkQueryDict
        bkPath = url[1]
      if bkPath[0] != '/':
        bkPath = '/' + bkPath
      if bkPath[0:2] == '//':
        bkPath = bkPath[1:]
      bkPath = bkPath.replace( "RealData", "Real Data" )
      i = 0
      processingPass = '/'
      defaultPP = False
      bk = bkPath.split( '/' )[1:] + len( bkFields ) * ['']
      for bpath in bk:
        gLogger.verbose( 'buildBKQuery.1. Item #%d, Field %s, From Path %s, ProcessingPass %s' % ( i,
                                                                                                   bkFields[i],
                                                                                                   bpath,
                                                                                                   processingPass ) )
        if bkFields[i] == 'ProcessingPass':
          if bpath != '' and bpath.upper() != 'ALL' and \
          not bpath.split( ',' )[0].split( ' ' )[0].isdigit() and \
          not bpath.upper() in self.__bkFileTypes:
            processingPass = os.path.join( processingPass, bpath )
            continue
          # Set the PP
          if processingPass != '/':
            bkQuery['ProcessingPass'] = processingPass
          else:
            defaultPP = True
          i += 1
        gLogger.verbose( 'buildBKQuery.2. Item #%d, Field %s, From Path %s, ProcessingPass %s' % ( i,
                                                                                                   bkFields[i],
                                                                                                   bpath,
                                                                                                   processingPass ) )
        if bkFields[i] == 'EventType' and bpath:
          eventTypes = []
          # print b
          for et in bpath.split( ',' ):
            eventTypes.append( et.split( ' ' )[0] )
          if type( eventTypes ) == type( [] ) and len( eventTypes ) == 1:
            eventTypes = eventTypes[0]
          bpath = eventTypes
          gLogger.verbose( 'buildBKQuery. Event types %s' % eventTypes )
        # Set the BK dictionary item
        if bpath != '':
          bkQuery[bkFields[i]] = bpath
        if defaultPP:
          # PP was empty, try once more to get the Event Type
          defaultPP = False
        else:
          # Go to next item
          i += 1
        if i == len( bkFields ):
          break

      gLogger.verbose( 'buildBKQuery. Query dict %s' % str( bkQuery ) )
      # Set default event type to real data
      if bkQuery.get( 'ConfigName' ) != 'MC' and not bkQuery.get( 'EventType' ):
        bkQuery['EventType'] = '90000000'

    # Run limits are given
    runs = bkQuery.get( "Runs", runs )
    if 'Runs' in bkQuery:
      bkQuery.pop( 'Runs' )
    if runs:
      if type( runs ) == type( '' ):
        runs = runs.split( ',' )
      elif type( runs ) == type( {} ):
        runs = runs.keys()
      elif type( runs ) == type( 0 ):
        runs = [str( runs )]
      if len( runs ) > 1:
        runList = []
        for run in runs:
          if run.isdigit():
            runList.append( int( run ) )
          else:
            runRange = run.split( ':' )
            if len( runRange ) == 2 and runRange[0].isdigit() and runRange[1].isdigit():
              runList += range( int( runRange[0] ), int( runRange[1] ) + 1 )
        bkQuery['RunNumber'] = runList
      else:
        runs = runs[0].split( ':' )
        if len( runs ) == 1:
          runs = runs[0].split( '-' )
          if len( runs ) == 1:
            bkQuery['RunNumber'] = int( runs[0] )
      if 'RunNumber' not in bkQuery:
        try:
          if runs[0] and runs[1] and int( runs[0] ) > int( runs[1] ):
            gLogger.warn( 'Warning: End run should be larger than start run: %d, %d' % ( int( runs[0] ),
                                                                                         int( runs[1] ) ) )
            return self.__bkQueryDict
          if runs[0].isdigit():
            bkQuery['StartRun'] = int( runs[0] )
          if runs[1].isdigit():
            bkQuery['EndRun'] = int( runs[1] )
        except IndexError, ex:  # The runs must be a list
          gLogger.warn( ex )
          print runs, 'is an invalid run range'
          return self.__bkQueryDict
      else:
        if 'StartRun' in bkQuery:
          bkQuery.pop( 'StartRun' )
        if 'EndRun' in bkQuery:
          bkQuery.pop( 'EndRun' )

    ###### Query given as a list of production ######
    if prods and str( prods[0] ).upper() != 'ALL':
      try:
        bkQuery.setdefault( 'Production', [] ).extend( [int( prod ) for prod in prods] )
      except ValueError, ex:  # The prods list does not contains numbers
        gLogger.warn( ex )
        print prods, 'invalid as production list'
        return self.__bkQueryDict

    # Set the file type(s) taking into account excludes file types
    fileTypes = bkQuery.get( 'FileType', fileTypes )
    if 'FileType' in bkQuery:
      bkQuery.pop( 'FileType' )
    self.__bkQueryDict = bkQuery.copy()
    fileType = self.__fileType( fileTypes )
    # print fileType
    if fileType:
      bkQuery['FileType'] = fileType

    # Remove all "ALL"'s in the dict, if any
    for i in self.__bkQueryDict:
      if type( bkQuery[i] ) == type( '' ) and bkQuery[i].upper() == 'ALL':
        bkQuery.pop( i )

    self.__bkQueryDict = bkQuery.copy()
    self.setVisible( visible )

    # Set both event type entries
    # print "Before setEventType",self.__bkQueryDict
    if not self.setEventType( bkQuery.get( 'EventType' ) ):
      self.__bkQueryDict = {}
      return self.__bkQueryDict
    # Set conditions
    # print "Before setConditions",self.__bkQueryDict
    self.setConditions( bkQuery.get( 'ConditionDescription',
                                     bkQuery.get( 'DataTakingConditions',
                                                  bkQuery.get( 'SimulationConditions' )
                                                  )
                                    ) )
    # print self.__bkQueryDict
    return self.__bkQueryDict

  def setOption( self, key, val ):
    """
    It insert an item to the dictionary. The key is an bookkeeping attribute (condition).
    """
    if val:
      self.__bkQueryDict[key] = val
    elif key in self.__bkQueryDict:
      self.__bkQueryDict.pop( key )
    return self.__bkQueryDict

  def setConditions( self, cond = None ):
    """ Set the dictionary items for a given condition, or remove it (cond=None) """
    if 'ConfigName' not in self.__bkQueryDict and cond:
      gLogger.warn( "Impossible to set Conditions to a BK Query without Configuration" )
      return self.__bkQueryDict
    # There are two items in the dictionary: ConditionDescription and Simulation/DataTaking-Conditions
    eventType = self.__bkQueryDict.get( 'EventType', 'ALL' )
    if self.__bkQueryDict.get( 'ConfigName' ) == 'MC' or \
    ( type( eventType ) == type( '' ) and eventType.upper() != 'ALL' and \
      eventType[0] != '9' ):
      conditionsKey = 'SimulationConditions'
    else:
      conditionsKey = 'DataTakingConditions'
    self.setOption( 'ConditionDescription', cond )
    return self.setOption( conditionsKey, cond )

  def setFileType( self, fileTypes = None ):
    """insert the file type to the Boookkeeping dictionary
    """
    return self.setOption( 'FileType', self.__fileType( fileTypes ) )

  def setDQFlag( self, dqFlag = 'OK' ):
    """
    Sets the data quality.
    """
    if type( dqFlag ) == type( '' ):
      dqFlag = dqFlag.upper()
    elif type( dqFlag ) == type( [] ):
      dqFlag = [dq.upper() for dq in dqFlag]
    return self.setOption( 'DataQuality', dqFlag )

  def setStartDate( self, startDate ):
    """
    Sets the start date.
    """
    return self.setOption( 'StartDate', startDate )

  def setEndDate( self, endDate ):
    """
    Sets the end date
    """
    return self.setOption( 'EndDate', endDate )

  def setProcessingPass( self, processingPass ):
    """
    Sets the processing pass
    """
    return self.setOption( 'ProcessingPass', processingPass )

  def setEventType( self, eventTypes = None ):
    """
    Sets the event type
    """
    if eventTypes:
      if type( eventTypes ) == type( '' ):
        eventTypes = eventTypes.split( ',' )
      elif type( eventTypes ) != type( [] ):
        eventTypes = [eventTypes]
      try:
        eventTypes = [str( int( et ) ) for et in eventTypes]
      except ValueError, ex:
        gLogger.warn( ex )
        print eventTypes, 'invalid as list of event types'
        return {}
      if type( eventTypes ) == type( [] ) and len( eventTypes ) == 1:
        eventTypes = eventTypes[0]
    return self.setOption( 'EventType', eventTypes )

  def setVisible( self, visible = None ):
    """
    Sets the visibility flag
    """
    if visible == True or ( type( visible ) == type( '' ) and visible[0].lower() == 'y' ):
      visible = 'Yes'
    if visible == False:
      visible = 'No'
    return self.setOption( 'Visible', visible )

  def setExceptFileTypes( self, fileTypes ):
    """
    Sets the expected file types
    """
    if type( fileTypes ) != type( [] ):
      fileTypes = [fileTypes]
    self.__exceptFileTypes.update( fileTypes )
    self.setFileType( [t for t in self.getFileTypeList() if t not in fileTypes] )

  def getQueryDict( self ):
    """
    Returns the bookkeeping dictionary
    """
    return self.__bkQueryDict

  def getPath( self ):
    """
    Returns the Bookkeeping path
    """
    return self.__bkPath

  def getFileTypeList( self ):
    """
    Returns the file types
    """
    fileTypes = self.__bkQueryDict.get( 'FileType', [] )
    if type( fileTypes ) != type( [] ):
      fileTypes = [fileTypes]
    return fileTypes

  def getEventTypeList( self ):
    """
    Returns the event types
    """
    eventType = self.__bkQueryDict.get( "EventType", [] )
    if eventType:
      if type( eventType ) != type( [] ):
        eventType = [eventType]
    return eventType

  def getProcessingPass( self ):
    """
    Returns the processing pass
    """
    return self.__bkQueryDict.get( 'ProcessingPass', '' )

  def getConditions( self ):
    """
    Returns the Simulation/data taking conditions
    """
    return self.__bkQueryDict.get( 'ConditionDescription', '' )

  def getConfiguration( self ):
    """
    Returns the configuration name and configuration version
    """
    configName = self.__bkQueryDict.get( 'ConfigName', '' )
    configVersion = self.__bkQueryDict.get( 'ConfigVersion', '' )
    if not configName or not configVersion:
      return ''
    return os.path.join( '/', configName, configVersion )

  def isVisible( self ):
    """
    Returns True/False depending on the visibility flag
    """
    return self.__bkQueryDict.get( 'Visible', 'All' )

  def __fileType( self, fileType = None, returnList = False ):
    """
    return the file types taking into account the expected file types
    """
    gLogger.verbose( "BKQuery.__fileType: %s, fileType: %s" % ( self, fileType ) )
    if not fileType:
      return []
    self.__getAllBKFileTypes()
    if type( fileType ) == type( [] ):
      fileTypes = fileType
    else:
      fileTypes = fileType.split( ',' )
    allRequested = None
    if fileTypes[0].lower() == "all":
      allRequested = True
      bkTypes = self.getBKFileTypes()
      gLogger.verbose( 'BKQuery.__fileType: bkTypes %s' % str( bkTypes ) )
      if bkTypes:
        fileTypes = list( set( bkTypes ) - self.__exceptFileTypes )
      else:
        fileTypes = []
    expandedTypes = set()
    # print "Requested", fileTypes
    for fileType in fileTypes:
      if fileType.lower() == 'all.hist':
        allRequested = False
        expandedTypes.update( [t for t in self.__exceptFileTypes.union( self.__bkFileTypes )
                              if t.endswith( 'HIST' )] )
      elif fileType.lower().find( "all." ) == 0:
        ext = '.' + fileType.split( '.' )[1]
        fileType = []
        if allRequested == None:
          allRequested = True
        expandedTypes.update( [t for t in set( self.getBKFileTypes() ) - self.__exceptFileTypes
                              if t.endswith( ext )] )
      else:
        expandedTypes.add( fileType )
    # Remove __exceptFileTypes only if not explicitly required
    # print "Obtained", fileTypes, expandedTypes
    gLogger.verbose( "BKQuery.__fileType: requested %s, expanded %s, except %s" % ( allRequested,
                                                                                    expandedTypes,
                                                                                    self.__exceptFileTypes ) )
    if allRequested or not expandedTypes & self.__exceptFileTypes :
      expandedTypes = ( expandedTypes & self.__bkFileTypes ) - self.__exceptFileTypes
    gLogger.verbose( "BKQuery.__fileType: result %s" % sorted( expandedTypes ) )
    if len( expandedTypes ) == 1 and not returnList:
      return list( expandedTypes )[0]
    else:
      return list( expandedTypes )

  def __getAllBKFileTypes( self ):
    """
    Returns the file types from the bookkeeping database
    """
    if not self.__bkFileTypes:
      self.__bkFileTypes = set( [self.__fakeAllDST] )
      res = self.__bkClient.getAvailableFileTypes()
      if res['OK']:
        dbresult = res['Value']
        for record in dbresult['Records']:
          if record[0].endswith( 'HIST' ) or record[0].endswith( 'ETC' ) or record[0] == 'LOG' or record[0].endswith( 'ROOT' ):
            self.__exceptFileTypes.add( record[0] )
          else:
            self.__bkFileTypes.add( record[0] )

  def getLFNsAndSize( self ):
    """
    Returns the LFNs and their size for a given data set
    """
    self.__getAllBKFileTypes()
    res = self.__bkClient.getFiles( self.__bkQueryDict )
    lfns = []
    lfnSize = 0
    if not res['OK']:
      print "***** ERROR ***** Error getting dataset from BK for %s:" % self.__bkQueryDict, res['Message']
    else:
      lfns = res['Value']
      exceptFiles = list( self.__exceptFileTypes )
      if exceptFiles and not self.__bkQueryDict.get( 'FileType' ):
        res = self.__bkClient.getFiles( BKQuery( self.__bkQueryDict ).setOption( 'FileType', exceptFiles ) )
        if res['OK']:
          lfnsExcept = [lfn for lfn in res['Value'] if lfn in lfns]
        else:
          print "***** ERROR ***** Error in getting dataset from BK for %s files:" % exceptFiles, res['Message']
          lfnsExcept = []
        if lfnsExcept:
          print "***** WARNING ***** Found %d files in BK query that will be \
          excluded (file type in %s)!" % ( len( lfnsExcept ), str( exceptFiles ) )
          print "                    If creating a transformation, set '--FileType ALL'"
          lfns = [lfn for lfn in lfns if lfn not in lfnsExcept]
        else:
          exceptFiles = False
      query = BKQuery( self.__bkQueryDict )
      query.setOption( "FileSize", True )
      res = self.__bkClient.getFiles( query.getQueryDict() )
      if res['OK'] and type( res['Value'] ) == type( [] ) and res['Value'][0]:
        lfnSize = res['Value'][0]
      if exceptFiles and not self.__bkQueryDict.get( 'FileType' ):
        res = self.__bkClient.getFiles( query.setOption( 'FileType', exceptFiles ) )
        if res['OK'] and type( res['Value'] ) == type( [] ) and res['Value'][0]:
          lfnSize -= res['Value'][0]

      lfnSize /= 1000000000000.
    return { 'LFNs' : lfns, 'LFNSize' : lfnSize }

  def getLFNSize( self, visible = None ):
    """
    Returns the size of a  given data set
    """
    if visible == None:
      visible = self.isVisible()
    res = self.__bkClient.getFiles( BKQuery( self.__bkQueryDict, visible = visible ).setOption( 'FileSize', True ) )
    if res['OK'] and type( res['Value'] ) == type( [] ) and res['Value'][0]:
      lfnSize = res['Value'][0]
    else:
      lfnSize = 0
    return lfnSize

  def getNumberOfLFNs( self, visible = None ):
    """
    Returns the number of LFNs correspond to a given data set
    """
    if visible == None:
      visible = self.isVisible()
    if  self.isVisible() != visible:
      query = BKQuery( self.__bkQueryDict, visible = visible )
    else:
      query = self
    fileTypes = query.getFileTypeList()
    nbFiles = 0
    size = 0
    for fileType in fileTypes:
      if fileType:
        res = self.__bkClient.getFilesSummary( query.setFileType( fileType ) )
        # print query, res
        if res['OK']:
          res = res['Value']
          ind = res['ParameterNames'].index( 'NbofFiles' )
          if res['Records'][0][ind]:
            nbFiles += res['Records'][0][ind]
            ind1 = res['ParameterNames'].index( 'FileSize' )
            size += res['Records'][0][ind1]
            # print 'Visible',query.isVisible(),fileType, 'Files:',
            # res['Records'][0][ind], 'Size:', res['Records'][0][ind1]
    return { 'NumberOfLFNs' : nbFiles, 'LFNSize': size }

  def getLFNs( self, printSEUsage = False, printOutput = True, visible = None ):
    """
    returns a list of lfns. It prints statistics about the data sets if it is requested.
    """
    if visible == None:
      visible = self.isVisible()

    prods = self.__bkQueryDict.get( 'Production' )
    if self.isVisible() != visible:
      query = BKQuery( self.__bkQueryDict, visible = visible )
    else:
      query = self
    if prods and type( prods ) == type( [] ):
      # It's faster to loop on a list of prods than query the BK with a list as argument
      lfns = []
      lfnSize = 0
      if query == self:
        query = BKQuery( self.__bkQueryDict, visible = visible )
      for prod in prods:
        query.setOption( 'Production', prod )
        lfnsAndSize = query.getLFNsAndSize()
        lfns += lfnsAndSize['LFNs']
        lfnSize += lfnsAndSize['LFNSize']
    else:
      lfnsAndSize = query.getLFNsAndSize()
      lfns = lfnsAndSize['LFNs']
      lfnSize = lfnsAndSize['LFNSize']
    if len( lfns ) == 0:
      gLogger.verbose( "No files found for BK query %s" % str( self.__bkQueryDict ) )
    lfns.sort()

    # Only for printing
    if lfns and printOutput:
      print "\n%d files (%.1f TB) in directories:" % ( len( lfns ), lfnSize )
      dirs = {}
      for lfn in lfns:
        directory = os.path.dirname( lfn )
        dirs[directory] = dirs.setdefault( directory, 0 ) + 1
      dirSorted = dirs.keys()
      dirSorted.sort()
      for directory in dirSorted:
        print directory, dirs[directory], "files"
      if printSEUsage:
        rpc = RPCClient( 'DataManagement/StorageUsage' )
        totalUsage = {}
        totalSize = 0
        for directory in dirs.keys():
          res = rpc.getStorageSummary( directory, '', '', [] )
          if res['OK']:
            for se in [se for se in res['Value'].keys() if not se.endswith( "-ARCHIVE" )]:
              if not totalUsage.has_key( se ):
                totalUsage[se] = 0
              totalUsage[se] += res['Value'][se]['Size']
              totalSize += res['Value'][se]['Size']
        ses = totalUsage.keys()
        ses.sort()
        totalUsage['Total'] = totalSize
        ses.append( 'Total' )
        print "\n%s %s" % ( "SE".ljust( 20 ), "Size (TB)" )
        for se in ses:
          print "%s %s" % ( se.ljust( 20 ), ( '%.1f' % ( totalUsage[se] / 1000000000000. ) ) )
    return lfns

  def getDirs( self, printOutput = False, visible = None ):
    """
    Returns the directories
    """
    if visible == None:
      visible = self.isVisible()
    lfns = self.getLFNs( printSEUsage = True, printOutput = printOutput, visible = visible )
    dirs = []
    for lfn in lfns:
      directory = os.path.dirname( lfn )
      if directory not in dirs:
        dirs.append( directory )
    dirs.sort()
    return dirs

  @staticmethod
  def __getProdStatus( prod ):
    """
    Returns the status of a given transformation
    """
    res = TransformationClient().getTransformation( prod, extraParams = False )
    if not res['OK']:
      gLogger.error( "Couldn't get information on production %d" % prod )
      return None
    return res['Value']['Status']

  def getBKRuns( self ):
    """
    It returns a list of runs from the bookkeeping.
    """
    if self.getProcessingPass().replace( '/', '' ) == 'Real Data':
      return self.getBKProductions()

  def getBKProductions( self, visible = None ):
    """
    It returns a list of productions
    """
    if visible == None:
      visible = self.isVisible()
    prodList = self.__bkQueryDict.get( 'Production' )
    if prodList:
      if type( prodList ) != type( [] ):
        prodList = [prodList]
      return sortList( prodList )
    res = self.__bkClient.getProductions( BKQuery( self.__bkQueryDict ).setVisible( visible ) )
    if not res['OK']:
      gLogger.error( 'Error getting productions from BK', res['Message'] )
      return []
    transClient = TransformationClient()
    if self.getProcessingPass().replace( '/', '' ) != 'Real Data':
      fileTypes = self.getFileTypeList()
      prodList = [prod for p in res['Value']['Records'] for prod in p
                  if self.__getProdStatus( prod ) not in ( 'Deleted' )]
      # print '\n', self.__bkQueryDict, res['Value']['Records'], '\nVisible:', visible, prodList
      pList = []
      if fileTypes:
        for prod in prodList:
          res = transClient.getBookkeepingQueryForTransformation( prod )
          if res['OK'] and res['Value']['FileType'] in fileTypes:
            if type( prod ) != type( [] ):
              prod = [prod]
            pList += [p for p in prod if p not in pList]
      if not pList:
        pList = prodList
    else:
      pList = [-run for r in res['Value']['Records'] for run in r]
      pList.sort()
      startRun = int( self.__bkQueryDict.get( 'StartRun', 0 ) )
      endRun = int( self.__bkQueryDict.get( 'EndRun', sys.maxint ) )
      pList = [run for run in pList if run >= startRun and run <= endRun]
    return sorted( pList )

  def getBKConditions( self ):
    """
    It returns the data taking / simulation condtions
    """
    conditions = self.__bkQueryDict.get( 'ConditionDescription' )
    if conditions:
      if type( conditions ) != type( [] ):
        conditions = [conditions]
      return conditions
    res = self.__bkClient.getConditions( self.__bkQueryDict )
    if res['OK']:
      res = res['Value']
    else:
      return []
    conditions = []
    for i in res:
      ind = i['ParameterNames'].index( 'Description' )
      if i['Records']:
        conditions += [p[ind] for p in i['Records']]
        break
    return sortList( conditions )

  def getBKEventTypes( self ):
    """
    It returns the event types
    """
    eventType = self.getEventTypeList()
    if eventType:
      return eventType
    res = self.__bkClient.getEventTypes( self.__bkQueryDict )['Value']
    ind = res['ParameterNames'].index( 'EventType' )
    eventTypes = sortList( [rec[ind] for rec in res['Records']] )
    return eventTypes

  def getBKFileTypes( self, bkDict = None ):
    """
    It returns the file types.
    """
    fileTypes = self.getFileTypeList()
    # print "Call getBKFileTypes:", self, fileTypes
    if not fileTypes:
      if not bkDict:
        bkDict = self.__bkQueryDict.copy()
      else:
        bkDict = bkDict.copy()
      bkDict.setdefault( 'Visible', 'All' )
      bkDict.pop( 'RunNumber', None )
      fileTypes = []
      eventTypes = bkDict.get( 'EventType' )
      if type( eventTypes ) == type( [] ):
        for et in eventTypes:
          bkDict['EventType'] = et
          fileTypes += self.getBKFileTypes( bkDict )
      else:
        res = self.__bkClient.getFileTypes( bkDict )
        if res['OK']:
          res = res['Value']
          ind = res['ParameterNames'].index( 'FileTypes' )
          fileTypes = [rec[ind] for rec in res['Records'] if rec[ind] not in self.__exceptFileTypes]
    if 'ALL.DST' in fileTypes:
      fileTypes.remove( 'ALL.DST' )
      fileTypes.append( self.__fakeAllDST )
    # print 'FileTypes1', fileTypes
    fileTypes = self.__fileType( fileTypes, returnList = True )
    # print 'FileTypes2', fileTypes
    if self.__fakeAllDST in fileTypes:
      fileTypes.remove( self.__fakeAllDST )
      fileTypes.append( 'ALL.DST' )
    # print 'FileTypes3', fileTypes
    return fileTypes

  def getBKProcessingPasses( self, queryDict = None ):
    """
    It returns the processing pass.
    """
    processingPasses = {}
    if not queryDict:
      queryDict = self.__bkQueryDict.copy()
    initialPP = queryDict.get( 'ProcessingPass', '/' )
    # print "Start", initialPP, queryDict
    res = self.__bkClient.getProcessingPass( queryDict, initialPP )
    if not res['OK']:
      if 'Empty Directory' not in res['Message']:
        print "ERROR getting processing passes for %s" % queryDict, res['Message']
      return {}
    ppRecords = res['Value'][0]
    if 'Name' in ppRecords['ParameterNames']:
      ind = ppRecords['ParameterNames'].index( 'Name' )
      passes = sorted( [os.path.join( initialPP, rec[ind] ) for rec in ppRecords['Records']] )
    else:
      passes = []
    evtRecords = res['Value'][1]
    if 'EventType' in evtRecords['ParameterNames']:
      ind = evtRecords['ParameterNames'].index( 'EventType' )
      eventTypes = [str( rec[ind] ) for rec in evtRecords['Records']]
    else:
      eventTypes = []

    if passes:
      nextProcessingPasses = {}
      for pName in passes:
        processingPasses[pName] = []
        queryDict['ProcessingPass'] = pName
        nextProcessingPasses.update( self.getBKProcessingPasses( queryDict ) )
      processingPasses.update( nextProcessingPasses )
    if eventTypes:
      processingPasses[initialPP] = eventTypes
    for pName in ( '/Real Data', '/' ):
      if pName in processingPasses:
        processingPasses.pop( pName )
    # print "End", initialPP, [( key, processingPasses[key] ) for key in sortList( processingPasses.keys() )]
    return processingPasses

  def browseBK( self ):
    """
    It builds the bookkeeping dictionary
    """
    from fnmatch import fnmatch
    configuration = self.getConfiguration()
    conditions = self.getBKConditions()
    # print conditions
    bkTree = {configuration : {}}
    requestedEventTypes = self.getEventTypeList()
    # requestedFileTypes = self.getFileTypeList()
    requestedPP = self.getProcessingPass()
    matchLength = 0
    if '...' in requestedPP:
      pp = requestedPP.split( '/' )
      initialPP = '/'
      for p in pp[1:]:
        if '...' not in p:
          initialPP = os.path.join( initialPP, p )
        else:
          break
      self.setProcessingPass( initialPP )
      requestedPP = requestedPP.replace( '...', '*' )
      if requestedPP.endswith( '*' ) and not requestedPP.endswith( '/*' ):
        matchLength = len( requestedPP.split( '/' ) )
    else:
      initialPP = requestedPP
    requestedConditions = self.getConditions()
    # print initialPP, requestedPP, matchLength
    for cond in conditions:
      self.setConditions( cond )
      processingPasses = self.getBKProcessingPasses()
      # print processingPasses
      for processingPass in [pp for pp in processingPasses if processingPasses[pp]]:
        # print processingPass
        if initialPP != requestedPP and not fnmatch( processingPass, requestedPP ):
          continue
        if matchLength and len( processingPass.split( '/' ) ) != matchLength:
          continue
        # print 'Matched!'
        if requestedEventTypes:
          eventTypes = [t for t in requestedEventTypes if t in processingPasses[processingPass]]
          if not eventTypes:
            continue
        else:
          eventTypes = processingPasses[processingPass]
        self.setProcessingPass( processingPass )
        # print eventTypes
        for eventType in eventTypes:
          self.setEventType( eventType )
          fileTypes = self.getBKFileTypes()
          bkTree[configuration].setdefault( cond, {} ).setdefault( processingPass, {} )[int( eventType )] = fileTypes
        self.setEventType( requestedEventTypes )
      self.setProcessingPass( initialPP )
    self.setConditions( requestedConditions )
    return bkTree
