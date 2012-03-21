"""
BKQuery is a class that decodes BK paths, queries the BK at a high level
"""

__RCSID__ = "$Id: BKQuery.py 42387 2011-09-07 13:53:37Z phicharp $"

import os, sys
import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Utilities.List                                         import sortList

class BKQuery():

  def __init__( self, bkQuery=None, prods=[], runs=[], fileTypes=[], visible=True ):
    from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient  import BookkeepingClient
    self.extraBKitems = ( "StartRun", "EndRun", "ProductionID", "RunNumbers" )
    self.bk = BookkeepingClient()
    bkPath = ''
    bkQueryDict = {}
    self.bkFileTypes = []
    self.exceptFileTypes = []
    if isinstance( bkQuery, BKQuery ):
      bkQueryDict = bkQuery.getQueryDict().copy()
    elif type( bkQuery ) == type( {} ):
      bkQueryDict = bkQuery.copy()
    elif type( bkQuery ) == type( '' ):
      bkPath = bkQuery
    bkQueryDict = self.buildBKQuery( bkPath, bkQueryDict=bkQueryDict, prods=prods, runs=runs, fileTypes=fileTypes, visible=visible )
    self.bkPath = bkPath
    self.bkQueryDict = bkQueryDict
    if not bkQueryDict.get( 'Visible' ):
      self.setVisible( visible )

  def __str__( self ):
    return str( self.bkQueryDict )

  def buildBKQuery( self, bkPath='', bkQueryDict={}, prods=[], runs=[], fileTypes=[], visible=True ):
    self.bkQueryDict = {}
    if not bkPath and not prods and not runs and not bkQueryDict:
      return {}
    if bkQueryDict:
      bkQuery = bkQueryDict.copy()
    else:
      bkQuery = {}

    if visible:
      bkQuery['Visible'] = 'Yes'

    ###### Query given as a path /ConfigName/ConfigVersion/ConditionDescription/ProcessingPass/EventType/FileType ######
    # or if prefixed with evt: /ConfigName/ConfigVersion/EventType/ConditionDescription/ProcessingPass/FileType
    if bkPath:
      self.__getAllBKFileTypes()
      bkFields = ( "ConfigName", "ConfigVersion", "ConditionDescription", "ProcessingPass", "EventTypeId", "FileType" )
      url = bkPath.split( ':', 1 )
      if len( url ) == 1:
        bkPath = url[0]
      else:
        if url[0] == 'evt':
          bkFields = ( "ConfigName", "ConfigVersion", "EventTypeId", "ConditionDescription", "ProcessingPass", "FileType" )
        elif url[0] == 'pp':
          bkFields = ( "ProcessingPass", "EventTypeId", "FileType" )
        elif url[0] == 'prod':
          bkFields = ( "ProductionID", "ProcessingPass", "EventTypeId", "FileType" )
        elif url[0] == 'runs':
          bkFields = ( "Runs", "ProcessingPass", "EventTypeId", "FileType" )
        elif url[0] not in ( 'sim', 'daq', 'cond' ):
          print 'Invalid BK path:', bkPath
          return self.bkQueryDict
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
      for b in bk:
        #print i,bkFields[i], b, processingPass
        if bkFields[i] == 'ProcessingPass':
          if b != '' and b.upper() != 'ALL' and not b.split( ',' )[0].split( ' ' )[0].isdigit() and not b.upper() in self.bkFileTypes:
            processingPass = os.path.join( processingPass, b )
            continue
          # Set the PP
          if processingPass != '/':
            bkQuery['ProcessingPass'] = processingPass
          else:
            defaultPP = True
          i += 1
        #print i,bkFields[i], b, processingPass
        if bkFields[i] == 'EventTypeId' and b:
          eventTypes = []
          #print b
          for et in b.split( ',' ):
            eventTypes.append( et.split( ' ' )[0] )
          if type( eventTypes ) == type( [] ) and len( eventTypes ) == 1:
            eventTypes = eventTypes[0]
          b = eventTypes
          #print eventTypes
        # Set the BK dictionary item
        if b != '':
          bkQuery[bkFields[i]] = b
        if defaultPP:
          # PP was empty, try once more to get the Event Type
          defaultPP = False
        else:
          # Go to next item
          i += 1
        if i == len( bkFields ):
          break

      #print bkQuery
      # Set default event type to real data
      if bkQuery.get( 'ConfigName' ) != 'MC' and not bkQuery.get( 'EventTypeId' ):
        bkQuery['EventTypeId'] = '90000000'

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
          bkQuery['RunNumbers'] = runList
      else:
        runs = runs[0].split( ':' )
        if len( runs ) == 1:
          runs = runs[0].split( '-' )
          if len( runs ) == 1:
            runs.append( runs[0] )
      if 'RunNumbers' not in bkQuery:
        try:
          if runs[0] and runs[1] and int( runs[0] ) > int( runs[1] ):
            print 'Warning: End run should be larger than start run:', runs[0], runs[1]
            return self.bkQueryDict
          if runs[0].isdigit():
            bkQuery['StartRun'] = int( runs[0] )
          if runs[1].isdigit():
            bkQuery['EndRun'] = int( runs[1] )
        except:
          print runs, 'is an invalid run range'
          return self.bkQueryDict
      else:
        if 'StartRun' in bkQuery: bkQuery.pop( 'StartRun' )
        if 'EndRun' in bkQuery: bkQuery.pop( 'EndRun' )

    ###### Query given as a list of production ######
    if prods and str( prods[0] ).upper() != 'ALL':
      try:
        bkQuery.setdefault( 'ProductionID', [] ).extend( [int( prod ) for prod in prods] )
      except:
        print prods, 'invalid as production list'
        return self.bkQueryDict

    # Set the file type(s) taking into account excludes file types
    fileTypes = bkQuery.get( 'FileType', fileTypes )
    if 'FileType' in bkQuery:
      bkQuery.pop( 'FileType' )
    self.bkQueryDict = bkQuery.copy()
    fileType = self.__fileType( fileTypes )
    if fileType:
      bkQuery['FileType'] = fileType

    # Remove all "ALL"'s in the dict, if any
    for i in self.bkQueryDict:
      if type( bkQuery[i] ) == type( '' ) and bkQuery[i].upper() == 'ALL':
        bkQuery.pop( i )

    self.bkQueryDict = bkQuery.copy()
    # Set both event type entries
    #print "Before setEventType",self.bkQueryDict
    if not self.setEventType( bkQuery.get( 'EventTypeId', bkQuery.get( 'EventType' ) ) ):
      self.bkQueryDict = {}
      return self.bkQueryDict
    # Set conditions
    #print "Before setConditions",self.bkQueryDict
    self.setConditions( bkQuery.get( 'ConditionDescription', bkQuery.get( 'DataTakingConditions', bkQuery.get( 'SimulationConditions' ) ) ) )
    return self.bkQueryDict

  def setOption( self, key, val ):
    if val:
      self.bkQueryDict[key] = val
    elif key in self.bkQueryDict:
      self.bkQueryDict.pop( key )
    return self.bkQueryDict

  def setConditions( self, cond=None ):
    """ Set the dictionary items for a given condition, or remove it (cond=None) """
    if 'ConfigName' not in self.bkQueryDict and cond:
      gLogger.warn( "Impossible to set Conditions to a BK Query without Configuration" )
      return self.bkQueryDict
    # There are two items in the dictionary: ConditionDescription and Simulation/DataTaking-Conditions
    eventType = self.bkQueryDict.get( 'EventTypeId', 'ALL' )
    if self.bkQueryDict.get( 'ConfigName' ) == 'MC' or ( type( eventType ) == type( '' ) and eventType.upper() != 'ALL' and eventType[0] != '9' ):
      conditionsKey = 'SimulationConditions'
    else:
      conditionsKey = 'DataTakingConditions'
    self.setOption( 'ConditionDescription', cond )
    return self.setOption( conditionsKey, cond )

  def setFileType( self, fileTypes=None ):
    return self.setOption( 'FileType', self.__fileType( fileTypes ) )

  def setDQFlag( self, dqFlag='OK' ):
    if type( dqFlag ) == type( '' ):
      dqFlag = dqFlag.upper()
    elif type( dqFlag ) == type( [] ):
      dqFlag = [dq.upper() for dq in dqFlag]
    self.setOption( 'Quality', dqFlag )
    return self.setOption( 'DataQualityFlag', dqFlag )

  def setStartDate( self, startDate ):
    return self.setOption( 'StartDate', startDate )

  def setEndDate( self, endDate ):
    return self.setOption( 'EndDate', endDate )

  def setProcessingPass( self, processingPass ):
    return self.setOption( 'ProcessingPass', processingPass )

  def setEventType( self, eventTypes=None ):
    if eventTypes:
      if type( eventTypes ) == type( '' ):
        eventTypes = eventTypes.split( ',' )
      try:
        eventTypes = [str( int( et ) ) for et in eventTypes]
      except:
        print eventTypes, 'invalid as list of event types'
        return {}
      if type( eventTypes ) == type( [] ) and len( eventTypes ) == 1:
        eventTypes = eventTypes[0]
    self.setOption( 'EventType', eventTypes )
    return self.setOption( 'EventTypeId', eventTypes )

  def setVisible( self, visible=True ):
    if visible:
      visible = 'Yes'
    return self.setOption( 'Visible', visible )

  def setExceptFileTypes( self, fileTypes ):
    if type( fileTypes ) != type( [] ):
      fileTypes = [fileTypes]
    self.exceptFileTypes += fileTypes
    self.setFileType( [t for t in self.getFileTypeList() if t not in self.exceptFileTypes] )

  def getQueryDict( self ):
    return self.bkQueryDict

  def getPath( self ):
    return self.bkPath

  def getFileTypeList( self ):
    fileTypes = self.bkQueryDict.get( 'FileType', [] )
    if type( fileTypes ) != type( [] ):
      fileTypes = [fileTypes]
    return fileTypes

  def getEventTypeList( self ):
    eventType = self.bkQueryDict.get( "EventTypeId", [] )
    if eventType:
      if type( eventType ) != type( [] ):
        eventType = [eventType]
    return eventType

  def getProcessingPass( self ):
    return self.bkQueryDict.get( 'ProcessingPass', '' )

  def getConditions( self ):
    return self.bkQueryDict.get( 'ConditionDescription', '' )

  def getConfiguration( self ):
    configName = self.bkQueryDict.get( 'ConfigName', '' )
    configVersion = self.bkQueryDict.get( 'ConfigVersion', '' )
    if not configName or not configVersion:
      return ''
    return os.path.join( '/', configName, configVersion )

  def isVisible( self ):
    return self.bkQueryDict.get( 'Visible', 'No' ) == 'Yes'

  def __fileType( self, fileType=None, returnList=False ):
    #print "Call __fileType:", self, fileType
    if not fileType:
        return []
    self.__getAllBKFileTypes()
    if type( fileType ) == type( [] ):
      fileTypes = fileType
    else:
      fileTypes = fileType.split( ',' )
    allRequested = False
    if fileTypes[0].lower() == "all":
      allRequested = True
      bkTypes = self.getBKFileTypes()
      #print 'bkTypes:',bkTypes
      if bkTypes:
        fileTypes = [t for t in bkTypes if t not in self.exceptFileTypes]
      else:
        fileTypes = []
    expandedTypes = []
    for fileType in fileTypes:
      if fileType.lower().find( "all." ) == 0:
        ext = '.' + fileType.split( '.' )[1]
        fileType = []
        allRequested = True
        expandedTypes += [t for t in self.getBKFileTypes() if t.endswith( ext ) and t not in self.exceptFileTypes]
      else:
        expandedTypes.append( fileType )
    # Remove exceptFileTypes only if not explicitly required
    #print allRequested,expandedTypes,self.exceptFileTypes
    if allRequested or not [t for t in self.exceptFileTypes if t in expandedTypes]:
      expandedTypes = [t for t in expandedTypes if t not in self.exceptFileTypes and t in self.bkFileTypes]
    if len( expandedTypes ) == 1 and not returnList:
      return expandedTypes[0]
    else:
      return expandedTypes

  def __getAllBKFileTypes( self ):
    if not self.bkFileTypes:
      res = self.bk.getAvailableFileTypes()
      if res['OK']:
        dbresult = res['Value']
        for record in dbresult['Records']:
          if record[0].endswith( 'HIST' ) or record[0].endswith( 'ETC' ) or record[0] == 'LOG':
            self.exceptFileTypes.append( record[0] )
          else:
            self.bkFileTypes.append( record[0] )

  def getLFNsAndSize( self ):
    self.__getAllBKFileTypes()
    res = self.bk.getFilesWithGivenDataSets( self.bkQueryDict )
    lfns = []
    lfnSize = 0
    if res['OK']:
      lfns = res['Value']
      exceptFiles = self.exceptFileTypes
      if exceptFiles and not self.bkQueryDict.get( 'FileType' ):
        res = self.bk.getFilesWithGivenDataSets( BKQuery( self.bkQueryDict ).setOption( 'FileType', exceptFiles ) )
        if res['OK']:
          lfnsExcept = [lfn for lfn in res['Value'] if lfn in lfns]
        if lfnsExcept:
          print "***** WARNING ***** Found %d files in BK query that will be excluded (file type in %s)!" % ( len( lfnsExcept ), str( exceptFiles ) )
          print "                    If creating a transformation, set '--FileType ALL'"
          lfns = [lfn for lfn in lfns if lfn not in lfnsExcept]
        else:
          exceptFiles = False
      query = BKQuery( self.bkQueryDict )
      query.setOption( "FileSize", True )
      res = self.bk.getFilesWithGivenDataSets( query.getQueryDict() )
      if res['OK'] and type( res['Value'] ) == type( [] ) and res['Value'][0]:
        lfnSize = res['Value'][0]
      if exceptFiles and not self.bkQueryDict.get( 'FileType' ):
        res = self.bk.getFilesWithGivenDataSets( query.setOption( 'FileType', exceptFiles ) )
        if res['OK'] and type( res['Value'] ) == type( [] ) and res['Value'][0]:
          lfnSize -= res['Value'][0]

      lfnSize /= 1000000000000.
    return { 'LFNs' : lfns, 'LFNSize' : lfnSize }

  def getLFNSize( self, visible=None ):
    if visible == None:
      visible = self.isVisible()
    res = self.bk.getFilesWithGivenDataSets( BKQuery( self.bkQueryDict, visible=visible ).setOptions( 'FileSize', True ) )
    if res['OK'] and type( res['Value'] ) == type( [] ) and res['Value'][0]:
      lfnSize = res['Value'][0]
    else:
      lfnSize = 0
    return lfnSize

  def getNumberOfLFNs( self, visible=None ):
    if visible == None:
      visible = self.isVisible()
    if  self.isVisible() != visible:
      query = BKQuery( self.bkQueryDict, visible=visible )
    else:
      query = self
    fileTypes = query.getFileTypeList()
    nbFiles = 0
    size = 0
    for fileType in fileTypes:
      if fileType:
        res = self.bk.getFilesSummary( query.setFileType( fileType ) )
        #print query, res
        if res['OK']:
          res = res['Value']
          ind = res['ParameterNames'].index( 'NbofFiles' )
          if res['Records'][0][ind]:
            nbFiles += res['Records'][0][ind]
            ind1 = res['ParameterNames'].index( 'FileSize' )
            size += res['Records'][0][ind1]
            #print 'Visible',query.isVisible(),fileType, 'Files:', res['Records'][0][ind], 'Size:', res['Records'][0][ind1]
    return { 'NumberOfLFNs' : nbFiles, 'LFNSize': size }

  def getLFNs( self, printSEUsage=False, printOutput=True, visible=None ):
    if visible == None:
      visible = self.isVisible()
    from DIRAC.Core.DISET.RPCClient                                  import RPCClient
    prods = self.bkQueryDict.get( 'ProductionID' )
    if self.isVisible() != visible:
      query = BKQuery( self.bkQueryDict, visible=visible )
    else:
      query = self
    if prods and type( prods ) == type( [] ):
      # It's faster to loop on a list of prods than query the BK with a list as argument
      lfns = []
      lfnSize = 0
      if query == self:
        query = BKQuery( self.bkQueryDict, visible=visible )
      for prod in prods:
        query.setOption( 'ProductionID', prod )
        lfnsAndSize = query.getLFNsAndSize()
        lfns += lfnsAndSize['LFNs']
        lfnSize += lfnsAndSize['LFNSize']
    else:
      lfnsAndSize = query.getLFNsAndSize()
      lfns = lfnsAndSize['LFNs']
      lfnSize = lfnsAndSize['LFNSize']
    if len( lfns ) == 0:
      gLogger.verbose( "No files found for BK query %s" % str( self.bkQueryDict ) )
    lfns.sort()

    # Only for printing
    if lfns and printOutput:
      print "\n%d files (%.1f TB) in directories:" % ( len( lfns ), lfnSize )
      dirs = {}
      for lfn in lfns:
        dir = os.path.dirname( lfn )
        dirs[dir] = dirs.setdefault( dir, 0 ) + 1
      dirSorted = dirs.keys()
      dirSorted.sort()
      for dir in dirSorted:
        print dir, dirs[dir], "files"
      if printSEUsage:
        rpc = RPCClient( 'DataManagement/StorageUsage' )
        totalUsage = {}
        totalSize = 0
        for dir in dirs.keys():
          res = rpc.getStorageSummary( dir, '', '', [] )
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

  def getDirs( self, printOutput=False, visible=None ):
    if visible == None:
      visible = self.isVisible()
    lfns = self.getLFNs( printSEUsage=True, printOutput=printOutput, visible=visible )
    dirs = []
    for lfn in lfns:
      dir = os.path.dirname( lfn )
      if dir not in dirs:
        dirs.append( dir )
    dirs.sort()
    return dirs

  def __getProdStatus( self, prod ):
    from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
    res = TransformationClient().getTransformation( prod, extraParams=False )
    if not res['OK']:
      gLogger.error( "Couldn't get information on production %d" % prod )
      return None
    return res['Value']['Status']

  def getBKRuns( self ):
    if self.getProcessingPass().replace( '/', '' ) == 'Real Data':
      return self.getBKProductions()

  def getBKProductions( self, visible=None ):
    if visible == None:
      visible = self.isVisible()
    prodList = self.bkQueryDict.get( 'ProductionID' )
    if prodList:
      if type( prodList ) != type( [] ):
        prodList = [prodList]
      return sortList( prodList )
    res = self.bk.getProductions( BKQuery( self.bkQueryDict ).setVisible( visible ) )
    if not res['OK']:
      return []
    if self.getProcessingPass().replace( '/', '' ) != 'Real Data':
      fileTypes = self.getFileTypeList()
      prodList = [prod for p in res['Value']['Records'] for prod in p if self.__getProdStatus( prod ) not in ( 'Cleaned', 'Deleted' )]
      #print '\n',self.bkQueryDict,'\nVisible:',visible,prodList
      pList = []
      if fileTypes:
        from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
        transClient = TransformationClient()
        for prod in prodList:
          res = transClient.getBookkeepingQueryForTransformation( prod )
          if res['OK'] and res['Value']['FileType'] in fileTypes:
              pList.append( prod )
      if not pList:
        pList = prodList
    else:
      pList = [-run for r in res['Value']['Records'] for run in r]
      pList.sort()
      startRun = int( self.bkQueryDict.get( 'StartRun', 0 ) )
      endRun = int( self.bkQueryDict.get( 'EndRun', sys.maxint ) )
      pList = [run for run in pList if run >= startRun and run <= endRun]
    return sortList( pList )

  def getBKConditions( self ):
    conditions = self.bkQueryDict.get( 'ConditionDescription' )
    if conditions:
      if type( conditions ) != type( [] ):
        conditions = [conditions]
      return conditions
    res = self.bk.getConditions( self.bkQueryDict )
    if res['OK']:
      res = res['Value']
    else:
      return []
    conditions = []
    for r in res:
      ind = r['ParameterNames'].index( 'Description' )
      if r['Records']:
        conditions += [p[ind] for p in r['Records']]
        break
    return sortList( conditions )

  def getBKEventTypes( self ):
    eventType = self.getEventTypeList()
    if eventType:
      return eventType
    res = self.bk.getEventTypes( self.bkQueryDict )['Value']
    ind = res['ParameterNames'].index( 'EventTypeId' )
    eventTypes = sortList( [f[ind] for f in res['Records']] )
    return eventTypes

  def getBKFileTypes( self, bkDict=None ):
    fileTypes = self.getFileTypeList()
    #print "Call getBKFileTypes:", self, fileTypes
    if not bkDict:
      bkDict = self.bkQueryDict.copy()
    if not fileTypes:
      fileTypes = []
      eventTypes = bkDict.get( 'EventType', bkDict.get( 'EventTypeId' ) )
      if type( eventTypes ) == type( [] ):
        for et in eventTypes:
          bkDict['EventTypeId'] = et
          fileTypes += self.getBKFileTypes( bkDict )
      else:
        res = self.bk.getFileTypes( bkDict )
        #print res
        if res['OK']:
          res = res['Value']
          ind = res['ParameterNames'].index( 'FileTypes' )
          fileTypes = [f[ind] for f in res['Records'] if f[ind] not in self.exceptFileTypes]
    return self.__fileType( fileTypes, returnList=True )

  def getBKProcessingPasses( self, queryDict=None ):
    processingPasses = {}
    if not queryDict:
      queryDict = self.bkQueryDict.copy()
    initialPP = queryDict.get( 'ProcessingPass', '/' )
    #print initialPP, queryDict
    res = self.bk.getProcessingPass( queryDict, initialPP )
    if not res['OK']:
      return {}
    r = res['Value'][0]
    if 'Name' in r['ParameterNames']:
      ind = r['ParameterNames'].index( 'Name' )
      passes = [os.path.join( initialPP, f[ind] ) for f in r['Records']]
    else:
      passes = []
    r = res['Value'][1]
    if 'EventTypeId' in r['ParameterNames']:
      ind = r['ParameterNames'].index( 'EventTypeId' )
      eventTypes = [str( f[ind] ) for f in r['Records']]
    else:
      eventTypes = []

    if passes:
      nextProcessingPasses = {}
      for p in passes:
        processingPasses[p] = []
        queryDict['ProcessingPass'] = p
        nextProcessingPasses.update( self.getBKProcessingPasses( queryDict ) )
      processingPasses.update( nextProcessingPasses )
    if eventTypes:
      processingPasses[initialPP] = eventTypes
    for p in ( '/Real Data', '/' ):
      if p in processingPasses:
        processingPasses.pop( p )
    #print initialPP, [(key,processingPasses[key]) for key in sortList(processingPasses.keys())]
    return processingPasses

  def browseBK( self ):
    configuration = self.getConfiguration()
    conditions = self.getBKConditions()
    bkTree = {configuration : {}}
    requestedEventTypes = self.getEventTypeList()
    requestedFileTypes = self.getFileTypeList()
    requestedPP = self.getProcessingPass()
    requestedConditions = self.getConditions()
    for cond in conditions:
      self.setConditions( cond )
      processingPasses = self.getBKProcessingPasses()
      #print processingPasses
      for processingPass in [pp for pp in processingPasses if processingPasses[pp]]:
        if requestedEventTypes:
          eventTypes = [t for t in requestedEventTypes if t in processingPasses[processingPass]]
          if not eventTypes: continue
        else:
          eventTypes = processingPasses[processingPass]
        self.setProcessingPass( processingPass )
        #print eventTypes
        for eventType in eventTypes:
          self.setEventType( eventType )
          fileTypes = self.getBKFileTypes()
          bkTree[configuration].setdefault( cond, {} ).setdefault( processingPass, {} )[int( eventType )] = fileTypes
          self.setEventType( requestedEventTypes )
        self.setProcessingPass( requestedPP )
      self.setConditions( requestedConditions )
    return bkTree
