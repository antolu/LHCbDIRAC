"""
DMScript is a class that creates default switches for DM scripts, decodes them and sets flags
"""

__RCSID__ = "$Id: DMScripts.py 42387 2011-09-07 13:53:37Z phicharp $"

import os
import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.List                                         import sortList

class BKQuery():

  def __init__( self, bkQuery = None, prods = [], runs = [], fileTypes = [], visible = True ):
    from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient  import BookkeepingClient
    self.bkFields = ( "ConfigName", "ConfigVersion", "ConditionDescription", "ProcessingPass", "EventTypeId", "FileType" )
    self.extraBKitems = ( "StartRun", "EndRun", "ProductionID", "RunNumbers" )
    self.bk = BookkeepingClient()
    bkPath = ''
    bkQueryDict = {}
    self.bkFileTypes = []
    self.exceptFileTypes = []
    if type( bkQuery ) == type( {} ):
      bkQueryDict = bkQuery.copy()
    if type( bkQuery ) == type( '' ):
      bkPath = bkQuery
    if not bkQueryDict :
      bkQueryDict = self.buildBKQuery( bkPath, prods = prods, runs = runs, fileTypes = fileTypes, visible = visible )
    self.bkPath = bkPath
    self.bkQueryDict = bkQueryDict
    if not bkQueryDict.get( 'Visible' ):
      self.setVisible( visible )

  def __str__( self ):
    return str( self.bkQueryDict )

  def buildBKQuery( self, bkPath = '', prods = [], runs = [], fileTypes = [], visible = True ):
    if not bkPath and not prods and not runs:
      return {}
    if visible:
      bkQuery = {'Visible': 'Yes'}
    else:
      bkQuery = {}

    # Run limits are given
    if runs:
      if type( runs ) != type( [] ):
        runs = runs.split( ',' )
        if len( runs ) > 1:
          bkQuery['RunNumbers'] = runs
        else:
          runs = runs.split( ':' )
          if len( runs ) == 1:
            runs.append( runs[0] )
      if 'RunNumbers' not in bkQuery:
        if runs[0]:
          bkQuery['StartRun'] = runs[0]
        if runs[1]:
          bkQuery['EndRun'] = runs[1]

    ###### Query given as a path /ConfigName/ConfigVersion/ConditionDescription/ProcessingPass/EventType/FileType ######
    if bkPath:
      if bkPath[0] != '/':
        return {}
      bkPath = bkPath.replace( "RealData", "Real Data" )
      bk = bkPath.split( '/' )[1:] + len( self.bkFields ) * ['']
      i = 0
      processingPass = '/'
      for b in bk:
        if self.bkFields[i] == 'ProcessingPass':
          if b != '' and b.upper() != 'ALL' and not b.isdigit():
            processingPass = os.path.join( processingPass, b )
            continue
          if processingPass != '/':
            bkQuery[self.bkFields[i]] = processingPass
          i += 1
        if b != '':
          bkQuery[self.bkFields[i]] = b
        i += 1
        if i == len( self.bkFields ):
          break

      # Set default event type to real data
      if bkQuery['ConfigName'] != 'MC' and not bkQuery.get( 'EventType' ):
        bkQuery['EventTypeId'] = '90000000'

    ###### Query given as a list of production ######
    if prods and str( prods[0] ).upper() != 'ALL':
        bkQuery.setdefault( 'ProductionID', [] ).extend( prods )

    # Set the file type(s) taking into account excludes file types
    fileType = self.__fileType( bkQuery.get( 'FileType', fileTypes ) )
    if fileType:
      bkQuery['FileType'] = fileType
    elif 'FileType' in bkQuery:
      # The requested file type is not available: set an impossible type
      bkQuery['FileType'] = 'None'

    # Remove all "ALL"'s in the dict
    for i in bkQuery.copy():
      if type( bkQuery[i] ) == type( '' ) and bkQuery[i].upper() == 'ALL':
        bkQuery.pop( i )

    self.bkQueryDict = bkQuery.copy()
    # Set both event type entries
    self.setEventType( bkQuery.get( 'EventTypeId' ) )
    # Set conditions
    self.setConditions( bkQuery.get( 'ConditionDescription' ) )
    return self.bkQueryDict

  def setOption( self, key, val ):
    if val:
      self.bkQueryDict[key] = val
    elif key in self.bkQueryDict:
      self.bkQueryDict.pop( key )
    return self.bkQueryDict

  def setConditions( self, cond = None ):
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

  def setFileType( self, fileTypes = None ):
    return self.setOption( 'FileType', self.__fileType( fileTypes ) )

  def setProcessingPass( self, processingPass ):
    return self.setOption( 'ProcessingPass', processingPass )

  def setEventType( self, eventTypes = None ):
    self.setOption( 'EventType', eventTypes )
    return self.setOption( 'EventTypeId', eventTypes )

  def setVisible( self, visible = True ):
    if visible:
      visible = 'Yes'
    return self.setOption( 'Visible', visible )

  def setExceptFileTypes( self, fileTypes ):
    if type( fileTypes ) != type( [] ):
      fileTypes = [fileTypes]
    self.exceptFileTypes += fileTypes
    self.setFileType( self.getFileTypeList() )

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
    return os.path.join( configName, configVersion )

  def isVisible( self ):
    return self.bkQueryDict.get( 'Visible', 'No' ) == 'Yes'

  def __fileType( self, fileType = None, returnList = False ):
    self.__getAllBKFileTypes()
    if not fileType:
        return
    if type( fileType ) == type( [] ):
      fileTypes = fileType
    else:
      fileTypes = fileType.split( ',' )
    if fileTypes[0].upper() == "ALL":
      fileTypes = self.bkFileTypes
    expandedTypes = []
    for fileType in fileTypes:
      if fileType.lower().find( "all." ) == 0:
        ext = '.' + fileType.split( '.' )[1]
        fileType = []
        expandedTypes += [ft for ft in self.bkFileTypes if ft.endswith( ext )]
      else:
        expandedTypes.append( fileType )
    expandedTypes = [t for t in expandedTypes if t not in self.exceptFileTypes]
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
          self.bkFileTypes.append( record[0] )
          if record[0].endswith( 'HIST' ) or record[0].endswith( 'ETC' ) or record[0] == 'LOG':
            self.exceptFileTypes.append( record[0] )

  def getLFNs( self, printSEUsage = False, printOutput = True, visible = True ):
    return self.makeBKQuery( printSEUsage = printSEUsage, printOutput = printOutput, visible = visible )

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

  def getLFNSize( self, visible = None ):
    if visible == None:
      visible = self.isVisible()
    res = self.bk.getFilesWithGivenDataSets( BKQuery( self.bkQueryDict, visible = visible ).setOptions( 'FileSize', True ) )
    if res['OK'] and type( res['Value'] ) == type( [] ) and res['Value'][0]:
      lfnSize = res['Value'][0]
    else:
      lfnSize = 0
    return lfnSize

  def getNumberOfLFNs( self, visible = None ):
    if visible == None:
      visible = self.isVisible()
    if  self.isVisible() != visible:
      query = BKQuery( self.bkQueryDict, visible = visible )
    else:
      query = self
    fileTypes = query.getFileTypeList()
    nbFiles = 0
    size = 0
    for fileType in fileTypes:
      if fileType:
        res = self.bk.getFilesSumary( query.setFileType( fileType ) )
        #print res
        if res['OK']:
          res = res['Value']
          ind = res['ParameterNames'].index( 'NbofFiles' )
          if res['Records'][0][ind]:
            nbFiles += res['Records'][0][ind]
            ind1 = res['ParameterNames'].index( 'FileSize' )
            size += res['Records'][0][ind1]
            #print 'Visible',query.isVisible(),fileType, 'Files:', res['Records'][0][ind], 'Size:', res['Records'][0][ind1]
    return { 'NumberOfLFNs' : nbFiles, 'LFNSize': size }

  def getLFNs( self, printSEUsage = False, printOutput = True, visible = None ):
    if visible == None:
      visible = self.isVisible()
    from DIRAC.Core.DISET.RPCClient                                  import RPCClient
    prods = self.bkQueryDict.get( 'ProductionID' )
    if self.isVisible() != visible:
      query = BKQuery( self.bkQueryDict, visible = visible )
    else:
      query = self
    if prods and type( prods ) == type( [] ):
      # It's faster to loop on a list of prods than query the BK with a list as argument
      lfns = []
      lfnSize = 0
      if query == self:
        query = BKQuery( self.bkQueryDict, visible = visible )
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
      gLogger.debug( "No files found for BK query %s" % str( self.bkQueryDict ) )
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

  def getDirs( self, printOutput = False, visible = None ):
    if visible == None:
      visible = self.isVisible()
    lfns = self.getLFNs( printSEUsage = True, printOutput = printOutput, visible = visible )
    dirs = []
    for lfn in lfns:
      dir = os.path.dirname( lfn )
      if dir not in dirs:
        dirs.append( dir )
    dirs.sort()
    return dirs

  def __getProdStatus( self, prod ):
    from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
    res = TransformationClient().getTransformation( prod, extraParams = False )
    if not res['OK']:
      gLogger.error( "Couldn't get information on production %d" % prod )
      return None
    return res['Value']['Status']

  def getBKRuns( self ):
    if self.getProcessingPass().replace( '/', '' ) == 'Real Data':
      return self.getBKProductions()

  def getBKProductions( self, visible = None ):
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
    return sortList( pList )

  def getBKConditions( self ):
    conditions = self.bkQueryDict.get( 'ConditionDescription' )
    if conditions:
      if type( conditions ) != type( [] ):
        conditions = [conditions]
      return conditions
    res = self.bk.getConditions( self.bkQueryDict )['Value']
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

  def getBKFileTypes( self ):
    #print bkQuery
    fileTypes = self.getFileTypeList()
    if not fileTypes:
      res = self.bk.getFileTypes( self.bkQueryDict )
      #print bkQuery, res
      if res['OK']:
        res = res['Value']
        ind = res['ParameterNames'].index( 'FileTypes' )
        fileTypes = [f[ind] for f in res['Records']]
    return self.__fileType( fileTypes, returnList = True )

  def getBKProcessingPasses( self, queryDict = None ):
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


class DMScript():

  options = {}
  bkQueryDict = {}
  def __init__( self, useBKQuery = False ):
    from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient  import BookkeepingClient
    self.bkFields = [ "ConfigName", "ConfigVersion", "ConditionDescription", "ProcessingPass", "EventType", "FileType" ]
    self.extraBKitems = [ "StartRun", "EndRun", "ProductionID" ]
    self.bk = BookkeepingClient()
    self.bkFileTypes = []
    self.exceptFileTypes = []
    self.prodCacheForBKQuery = {}
    self.useBKQuery = useBKQuery
    self.bkQuery = None

  def registerDMSwitches( self ) :
    self.registerBKSwitches()
    self.registerNamespaceSwitches()
    self.registerSiteSwitches()
    self.registerFileSwitches()

  def registerBKSwitches( self ):
    # BK query switches
    Script.registerSwitch( "P:", "Productions=", "   Production ID to search (comma separated list)", self.setProductions )
    Script.registerSwitch( "f:", "FileType=", "   File type (comma separated list, to be used with --Production) [All]", self.setFileType )
    Script.registerSwitch( '', "ExceptFileType=", "   Exclude the (list of) file types when all are requested", self.setExceptFileType )
    Script.registerSwitch( "B:", "BKQuery=", "   Bookkeeping query path", self.setBKQuery )
    Script.registerSwitch( "r:", "Runs=", "   Run or range of runs (r1:r2)", self.setRuns )

  def registerNamespaceSwitches( self ):
    # namespace switches
    Script.registerSwitch( "D:", "Directory=", "   Directory to search [ALL]", self.setDirectory )

  def registerSiteSwitches( self ):
    # SE switches
    Script.registerSwitch( "g:", "Sites=", "  Sites to consider [ALL] (comma separated list)", self.setSites )
    Script.registerSwitch( "S:", "SEs=", "  SEs to consider [ALL] (comma separated list)", self.setSEs )

  def registerFileSwitches( self ):
    # File switches
    Script.registerSwitch( "f:", "File=", "File containing list of LFNs", self.setLFNsFromFile )
    Script.registerSwitch( "l:", "LFNs=", "List of LFNs (comma separated)", self.setLFNs )

  def setProductions( self, arg ):
    prods = []
    if arg.upper() == "ALL":
      self.options['Productions'] = arg
      return DIRAC.S_OK()
    try:
      for p in arg.split( ',' ):
        if p.find( ":" ) > 0:
          pr = p.split( ":" )
          for i in range( int( pr[0] ), int( pr[1] ) + 1 ):
            prods.append( i )
        else:
          prods.append( p )
      self.options['Productions'] = [int( p ) for p in prods]
    except:
      gLogger.warn( "Wrong production switch value: %s" % arg )
      return DIRAC.S_ERROR()
    return DIRAC.S_OK()

  def setFileType( self, arg ):
    fileTypes = arg.split( ',' )
    self.options['FileType'] = fileTypes
    return DIRAC.S_OK()

  def setExceptFileType( self, arg ):
    self.exceptFileTypes += arg.split( ',' )
    return DIRAC.S_OK()

  def setBKQuery( self, arg ):
    # BKQuery could either be a BK path or a file path that contains the BK items
    try:
      f = open( arg, 'r' )
      content = f.readlines()
      f.close()
      items = [( l[0].strip(), l[1].strip() ) for l in [line.split( '=' ) for line in content]]
      for ( i, j ) in items:
        try:
          j = int( j )
        except:
          pass
        if i in self.bkFields + self.extraBKitems and j:
          self.bkQueryDict[i] = j
    except:
      self.bkQueryDict = {}
      self.options['BKQuery'] = arg
    return DIRAC.S_OK()

  def setRuns( self, arg ):
    runs = arg.split( ':' )
    if len( runs ) == 1:
      runs.append( runs[0] )
    self.options['Runs'] = runs
    return DIRAC.S_OK()

  def setDirectory( self, arg ):
    self.options['Directory'] = arg.split( ',' )
    return DIRAC.S_OK()

  def setSites( self, arg ):
    self.options['Sites'] = arg.split( ',' )
    return DIRAC.S_OK()

  def setSEs( self, arg ):
    self.options['SEs'] = arg.split( ',' )
    return DIRAC.S_OK()

  def setLFNs( self, arg ):
    self.options['LFNs'] = arg.split( ',' )
    return DIRAC.S_OK()

  def setLFNsFromFile( self, arg ):
    try:
      f = open( arg, 'r' )
      self.options['LFNs'] = f.read().splitlines()
      f.close()
    except:
      pass
    return DIRAC.S_OK()

  def getOptions( self ):
    return self.options

  def getOption( self, switch, default = None ):
    return self.options.get( switch, default )

  def getBKQuery( self, visible = True ):
    self.buildBKQuery( visible = visible )
    return self.bkQuery

  def __getAllBKFileTypes( self ):
    if not self.bkFileTypes:
      res = self.bk.getAvailableFileTypes()
      if res['OK']:
        dbresult = res['Value']
        for record in dbresult['Records']:
          self.bkFileTypes.append( record[0] )
          if record[0].endswith( 'HIST' ):
            self.exceptFileTypes.append( record[0] )

  def buildBKQuery( self, visible = True ):
    if self.bkQueryDict:
      return self.bkQueryDict
    bkPath = self.options.get( 'BKQuery' )
    prods = self.options.get( 'Productions' )
    runs = self.options.get( 'Runs' )
    fileTypes = self.options.get( 'FileType' )
    ###############################
    if self.useBKQuery:
      if not self.bkQuery:
        #print bkPath, prods, runs, fileTypes
        self.bkQuery = BKQuery( bkPath, prods, runs, fileTypes, visible )
        self.bkQuery.setExceptFileTypes( self.exceptFileTypes )
        self.bkQueryDict = self.bkQuery.getQueryDict()
        #print self.bkQueryDict
      return self.bkQueryDict
    ###############################
    if not bkPath and not prods and not runs:
      return {}
    if visible:
      bkQuery = {'Visible': 'Yes'}
    else:
      bkQuery = {}
    if runs:
      if runs[0]:
        bkQuery['StartRun'] = runs[0]
      if runs[1]:
        bkQuery['EndRun'] = runs[1]

    ###### Query given as a path /ConfigName/ConfigVersion/ConditionDescription/ProcessingPass/EventType/FileType ######
    if bkPath:
      if bkPath[0] != '/':
        return {}
      bkPath = bkPath.replace( "RealData", "Real Data" )
      bk = bkPath.split( '/' )[1:] + len( self.bkFields ) * ['']
      i = 0
      processingPass = '/'
      for b in bk:
        if self.bkFields[i] == 'ProcessingPass':
          if b != '' and b.upper() != 'ALL' and not b.isdigit():
            processingPass = os.path.join( processingPass, b )
            continue
          if processingPass != '':
            bkQuery[self.bkFields[i]] = processingPass
          i += 1
        if b != '':
          bkQuery[self.bkFields[i]] = b
        i += 1
        if i == len( self.bkFields ):
          break

      # Set default event type to real data
      if bkQuery['ConfigName'] != 'MC' and not bkQuery.get( 'EventType' ):
        bkQuery['EventType'] = '90000000'
      # Set conditions
      bkQuery = self.setBKConditions( bkQuery, bkQuery.get( 'ConditionDescription' ) )
      # Set both event type entries
      if 'EventType' in bkQuery:
        bkQuery['EventTypeId'] = bkQuery['EventType']

    ###### Query given as a list of production ######
    if prods and str( prods[0] ).upper() != 'ALL':
        bkQuery.setdefault( 'ProductionID', [] ).extend( prods )

    # Set the file type(s) taking into account excludes file types
    fileType = self.__fileType( bkQuery.get( 'FileType' ) )
    if fileType:
      bkQuery['FileType'] = fileType
    elif 'FileType' in bkQuery:
      # The requested file type is not available: set an impossible type
      bkQuery['FileType'] = 'None'

    # Remove all "ALL"'s in the dict
    for i in bkQuery.copy():
      if type( bkQuery[i] ) == type( '' ) and bkQuery[i].upper() == 'ALL':
        bkQuery.pop( i )
    # Keep a copy of the BK dictionary and return it to the user
    self.bkQueryDict = bkQuery.copy()
    #print bkQuery
    return self.bkQueryDict

  def setBKConditions( self, bkQuery = bkQueryDict, cond = None ):
    """ Set the dictionary items for a given condition, or remove it (cond=None) """
    ###############################
    if self.useBKQuery:
      if bkQuery == self.bkQueryDict:
        self.bkQueryDict = self.bkQuery.setBKConditions( cond )
        return self.bkQueryDict
      else:
        return BKQuery( bkQuery ).setBKConditions( cond )
    ###############################
    # There are two items in the dictionary: ConditionDescription and Simulation/DataTaking-Conditions
    eventType = bkQuery.get( 'EventType', 'ALL' )
    if bkQuery['ConfigName'] == 'MC' or ( eventType.upper() != 'ALL' and eventType[0] != '9' ):
      conditionsKey = 'SimulationConditions'
    else:
      conditionsKey = 'DataTakingConditions'
    if cond == None:
      try:
        bkQuery.pop( 'ConditionDescription' )
        bkQuery.pop( conditionKey )
      except:
        pass
    else:
      bkQuery['ConditionDescription'] = cond
      bkQuery[conditionsKey] = bkQuery['ConditionDescription']
    return bkQuery

  def setBKFileType( self, bkQuery = bkQueryDict, fileTypes = None ):
    ###############################
    if self.useBKQuery:
      if bkQuery == self.bkQueryDict:
        self.bkQueryDict = self.bkQuery.setBKFileType( fileTypes )
        return self.bkQueryDict
      else:
        return BKQuery( bkQuery ).setBKFileType( fileTypes )
    ###############################
    if fileTypes == None:
      if 'FileType' in bkQuery:
        bkQuery.pop( 'FileType' )
    else:
      if type( fileTypes ) == type( [] ) and len( fileTypes ) == 1:
        fileTypes = fileTypes[0]
      bkQuery['FileType'] = fileTypes
    return bkQuery

  def setBKEventType( self, bkQuery = bkQueryDict, eventTypes = None ):
    ###############################
    if self.useBKQuery:
      if bkQuery == self.bkQueryDict:
        self.bkQueryDict = self.bkQuery.setBKEventType( eventTypes )
        return self.bkQueryDict
      else:
        return BKQuery( bkQuery ).setBKEventType( eventTypes )
    ###############################
    if eventTypes == None:
      try:
        bkQuery.pop( 'EventType' )
        bkQuery.pop( 'EventTypeId' )
      except:
        pass
    else:
      bkQuery['EventType'] = eventTypes
      bkQuery['EventTypeId'] = eventTypes
    return bkQuery

  def __fileType( self, fileType = None ):
    self.__getAllBKFileTypes()
    if not fileType:
      fileType = self.options.get( 'FileType' )
      if not fileType:
        return None
    if type( fileType ) == type( [] ):
      fileTypes = fileType
    else:
      fileTypes = fileType.split( ',' )
    if fileTypes[0].upper() == "ALL":
      fileTypes = self.bkFileTypes
    expandedTypes = []
    for fileType in fileTypes:
      if fileType.lower().find( "all." ) == 0:
        ext = '.' + fileType.split( '.' )[1]
        fileType = []
        expandedTypes += [ft for ft in self.bkFileTypes if ft.endswith( ext )]
      else:
        expandedTypes.append( fileType )
    expandedTypes = [t for t in expandedTypes if t not in self.exceptFileTypes]
    if len( expandedTypes ) == 1:
      return expandedTypes[0]
    else:
      return expandedTypes

  def getRequestID( self, prod = None ):
    """ Get the request ID for a single production """
    from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
    if not prod:
      prod = self.options.get( 'Productions', [] )
    requestID = None
    if type( prod ) == type( '' ):
      prods = [prod]
    else:
      prods = prod
    if len( prods ) == 1 and str( prods[0] ).upper() != 'ALL':
          res = TransformationClient().getTransformation( prods[0] )
          if res['OK']:
            requestID = int( res['Value']['TransformationFamily'] )
    return requestID

  def getDirsForBKQuery( self, printOutput = False, visible = True ):
    ###############################
    if self.useBKQuery:
      if not self.bkQuery:
        self.buildBKQuery( visible = visible )
      return self.bkQuery.getDirs( printOutput = printOutput, visible = visible )
    ###############################
    return self.__makeBKQuery( printSEUsage = True, printOutput = printOutput, visible = visible )[1]

  def getLFNsForBKQuery( self, printSEUsage = False, printOutput = True, visible = True ):
    ###############################
    if self.useBKQuery:
      if not self.bkQuery:
        self.buildBKQuery( visible = visible )
      return self.bkQuery.getLFNs( printOutput = printOutput, visible = visible )
    ###############################
    return self.__makeBKQuery( printSEUsage = printSEUsage, printOutput = printOutput, visible = visible )[0]

  def getFilesFromBK( self, bkQuery = bkQueryDict, printOutput = False ):
    self.__getAllBKFileTypes()
    query = bkQuery.copy()
    res = self.bk.getFilesWithGivenDataSets( query )
    lfns = []
    lfnSize = 0
    if res['OK']:
      lfns = res['Value']
      exceptFiles = self.exceptFileTypes
      if exceptFiles and not query.get( 'FileType' ):
        query['FileType'] = exceptFiles
        res = self.bk.getFilesWithGivenDataSets( query )
        if 'FileType' in bkQuery:
          query['FileType'] = bkQuery['FileType']
        else:
          query.pop( 'FileType' )
        if res['OK']:
          lfnsExcept = [lfn for lfn in res['Value'] if lfn in lfns]
        if lfnsExcept:
          print "***** WARNING ***** Found %d files in BK query that will be excluded (file type in %s)!" % ( len( lfnsExcept ), str( exceptFiles ) )
          print "                    If creating a transformation, set '--FileType ALL'"
          lfns = [lfn for lfn in lfns if lfn not in lfnsExcept]
        else:
          exceptFiles = False
      if printOutput:
        query["FileSize"] = True
        res = self.bk.getFilesWithGivenDataSets( query )
        if res['OK'] and type( res['Value'] ) == type( [] ) and res['Value'][0]:
          lfnSize = res['Value'][0]
        if exceptFiles:
          query['FileType'] = exceptFiles
          res = self.bk.getFilesWithGivenDataSets( query )
          if res['OK'] and type( res['Value'] ) == type( [] ) and res['Value'][0]:
            lfnSize -= res['Value'][0]

    lfnSize /= 1000000000000.
    return ( lfns, lfnSize )

  def getLFNSize( self, bkQuery = bkQueryDict, visible = True ):
    ###############################
    if self.useBKQuery:
      if bkQuery == self.bkQueryDict:
        bkQuery = self.bkQuery
      else:
        bkQuery = BKQuery( bkQuery )
      return bkQuery.getLFNSize( visible = visible )
    ###############################
    query = bkQuery.copy()
    if visible:
      query['Visible'] = 'Yes'
    elif 'Visible' in query:
      query.pop( 'Visible' )
    query.update( {"FileSize":True} )
    res = self.bk.getFilesWithGivenDataSets( query )
    if res['OK'] and type( res['Value'] ) == type( [] ) and res['Value'][0]:
      lfnSize = res['Value'][0]
    else:
      lfnSize = 0
    return lfnSize

  def getNumberOfFiles( self, bkQuery = bkQueryDict, visible = True ):
    ###############################
    if self.useBKQuery:
      if bkQuery == self.bkQueryDict:
        bkQuery = self.bkQuery
      else:
        bkQuery = BKQuery( bkQuery )
      nbAndSize = bkQuery.getNumberOfLFNs( visible = visible )
      return nbAndSize['NumberOfLFNs'], nbAndSize['LFNSize']
    ###############################
    fileTypes = bkQuery.get( 'FileType' )
    if not fileTypes:
      fileTypes = ['']
    elif type( fileTypes ) != type( [] ):
      fileTypes = [fileTypes]
    query = bkQuery.copy()
    if visible:
      query['Visible'] = 'Yes'
    elif 'Visible' in query:
      query.pop( 'Visible' )
    nbFiles = 0
    size = 0
    for fileType in fileTypes:
      if fileType:
        query['FileType'] = fileType
        res = self.bk.getFilesSumary( query )
        #print res
        if res['OK']:
          res = res['Value']
          ind = res['ParameterNames'].index( 'NbofFiles' )
          nbFiles += res['Records'][0][ind]
          ind = res['ParameterNames'].index( 'FileSize' )
          size += res['Records'][0][ind]
    return nbFiles, size

  def __makeBKQuery( self, printSEUsage = False, printOutput = True, visible = True ):
    from DIRAC.Core.DISET.RPCClient                                  import RPCClient
    bkQueryDict = self.bkQueryDict
    if not bkQueryDict:
      bkQueryDict = self.buildBKQuery( visible = visible )
    elif visible:
      bkQueryDict['Visible'] = 'Yes'
    elif 'Visible' in bkQueryDict:
      bkQueryDict.pop( 'Visible' )
    prods = bkQueryDict.get( 'ProductionID' )
    bkQuery = bkQueryDict.copy()
    if prods and type( prods ) == type( [] ):
      # It's faster to loop on a list of prods than query the BK with a list as argument
      lfns = []
      lfnSize = 0
      for prod in prods:
        bkQuery['ProductionID'] = prod
        lfnList, size = self.getFilesFromBK( bkQuery, printOutput )
        lfns += lfnList
        lfnSize += size
    else:
      lfns, lfnSize = self.getFilesFromBK( bkQuery, printOutput )
    if len( lfns ) == 0:
      gLogger.debug( "No files found for BK query %s" % str( self.bkQuery ) )
      return ( [], [] )
    lfns.sort()
    dirs = {}
    for lfn in lfns:
      dir = os.path.dirname( lfn )
      dirs[dir] = dirs.setdefault( dir, 0 ) + 1
    if printOutput:
      print "\n%d files (%.1f TB) in directories:" % ( len( lfns ), lfnSize )
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
    return ( lfns, dirs.keys() )

  def getProductionsFromBKQuery( self, bkQuery = bkQueryDict, visible = True ):
    ###############################
    if self.useBKQuery:
      if bkQuery == self.bkQueryDict:
        bkQuery = self.bkQuery
      else:
        bkQuery = BKQuery( bkQuery )
      return bkQuery.getProductions( visible = visible )
    ###############################
    if not bkQuery:
      bkQuery = self.buildBKQuery( visible = visible )
    query = bkQuery.copy()
    if visible:
      query['Visible'] = 'Yes'
    elif 'Visible' in query:
      query.pop( 'Visible' )
    bkStr = str( query )
    if bkStr in self.prodCacheForBKQuery:
      return self.prodCacheForBKQuery[bkStr]
    res = self.bk.getProductions( query )
    if not res['OK']:
      return []
    prodList = [prod for p in res['Value']['Records'] for prod in p]
    fileTypes = query.get( 'FileType', None )
    if type( fileTypes ) != type( [] ):
      fileTypes = [fileTypes]
    from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
    transClient = TransformationClient()
    pList = []
    if fileTypes:
      for prod in prodList:
        res = transClient.getBookkeepingQueryForTransformation( prod )
        if res['OK']:
          prodBKQuery = res['Value']
          if prodBKQuery['FileType'] in fileTypes:
            pList.append( prod )
    if not pList:
      pList = prodList
    self.prodCacheForBKQuery[bkStr] = pList
    return pList

  def getBKConditions( self, bkQuery = bkQueryDict ):
    ###############################
    if self.useBKQuery:
      if bkQuery == self.bkQueryDict:
        bkQuery = self.bkQuery
      else:
        bkQuery = BKQuery( bkQuery )
      return bkQuery.getBKConditions()
    ###############################
    for conditionsKey in ( 'ConditionDescription', "DataTakingConditions", "SimulationConditions" ):
      conditions = bkQuery.get( conditionsKey )
      if conditions:
        if type( conditions ) != type( [] ):
          conditions = [conditions]
        return conditions
    res = self.bk.getConditions( bkQuery )['Value']
    conditions = []
    for r in res:
      ind = r['ParameterNames'].index( 'Description' )
      if r['Records']:
        conditions += [p[ind] for p in r['Records']]
        break
    return sortList( conditions )

  def getBKEventTypes( self, bkQuery = bkQueryDict ):
    ###############################
    if self.useBKQuery:
      if bkQuery == self.bkQueryDict:
        bkQuery = self.bkQuery
      else:
        bkQuery = BKQuery( bkQuery )
      return bkQuery.getBKEventTypess()
    ###############################
    eventType = bkQuery.get( "EventType" )
    if eventType:
      if type( eventType ) != type( [] ):
        eventType = [eventType]
      return eventType
    #print bkQuery
    res = self.bk.getEventTypes( bkQuery )['Value']
    ind = res['ParameterNames'].index( 'EventTypeId' )
    eventTypes = sortList( [f[ind] for f in res['Records']] )
    #print eventTypes
    return eventTypes

  def getBKFileTypes( self, bkQuery = bkQueryDict ):
    ###############################
    if self.useBKQuery:
      if bkQuery == self.bkQueryDict:
        bkQuery = self.bkQuery
      else:
        bkQuery = BKQuery( bkQuery )
      return bkQuery.getBKFileTypess()
    ###############################
    #print bkQuery
    fileTypes = bkQuery.get( 'FileType' )
    if not fileTypes:
      res = self.bk.getFileTypes( bkQuery )
      #print bkQuery, res
      if res['OK']:
        res = res['Value']
        ind = res['ParameterNames'].index( 'FileTypes' )
        fileTypes = [f[ind] for f in res['Records']]
    fileTypes = self.__fileType( fileTypes )
    if type( fileTypes ) != type( [] ):
      fileTypes = [fileTypes]
    else:
      fileTypes.sort()
    return fileTypes

  def getBKProcessingPasses( self, bkQuery = bkQueryDict ):
    ###############################
    if self.useBKQuery:
      if bkQuery == self.bkQueryDict:
        bkQuery = self.bkQuery
      else:
        bkQuery = BKQuery( bkQuery )
      return bkQuery.getBKProcessingPasses()
    ###############################
    processingPasses = {}
    query = bkQuery.copy()
    initialPP = query.get( 'ProcessingPass', '/' )
    #print initialPP, query
    res = self.bk.getProcessingPass( query, initialPP )
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
        query['ProcessingPass'] = p
        nextProcessingPasses.update( self.getBKProcessingPasses( query ) )
    if eventTypes:
      processingPasses[initialPP] = eventTypes
    for p in ( '/Real Data', '/' ):
      if p in processingPasses:
        processingPasses.pop( p )
    #print initialPP, sortList(processingPass.keys())
    return processingPasses

  def printResult( self, dictionary, offset = 0, shift = 0, empty = "Empty directory" ):
    """ Dictionary pretty printing """
    key_max = 0
    value_max = 0
    for key, value in dictionary.items():
      if len( key ) > key_max:
        key_max = len( key )
      if len( str( value ) ) > value_max:
        value_max = len( str( value ) )
    center = key_max + offset
    if shift:
      offset += shift
    else:
      offset += key_max
    for key in sortList( dictionary.keys() ):
      value = dictionary[key]
      if type( value ) == type( {} ):
        if value != {}:
          print key.rjust( center ), ' : '
          self.printResult( value, offset = offset, shift = shift, empty = empty )
        elif key not in ( 'Failed', 'Successful' ):
          print key.rjust( center ), ' : ', empty
      else:
        print key.rjust( center ), ' : ', str( value ).ljust( value_max )

