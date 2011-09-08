"""
DMScript is a class that creates default switches for DM scripts, decodes them and sets flags
"""

import DIRAC
from DIRAC.Core.Base import Script

def printResult( dictionary, offset = 0, shift = 0, empty = "Empty directory" ):
  from DIRAC.Core.Utilities.List                                         import sortList
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
      if not value == {}:
        print key.rjust( center ), ' : '
        printResult( value, offset = offset, shift = shift, empty = empty )
      elif key not in ['Failed', 'Successful']:
        print key.rjust( center ), ' : ', empty
    else:
      print key.rjust( center ), ' : ', str( value ).ljust( value_max )

class DMScript():

  options = {}
  bkQueryDict = {}
  def __init__( self ):
    from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient  import BookkeepingClient
    self.bkFields = [ "ConfigName", "ConfigVersion", "DataTakingConditions", "ProcessingPass", "EventType", "FileType" ]
    self.extraBKitems = [ "StartRun", "EndRun", "ProductionID" ]
    self.bk = BookkeepingClient()

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
      DIRAC.gLogger.warn( "Wrong production switch value: %s" % arg )
      return DIRAC.S_ERROR()
    return DIRAC.S_OK()

  def setFileType( self, arg ):
    self.options['FileType'] = arg.split( ',' )
    return DIRAC.S_OK()

  def setExceptFileType( self, arg ):
    self.options['ExceptFileType'] = arg.split( ',' )
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

  def buildBKQuery( self, visible = True ):
    bkQuery = self.options.get( 'BKQuery' )
    prods = self.options.get( 'Productions' )
    runs = self.options.get( 'Runs' )
    if not bkQuery and not prods and not runs:
      return {}
    fileType = None
    if visible:
      bkQueryDict = {'Visible': 'Yes'}
    else:
      bkQueryDict = {}
    if runs:
      if runs[0]:
        bkQueryDict['StartRun'] = runs[0]
      if runs[1]:
        bkQueryDict['EndRun'] = runs[1]

    if bkQuery:
      bkQuery = bkQuery.replace( "RealData", "Real Data" )
      if bkQuery[0] == '/':
        bkQuery = bkQuery[1:].replace( "RealData", "Real Data" )
      bk = bkQuery.split( '/' )
      missingNodes = len( self.bkFields ) - len( bk )
      if missingNodes > 0:
        bk += missingNodes * ['']
      if bk[0] != 'MC' and not bk[-2]:
        # Set default event type to real data
        bk[-2] = '90000000'
      try:
        bkNodes = [bk[0], bk[1], bk[2], '/' + '/'.join( bk[3:-2] ), bk[-2], bk[-1]]
      except:
        print "Incorrect BKQuery...\nSyntax: %s" % '/'.join( self.bkFields )
        return ( None, None )
      if bkNodes[0] == "MC" or bk[-2][0] != '9':
        self.bkFields[2] = "SimulationConditions"
      for i in range( len( self.bkFields ) ):
          if bkNodes[i] and bkNodes[i] != '/' and not bkNodes[i].upper().endswith( 'ALL' ):
            bkQueryDict[self.bkFields[i]] = bkNodes[i]
      fileType = bkQueryDict.get( 'FileType' )

    if prods and str( prods[0] ).upper() != 'ALL':
        bkQueryDict.setdefault( 'ProductionID', [] ).extend( prods )
    fileType = self.__fileType( fileType )
    if fileType:
      bkQueryDict['FileType'] = fileType

    self.bkQueryDict.update( bkQueryDict )
    return self.bkQueryDict

  def __fileType( self, fileType = None ):
    if not fileType:
      fileType = self.options.get( 'FileType' )
      if not fileType:
        return None
    if type( fileType ) == type( [] ):
      fileTypes = fileType
    else:
      fileTypes = fileType.split( ',' )
    expandedTypes = []
    for fileType in fileTypes:
      if fileType.lower().find( "all." ) == 0:
        ext = '.' + fileType.split( '.' )[1]
        fileType = []
        res = self.bk.getAvailableFileTypes()

        if res['OK']:
          dbresult = res['Value']
          for record in dbresult['Records']:
            if record[0].endswith( ext ):
              expandedTypes.append( record[0] )
      else:
        expandedTypes.append( fileType )
    if len( expandedTypes ) == 1:
      expandedTypes = expandedTypes[0]
    else:
      expandedTypes = [t for t in expandedTypes if t not in self.options.get( 'ExceptFileType', [] ) and not t.endswith( 'HIST' )]
    return expandedTypes

  def getRequestID( self ):
    from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
    prods = self.options.get( 'Productions', [] )
    requestID = None
    if type( prods ) == type( '' ):
      prods = [prods]
    if len( prods ) == 1 and str( prods[0] ).upper() != 'ALL':
          res = TransformationClient().getTransformation( prods[0] )
          if res['OK']:
            requestID = int( res['Value']['TransformationFamily'] )
    return requestID

  def getDirsForBKQuery( self, printOutput = False, visible = True ):
    return self.__makeBKQuery( printSEUsage = True, printOutput = printOutput, visible = visible )[1]

  def getLFNsForBKQuery( self, printSEUsage = False, printOutput = True, visible = True ):
    return self.__makeBKQuery( printSEUsage = printSEUsage, printOutput = printOutput, visible = visible )[0]

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
    lfns = []
    if prods and type( prods ) == type( [] ):
      bkDict = bkQueryDict.copy()
      lfnSize = 0
      for prod in prods:
        bkDict['ProductionID'] = prod
        res = self.bk.getFilesWithGivenDataSets( bkDict )
        if res['OK']:
          lfns += res['Value']
          if printOutput:
            bkDict.update( {"FileSize":True} )
            res = self.bk.getFilesWithGivenDataSets( bkDict )
            if res['OK'] and type( res['Value'] ) == type( [] ) and res['Value'][0]:
              lfnSize += res['Value'][0] / 1000000000000.
    else:
      bkDict = bkQueryDict.copy()
      res = self.bk.getFilesWithGivenDataSets( bkDict )
      if res['OK']:
        lfns = res['Value']
        if printOutput:
          bkDict.update( {"FileSize":True} )
          res = self.bk.getFilesWithGivenDataSets( bkDict )
          lfnSize = 0
          if res['OK'] and type( res['Value'] ) == type( [] ) and res['Value'][0]:
            lfnSize = res['Value'][0] / 1000000000000.
    if len( lfns ) == 0:
      print "No files found for BK query..."
      return ( [], [] )
    lfns.sort()
    dirs = {}
    import os
    for lfn in lfns:
      dir = os.path.dirname( lfn )
      dirs[dir] = dirs.setdefault( dir, 0 ) + 1
    if printOutput:
      print "\n%d files (%.1f TB) in directories:" % ( len( lfns ), lfnSize )
      for dir in dirs.keys():
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
    if not bkQuery:
      bkQuery = self.buildBKQuery( visible = visible )
    elif visible:
      bkQuery['Visible'] = 'Yes'
    elif 'Visible' in bkQuery:
      bkQuery.pop( 'Visible' )
    if 'ConditionDescription' not in bkQuery and 'DataTakingConditions' in bkQuery:
      bkQuery['ConditionDescription'] = bkQuery['DataTakingConditions']
    res = self.bk.getProductions( bkQuery )
    if not res['OK']:
      return []
    fileTypes = bkQuery.get( 'FileType', None )
    if type( fileTypes ) == type( '' ):
      fileTypes = [fileTypes]
    from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
    transClient = TransformationClient()
    prodList = [prod for p in res['Value']['Records'] for prod in p]
    pList = []
    for prod in prodList:
      res = transClient.getBookkeepingQueryForTransformation( prod )
      if res['OK']:
        prodBKQuery = res['Value']
        if not fileTypes or prodBKQuery['FileType'] in fileTypes:
          pList.append( prod )
    if not pList:
      return prodList
    else:
      return pList

