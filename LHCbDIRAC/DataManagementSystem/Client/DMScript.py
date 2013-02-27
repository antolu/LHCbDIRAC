"""
  DMScript is a class that creates default switches for DM scripts, decodes them and sets flags
  The module also provides a function for printing pretty results from DMS queries
"""

import DIRAC
from DIRAC           import gLogger, gConfig
from DIRAC.Core.Base import Script

from LHCbDIRAC.BookkeepingSystem.Client.BKQuery import BKQuery

__RCSID__ = "$Id: DMScripts.py 42387 2011-09-07 13:53:37Z phicharp $"

def __printDictionary( dictionary, offset = 0, shift = 0, empty = "Empty directory", depth = 9999 ):
  """ Dictionary pretty printing """
  key_max = 0
  value_max = 0
  for key, value in dictionary.items():
    key_max = max( key_max, len( key ) )
    value_max = max( value_max, len( str( value ) ) )
  center = key_max + offset
  newOffset = offset + ( shift if shift else key_max )
  for key in sorted( dictionary ):
    value = dictionary[key]
    if type( value ) == type( {} ):
      if not depth:
        value = value.keys()
      elif value != {}:
        print '%s%s : ' % ( offset * ' ', key )
        __printDictionary( value, offset = newOffset, shift = shift, empty = empty, depth = depth - 1 )
      elif key not in ( 'Failed', 'Successful' ):
        print '%s%s : %s' % ( offset * ' ', key, empty )
    if type( value ) == type( [] ):
      if value == []:
        print '%s%s : %s' % ( offset * ' ', key, '[]' )
      else:
        print '%s%s : ' % ( offset * ' ', key )
        for val in value:
          print '%s%s' % ( newOffset * ' ', val )
    elif type( value ) != type( {} ):
      print '%s : %s' % ( key.rjust( center ), str( value ) )

def printDMResult( result, shift = 4, empty = "Empty directory", script = None, depth = 999 ):
  """ Printing results returned with 'Successful' and 'Failed' items """
  if not script:
    script = Script.scriptName
  try:
    if result['OK']:
      __printDictionary( result['Value'], shift = shift, empty = empty, depth = depth )
      return 0
    else:
      print "Error in", script, ":", result['Message']
      return 2
  except:
    print "Exception while printing results in", script, "- Results:"
    print result
    return 2

def convertSEs( ses ):
  seList = []
  for se in ses:
    seConfig = gConfig.getValue( '/Resources/StorageElementGroups/%s' % se, se )
    if seConfig != se:
      seList += [se.strip() for se in seConfig.split( ',' )]
      #print seList
    else:
      seList.append( se )
    res = gConfig.getSections( '/Resources/StorageElements' )
    if not res['OK']:
      gLogger.fatal( 'Error getting list of SEs from CS', res['Message'] )
      DIRAC.exit( 1 )
    for se in seList:
      if se not in res['Value']:
        gLogger.fatal( '%s is not a valid SE' % se )
        seList = []
        break

  return seList

class DMScript():
  """
  DMScript is a class that creates default switches for DM scripts, decodes them and sets flags
  """

  def __init__( self ):
    from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient  import BookkeepingClient
    self.bkFields = [ "ConfigName", "ConfigVersion", "ConditionDescription", "ProcessingPass", "EventType", "FileType" ]
    self.extraBKitems = [ "StartRun", "EndRun", "ProductionID" ]
    self.bk = BookkeepingClient()
    self.bkFileTypes = []
    self.exceptFileTypes = []
    self.prodCacheForBKQuery = {}
    self.bkQuery = None
    self.bkQueryDict = {}
    self.options = {}

  def registerDMSwitches( self ) :
    self.registerBKSwitches()
    self.registerNamespaceSwitches()
    self.registerSiteSwitches()
    self.registerFileSwitches()

  def registerBKSwitches( self ):
    # BK query switches
    Script.registerSwitch( "P:", "Productions=",
                           "   Production ID to search (comma separated list)", self.setProductions )
    Script.registerSwitch( "f:", "FileType=",
                           "   File type (comma separated list, to be used with --Production) [All]", self.setFileType )
    Script.registerSwitch( '', "ExceptFileType=",
                           "   Exclude the (list of) file types when all are requested", self.setExceptFileTypes )
    Script.registerSwitch( "B:", "BKQuery=", "   Bookkeeping query path", self.setBKQuery )
    Script.registerSwitch( "r:", "Runs=", "   Run or range of runs (r1:r2)", self.setRuns )
    Script.registerSwitch( '', "DQFlags=", "   DQ flag used in query", self.setDQFlags )
    Script.registerSwitch( '', "StartDate=", "   Start date for the BK query", self.setStartDate )
    Script.registerSwitch( '', "EndDate=", "   End date for the BK query", self.setEndDate )
    Script.registerSwitch( '', "Invisible", "   See also invisible files", self.setInvisible )


  def registerNamespaceSwitches( self ):
    ''' namespace switches '''
    Script.registerSwitch( "D:", "Directory=", "   Directory to search [ALL]", self.setDirectory )

  def registerSiteSwitches( self ):
    ''' SE switches '''
    Script.registerSwitch( "g:", "Sites=", "  Sites to consider [ALL] (comma separated list)", self.setSites )
    Script.registerSwitch( "S:", "SEs=", "  SEs to consider [ALL] (comma separated list)", self.setSEs )

  def registerFileSwitches( self ):
    ''' File switches '''
    Script.registerSwitch( "", "File=", "File containing list of LFNs", self.setLFNsFromFile )
    Script.registerSwitch( "l:", "LFNs=", "List of LFNs (comma separated)", self.setLFNs )
    Script.registerSwitch( "", "Terminal", "LFNs are entered from stdin (--File /dev/stdin)", self.setLFNsFromTerm )

  def setProductions( self, arg ):
    prods = []
    if arg.upper() == "ALL":
      self.options['Productions'] = arg
      return DIRAC.S_OK()
    try:
      for prod in arg.split( ',' ):
        if prod.find( ":" ) > 0:
          pr = prod.split( ":" )
          for i in range( int( pr[0] ), int( pr[1] ) + 1 ):
            prods.append( i )
        else:
          prods.append( prod )
      self.options['Productions'] = [int( prod ) for prod in prods]
    except:
      gLogger.error( "Invalid production switch value: %s" % arg )
      self.options['Productions'] = ['Invalid']
      return DIRAC.S_ERROR()
    return DIRAC.S_OK()

  def setStartDate( self, arg ):
    self.options['StartDate'] = arg
    return DIRAC.S_OK()

  def setEndDate( self, arg ):
    self.options['EndDate'] = arg
    return DIRAC.S_OK()

  def setFileType( self, arg ):
    fileTypes = arg.split( ',' )
    self.options['FileType'] = fileTypes
    return DIRAC.S_OK()

  def setExceptFileTypes( self, arg ):
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
      self.bkQuery = None
      self.bkQueryDict = {}
      self.options['BKPath'] = arg
    return DIRAC.S_OK()

  def setRuns( self, arg ):
    self.options['Runs'] = arg
    return DIRAC.S_OK()

  def setDQFlags( self, arg ):
    dqFlags = arg.split( ',' )
    self.options['DQFlags'] = dqFlags
    return DIRAC.S_OK()

  def setInvisible( self, arg ):
    self.options['Invisible'] = True
    return DIRAC.S_OK()

  def setDirectory( self, arg ):
    self.options['Directory'] = arg.split( ',' )
    return DIRAC.S_OK()

  def setSites( self, arg ):
    siteShortNames = { 'CERN':'LCG.CERN.ch', 'CNAF':'LCG.CNAF.it', 'GRIDKA':'LCG.GRIDKA.de',
                      'NIKHEF':'LCG.NIKHEF.nl', 'SARA':'LCG.SARA.nl', 'PIC':'LCG.PIC.es',
                      'RAL':'LCG.RAL.uk', 'IN2P3':'LCG.IN2P3.fr' }
    sites = arg.split( ',' )
    self.options['Sites'] = [siteShortNames.get( site.upper(), site ) for site in sites]
    return DIRAC.S_OK()

  def setSEs( self, arg ):
    self.options['SEs'] = arg.split( ',' )
    return DIRAC.S_OK()

  def setLFNs( self, arg ):
    if arg:
      self.options.setdefault( 'LFNs', [] ).extend( arg.split( ',' ) )
    return DIRAC.S_OK()

  def setLFNsFromTerm( self, arg ):
    return self.setLFNsFromFile( '/dev/stdin' )

  def setLFNsFromFile( self, arg ):
    try:
      f = open( arg, 'r' )
      lfns = [l.split( 'LFN:' )[-1].strip().replace( '"', ' ' ).replace( ',', ' ' ).replace( "'", " " ) for l in f.read().splitlines()]
      lfns = [ '/lhcb' + lfn.split( '/lhcb' )[-1].split()[0] for lfn in lfns]
      f.close()
    except:
      lfns = [arg]
    self.options.setdefault( 'LFNs', [] ).extend( lfns )
    return DIRAC.S_OK()

  def getOptions( self ):
    return self.options

  def getOption( self, switch, default = None ):
    if switch == 'SEs':
      return convertSEs( self.options.get( switch, default ) )
    return self.options.get( switch, default )

  def getBKQuery( self, visible = None ):
    if self.bkQuery:
      return self.bkQuery
    if self.bkQueryDict:
      self.bkQuery = BKQuery( self.bkQueryDict )
    else:
      visible = not self.options.get( 'Invisible', False ) if visible == None else visible
      bkPath = self.options.get( 'BKPath' )
      prods = self.options.get( 'Productions' )
      runs = self.options.get( 'Runs' )
      fileTypes = self.options.get( 'FileType' )
      self.bkQuery = BKQuery( bkPath, prods, runs, fileTypes, visible )
    self.bkQuery.setExceptFileTypes( self.exceptFileTypes )
    if 'DQFlags' in self.options:
      self.bkQuery.setDQFlag( self.options['DQFlags'] )
    if 'StartDate' in self.options:
      self.bkQuery.setOption( 'StartDate', self.options['StartDate'] )
    if 'EndDate' in self.options:
      self.bkQuery.setOption( 'EndDate', self.options['EndDate'] )
    return self.bkQuery

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

