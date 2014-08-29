"""
  DMScript is a class that creates default switches for DM scripts, decodes them and sets flags
  The module also provides a function for printing pretty results from DMS queries
"""

import DIRAC
from DIRAC           import gLogger, gConfig
from DIRAC.Core.Base import Script

from LHCbDIRAC.BookkeepingSystem.Client.BKQuery import BKQuery

__RCSID__ = "$Id$"

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
    if type( value ) == type( [] ) or type( value ) == type( set() ):
      if not value:
        print '%s%s : %s' % ( offset * ' ', key, '[]' )
      else:
        print '%s%s : ' % ( offset * ' ', key )
        for val in sorted( value ):
          print '%s%s' % ( newOffset * ' ', val )
    elif type( value ) != type( {} ):
      print '%s : %s' % ( key.rjust( center ), str( value ) )

def printDMResult( result, shift = 4, empty = "Empty directory", script = None, depth = 999, offset = 0 ):
  """ Printing results returned with 'Successful' and 'Failed' items """
  if not script:
    script = Script.scriptName
  try:
    if result['OK']:
      __printDictionary( result['Value'], offset = offset, shift = shift, empty = empty, depth = depth )
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
      # print seList
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
    Script.registerSwitch( '', "Visibility=", "   Set visibility (Yes, No, All) [Yes]", self.setVisibility )
    Script.registerSwitch( '', 'ReplicaFlag=', '   Set visibility (Yes, No, All) [Yes]', self.setReplicaFlag )


  def registerNamespaceSwitches( self, action = 'search [ALL]' ):
    ''' namespace switches '''
    Script.registerSwitch( "D:", "Directory=", "   Directory to " + action, self.setDirectory )

  def registerSiteSwitches( self ):
    ''' SE switches '''
    Script.registerSwitch( "g:", "Sites=", "  Sites to consider [ALL] (comma separated list)", self.setSites )
    Script.registerSwitch( "S:", "SEs=", "  SEs to consider [ALL] (comma separated list)", self.setSEs )

  def registerFileSwitches( self ):
    ''' File switches '''
    Script.registerSwitch( "", "File=", "File containing list of LFNs", self.setLFNsFromFile )
    Script.registerSwitch( "l:", "LFNs=", "List of LFNs (comma separated)", self.setLFNs )
    Script.registerSwitch( "", "Terminal", "LFNs are entered from stdin (--File /dev/stdin)", self.setLFNsFromTerm )

  def registerJobsSwitches( self ):
    ''' Job switches '''
    Script.registerSwitch( "", "File=", "File containing list of DIRAC jobIds", self.setJobidsFromFile )
    Script.registerSwitch( "j:", "DIRACJobids=", "List of DIRAC Jobids (comma separated)", self.setJobids )
    Script.registerSwitch( "", "Terminal",
                           "DIRAC Jobids are entered from stdin (--File /dev/stdin)", self.setJobidsFromTerm )

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

  def setVisibility( self, arg ):
    if arg.lower() in ( 'yes', 'no', 'all' ):
      self.options['Visibility'] = arg
    else:
      gLogger.error( 'Unknown visibility flag: %s' % arg )
      return DIRAC.exit( 1 )
    return DIRAC.S_OK()

  def setReplicaFlag( self, arg ):
    if arg.lower() in ( 'yes', 'no', 'all' ):
      self.options['ReplicaFlag'] = arg
    else:
      gLogger.error( 'Unknown replica flag: %s' % arg )
      return DIRAC.exit( 1 )
    return DIRAC.S_OK()

  def setDirectory( self, arg ):
    try:
      import sys
      f = open( arg, 'r' ) if arg else sys.stdin
      directories = self.getLFNsFromList( f.read().splitlines() )
      if arg:
        f.close()
    except:
      directories = self.getLFNsFromList( arg )
    self.options.setdefault( 'Directory', set() ).update( directories )
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
      self.options.setdefault( 'LFNs', set() ).update( self.getLFNsFromList( arg ) )
    return DIRAC.S_OK()

  def setLFNsFromTerm( self, arg = None ):
    return self.setLFNsFromFile( None )

  def getLFNsFromList( self, lfns ):
    if type( lfns ) == type( {} ):
      lfnList = lfns.keys()
    elif type( lfns ) == type( '' ):
      lfnList = lfns.split( ',' )
    elif type( lfns ) == type( [] ):
      lfnList = [lfn for lfn1 in lfns for lfn in lfn1.split( ',' )]
    else:
      gLogger.error( 'getLFNsFromList: invalid type %s' % type( lfns ) )
      return []
    lfnList = [l.split( 'LFN:' )[-1].strip().replace( '"', ' ' ).replace( ',', ' ' ).replace( "'", " " ).replace( ':', ' ' ) for l in lfnList]
    lfnList = [ '/lhcb' + lfn.split( '/lhcb' )[-1].split()[0] if '/lhcb' in lfn else '' for lfn in lfnList]
    lfnList = [lfn.split( '?' )[0] for lfn in lfnList]
    return sorted( [lfn for lfn in set( lfnList ) if lfn] )

  @staticmethod
  def getJobIDsFromList( jobids ):
    """it returns a list of jobids using a string"""
    jobidsList = []
    if type( jobids ) == type( '' ):
      jobidsList = jobids.split( ',' )
    elif type( jobids ) == type( [] ):
      jobidsList = [lfn for lfn1 in jobids for lfn in lfn1.split( ',' )]
    jobidsList = [ jobid for jobid in jobidsList if jobid != '']
    return jobidsList

  def setLFNsFromFile( self, arg ):
    try:
      import sys
      f = open( arg, 'r' ) if arg else sys.stdin
      lfns = self.getLFNsFromList( f.read().splitlines() )
      if arg:
        f.close()
    except:
      lfns = self.getLFNsFromList( arg )
    self.options.setdefault( 'LFNs', set() ).update( lfns )
    return DIRAC.S_OK()

  def getOptions( self ):
    return self.options

  def getOption( self, switch, default = None ):
    if switch == 'SEs':
      return convertSEs( self.options.get( switch, default ) )
    value = self.options.get( switch, default )
    if switch == 'LFNs' and not value:
      import sys
      if not sys.stdin.isatty():
        self.setLFNsFromTerm()
        value = self.options.get( switch, default )
    if type( value ) == type( set() ):
      value = sorted( value )
    return value

  def getBKQuery( self, visible = None ):
    if self.bkQuery:
      return self.bkQuery
    if self.bkQueryDict:
      self.bkQuery = BKQuery( self.bkQueryDict )
    else:
      visible = self.options.get( 'Visibility', 'Yes' ) if visible == None else visible
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
    if 'ReplicaFlag' in self.options:
      self.bkQuery.setOption( 'ReplicaFlag', self.options['ReplicaFlag'] )
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

  def setJobidsFromFile( self, arg ):
    """It fill a list with DIRAC jobids read from a file
    NOTE: The file format is equivalent to the file format when the content is
    a list of LFNs."""
    try:
      import sys
      in_file = open( arg, 'r' ) if arg else sys.stdin
      jobids = self.getJobIDsFromList( in_file.read().splitlines() )
      if arg:
        in_file.close()
    except IOError, error:
      gLogger.exception( 'Reading jobids from a file is failed with exception: "%s"' % error )
      jobids = self.getJobIDsFromList( arg )
    self.options.setdefault( 'JobIDs', set() ).update( jobids )
    return DIRAC.S_OK()

  def setJobids( self, arg ):
    """It fill a list with DIRAC Jobids."""
    if arg:
      self.options.setdefault( 'JobIDs', set() ).update( self.getJobIDsFromList( arg ) )
    return DIRAC.S_OK()

  def setJobidsFromTerm( self ):
    """It is used to fill a list with jobids"""
    return self.setJobidsFromFile( None )
