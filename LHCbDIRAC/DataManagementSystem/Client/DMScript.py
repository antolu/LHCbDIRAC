"""
  DMScript is a class that creates default switches for DM scripts, decodes them and sets flags
  The module also provides a function for printing pretty results from DMS queries
"""

import DIRAC
from DIRAC           import gLogger, gConfig
from DIRAC.Core.Base import Script
import os
import sys
import time

from LHCbDIRAC.BookkeepingSystem.Client.BKQuery import BKQuery
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient  import BookkeepingClient
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import resolveSEGroup

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
    if isinstance( value, dict ):
      if not depth:
        value = value.keys()
      elif value != {}:
        gLogger.notice( '%s%s : ' % ( offset * ' ', key ) )
        __printDictionary( value, offset = newOffset, shift = shift, empty = empty, depth = depth - 1 )
      elif key not in ( 'Failed', 'Successful' ):
        gLogger.notice( '%s%s : %s' % ( offset * ' ', key, empty ) )
    if isinstance( value, ( list, set ) ):
      if not value:
        gLogger.notice( '%s%s : %s' % ( offset * ' ', key, '[]' ) )
      else:
        gLogger.notice( '%s%s : ' % ( offset * ' ', key ) )
        for val in sorted( value ):
          gLogger.notice( '%s%s' % ( newOffset * ' ', val ) )
    elif not isinstance( value, dict ):
      gLogger.notice( '%s : %s' % ( key.rjust( center ), str( value ) ) )

def printDMResult( result, shift = 4, empty = "Empty directory", script = None, depth = 999, offset = 0 ):
  """ Printing results returned with 'Successful' and 'Failed' items """
  if not script:
    script = Script.scriptName
  try:
    if result['OK']:
      __printDictionary( result['Value'], offset = offset, shift = shift, empty = empty, depth = depth )
      return 0
    else:
      gLogger.notice( "Error in %s :" % script, result['Message'] )
      return 2
  except:
    gLogger.notice( "Exception while printing results in %s - Results:" % script )
    gLogger.notice( result )
    return 2

class ProgressBar( object ):
  def __init__( self, items, width = None, title = None, chunk = None, step = None, interactive = None ):
    if title is None:
      title = ''
    else:
      title += ' '
    if width is None:
      width = 50
    if chunk is None:
      chunk = 1
    if step is None:
      step = 1
    if interactive is None:
      interactive = True
    self._step = step
    self._width = width
    self._loopNumber = 0
    self._itemCounter = 0
    self._items = items
    self._chunk = chunk
    self._startTime = time.time()
    self._progress = 0
    self._showBar = bool( items > chunk ) and bool( items > step ) and interactive
    self._interactive = interactive
    self._title = title
    self._backspace = 0
    self._writeTitle()
  def _writeTitle( self ):
    if not self._interactive:
      return
    if self._showBar:
      sys.stdout.write( "%s|%s|" % ( self._title, self._width * ' ' ) )
      self._backspace = self._width + 1
    else:
      sys.stdout.write( self._title )
    sys.stdout.flush()
  def loop( self, increment = True ):
    if not self._interactive:
      return
    showBar = self._showBar and ( self._loopNumber % self._step ) == 0
    fraction = min( float( self._itemCounter ) / float( self._items ), 1. )
    if increment:
      self._loopNumber += 1
      self._itemCounter += self._chunk
    else:
      showBar = self._showBar
    if showBar:
      progress = int( round( self._width * fraction ) )
      elapsed = time.time() - self._startTime
      if elapsed > 30. and fraction:
        rest = int( elapsed * ( 1 - fraction ) / fraction )
        estimate = ' (%d seconds left)' % rest
      else:
        estimate = ''
      blockBlue = '\033[46m'
      endblock = '\033[0m'
      sys.stdout.write( "%s%s%s| %5.1f%%%s\033[K" % ( self._backspace * '\b',
                                                  blockBlue + ( progress - self._progress ) * ' ' + endblock,
                                                  ( self._width - progress ) * ' ',
                                                  100. * fraction,
                                                  estimate ) )
      self._backspace = self._width + 8 - progress + len( estimate )
      self._progress = progress
      sys.stdout.flush()
  def endLoop( self, message = None, timing = True ):
    if not self._interactive:
      return
    if message is None:
      message = 'completed'
    if self._showBar:
      sys.stdout.write( "%s\033[K: %s" % ( ( self._progress + self._backspace + 1 ) * '\b',
                                          message ) )
    if timing:
      if not self._showBar:
        sys.stdout.write( ': %s' % message )
      sys.stdout.write( ' in %.1f seconds' % ( time.time() - self._startTime ) )
    sys.stdout.write( '\n' )
    sys.stdout.flush()
  def comment( self, message, optMsg = '' ):
    fullMsg = '\n' + message + ' %s' % optMsg if optMsg else ''
    gLogger.notice( fullMsg )
    self._writeTitle()
    self.loop( increment = False )


class WithDots( object ):
  """
  WithDots class allows to print a message and a series of dots that are erased every 'chunk' times the loop() method is called.
  The endLoop method prints out a completion message and possibly the timing between the creation of the object and now
  """
  def __init__( self, items, title = None, chunk = None, mindots = None ):
    if title is None:
      title = ''
    else:
      title += ' '
    if chunk is None:
      chunk = 1
    if mindots is None:
      mindots = 0
    self._chunk = chunk
    self._startTime = time.time()
    ndots = int( ( items - 1 ) / chunk + 1 )
    self._writeDots = bool( ndots > mindots )
    if self._writeDots:
      sys.stdout.write( '%s%s' % ( title, ndots * '.' ) )
      sys.stdout.flush()
    elif title:
      gLogger.notice( title )
    self._loopNumber = self._chunk
  def loop( self ):
    if self._writeDots:
      if self._loopNumber == self._chunk:
        self._loopNumber = 0
        sys.stdout.write( '\b\033[K' )
        sys.stdout.flush()
      self._loopNumber += 1
  def endLoop( self, timing = True ):
    if self._writeDots:
      sys.stdout.write( 'completed' )
      if timing:
        sys.stdout.write( ' (%.1f seconds)' % ( time.time() - self._startTime ) )
      sys.stdout.write( '\n' )
      sys.stdout.flush()

class DMScript( object ):
  """ DMScript is a class that creates default switches for DM scripts, decodes them and sets flags
  """

  def __init__( self ):
    """ c'tor
    """
    self.bkFields = [ "ConfigName", "ConfigVersion", "ConditionDescription", "ProcessingPass", "EventType", "FileType" ]
    self.extraBKitems = [ "StartRun", "EndRun", "ProductionID" ]
    self.bk = BookkeepingClient()
    self.bkFileTypes = []
    self.exceptFileTypes = []
    self.prodCacheForBKQuery = {}
    self.bkQuery = None
    self.bkQueryDict = {}
    self.options = {}
    self.lastFile = os.path.join( os.environ.get( 'TMPDIR', '/tmp' ), '%d.lastLFNs' % os.getppid() )
    self.setLastFile = False
    self.voName = None

  def __voName( self ):
    if self.voName is None:
      self.voName = gConfig.getValue( '/DIRAC/VirtualOrganization', '' )
    gLogger.verbose( 'VO', self.voName )
    return self.voName

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
    Script.registerSwitch( '', "Visibility=", "   Required visibility (Yes, No, All) [Yes]", self.setVisibility )
    Script.registerSwitch( '', 'ReplicaFlag=', '   Required replica flag (Yes, No, All) [Yes]', self.setReplicaFlag )
    Script.registerSwitch( '', 'TCK=', '   Get files with a given TCK', self.setTCK )


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
    Script.registerSwitch( "", "LastLFNs", "Use last set of LFNs", self.setLFNsFromLast )

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
          for i in xrange( int( pr[0] ), int( pr[1] ) + 1 ):
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

  def setTCK( self, arg ):
    tcks = arg.split( ',' )
    self.options['TCK'] = tcks
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
    if os.path.exists( arg ) and not os.path.isdir( arg ):
      f = open( arg, 'r' )
      directories = [line.split()[0] for line in f.read().splitlines()]
      if arg:
        f.close()
    else:
      directories = arg.split( ',' )
    self.options.setdefault( 'Directory', set() ).update( directories )
    return DIRAC.S_OK()

  def setSites( self, arg ):
    siteShortNames = { 'CERN':'LCG.CERN.ch', 'CNAF':'LCG.CNAF.it', 'GRIDKA':'LCG.GRIDKA.de',
                      'NIKHEF':'LCG.NIKHEF.nl', 'SARA':'LCG.SARA.nl', 'PIC':'LCG.PIC.es',
                      'RAL':'LCG.RAL.uk', 'IN2P3':'LCG.IN2P3.fr', 'RRCKI':'LCG.RRCKI.ru' }
    sites = arg.split( ',' )
    self.options['Sites'] = [siteShortNames.get( site.upper(), site ) for site in sites]
    return DIRAC.S_OK()

  def setSEs( self, arg ):
    self.options['SEs'] = arg.split( ',' )
    return DIRAC.S_OK()

  def setLFNs( self, arg ):
    if arg:
      self.options.setdefault( 'LFNs', set() ).update( arg.split( ',' ) )
    return DIRAC.S_OK()

  def setLFNsFromTerm( self, arg = None ):
    return self.setLFNsFromFile( arg )

  def getLFNsFromList( self, lfns, directories = False ):
    if isinstance( lfns, dict ):
      lfnList = [lfn.strip() for lfn in lfns]
    elif isinstance( lfns, basestring ):
      lfnList = lfns.split( ',' )
    elif isinstance( lfns , list ):
      lfnList = [lfn.strip() for lfn1 in lfns for lfn in lfn1.split( ',' )]
    elif isinstance( lfns, set ):
      lfnList = sorted( lfns )
    else:
      gLogger.error( 'getLFNsFromList: invalid type %s' % type( lfns ) )
      return []
    if not directories:
      vo = self.__voName()
      if vo:
        vo = '/%s/' % vo
        lfnList = [l.split( 'LFN:' )[-1].strip().replace( '"', ' ' ).replace( ',', ' ' ).replace( "'", " " ).replace( ':', ' ' ) for l in lfnList]
        lfnList = [ vo + lfn.split( vo )[-1].split()[0] if vo in lfn else lfn if lfn == vo[:-1] else '' for lfn in lfnList]
        lfnList = [lfn.split( '?' )[0] for lfn in lfnList]
        lfnList = [lfn for lfn in lfnList if not lfn.endswith( '/' )]
    return sorted( [lfn for lfn in set( lfnList ) if lfn or directories] )

  @staticmethod
  def getJobIDsFromList( jobids ):
    """it returns a list of jobids using a string"""
    jobidsList = []
    if isinstance( jobids, basestring ):
      jobidsList = jobids.split( ',' )
    elif isinstance( jobids, list ):
      jobidsList = [lfn for lfn1 in jobids for lfn in lfn1.split( ',' )]
    jobidsList = [ jobid for jobid in jobidsList if jobid != '']
    return jobidsList

  def setLFNsFromLast( self, _val ):
    if os.path.exists( self.lastFile ):
      return self.setLFNsFromFile( self.lastFile )
    gLogger.fatal( 'Last file %s does not exist' % self.lastFile )
    DIRAC.exit( 2 )

  def setLFNsFromFile( self, arg ):
    if isinstance( arg, basestring ) and arg.lower() == 'last':
      arg = self.lastFile
    # Make a list of files
    if isinstance( arg, basestring ):
      files = arg.split( ',' )
    elif isinstance( arg, list ):
      files = arg
    elif not arg:
      files = [arg]
    nfiles = 0
    for fName in files:
      try:
        f = open( fName, 'r' ) if fName else sys.stdin
        lfns = f.read().splitlines()
        if fName:
          f.close()
        nfiles += len( lfns )
      except:
        lfns = fName.split( ',' )
      self.options.setdefault( 'LFNs', set() ).update( lfns )
    if nfiles:
      self.setLastFile = arg if arg else 'term'
    return DIRAC.S_OK()

  def getOptions( self ):
    return self.options

  def getOption( self, switch, default = None ):
    if switch == 'SEs':
      return resolveSEGroup( self.options.get( switch, default ) )
    value = self.options.get( switch, default )
    if switch in ( 'LFNs', 'Directory' ):
      if not value:
        if not sys.stdin.isatty():
          self.setLFNsFromTerm()
          value = self.options.get( switch, default )
      if value:
        value = self.getLFNsFromList( value, directories = switch == 'Directory' )
      if value and self.setLastFile and switch == 'LFNs':
        gLogger.always( "Got %d LFNs" % len( value ) )
        if self.setLastFile != self.lastFile:
          self. setLastFile = False
          open( self.lastFile, 'w' ).write( '\n'.join( sorted( value ) ) )
    if isinstance( value, set ):
      value = sorted( value )
    return value

  def getBKQuery( self, visible = None ):
    mandatoryKeys = set( ( 'ConfigName', 'ConfigVersion', 'Production' ) )
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
    bkQueryDict = self.bkQuery.getQueryDict()
    if not set( bkQueryDict ) & mandatoryKeys:
      self.bkQuery = None
      return None
    self.bkQuery.setExceptFileTypes( self.exceptFileTypes )
    if 'DQFlags' in self.options:
      self.bkQuery.setDQFlag( self.options['DQFlags'] )
    if 'StartDate' in self.options:
      self.bkQuery.setOption( 'StartDate', self.options['StartDate'] )
    if 'EndDate' in self.options:
      self.bkQuery.setOption( 'EndDate', self.options['EndDate'] )
    if 'ReplicaFlag' in self.options:
      self.bkQuery.setOption( 'ReplicaFlag', self.options['ReplicaFlag'] )
    if 'TCK' in self.options:
      self.bkQuery.setOption( 'TCK', self.options['TCK'] )
    return self.bkQuery

  def getRequestID( self, prod = None ):
    """ Get the request ID for a single production """
    from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
    if not prod:
      prod = self.options.get( 'Productions', [] )
    requestID = None
    if isinstance( prod, basestring ):
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
