################################################################################
# $HeadURL:  $
################################################################################
__RCSID__  = "$Id:  $"

from DIRAC import gLogger, gConfig

from xml.dom.minidom import Document

import threading, time, signal

class TimedOutError( Exception ):
  pass

class TestBase( threading.Thread ):
  '''
    Base class for all SLS tests.
  '''
  def __init__( self, testName, testPath, workdir ):
    # Initialize the Threading
    threading.Thread.__init__( self )
    
    # Get fresh data from the CS
    self.testConfig = self.getConfig( testPath )
    self.workdir    = workdir
    
    timeout = 600
    
    # If not timeout provided, the default is 600 secs.
    #timeout   = testConfig.get( 'timeout', 600 ) 
#    self.t    = threading.Timer( timeout, self.nuke )
    self.name = testName 

  def getConfig( self, path ):
    return 1

  def nuke( self ):
    pass
  
  def handler( self, signum, frame ):
    gLogger.info( 'Handler' )
    print 'handler'
    raise TimedOutError()
  
  def run( self ):
    
    saveHandler = signal.signal( signal.SIGALRM, self.handler )
    signal.alarm( 10 )
    try:
      gLogger.info( 'Start run' )
      print 'Start run'
      time.sleep( 15 )
    except TimedOutError:
      gLogger.info( 'Start run' )    
      print 'End run'
    finally:
      signal.signal( signal.SIGALRM, saveHandler )  
    signal.alarm( 0 )
 
 
#def timedExecution( timeOut ):
#  print "in timedExecution, timeOut is %s sec" % timeOut
#  class TimedOutError( Exception ): pass
#  def timeout(f):
#    print "in timeout"
#    def inner(*args):
#      print "in inner"
#      def handler(signum, frame):
#        raise TimedOutError()
#      saveHandler = signal.signal(signal.SIGALRM, handler)
#      signal.alarm(timeOut)
#      try:
#        print "start executing"
#        ret = f()
#      except TimedOutError:
#        return { "OK" : False, "Message" : "timed out after %s sec" % timeOut }
#      finally:
#        signal.signal(signal.SIGALRM, saveHandler)
#      signal.alarm(0)
#      return ret
#    return inner
#  return timeout 
      
  
#  def getConfig( self, testPath ):
#    
#    try:
#      
#      modConfig = gConfig.getOptionsDict( testPath )
#      if not modConfig[ 'OK' ]:
#         gLogger.exception( 'Error loading "%s".\n %s' % ( testPath, modConfig[ 'Message' ] ) )
#         self.nuke()
#      modConfig = modConfig[ 'Value' ]
#      
#      sections = gConfig.getSections( testPath ) 
#      if not sections[ 'OK' ]:
#         gLogger.exception( 'Error loading "%s".\n %s' % ( testPath, sections[ 'Message' ] ) )
#         self.nuke()
#      sections = sections[ 'Value' ]
#      
#      for section in sections:
#        
#        sectionPath   = '%s/%s' % ( testPath, section )
#        sectionConfig = gConfig.getOptionsDict( sectionPath )
#        
#        if not sectionConfig[ 'OK' ]:
#          gLogger.exception( 'Error loading "%s".\n %s' % ( sectionPath, sectionConfig[ 'Message' ] ) )
#          self.nuke()
#        sectionConfig = sectionConfig[ 'Value' ]
#          
#        modConfig[ section ] = sectionConfig
#        
#      return modConfig  
#          
#    except Exception, e:
#      
#      gLogger.exception( 'Exception loading configuration.\n %s' % e )
#      self.nuke()
   
#  def run( self ):
#    
#    gLogger.info( 'Starting %s thread' % self.name )
#    # Start timer
#    self.t.start()
#    
#    try:
#      
#      self.launchTest()
#      
#    except Exception, e:
#      
#      gLogger.exception( '%s test crashed. \n %s' % ( self.name, e ) )
#        
#    # kill via a schedule
#    gLogger.info( str( self ) + ' Happily scheduling my own destruction.' )
#    self.t.cancel()
#    self.t = threading.Timer( 1, self.nuke )
#    self.t.start() 
#   
#  def launchTest( self ):
#    '''
#      This function should have all logic needed to run the test.
#      Overwrite me.
#    '''
#    pass 
#   
#  def nuke( self ):
#    '''
#      Self destruction by exiting thread.
#    '''
#    gLogger.info( str( self ) + ' au revoir.' )
#    sys.exit()    
#    
#  def writeText( self ):
#    pass  
    
  def writeXml( self, xmlList, fileName, useStub = True, path = None ):

    if path is None:
      path = self.workdir
    
    XML_STUB = [ { 
                  'tag'   : 'serviceupdate',
                  'attrs' : [ ( 'xmlns', 'http://sls.cern.ch/SLS/XML/update' ),
                              ( 'xmlns:xsi', 'http://www.w3.org/2001/XMLSchema-instance' ),
                              ( 'xsi:schemaLocation', 'http://sls.cern.ch/SLS/XML/update http://sls.cern.ch/SLS/XML/update.xsd' )
                            ],
                  'nodes' : xmlList }
                ]
    
    
    d = Document()
    d = self._writeXml( d, d, ( XML_STUB and useStub ) or xmlList )
    
    gLogger.info( d.toxml() )
    
    file = open( '%s/%s' % ( path, name ), 'w' )
    try:
      file.write( d.toxml() )
    finally:  
      file.close()

  def _writeXml( self, doc, topElement, elementList ):

    if elementList is None:
      return topElement
    
    elif not isinstance( elementList, list ):
      tn = doc.createTextNode( str( elementList ) )
      topElement.appendChild( tn )
      return topElement

    for d in elementList:
      
      el = doc.createElement( d[ 'tag' ] )
      
      for attr in d.get( 'attrs', [] ):
        el.setAttribute( attr[0], attr[1] )
        
      el = self._writeXml( doc, el, d.get( 'nodes', None ) )
      topElement.appendChild( el )
    
    return topElement   

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF