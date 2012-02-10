################################################################################
# $HeadURL:  $
################################################################################
__RCSID__  = "$Id:  $"

from DIRAC import gLogger, gConfig

from xml.dom.minidom import Document

import threading, sys

class TestBase( threading.Thread ):
  '''
    Base class for all SLS tests.
  '''
  def __init__( self, testName, testPath ):
    # Initialize the Threading
    threading.Thread.__init__( self )
    
    # Get fresh data from the CS
    self.testConfig = self.getConfig( testPath )
    
    timeout = 600
    
    # If not timeout provided, the default is 600 secs.
    #timeout   = testConfig.get( 'timeout', 600 ) 
    self.t    = threading.Timer( timeout, self.nuke )
    self.name = testName 
  
  def getConfig( self, testPath ):
    
    try:
      
      modConfig = gConfig.getOptionsDict( testPath )
      if not modConfig[ 'OK' ]:
         gLogger.exception( 'Error loading "%s".\n %s' % ( testPath, modConfig[ 'Message' ] ) )
         self.nuke()
      modConfig = modConfig[ 'Value' ]
      
      sections = gConfig.getSections( testPath ) 
      if not sections[ 'OK' ]:
         gLogger.exception( 'Error loading "%s".\n %s' % ( testPath, sections[ 'Message' ] ) )
         self.nuke()
      sections = sections[ 'Value' ]
      
      for section in sections:
        
        sectionPath   = '%s/%s' % ( testPath, section )
        sectionConfig = gConfig.getOptionsDict( sectionPath )
        
        if not sectionConfig[ 'OK' ]:
          gLogger.exception( 'Error loading "%s".\n %s' % ( sectionPath, sectionConfig[ 'Message' ] ) )
          self.nuke()
        sectionConfig = sectionConfig[ 'Value' ]
          
        modConfig[ section ] = sectionConfig
        
      return modConfig  
          
    except Exception, e:
      
      gLogger.exception( 'Exception loading configuration.\n %s' % e )
      self.nuke()
   
  def run( self ):
    
    gLogger.info( 'Starting %s thread' % self.name )
    # Start timer
    self.t.start()
    
    try:
      
      self.launchTest()
      
    except Exception, e:
      
      gLogger.exception( '%s test crashed. \n %s' % ( self.name, e ) )
        
    # kill via a schedule
    gLogger.info( str( self ) + ' Happily scheduling my own destruction.' )
    self.t.cancel()
    self.t = threading.Timer( 1, self.nuke )
    self.t.start() 
   
  def launchTest( self ):
    '''
      This function should have all logic needed to run the test.
      Overwrite me.
    '''
    pass 
   
  def nuke( self ):
    '''
      Self destruction by exiting thread.
    '''
    gLogger.info( str( self ) + ' au revoir.' )
    sys.exit()    
    
  def writeXml( self, xmlList = None ):
    
    d = Document()
#    el = d.createElement( 'serviceupdate' )
#    el.setAttribute( 'xmlns', 'http://sls.cern.ch/SLS/XML/update' )
#    el.setAttribute( 'xmlns:xsi', 'http://www.w3.org/2001/XMLSchema-instance' )
#    el.setAttribute( 'xsi:schemaLocation', 'http://sls.cern.ch/SLS/XML/update http://sls.cern.ch/SLS/XML/update.xsd' )
#    d.appendChild( el )
    
    XML_STUB = { 
                'serviceupdate' : { 
                             'attrs' : [ ( 'xmlns', 'http://sls.cern.ch/SLS/XML/update' ),
                                         ( 'xmlns:xsi', 'http://www.w3.org/2001/XMLSchema-instance' ),
                                         ( 'xsi:schemaLocation', 'http://sls.cern.ch/SLS/XML/update http://sls.cern.ch/SLS/XML/update.xsd' )
                                       ]
                                  } 
                } 
    
    XML_STUB2 = [ { 
                  'tag'   : 'serviceupdate',
                  'attrs' : [ ( 'xmlns', 'http://sls.cern.ch/SLS/XML/update' ),
                              ( 'xmlns:xsi', 'http://www.w3.org/2001/XMLSchema-instance' ),
                              ( 'xsi:schemaLocation', 'http://sls.cern.ch/SLS/XML/update http://sls.cern.ch/SLS/XML/update.xsd' )
                            ]
                  }
                ]
    
    
    d = self._writeXml2( d, d, XML_STUB2 )
    
    gLogger.info( d.toxml() )

  def _writeXml2( self, doc, topElement, elementList ):

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
    
  def _writeXml( self, doc, topElement, elementDict ):

    if elementDict is None:
      return topElement
    elif not isinstance( elementDict, dict ):
      tn = doc.createTextNode( str( elementDict ) )
      topElement.appendChild( tn )
      return topElement

    for k,v in elementDict.items():
      
      el = doc.createElement( k )
      for attr in v.get( 'attrs', [] ):
        el.setAttribute( attr[0], attr[1] )
        
      el = self._writeXml( doc, el, v.get( 'nodes', None ) )
      topElement.appendChild( el )
    
    return topElement  

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF