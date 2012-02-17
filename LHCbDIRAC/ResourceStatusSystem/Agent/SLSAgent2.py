################################################################################
# $HeadURL $
################################################################################
__RCSID__  = "$Id:  $"
AGENT_NAME = 'ResourceStatus/SLSAgent2'

from DIRAC                                import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.Core.Base.AgentModule          import AgentModule
from DIRAC.ResourceStatusSystem.Utilities import Utils
from DIRAC.Core.Utilities.ProcessPool     import ProcessPool

from xml.dom.minidom                      import Document

import signal

class SLSAgent2( AgentModule ):
  '''
   bla bla bla
  '''
  
  def initialize( self ):
    '''
    ble ble ble
    '''

    self.workdir  = self.am_getWorkDirectory()
    self.tModules = {}
    self.tests    = []
    
    testPath = '%s/tests' % self.am_getModuleParam( 'section' )
    tNames   = gConfig.getSections( testPath, [] ).get( 'Value', [] )
   
    self.processPool = ProcessPool( maxSize = 4, maxQueuedRequests = 25 )

    # Load test modules 
    for tName in tNames:
        
      try:
         
        modPath = 'DIRAC.ResourceStatusSystem.Agent.SLSTests.%s' % tName
        testMod = Utils.voimport( modPath )
          
        self.tModules[ tName ] = { 'mod' : testMod, 'path' : '%s/%s' % ( testPath, tName ) }
          
        gLogger.info( '-> Loaded test module %s' % tName )
          
      except ImportError:
        gLogger.warn( 'Error loading test module %s' % tName )          
   
    return S_OK()
  
  def execute( self ):
    '''
      bli bli bli
    '''
        
    for tName, tModule in self.tModules.items():
      
      gLogger.info( '%s: Getting test configuration' % tName )
      testPath = tModule[ 'path' ]
      
      testConfig = self.getTestConfig( testPath )
      if not testConfig[ 'OK' ]:
        gLogger.error( testConfig[ 'Message' ] )
        continue
      testConfig = testConfig[ 'Value' ]
      
      gLogger.info( '%s: Getting test probes' % tName )        
      mTest           = tModule[ 'mod' ]
      elementsToCheck = mTest.getElementsToCheck()

      if not elementsToCheck[ 'OK' ]:
        gLogger.error( elementsToCheck[ 'Message' ] )
        continue
      elementsToCheck = elementsToCheck[ 'Value' ]

      gLogger.info( '%s: Launching test probes' % tName )
      for elementToCheck in elementsToCheck:
        
        res = self.processPool.createAndQueueTask( runSLSProbe,
                                                   args              = ( mTest.runProbe, elementToCheck ),
                                                   kwargs            = { 'testConfig' : testConfig },
                                                   callback          = self.writeXml,
                                                   exceptionCallback = slsExceptionCallback )
        
        if not res[ 'OK' ]:
          gLogger.error( 'Error queuing task %s' % res[ 'Message' ] )
        
    self.processPool.processAllResults() 
    self.processPool.finalize()           
           
    return S_OK()        

  def getTestConfig( self, testPath ):
    
    try:
      
      modConfig = gConfig.getOptionsDict( testPath )
      if not modConfig[ 'OK' ]:
         return S_ERROR( 'Error loading "%s".\n %s' % ( testPath, modConfig[ 'Message' ] ) )
       
      modConfig = modConfig[ 'Value' ]
      sections  = gConfig.getSections( testPath ) 
      
      if not sections[ 'OK' ]:
         return S_ERROR( 'Error loading "%s".\n %s' % ( testPath, sections[ 'Message' ] ) )
      sections = sections[ 'Value' ]
      
      for section in sections:
        
        sectionPath   = '%s/%s' % ( testPath, section )
        sectionConfig = gConfig.getOptionsDict( sectionPath )
        
        if not sectionConfig[ 'OK' ]:
          return S_ERROR( 'Error loading "%s".\n %s' % ( sectionPath, sectionConfig[ 'Message' ] ) )
        
        sectionConfig        = sectionConfig[ 'Value' ]  
        modConfig[ section ] = sectionConfig
      
      modConfig[ 'workdir' ] = self.workdir
        
      return S_OK( modConfig )  
          
    except Exception, e:
      
      return S_ERROR( 'Exception loading configuration.\n %s' % e )
  
  def finalize( self ):
    '''
     blu blu blu
    '''
    gLogger.info( 'Terminating all threads' )
        
    return S_OK()      

################################################################################

class TimedOutError( Exception ): pass

def handler( signum, frame ):
  raise TimedOutError() 

def runSLSProbe( testArgs, testConfig = {} ):
    
  func      = testArgs[ 0 ]
  probeInfo = testArgs[ 1 ]

  gLogger.info( probeInfo )
  
  saveHandler = signal.signal( signal.SIGALRM, handler )
  signal.alarm( 1 )
  
  try:
    gLogger.info( 'Start run' )
    res = func( probeInfo, testConfig )    
    gLogger.info( 'End run' )
  except TimedOutError:
    gLogger.info( 'Killed' )
    res = S_ERROR( 'Timeout' )    
  finally:
    signal.signal( signal.SIGALRM, saveHandler )
      
  signal.alarm( 0 )  
  return res

#def writeXml( xmlList, fileName, useStub = True, path = None ):
def writeXml( xmlTuple ):  

  print xmlTuple

  xmlList, testConfig = xmlTuple

  

  return 1

  if path is None:
    path = workdir
    
  XML_STUB = [ { 
                'tag'   : 'serviceupdate',
                'attrs' : [ ( 'xmlns', 'http://sls.cern.ch/SLS/XML/update' ),
                            ( 'xmlns:xsi', 'http://www.w3.org/2001/XMLSchema-instance' ),
                            ( 'xsi:schemaLocation', 'http://sls.cern.ch/SLS/XML/update http://sls.cern.ch/SLS/XML/update.xsd' )
                          ],
                'nodes' : xmlList }
              ]
    
    
  d = Document()
  d = _writeXml( d, d, ( XML_STUB and useStub ) or xmlList )
    
  gLogger.info( d.toxml() )
    
  file = open( '%s/%s' % ( path, name ), 'w' )
  try:
    file.write( d.toxml() )
  finally:  
    file.close()

def _writeXml( doc, topElement, elementList ):

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
        
    el = _writeXml( doc, el, d.get( 'nodes', None ) )
    topElement.appendChild( el )
    
  return topElement 


#def slsCallback( task, taskResult ):
#  gLogger.info( 'slsCallback' )
#  gLogger.info( taskResult )
  
def slsExceptionCallback( task, exec_info ):
  gLogger.info( 'slsExceptionCallback' )
  gLogger.info( exec_info )  
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF