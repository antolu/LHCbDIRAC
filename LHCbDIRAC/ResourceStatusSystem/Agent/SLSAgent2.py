################################################################################
# $HeadURL $
################################################################################
__RCSID__  = "$Id:  $"
AGENT_NAME = 'ResourceStatus/SLSAgent2'

from DIRAC                                import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.Core.Base.AgentModule          import AgentModule
from DIRAC.ResourceStatusSystem.Utilities import Utils
from DIRAC.Core.Utilities.ProcessPool     import ProcessPool

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
      
      gLogger.info( '%s: Launching probes' % tName )
        
      mTest           = tModule[ 'mod' ]
      elementsToCheck = mTest.getElementsToCheck()

      if not elementsToCheck[ 'OK' ]:
        gLogger.error( elementsToCheck[ 'Message' ] )
        continue
      elementsToCheck = elementsToCheck[ 'Value' ]

      for elementToCheck in elementsToCheck:
        
        res = self.processPool.createAndQueueTask( runSLSProbe,
                                                   args              = ( mTest.runProbe, elementToCheck ),
                                                   callback          = slsCallback,
                                                   exceptionCallback = slsExceptionCallback )
        
        if not res[ 'OK' ]:
          gLogger.error( 'Error queuing task %s' % res[ 'Message' ] )
        
    print self.processPool.processAllResults() 
    print self.processPool.finalize()           
           
    return S_OK()        
  
  def finalize( self ):
    '''
     blu blu blu
    '''
    
    gLogger.info( 'Terminating all threads' )
    
    # Stop threads by force
    for test in self.tests:
      
      test[1].nuke()
      
    return S_OK()      

################################################################################

class TimedOutError( Exception ): pass

def handler( signum, frame ):
  raise TimedOutError() 

def runSLSProbe( func, probeInfo ):
    
  gLogger.info( probeInfo )
  
  saveHandler = signal.signal( signal.SIGALRM, handler )
  signal.alarm( 1 )
  
  try:
    gLogger.info( 'Start run' )
    res = func( probeInfo )    
    gLogger.info( 'End run' )
  except TimedOutError:
    gLogger.info( 'Killed' )
    res = S_ERROR( 'Timeout' )    
  finally:
    signal.signal( signal.SIGALRM, saveHandler )
      
  signal.alarm( 0 )  
  return res

def slsCallback( task, taskResult ):
  gLogger.info( 'slsCallback' )
  gLogger.info( taskResult )
  
def slsExceptionCallback( task, exec_info ):
  gLogger.info( 'slsExceptionCallback' )
  gLogger.info( exec_info )  
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF