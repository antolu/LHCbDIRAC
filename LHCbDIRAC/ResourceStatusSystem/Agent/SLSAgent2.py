################################################################################
# $HeadURL $
################################################################################
__RCSID__  = "$Id:  $"
AGENT_NAME = 'ResourceStatus/SLSAgent2'

from DIRAC                                import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.Core.Base.AgentModule          import AgentModule
from DIRAC.Core.Utilities.ThreadPool      import ThreadPool
from DIRAC.ResourceStatusSystem.Utilities import Utils

import Queue

class SLSAgent2( AgentModule ):
  '''
   bla bla bla
  '''
  
  def initialize( self ):
    '''
    ble ble ble
    '''
    
    try:
    
      _testPath     = '%s/tests' % self.am_getModuleParam( 'section' )
      _maxThreads   = self.am_getOption( 'maxThreadsInPool', 1 )
      _tNames       = gConfig.getSections( _testPath, [] )
      
      self.tModules = {}
      self.tests    = []
      
      # Load test modules 
      for tName in _tNames:
        
        try:
      
          modConfig = gConfig.getOptionsDict( '%s/%s' % ( _testPath, tName ) )
          if not modConfig[ 'OK' ]:
            modConfig = {}
          else:
            modConfig = modConfig[ 'Value' ]
           
          _modPath = 'DIRAC.ResourceStatusSystem.Agent.SLSTests.%s' % tName
          testMod  = Utils.voimport( _modPath )
          
          self.tModules[ tName ] = { 'mod' : testMod, 'config' : modConfig }
          
        except ImportError:
          gLogger.warn( 'Error loading test module %s' % _tName )          
      
      self.testQueue  = Queue.Queue()
      self.threadPool = ThreadPool( _maxThreads, _maxThreads )
    
      if not self.threadPool:
        self.log.error( 'Can not create Thread Pool' )
        return S_ERROR( 'Can not create Thread Pool' )

      for _i in xrange( _maxThreads ):
        self.threadPool.generateJobAndQueueIt( self._executeTest, args = ( None, ) )
    
      return S_OK()
    
    except Exception, e:
      _msg = 'Error initializing: %s' %e
      gLogger.exception( _msg )
      return S_ERROR( _msg )  
  
  def execute( self ):
    '''
      bli bli bli
    '''
        
    # If are there running old threads, we terminate them. Well, we terminate all
    # them, via scheduled nuke.
    for test in self.tests:
  
      if test[1].isAlive():
        gLogger.error( '%s thread is taking too long, killing it.' % th[0] )
        
      test[1].nuke()
      self.tests.remove( test )
        
    for tName, tModule in self.tModules.items():
      
      try:
        
        testConfig = tModule.get( 'config', {} )
        testModule = tModule[ 'mod' ]
        
        cTest = testModule( tName, testConfig )
        self.tests.append( [ tName, cTest ] )
        cTest.start()
        del cTest    
        
      except Exception, e:
        _msg = 'Error running %s, %s' % ( testModule.__class__, e )
        gLogger.exception( _msg )    
        
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
  
  def _executeTest( self, args ):
    
    while True:
      
      tModule = self.testQueue.get()
      
      testModule = tModule[ 'mod' ]
      testConfig = tModule[ 'config' ]
        
      gLooger.info( 'Starting %s test' % ( testModule.__class__ ) )
      try:
        testModule( testConfig ).launch()
      except Exception, e:
        _msg = 'Error running %s, %s' % ( testModule.__class__, e )
        gLogger.exception( _msg )    
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF