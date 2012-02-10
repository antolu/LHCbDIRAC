################################################################################
# $HeadURL $
################################################################################
__RCSID__  = "$Id:  $"
AGENT_NAME = 'ResourceStatus/SLSAgent2'

from DIRAC                                import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.Core.Base.AgentModule          import AgentModule
from DIRAC.ResourceStatusSystem.Utilities import Utils

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
      _tNames       = gConfig.getSections( _testPath, [] ).get( 'Value', [] )
    
      self.tModules = {}
      self.tests    = []
      
      # Load test modules 
      for tName in _tNames:

        if tName in [ 'test1', 'test2' ]:
          continue
        
        try:
      
          modConfig = gConfig.getOptionsDict( '%s/%s' % ( _testPath, tName ) )
          if not modConfig[ 'OK' ]:
            modConfig = {}
          else:
            modConfig = modConfig[ 'Value' ]
           
          _modPath = 'DIRAC.ResourceStatusSystem.Agent.SLSTests.%s.%s' % ( tName, tName )
          testMod  = Utils.voimport( _modPath )
          
          self.tModules[ tName ] = { 'mod' : testMod, 'config' : modConfig }
          
          gLogger.info( '-> Loaded test module %s' % tName )
          
        except ImportError:
          gLogger.warn( 'Error loading test module %s' % tName )          
   
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
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF