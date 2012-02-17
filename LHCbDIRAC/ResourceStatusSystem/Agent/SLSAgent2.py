################################################################################
# $HeadURL $
################################################################################
__RCSID__  = "$Id:  $"
AGENT_NAME = 'ResourceStatus/SLSAgent2'

from DIRAC                                import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.Core.Base.AgentModule          import AgentModule
from DIRAC.ResourceStatusSystem.Utilities import Utils

import signal, time

class TimedOutError( Exception ): pass

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
    
#    except Exception, e:
#      _msg = 'Error initializing: %s' %e
#      gLogger.exception( _msg )
#      return S_ERROR( _msg )  
  
  def execute( self ):
    '''
      bli bli bli
    '''
        
    # If are there running old threads, we terminate them. Well, we terminate all
    # them, via scheduled nuke.
#    for test in self.tests:
#  
#      if test[1].isAlive():
#        gLogger.error( '%s thread is taking too long, killing it.' % th[0] )
#        
#      test[1].nuke()
#      self.tests.remove( test )
        
    for tName, tModule in self.tModules.items():
      
      gLogger.info( tName )
    
      tClass          = getattr( tModule[ 'mod' ], '%sTest' %tName )
      cTest           = tClass( tModule[ 'path' ], self.workdir )
      
      elementsToCheck = cTest.getElementsToCheck()
          
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
        
        
#      try:
#        
#        cTest = tModule[ 'mod' ].TestModule( tName, tModule[ 'path' ], self.workdir )
#        self.tests.append( [ tName, cTest ] )
#        cTest.start()
#        del cTest    
#        
#      except Exception, e:
#        _msg = 'Error running %s, %s' % ( tName, e )
#        gLogger.exception( _msg )  
#        
    return S_OK()        
  
  def sigHandler( self, signum, frame ):
    raise TimedOutError()
        
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