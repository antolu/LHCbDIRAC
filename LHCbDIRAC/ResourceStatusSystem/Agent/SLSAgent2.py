################################################################################
# $HeadURL $
################################################################################
__RCSID__  = "$Id:  $"
AGENT_NAME = 'ResourceStatus/SLSAgent2'

from DIRAC                                import S_OK, S_ERROR, gConfig, gLogger, rootPath
from DIRAC.Core.Base.AgentModule          import AgentModule
from DIRAC.ResourceStatusSystem.Utilities import Utils
from DIRAC.Core.Utilities.ProcessPool     import ProcessPool

from LHCbDIRAC.ResourceStatusSystem.Utilities import SLSXML

import signal

class SLSAgent2( AgentModule ):
  '''
  SLSAgent, it runs all SLS sensors defined on the agent configuration, more
  specifically under tests.
  
  This agent runs tests sequentially, but using a process pool of size four.
  In case some process gets stuck, it applies a timeout by default of 5 minutes,
  which can be overwritten on the test configuration.
  '''
  
  def initialize( self ):
    '''
    Gets the tests from the CS ( only their names ) and generates the process 
    pool. 
    
    Note: if a new test is added in the CS, the agent will need to be restarted
    to pick it up.
    '''

    webRoot = self.am_getOption( 'webRoot', 'webRoot/www/sls2/')

    self.workdir  = '%s/%s' % ( rootPath, webRoot )
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
    Let's run the tests:
    1.- get test configuration from the CS
    2.- get probes that the test will run
    3.- set up ( if needed ) environment, scripts for the probes
    4.- add the probes to the processPool.
    5.- after execution, if successful, it will write an xml report.
    '''
        
    for tName, tModule in self.tModules.items():
      
      gLogger.info( '%s: Getting test configuration' % tName )
      testPath = tModule[ 'path' ]
      
      testConfig = self.getTestConfig( tName, testPath )
      if not testConfig[ 'OK' ]:
        gLogger.error( testConfig[ 'Message' ] )
        gLogger.info( '%s: Skipping... after getTestConfig' % tName )
        continue
      testConfig = testConfig[ 'Value' ]
      
      timeout = testConfig.get( 'timeout', 120 )
      
      gLogger.info( '%s: Getting test probes' % tName )        
      mTest           = tModule[ 'mod' ]

      elementsToCheck = mTest.getProbeElements()
      if not elementsToCheck[ 'OK' ]:
        gLogger.error( elementsToCheck[ 'Message' ] )
        gLogger.info( '%s: Skipping... after getProbeElements' % tName )
        continue
      elementsToCheck = elementsToCheck[ 'Value' ]

      setupProbe = mTest.setupProbes( testConfig )
      if not setupProbe[ 'OK' ]:
        gLogger.error( setupProbe[ 'Message' ] )
        gLogger.info( '%s: Skipping... after setupProbes' % tName )
        continue

      gLogger.info( '%s: Launching test probes' % tName )
      for elementToCheck in elementsToCheck:
        
        res = self.processPool.createAndQueueTask( mTest.runProbe,
                                                   args              = ( elementToCheck, ),
                                                   kwargs            = { 'testConfig' : testConfig },
                                                   callback          = SLSXML.writeSLSXml,
                                                   timeOut           = timeout )
        
        if not res[ 'OK' ]:
          gLogger.error( 'Error queuing task %s' % res[ 'Message' ] )
        
    self.processPool.processAllResults() 
    self.processPool.finalize()           
           
    return S_OK()        

  def getTestConfig( self, testName, testPath ):
    '''
    Generic function that gets the configuration for the test from the CS.
    It also adds the agent working dir to the configuration ( used to write 
    scripts, temp files, etc... )
    '''
    
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
      
      modConfig[ 'workdir'  ] = self.workdir
      modConfig[ 'testName' ] = testName
        
      return S_OK( modConfig )  
          
    except Exception, e:
      
      return S_ERROR( 'Exception loading configuration.\n %s' % e )
  
  def finalize( self ):
    '''
    To be done, but it sould kill all running processes.
    '''
    gLogger.info( 'Terminating all threads' )
        
    return S_OK()      
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF