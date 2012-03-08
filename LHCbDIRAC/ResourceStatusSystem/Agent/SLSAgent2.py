# $HeadURL: $
''' SLSAgent2

  This agent is a replacement for the current SLSAgent, which is sequential and
  not handling properly timeouts and unexpected exceptions. In contrast, this
  agent sets by default a timeout of 120 secs on the processes running the 
  tests. Tests are defined in the CS, on the agent directory.   
    
'''

from DIRAC                                import S_OK, S_ERROR, gConfig, rootPath
from DIRAC.Core.Base.AgentModule          import AgentModule
from DIRAC.Core.Utilities.ProcessPool     import ProcessPool
from DIRAC.ResourceStatusSystem.Utilities import Utils

from LHCbDIRAC.ResourceStatusSystem.Utilities import SLSXML

__RCSID__  = '$Id:  $'
AGENT_NAME = 'ResourceStatus/SLSAgent2'

class SLSAgent2( AgentModule ):
  '''
  SLSAgent, it runs all SLS sensors defined on the agent configuration, more
  specifically under tests.
  
  This agent runs tests sequentially, but using a process pool of size four.
  In case some process gets stuck, it applies a timeout by default of 5 minutes,
  which can be overwritten on the test configuration.
  '''

  # Too many public methods
  # pylint: disable-msg=R0904
  
  def initialize( self ):
    '''
    Gets the tests from the CS ( only their names ) and generates the process 
    pool. 
    
    Note: if a new test is added in the CS, the agent will need to be restarted
    to pick it up.
    '''

    # Attribute defined outside __init__  
    # pylint: disable-msg=W0201

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
          
        self.log.info( '-> Loaded test module %s' % tName )
          
      except ImportError:
        self.log.warn( 'Error loading test module %s' % tName )          
   
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
      
      self.log.info( '%s: Getting test configuration' % tName )
      testPath = tModule[ 'path' ]
      
      testConfig = self.getTestConfig( tName, testPath )
      if not testConfig[ 'OK' ]:
        self.log.error( testConfig[ 'Message' ] )
        self.log.info( '%s: Skipping... after getTestConfig' % tName )
        continue
      testConfig = testConfig[ 'Value' ]
      
      timeout = testConfig.get( 'timeout', 120 )
      
      self.log.info( '%s: Getting test probes' % tName )        
      mTest           = tModule[ 'mod' ]

      elementsToCheck = mTest.getProbeElements()
      if not elementsToCheck[ 'OK' ]:
        self.log.error( elementsToCheck[ 'Message' ] )
        self.log.info( '%s: Skipping... after getProbeElements' % tName )
        continue
      elementsToCheck = elementsToCheck[ 'Value' ]

      setupProbe = mTest.setupProbes( testConfig )
      if not setupProbe[ 'OK' ]:
        self.log.error( setupProbe[ 'Message' ] )
        self.log.info( '%s: Skipping... after setupProbes' % tName )
        continue

      self.log.info( '%s: Launching test probes' % tName )
      for elementToCheck in elementsToCheck:
        
        res = self.processPool.createAndQueueTask( mTest.runProbe,
                                                   args              = ( elementToCheck, ),
                                                   kwargs            = { 'testConfig' : testConfig },
                                                   callback          = SLSXML.writeSLSXml,
                                                   timeOut           = timeout )
        
        if not res[ 'OK' ]:
          self.log.error( 'Error queuing task %s' % res[ 'Message' ] )
        
    self.processPool.processAllResults() 
    self.processPool.finalize()           
           
    return S_OK()        

  def getTestConfig( self, testName, testPath ):
    '''
    Generic function that gets the configuration for the test from the CS.
    It also adds the agent working dir to the configuration ( used to write 
    scripts, temp files, etc... )
    '''
        
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
  
  def finalize( self ):
    '''
    To be done, but it sould kill all running processes.
    '''
    self.log.info( 'Terminating all threads' )
        
    return S_OK()      
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF