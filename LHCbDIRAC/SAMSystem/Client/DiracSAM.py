''' LHCb SAM Dirac Class

   The Dirac SAM class inherits generic VO functionality from the Dirac API base class.
'''

from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.SiteCEMapping                  import getCESiteMapping
from DIRAC.Interfaces.API.Dirac                          import Dirac, S_OK, S_ERROR

from LHCbDIRAC.Core.Utilities.DetectOS     import NativeMachine
from LHCbDIRAC.Interfaces.API.LHCbJob      import LHCbJob
from LHCbDIRAC.SAMSystem.Client.LHCbSAMJob import LHCbSAMJob
from LHCbDIRAC.Workflow.Utilities.Utils    import getStepDefinition, addStepToWorkflow

COMPONENT_NAME = 'DiracSAMAPI'
__RCSID__      = '$Id$'

class DiracSAM( Dirac ):
  '''
    DiracSAM: extension of Dirac Interface for SAM jobs
    
    It provides the following methods:
    - getSuitableCEs
    - submitSAMJob
    - _promptUser
  '''

  def __init__( self ):
    '''
       Instantiates the Workflow object and some default parameters.
    '''
    Dirac.__init__( self )
    
    self.opsH = Operations()
    
    self.gridType    = 'LCG'
    self.bannedSites = self.opsH.getValue( 'SAM/BannedSites', [] )

  def getSuitableCEs( self ):
    '''
      Gets all CEs ( excluding the ones of banned sites )
    '''
    
    self.log.info( 'Banned SAM sites are: %s' % ( ', '.join( self.bannedSites ) ) )
    
    ceMapping = getCESiteMapping( self.gridType )
    if not ceMapping[ 'OK' ]:
      return ceMapping
    ceMapping = ceMapping[ 'Value' ]
    
    validCEs = []
    
    for ce, site in ceMapping.iteritems():
      if not site in self.bannedSites:
        validCEs.append( ce )
    
    return S_OK( validCEs )

  def submitSAMSWJob( self, ce, lockDeletion = False, areaDeletion = False,
                      installSW = True, projectURL = '', removeSW = True,
                      scriptName = '' ):
    '''
      Submit a SAMsw job.
    '''
    
    job = LHCbSAMJob()
    # setDefaults
    res = job.setDefaults()   
    if res[ 'OK' ]:
      # setDestinationCE
      res = job.setDestinationCE( ce )    
    if res[ 'OK' ]:
      # setSharedAreaLock
      self.log.verbose( 'Flag to remove lock on shared area is %s' % lockDeletion )
      res = job.setSharedAreaLock( forceDeletion = lockDeletion )
    if res[ 'OK' ]:
      # checkSystemConfiguration
      res = job.checkSystemConfiguration()    
    if res[ 'OK' ]:
      config = NativeMachine().CMTSupportedConfig()[ 0 ]
      res = job.setSystemConfig( config )
    if res[ 'OK' ]:
      res = job.installSoftware( forceDeletion = areaDeletion, 
                                 enableFlag = installSW, 
                                 installProjectURL = projectURL )
    if res[ 'OK' ]:
      res = job.reportSoftware( enableFlag = removeSW  )

    if res[ 'OK' ]:
      res = job.runTestScript( scriptName = scriptName )
      
    if res[ 'OK' ]:
      res = job.setSAMGroup( "SAMsw" )
    
    self.log.verbose( 'Job JDL is: \n%s' % job._toJDL() )
        
    return self.submit( job )      
              
    

  def submitSAMJob( self, ce, removeLock = False, deleteSharedArea = False, 
                    logUpload = True, mode = 'wms', enable = True, 
                    softwareEnable = True, reportEnable = True, install_project = None, 
                    scriptName = '' ):
    '''
       Submit a SAM test job to an individual CE.
    '''
    
    #FIXME: why ?
    # if we install the applications we do not run the report
    if softwareEnable:
      reportEnable = False
      
    config = NativeMachine().CMTSupportedConfig()[ 0 ]  
      
    job = LHCbSAMJob()
    
    # setDefaults
    res = job.setDefaults()   
    if res[ 'OK' ]:
      # setDestinationCE
      res = job.setDestinationCE( ce )
    else:
      self.log.error( res['Message'] )
      return res
    if res[ 'OK' ]:
      # setSharedAreaLock
      self.log.verbose( 'Flag to remove lock on shared area is %s' % ( removeLock ) )
      res = job.setSharedAreaLock( forceDeletion = removeLock, enableFlag = enable )
    else:
      self.log.error( res['Message'] )
      return res
    if res[ 'OK' ]:
      # checkSystemConfiguration
      res = job.checkSystemConfiguration( enableFlag = enable )
    else:
      self.log.error( res['Message'] )
      return res
    if res[ 'OK' ]:
      res = job.setSystemConfig( config )
#    if res[ 'OK' ]:
#      res = job.setPlatform( Operations().getValue( 'SAM/Platform', 'gLite' ) )  
    else:
      self.log.error( res['Message'] )
      return res
    if res[ 'OK' ]:
      # installSoftware
      self.log.verbose( 'Flag to force deletion of shared area is %s' % ( deleteSharedArea ) )
      if install_project:
        self.log.verbose( 'Optional install_project URL is set to %s' % ( install_project ) )
      res = job.installSoftware( forceDeletion = deleteSharedArea, enableFlag = softwareEnable, 
                                 installProjectURL = install_project )
    else:
      self.log.error( res['Message'] )
      return res
    if res[ 'OK' ]:
      # reportSoftware
      res = job.reportSoftware( enableFlag = reportEnable  )      
    else:
      self.log.error( res['Message'] )
      return res
      
    if not res[ 'OK' ]:
      # runTestScript
      if scriptName:
        res = job.runTestScript( scriptName = scriptName, enableFlag = enable )
    if not res[ 'OK' ]:
      # finalizeAndPublish
      res = job.finalizeAndPublish( logUpload = logUpload, enableFlag = enable )

    if softwareEnable:
      res = job.setSAMGroup( "SAMsw" )
    
    if not res[ 'OK' ]:
      self.log.warn( res[ 'Message' ] )
      return res
    
    self.log.verbose( 'Job JDL is: \n%s' % job._toJDL() )
    return self.submit( job, mode )

  def defineSAMJob( self, ce ):
    '''
      Defines an LHCbJob which is going to be submitted to a given ce, which the
      following properties ( defaults ):
      - logLevel      : verbose
      - CPUTime       : 50000
      - platform      : gLite-SAM
      - outputSandbox : ['*.log']
      - jobGroup      : SAM
      - setType       : SAM
      - priority      : 1
      - submitPools   : SAM
      
      Steps:
      - GaudiApplication x 4 [ Gauss, Boole, Brunel, DaVinci ]
      - UploadSAMLogs
    '''

    #Get Job properties
    appTests          = self.opsH.getValue( 'SAM/ApplicationTests', [] )
    samLogLevel       = self.opsH.getValue( 'SAM/LogLevel', 'verbose' )
    samDefaultCPUTime = self.opsH.getValue( 'SAM/CPUTime', 50000 )
    samPlatform       = self.opsH.getValue( 'SAM/Platform', 'gLite-SAM' )
    samOutputFiles    = self.opsH.getValue( 'SAM/OutputSandbox', ['*.log'] )
    samGroup          = self.opsH.getValue( 'SAM/JobGroup', 'SAM' )
    samType           = self.opsH.getValue( 'SAM/JobType', 'SAM' )
    samPriority       = self.opsH.getValue( 'SAM/Priority', 1 )
    if not isinstance( samPriority, int ):
      try:
        samPriority = int( samPriority )
      except ValueError:
        return S_ERROR( 'Expected Integer for User priority' )
    appSystemConfig   = NativeMachine().CMTSupportedConfig()[ 0 ]
    
    #LHCbJob definition
    samJob = LHCbJob( stdout = 'std.out', stderr =  'std.err' )
    res = samJob.setName( 'SAM-%s' % ce )
    if not res[ 'OK' ]:
      return res

    res = samJob.setDestinationCE( ce )
    if not res[ 'OK' ]: 
      return res    
    res = samJob.setLogLevel( samLogLevel )
    if not res[ 'OK' ]: 
      return res
    res = samJob.setCPUTime( samDefaultCPUTime )
    if not res[ 'OK' ]: 
      return res
    res = samJob.setPlatform( samPlatform )
    if not res[ 'OK' ]:
      return res
    res = samJob.setOutputSandbox( samOutputFiles )
    if not res[ 'OK' ]:
      return res
    res = samJob.setJobGroup( samGroup )
    if not res[ 'OK' ]:
      return res
    res = samJob.setType( samType )
    if not res[ 'OK' ]:
      return res
    res = samJob.setSystemConfig( appSystemConfig )
    if not res[ 'OK' ]:
      return res
    
    samJob._addParameter( samJob.workflow, 'Priority', 'JDL', samPriority, 'User Job Priority' )
    samJob._addJDLParameter( 'SubmitPools', 'SAM' )
    
    # Steps definition
    for appTest in appTests:
      
      appTestOptions = self.opsH.getOptionsDict( 'SAM/ApplicationTestOptions/%s' % appTest )
      if not appTestOptions[ 'OK' ]:
        return S_ERROR( '"SAM/ApplicationTestOptions/%s" is not defined or could not be retrieved' % appTest )
      appTestOptions = appTestOptions[ 'Value' ]
            
      setSAMJobApplicationStep( samJob, appTest, appTestOptions )
  
    # Adding gaudiSteps
    samJob._addParameter( samJob.workflow, 'gaudiSteps', 'list', appTests, 'list of Gaudi Steps' )
    
    stepName = 'UploadSAMLogs' 
    step     = getStepDefinition( stepName, modulesNameList = [ 'UploadSAMLogs' ], 
                                  importLine = "LHCbDIRAC.SAMSystem.Modules" )
    addStepToWorkflow( samJob.workflow, step, stepName )
    
    return samJob
  
  def submitNewSAMJob( self, ce, runLocal = False ):
    '''
      Method that generates a NewStyle SAM Job and submits it to the given ce
      if mode is wms. If mode is local, it will be run locally
    '''
    
    mode = ( runLocal and 'local' ) or 'wms'
    
    samJob = self.defineSAMJob( ce )
    return self.submit( samJob, mode )

  #FIXME: this method already exists on DiracAdmin, it is a copy.
  def _promptUser( self, message ):
    '''
      Internal function to prompt user before submitting all SAM test jobs.
    '''
    
    self.log.verbose( '%s %s' % ( message, '[yes/no] : ' ) )
    response = raw_input( '%s %s' % ( message, '[yes/no] : ' ) )
    responses = ['yes', 'y', 'n', 'no']
    if not response.strip() or response == '\n':
      self.log.info( 'Possible responses are: %s' % ( ', '.join( responses ) ) )
      response = raw_input( '%s %s' % ( message, '[yes/no] : ' ) )

    if not response.strip().lower() in responses:
      self.log.info( 'Problem interpreting input "%s", assuming negative response.' % ( response ) )
      return S_ERROR( response )

    if response.strip().lower() == 'y' or response.strip().lower() == 'yes':
      return S_OK( response )
    else:
      return S_ERROR( response )
    
# functions
#...............................................................................  
    
def setInputFile( appName ):
  '''
    Given an application, returns the extension of the files that the previous
    step generated ( only applies to SAM jobs ).
  '''
  
  inputTypes = {
                'Boole'   : 'SIM',
                'Brunel'  : 'DIGI',
                'DaVinci' : 'DST'
               }
    
  return inputTypes.get( appName, '' )  
  
def setOutputFile( appName, outputFilePrefix ):
  '''
    Given an application, returns a dictionary with its formatted output.
    Note that outputDataSE is a dummy value ( this method only applies to
    SAM Jobs ). 
  '''

  outputType = { 
                'Gauss'  : 'sim', 
                'Boole'  : 'digi',
                'Brunel' : 'dst'
               }
  fileTypesOut = {}
  if appName in outputType:
    fileTypesOut     = { 'outputDataType' : outputType[ appName ], 
                         'outputDataName' : '%s.%s' % ( outputFilePrefix,  outputType[ appName ] ),
                         'outputDataSE'   : 'Spock' }

  return fileTypesOut
     
def setSAMJobApplicationStep( samJob, appName, appOptions ):
  '''
    Given a SAM job, sets an step ( GaudiApplication ) for the given app name.
    It also adds to the step the options passed on the appOptions dictionary
    if they apply. 
  '''

  #StepDefinition
 
  modules    = [ 'GaudiApplication' ]
  paramsList = [ ( 'applicationName', 'string', '', 'Application Name' ), 
                 ( 'applicationVersion', 'string', '', 'Application Version' ), 
                 ( 'applicationLog', 'string', '', 'Application output file' ), 
                 ( 'numberOfEvents', 'string', '', 'Events treated' ), 
                 ( 'outputFilePrefix', 'string', '', 'Data file name' ),
                 ( 'XMLSummary', 'string', '', 'XMLSummaryFile name' ),
                 ( 'extraPackages', 'string', '', 'extraPackages' ), 
                 ( 'optionsFile','string','','optionsFile' ),
                 ( 'listoutput','list',[],'loutputs' ),
                 ( 'DDDBTag','string','','DDDBTag' ),
                 ( 'CondDBTag','string','','CondDBTag' ),
                 ( 'inputData','string','','iData' ),
                 ( 'inputDataType','string','','iDataType' )
                ]
    
  stepName = appName

  step = getStepDefinition( stepName, modulesNameList = modules, parametersList = paramsList )
  stepInstance = addStepToWorkflow( samJob.workflow, step, stepName )

  #Step Parameters Passing  
  appVersion     = appOptions.get( 'applicationVersion', '' )
  numberOfEvents = 2
  filePrefix     = samJob.systemConfig
  optionsFile    = appOptions.get( 'optionFiles', '' )
  extraPackages  = 'ProdConf'
    
  dddbtag        = appOptions.get( 'DDDBTag', '' ) 
  conddbtag      = appOptions.get( 'CondDBTag', '' )
    
  # Depends on the appName
  inputDataType  = setInputFile( appName )
  outputFiles    = setOutputFile( appName, filePrefix )
    
  stepInstance.setValue( 'XMLSummary',         'summary@{applicationName}_@{STEP_ID}.xml' )
  stepInstance.setValue( 'applicationName',    appName )
  stepInstance.setValue( 'applicationVersion', appVersion )
  stepInstance.setValue( 'applicationLog',     '%s.log' % appName )
  stepInstance.setValue( 'numberOfEvents',     numberOfEvents )
  stepInstance.setValue( 'outputFilePrefix',   filePrefix )
  stepInstance.setValue( 'optionsFile',        optionsFile )
  stepInstance.setValue( 'extraPackages',      extraPackages )
  if dddbtag:
    stepInstance.setValue( 'DDDBTag',    dddbtag )
  if conddbtag:
    stepInstance.setValue( 'CondDBTag',  conddbtag )  
  if inputDataType:
    stepInstance.setValue( 'inputData',    'previousStep' )
    stepInstance.setValue( 'inputDataType', inputDataType )
  if outputFiles:
    stepInstance.setValue( 'listoutput', [ outputFiles ] )
      
  return stepInstance  
      
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
