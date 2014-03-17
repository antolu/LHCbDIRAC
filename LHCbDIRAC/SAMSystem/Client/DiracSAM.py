""" LHCb SAM Dirac Class

   The Dirac SAM class inherits generic VO functionality from the Dirac API base class.
"""

from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.SiteCEMapping                  import getCESiteMapping
from DIRAC.Interfaces.API.Dirac                          import Dirac, S_OK, S_ERROR
from DIRAC.Workflow.Utilities.Utils                      import getStepDefinition, addStepToWorkflow

from LHCbDIRAC.Interfaces.API.LHCbJob      import LHCbJob

COMPONENT_NAME = 'DiracSAMAPI'

class DiracSAM( Dirac ):
  """
    DiracSAM: extension of Dirac Interface for SAM jobs
    
    It provides the following methods:
    - getSuitableCEs
    - submitSAMJob
  """

  def __init__( self ):
    """
       Instantiates the Workflow object and some default parameters.
    """
    Dirac.__init__( self )
    
    self.opsH = Operations()
    
    self.gridType    = 'LCG'
    self.bannedSites = self.opsH.getValue( 'SAM/BannedSites', [] )

  def getSuitableCEs( self ):
    """
      Gets all CEs ( excluding the ones of banned sites )
    """
    
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

  def defineSAMJob( self, ce ):
    """
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
    """

    #Get Job properties
    appTests = self.opsH.getValue( 'SAM/ApplicationTests', [] )
    samLogLevel = self.opsH.getValue( 'SAM/LogLevel', 'verbose' )
    samDefaultCPUTime = self.opsH.getValue( 'SAM/CPUTime', 50000 )
#    samPlatform = self.opsH.getValue( 'SAM/Platform', 'gLite-SAM' )
    samOutputFiles = self.opsH.getValue( 'SAM/OutputSandbox', ['*.log'] )
    samGroup = self.opsH.getValue( 'SAM/JobGroup', 'Test' )
    samType = self.opsH.getValue( 'SAM/JobType', 'Test' )
    samPriority = self.opsH.getValue( 'SAM/Priority', 1 )
    if not isinstance( samPriority, int ):
      try:
        samPriority = int( samPriority )
      except ValueError:
        return S_ERROR( 'Expected Integer for User priority' )
    
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
#     res = samJob.setPlatform( samPlatform )
#     if not res[ 'OK' ]:
#       return res
    res = samJob.setOutputSandbox( samOutputFiles )
    if not res[ 'OK' ]:
      return res
    res = samJob.setJobGroup( samGroup )
    if not res[ 'OK' ]:
      return res
    res = samJob.setType( samType )
    if not res[ 'OK' ]:
      return res
    
    samJob._addParameter( samJob.workflow, 'Priority', 'JDL', samPriority, 'User Job Priority' )
    samJob._addJDLParameter( 'SubmitPools', 'Test' )
    
    # CVMFS step definition
    stepName = 'CVMFSCheck'
    step     = getStepDefinition( stepName, modulesNameList = [ 'CVMFSCheck' ],
                                  importLine = "LHCbDIRAC.SAMSystem.Modules" )
    addStepToWorkflow( samJob.workflow, step, stepName )
   
    # Application Step definitions
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
    
    return S_OK( samJob )
  
  def submitNewSAMJob( self, ce, runLocal = False ):
    """
      Method that generates a NewStyle SAM Job and submits it to the given ce
      if mode is wms. If mode is local, it will be run locally
    """
    
    mode = ( runLocal and 'local' ) or 'wms'
    
    samJob = self.defineSAMJob( ce )
    if not samJob[ 'OK' ]:
      return samJob
    return self.submit( samJob[ 'Value' ], mode )

# functions
#...............................................................................  
    
def setInputFile( appName ):
  """
    Given an application, returns the extension of the files that the previous
    step generated ( only applies to SAM jobs ).
  """
  
  inputTypes = {
                'Boole'   : 'SIM',
                'Brunel'  : 'DIGI',
                'DaVinci' : 'DST'
               }
    
  return inputTypes.get( appName, '' )  
  
def setOutputFile( appName, outputFilePrefix ):
  """
    Given an application, returns a dictionary with its formatted output.
    Note that outputDataSE is a dummy value ( this method only applies to
    SAM Jobs ). 
  """

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
  """
    Given a SAM job, sets an step ( GaudiApplication ) for the given app name.
    It also adds to the step the options passed on the appOptions dictionary
    if they apply. 
  """

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
    
  step = getStepDefinition( appName, modulesNameList = modules, parametersList = paramsList )
  stepInstance = addStepToWorkflow( samJob.workflow, step, appName )

  #Step Parameters Passing  
  appVersion = appOptions.get( 'applicationVersion', '' )
  filePrefix = appName + appVersion

  dddbtag = appOptions.get( 'DDDBTag', '' )
  conddbtag = appOptions.get( 'CondDBTag', '' )

  # Depends on the appName
  inputDataType = setInputFile( appName )
  outputFiles = setOutputFile( appName, filePrefix )
    
  stepInstance.setValue( 'XMLSummary', 'summary@{applicationName}_@{STEP_ID}.xml' )
  stepInstance.setValue( 'applicationName', appName )
  stepInstance.setValue( 'applicationVersion', appVersion )
  stepInstance.setValue( 'applicationLog', '%s.log' % appName )
  stepInstance.setValue( 'numberOfEvents', 2 )
  stepInstance.setValue( 'outputFilePrefix', filePrefix )
  stepInstance.setValue( 'optionsFile', appOptions.get( 'optionFiles', '' ) )
  stepInstance.setValue( 'extraPackages', 'ProdConf' )
  if dddbtag:
    stepInstance.setValue( 'DDDBTag', dddbtag )
  if conddbtag:
    stepInstance.setValue( 'CondDBTag', conddbtag )
  if inputDataType:
    stepInstance.setValue( 'inputData', 'previousStep' )
    stepInstance.setValue( 'inputDataType', inputDataType )
  if outputFiles:
    stepInstance.setValue( 'listoutput', [ outputFiles ] )

  return stepInstance  

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
