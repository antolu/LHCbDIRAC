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
  """ DiracSAM: extension of Dirac Interface for SAM jobs
    
      It provides the following methods:
      - getSuitableCEs
      - submitSAMJob
  """

  def __init__( self ):
    """ Instantiates the Workflow object and some default parameters.
    """
    Dirac.__init__( self )
    
    self.opsH = Operations()
    
    self.gridType    = 'LCG'
    self.bannedSites = self.opsH.getValue( 'SAM/BannedSites', [] )

  def getSuitableCEs( self ):
    """ Gets all CEs ( excluding the ones of banned sites )
    """
    
    self.log.info( "Banned SAM sites are: %s" % ( ', '.join( self.bannedSites ) ) )
    
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
    """ Defines an LHCbJob which is going to be submitted to a given ce
      
        Steps:
        - CVMFSCheck
        - GaudiApplication x 4 [ Gauss, Boole, Brunel, DaVinci ]
        - UploadSAMLogs
    """

    inputTypes = {'Boole'   : 'SIM',
                  'Brunel'  : 'DIGI',
                  'DaVinci' : 'DST'}

    #Get Job properties
    appTests = self.opsH.getValue( 'SAM/ApplicationTests', [] )
    samLogLevel = self.opsH.getValue( 'SAM/LogLevel', 'verbose' )
    samDefaultCPUTime = self.opsH.getValue( 'SAM/CPUTime', 50000 )
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
    samJob = LHCbJob()
    res = samJob.setName( 'SAM-%s' % ce )
    if not res['OK']:
      return res

    res = samJob.setDestinationCE( ce )
    if not res['OK']:
      return res    
    res = samJob.setLogLevel( samLogLevel )
    if not res['OK']:
      return res
    res = samJob.setCPUTime( samDefaultCPUTime )
    if not res['OK']:
      return res
    res = samJob.setOutputSandbox( samOutputFiles )
    if not res['OK']:
      return res
    res = samJob.setJobGroup( samGroup )
    if not res['OK']:
      return res
    res = samJob.setType( samType )
    if not res['OK']:
      return res
    
    samJob._addParameter( samJob.workflow, 'Priority', 'JDL', samPriority, 'User Job Priority' )
    samJob._addJDLParameter( 'SubmitPools', 'Test' )
    
    # CVMFS step definition
    stepName = 'CVMFSCheck'
    step = getStepDefinition( stepName, modulesNameList = [stepName], importLine = "LHCbDIRAC.SAMSystem.Modules" )
    addStepToWorkflow( samJob.workflow, step, stepName )
   
    gaudiSteps = []
    # Application Step definitions
    for appTest in appTests:
      
      appTestOptions = self.opsH.getOptionsDict( 'SAM/ApplicationTestOptions/%s' % appTest )
      if not appTestOptions['OK']:
        return S_ERROR( "'SAM/ApplicationTestOptions/%s' is not defined or could not be retrieved" % appTest )
      appTestOptions = appTestOptions['Value']

      inputDataType = inputTypes.get( appTest, '' )
      if inputDataType:
        inputData = 'previousStep'
      else:
        inputData = ''

      applicationStep = samJob.setApplication( appName = appTest,
                                               appVersion = appTestOptions.get( 'applicationVersion', '' ),
                                               optionsFiles = appTestOptions.get( 'optionFiles', '' ),
                                               inputData = inputData,
                                               inputDataType = inputDataType,
                                               events = 2,
                                               extraPackages = 'ProdConf',
                                               modulesNameList = ['GaudiApplication'],
                                               parametersList = [
                                                    ( 'applicationName', 'string', '', 'Application Name' ),
                                                    ( 'applicationVersion', 'string', '', 'Application Version' ),
                                                    ( 'applicationLog', 'string', '', 'Application output file' ),
                                                    ( 'numberOfEvents', 'string', '', 'Events treated' ),
                                                    ( 'outputFilePrefix', 'string', '', 'Data file name' ),
                                                    ( 'XMLSummary', 'string', '', 'XMLSummaryFile name' ),
                                                    ( 'extraPackages', 'string', '', 'extraPackages' ),
                                                    ( 'optionsFile', 'string', '', 'optionsFile' ),
                                                    ( 'listoutput', 'list', [], 'loutputs' ),
                                                    ( 'DDDBTag', 'string', '', 'DDDBTag' ),
                                                    ( 'CondDBTag', 'string', '', 'CondDBTag' ),
                                                    ( 'inputData', 'string', '', 'iData' ),
                                                    ( 'inputDataType', 'string', '', 'iDataType' ),
                                                    ( 'SystemConfig', 'string', '', 'CMT Config' )
                                                    ] )

      if not applicationStep['OK']:
        return applicationStep
      filePrefix = appTest + appTestOptions.get( 'applicationVersion', '' )
      outputFiles = self.__setOutputFile( appTest, filePrefix )
      applicationStep['Value'].setValue( 'listoutput', [outputFiles] )
      applicationStep['Value'].setValue( 'outputFilePrefix', filePrefix )

      gaudiSteps.append( applicationStep['Value']['name'] )

      if appTestOptions.get( 'DDDBTag', '' ):
        applicationStep['Value'].setValue( 'DDDBTag', appTestOptions.get( 'DDDBTag', '' ) )
      if appTestOptions.get( 'CondDBTag', '' ):
        applicationStep['Value'].setValue( 'CondDBTag', appTestOptions.get( 'CondDBTag', '' ) )

    samJob._addParameter( samJob.workflow, 'gaudiSteps', 'list', gaudiSteps, 'list of Gaudi Steps' )
    
    stepName = 'UploadSAMLogs' 
    step = getStepDefinition( stepName, modulesNameList = [stepName],
                              importLine = "LHCbDIRAC.SAMSystem.Modules" )
    addStepToWorkflow( samJob.workflow, step, stepName )
    
    samJob.setDIRACPlatform()

    return S_OK( samJob )
  
  def submitNewSAMJob( self, ce, runLocal = False ):
    """ Method that generates a NewStyle SAM Job and submits it to the given ce if mode is wms.
        If mode is local, it will be run locally
    """
    
    mode = ( runLocal and 'local' ) or 'wms'
    
    samJob = self.defineSAMJob( ce )
    if not samJob[ 'OK' ]:
      return samJob
    return self.submit( samJob[ 'Value' ], mode )

  @staticmethod
  def __setOutputFile( appName, outputFilePrefix ):
    """ Given an application, returns a dictionary with its formatted output.
        Note that outputDataSE is a dummy value ( this method only applies to SAM Jobs ).
    """

    outputType = {'Gauss'  : 'sim',
                  'Boole'  : 'digi',
                  'Brunel' : 'dst',
                  'DaVinci': 'AllStreams.dst' }
    fileTypesOut = {}
    if appName in outputType:
      fileTypesOut = {'outputDataType': outputType[appName],
                      'outputDataName': '%s.%s' % ( outputFilePrefix, outputType[appName] ),
                      'outputBKType': outputType[appName].upper(),
                      'outputDataSE': 'Spock' }

    return fileTypesOut
