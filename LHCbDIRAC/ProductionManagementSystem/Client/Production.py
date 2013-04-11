""" Production API

    A production is an augmented version of an LHCbJob

    Notes:
    - Supports all workflows
    - create() method that takes a workflow or Production object
      and publishes to the production management system, in addition this
      can automatically construct and publish the BK pass info and transformations
    - Uses getOutputLFNs() function to add production output directory parameter
"""

import shutil, re, os, copy

from DIRAC import gConfig, S_OK, S_ERROR
from DIRAC.Core.Workflow.Workflow import Workflow, fromXMLString
from DIRAC.Core.Utilities.List import removeEmptyElements, uniqueElements

from LHCbDIRAC.Core.Utilities.ProductionData import preSubmissionLFNs
from LHCbDIRAC.Workflow.Utilities.Utils import getStepDefinition
from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

__RCSID__ = "$Id$"

class Production():
  """ Production does not inherits from LHCbJob, but uses an LHCbJob object.
  """

  #############################################################################

  def __init__( self, script = None, lhcbJobIn = None, BKKClientIn = None,
                transClientIn = None ):
    """Instantiates the Workflow object and some default parameters.
    """

    if lhcbJobIn is not None:
      self.LHCbJob = lhcbJobIn
    else:
      self.LHCbJob = LHCbJob( script )

    if BKKClientIn is not None:
      self.BKKClient = BKKClientIn
    else:
      self.BKKClient = BookkeepingClient()

    if transClientIn is not None:
      self.transClient = transClientIn
    else:
      self.transClient = TransformationClient()

    self.histogramName = self.LHCbJob.opsHelper.getValue( 'Productions/HistogramName',
                                                          '@{applicationName}_@{STEP_ID}_Hist.root' )
    self.histogramSE = self.LHCbJob.opsHelper.getValue( 'Productions/HistogramSE', 'CERN-HIST' )
    self.bkSteps = {}
    self.prodGroup = ''
    self.plugin = ''
    self.inputFileMask = ''
    self.inputBKSelection = {}
    self.jobFileGroupSize = 0
    self.ancestorProduction = 0
    self.transformationFamily = 0
    self.priority = 1
    self.gaudiSteps = []
    if not script:
      self.__setDefaults()

  #############################################################################

  def __setDefaults( self ):
    """Sets some default parameters.
    """

    self.LHCbJob.stepCount = 0
    self.LHCbJob.setOutputSandbox( self.LHCbJob.opsHelper.getValue( 'Productions/inOutputSandbox',
                                                            ['std.out', 'std.err', '*.log'] ) )
    self.setJobParameters( {
                           'Type'         : 'MCSimulation',
                           'SystemConfig' : 'ANY',
                           'CPUTime'      : '1000000',
                           'LogLevel'     : 'verbose',
                           'JobGroup'     : '@{PRODUCTION_ID}'
                           } )

    self.setFileMask( '' )

    # version control
    self.setParameter( 'productionVersion', 'string', __RCSID__, 'ProdAPIVersion' )

    # General workflow parameters
    self.setParameter( 'PRODUCTION_ID', 'string', '00012345', 'ProductionID' )
    self.setParameter( 'JOB_ID', 'string', '00006789', 'ProductionJobID' )
    self.setParameter( 'poolXMLCatName', 'string', 'pool_xml_catalog.xml', 'POOLXMLCatalogName' )
    self.setParameter( 'outputMode', 'string', 'Any', 'SEResolutionPolicy' )
    self.setParameter( 'outputDataFileMask', 'string', '', 'outputDataFileMask' )

    # BK related parameters
    self.setParameter( 'configName', 'string', 'MC', 'ConfigName' )
    self.setParameter( 'configVersion', 'string', '2009', 'ConfigVersion' )
    self.setParameter( 'conditions', 'string', '', 'SimOrDataTakingCondsString' )

  #############################################################################

  def setJobParameters( self, parametersDict ):
    """ Set an (LHCb)Job parameter

        The parametersDict is in the form {'parameterName': 'value'}
        Each parameter calls LHCbJob.setparameterName(value)
    """

    for parameter in parametersDict.keys():
      getattr( self.LHCbJob, 'set' + parameter )( parametersDict[parameter] )

  #############################################################################

  def setParameter( self, name, parameterType, parameterValue, description ):
    """Set parameters checking in CS in case some defaults need to be changed.
    """
    proposedParam = self.LHCbJob.opsHelper.getValue( 'Productions/%s' % name, '' )
    if proposedParam:
      self.LHCbJob.log.debug( 'Setting %s from CS defaults = %s' % ( name, proposedParam ) )
      self.LHCbJob._addParameter( self.LHCbJob.workflow, name, parameterType, proposedParam, description )
    else:
      self.LHCbJob.log.debug( 'Setting parameter %s = %s' % ( name, parameterValue ) )
      self.LHCbJob._addParameter( self.LHCbJob.workflow, name, parameterType, parameterValue, description )

  #############################################################################

  def __checkArguments( self, extraPackages, optionsFile ):
    """ Checks for typos in the structure of standard arguments to workflows.
        In case of any non-standard settings will raise an exception preventing
        creation of the production. Must be called after setting the first event type
        of the production.
    """
    if not extraPackages:
      extraPackages = []

    if not optionsFile:
      optionsFile = []

    if extraPackages:
      if not re.search( ';', extraPackages ):
        extraPackages = [extraPackages]
      else:
        extraPackages = extraPackages.split( ';' )
    if optionsFile:
      if not re.search( ';', optionsFile ):
        optionsFile = [optionsFile]

    for p in extraPackages:
      self.LHCbJob.log.verbose( 'Checking extra package: %s' % ( p ) )
      if not re.search( '.', p ):
        raise TypeError, 'Must have extra packages in the following format "Name.Version" not %s' % ( p )

    self.LHCbJob.log.verbose( 'Extra packages and event type options are correctly specified' )
    return S_OK()

  #############################################################################

  def addApplicationStep( self, stepDict, outputSE, optionsLine, inputData = None,
                          modules = [ 'GaudiApplication', 'AnalyseLogFile', 'AnalyseXMLSummary',
                                     'ErrorLogging', 'BookkeepingReport', 'StepAccounting' ] ):
    """ stepDict contains everything that is in the step, for this production, e.g.:
        {'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': 13718, 'ApplicationVersion': 'v28r3p1',
        'ExtraPackages': 'AppConfig.v3r104', 'StepName': 'Stripping14-Merging',
        'ProcessingPass': 'Merging', 'Visible': 'N', 'OptionsFormat': '',
        'OptionFiles': '$APPCONFIGOPTS/Merging/DV-Stripping14-Merging.py',
        'DDDB': 'head-20110302', 'CONDDB': 'head-20110407', 'DQTag': '',
        'fileTypesIn': ['SDST'],
        'fileTypesOut': ['BHADRON.DST', 'CALIBRATION.DST', 'CHARM.MDST', 'CHARMCOMPLETEEVENT.DST']}

        Note: this step treated here does not necessarily corresponds to a step of the BKK:
        the case where they might be different is the merging case.
    """

    appName = stepDict['ApplicationName']
    appVersion = stepDict['ApplicationVersion']
    optionsFile = stepDict['OptionFiles']
    stepID = stepDict['StepId']
    stepName = stepDict['StepName']
    stepVisible = stepDict['Visible']
    extraPackages = stepDict['ExtraPackages']
    fileTypesIn = stepDict['fileTypesIn']
    fileTypesOut = stepDict['fileTypesOut']
    stepPass = stepDict['ProcessingPass']
    optionsFormat = stepDict['OptionsFormat']
    dddbOpt = stepDict['DDDB']
    conddbOpt = stepDict['CONDDB']
    DQOpt = stepDict['DQTag']

    if extraPackages:
      if type( extraPackages ) == type( [] ):
        extraPackages = ';'.join( extraPackages )
      if type( extraPackages ) != type( '' ):
        raise TypeError, 'extraPackages is not a string (nor a list)'
      if ',' in extraPackages:
        extraPackages = ';'.join( extraPackages.split( ',' ) )
      if 'ProdConf' not in extraPackages:
        extraPackages = extraPackages + ';ProdConf'
      extraPackages = extraPackages.replace( ' ', '' )
    else:
      extraPackages = 'ProdConf'

    self.__checkArguments( extraPackages, optionsFile )

    try:
      if not dddbOpt.lower() == 'global':
        self.LHCbJob.log.verbose( 'Specific DDDBTag setting found for %s step, setting to: %s' % ( appName, dddbOpt ) )
        dddbOpt = dddbOpt.replace( ' ', '' )
    except AttributeError:
      pass
    try:
      if not conddbOpt.lower() == 'global':
        self.LHCbJob.log.verbose( 'Specific CondDBTag setting found for %s step, setting to: %s' % ( appName, conddbOpt ) )
        conddbOpt = conddbOpt.replace( ' ', '' )
    except AttributeError:
      pass
    try:
      if not DQOpt.lower() == 'global':
        self.LHCbJob.log.verbose( 'Specific DQTag setting found for %s step, setting to: %s' % ( appName, DQOpt ) )
        DQOpt = DQOpt.replace( ' ', '' )
    except AttributeError:
      pass


    # starting real stuff
    self.LHCbJob.stepCount += 1

    if 'Gaudi_App_Step' not in self.LHCbJob.workflow.step_definitions.keys():

      if 'GaudiApplication' in modules:
        gaudiPath = 'Productions/GaudiStep_Modules'
        modulesNameList = self.LHCbJob.opsHelper.getValue( gaudiPath, modules )
      else:
        modulesNameList = modules
      # pName, pType, pValue, pDesc
      parametersList = [
                        ['inputData', 'string', '', 'StepInputData'],
                        ['inputDataType', 'string', '', 'InputDataType'],
                        ['outputFilePrefix', 'string', '', 'OutputFilePrefix'],
#                        ['outputData', 'string', '', 'OutputData'],
                        ['applicationName', 'string', '', 'ApplicationName'],
                        ['applicationVersion', 'string', '', 'ApplicationVersion'],
                        ['runTimeProjectName', 'string', '', 'runTimeProjectName'],
                        ['runTimeProjectVersion', 'string', '', 'runTimeProjectVersion'],
                        ['applicationType', 'string', '', 'ApplicationType'],
                        ['applicationLog', 'string', '', 'ApplicationLogFile'],
                        ['XMLSummary', 'string', '', 'XMLSummaryFile'],
                        ['optionsFile', 'string', '', 'OptionsFile'],
                        ['extraOptionsLine', 'string', '', 'extraOptionsLines'],
                        ['numberOfEventsInput', 'string', '', 'NumberOfEventsInput'],
                        ['listoutput', 'list', [], 'StepOutputList'],
                        ['extraPackages', 'string', '', 'ExtraPackages'],
                        ['BKStepID', 'string', '', 'BKKStepID'],
                        ['StepProcPass', 'string', '', 'StepProcessingPass'],
                        ['HistogramName', 'string', '', 'NameOfHistogram'],
                        ['optionsFormat', 'string', '', 'ProdConf configuration'],
                        ['CondDBTag', 'string', '', 'ConditionDatabaseTag'],
                        ['DDDBTag', 'string', '', 'DetDescTag' ],
                        ['DQTag', 'string', '', 'DataQualityTag']
                        ]

      gaudiStepDef = getStepDefinition( 'Gaudi_App_Step', modulesNameList = modulesNameList,
                                        parametersList = parametersList )
      self.LHCbJob.workflow.addStep( gaudiStepDef )

    # create the step instance add it to the wf, and return it
    name = '%s_%s' % ( appName, self.LHCbJob.stepCount )
    gaudiStepInstance = self.LHCbJob.workflow.createStepInstance( 'Gaudi_App_Step', name )

    valuesToSet = [
                   ['applicationName', appName ],
                   ['applicationVersion', appVersion ],
                   ['optionsFile', optionsFile ],
                   ['extraOptionsLine', optionsLine],
                   ['outputFilePrefix', '@{STEP_ID}'],
                   ['applicationLog', '@{applicationName}_@{STEP_ID}.log'],
                   ['XMLSummary', 'summary@{applicationName}_@{STEP_ID}.xml'],
                   ['extraPackages', extraPackages],
#                   ['outputData', '@{STEP_ID}.' + fileTypesOut[0] if len( fileTypesOut ) == 1 else 'multiple'],
                   ['BKStepID', str( stepID )],
                   ['StepProcPass', stepPass],
                   ['HistogramName', self.histogramName],
                   ['optionsFormat', optionsFormat],
                   ['CondDBTag', conddbOpt],
                   ['DDDBTag', dddbOpt],
                   ['DQTag', DQOpt]
                   ]

    if fileTypesIn:
      valuesToSet.append( [ 'inputDataType', ';'.join( ftIn.upper() for ftIn in fileTypesIn ) ] )

    if not inputData:
      self.LHCbJob.log.verbose( '%s step has no data requirement or is linked to the overall input data' % appName )
      gaudiStepInstance.setLink( 'inputData', 'self', 'InputData' )
    elif inputData == 'previousStep':
      self.LHCbJob.log.verbose( 'Taking input data as output from previous Gaudi step' )
      valuesToSet.append( [ 'inputData', inputData ] )
    else:
      self.LHCbJob.log.verbose( 'Assume input data requirement should be added to job' )
      self.LHCbJob.setInputData( inputData )
      gaudiStepInstance.setLink( 'inputData', 'self', 'InputData' )

    for pName, value in valuesToSet:
      if value:
        gaudiStepInstance.setValue( pName, value )

    outputFilesDict = self._constructOutputFilesDict( fileTypesOut, outputSE )
    gaudiStepInstance.setValue( 'listoutput', ( outputFilesDict ) )

    # now we have to tell DIRAC to install the necessary software
    if appName.lower() != 'mergemdf':
      self.__addSoftwarePackages( '%s.%s' % ( appName, appVersion ) )
    self.__addSoftwarePackages( extraPackages )

    # to construct the BK processing pass structure, starts from step '0'
    stepIDInternal = 'Step%s' % ( self.LHCbJob.stepCount - 1 )
    bkOptionsFile = optionsFile

    stepBKInfo = {'ApplicationName':appName,
                  'ApplicationVersion':appVersion,
                  'OptionFiles':bkOptionsFile,
                  'DDDb':dddbOpt,
                  'CondDb':conddbOpt,
                  'DQTag':DQOpt,
                  'ExtraPackages':extraPackages,
                  'BKStepID':stepID,
                  'StepName':stepName,
                  'StepVisible':stepVisible}

    self.bkSteps[stepIDInternal] = stepBKInfo
    self.__addBKPassStep()

    return name

  #############################################################################


  def _constructOutputFilesDict( self, filesTypesList, outputSE, histoName = None, histoSE = None ):
    """ build list of dictionary of output files, including HIST case, and fix outputSE for file
    """

    if not histoName:
      histoName = self.histogramName

    if not histoSE:
      histoSE = self.histogramSE

    outputList = []

    for fileType in filesTypesList:
      fileDict = {}
      if 'hist' in fileType.lower():
        fileDict['outputDataName'] = histoName
        fileDict['outputDataSE'] = histoSE
      else:
        fileDict['outputDataName'] = '@{STEP_ID}.' + fileType.lower()
        fileDict['outputDataSE'] = outputSE
      fileDict['outputDataType'] = fileType.lower()

      outputList.append( fileDict )

    return outputList

  #############################################################################

  def __addSoftwarePackages( self, nameVersion ):
    """ Internal method to accumulate software packages.
    """
    swPackages = 'SoftwarePackages'
    description = 'LHCbSoftwarePackages'
    if not self.LHCbJob.workflow.findParameter( swPackages ):
      self.LHCbJob._addParameter( self.LHCbJob.workflow, swPackages, 'JDL', nameVersion, description )
    else:
      apps = self.LHCbJob.workflow.findParameter( swPackages ).getValue()
      apps = apps.split( ';' )
      apps.append( nameVersion )
      apps = uniqueElements( removeEmptyElements( apps ) )
      apps = ';'.join( apps )
      self.LHCbJob._addParameter( self.LHCbJob.workflow, swPackages, 'JDL', apps, description )

  #############################################################################

  def __addBKPassStep( self ):
    """ Internal method to add BKK parameters
    """
    bkPass = 'BKProcessingPass'
    description = 'BKProcessingPassInfo'
    self.LHCbJob._addParameter( self.LHCbJob.workflow, bkPass, 'dict', self.bkSteps, description )

  #############################################################################

  def addFinalizationStep( self, modulesList = [] ):
    """ Add the finalization step (some defaults are inserted)
    """

    if 'Job_Finalization' not in self.LHCbJob.workflow.step_definitions.keys():

      if not modulesList:
        modulesList = self.LHCbJob.opsHelper.getValue( 'Productions/FinalizationStep_Modules',
                                                   [ 'UploadOutputData', 'UploadLogFile', 'FailoverRequest' ] )

      jobFinalizationStepDef = getStepDefinition( 'Job_Finalization', modulesNameList = modulesList )
      self.LHCbJob.workflow.addStep( jobFinalizationStepDef )

    # create the step instance add it to the wf
    self.LHCbJob.workflow.createStepInstance( 'Job_Finalization', 'finalization' )

  #############################################################################
  def createWorkflow( self, name = '' ):
    """ Create XML of the workflow
    """
    self.LHCbJob._addParameter( self.LHCbJob.workflow, 'gaudiSteps', 'list', self.gaudiSteps, 'list of Gaudi Steps' )

    if not name:
      name = self.LHCbJob.workflow.getName()
    if not re.search( 'xml$', name ):
      name = '%s.xml' % name
    if os.path.exists( name ):
      shutil.move( name, '%s.backup' % name )
    name = name.replace( '/', '' ).replace( '\\', '' )
    self.LHCbJob.workflow.toXMLFile( name )
    return S_OK( name )

  #############################################################################

  def runLocal( self, diracLHCb = None ):
    """ Create XML workflow for local testing then reformulate as a job and run locally.
    """

    name = self.createWorkflow()['Value']
    # this "name" is the xml file
    j = LHCbJob( name )
    # it makes a job (a Worklow, with Parameters), out of the xml file

    if diracLHCb is None:
      diracLHCb = DiracLHCb()

    return j.runLocal( diracLHCb, self.BKKClient )

  #############################################################################

  def _getProductionParameters( self, prodXMLFile, prodID, groupDescription = '',
                                bkPassInfo = {}, bkInputQuery = {},
                                derivedProd = 0, reqID = 0 ):
    """ This method will publish production parameters.
    """

    prodWorkflow = Workflow( prodXMLFile )

    parameters = {}
    info = []

    for parameterName in ( 'Priority', 'CondDBTag', 'DDDBTag', 'DQTag', 'eventType', 'FractionToProcess',
                           'MinFilesToProcess', 'configName', 'configVersion',
                           'outputDataFileMask', 'JobType', 'MaxNumberOfTasks' ):
      try:
        parameters[parameterName] = prodWorkflow.findParameter( parameterName ).getValue()
        info.append( "%s: %s" % ( parameterName, prodWorkflow.findParameter( parameterName ).getValue() ) )
      except AttributeError:
        continue

    parameters['SizeGroup'] = self.jobFileGroupSize

    if prodWorkflow.findParameter( 'InputData' ):  # now only comes from BK query
      prodWorkflow.findParameter( 'InputData' ).setValue( '' )
      self.LHCbJob.log.verbose( 'Resetting input data for production to null, this comes from a BK query...' )
      prodXMLFile = self.createWorkflow( prodXMLFile )['Value']
      # prodWorkflow.toXMLFile(prodXMLFile)

    if self.transformationFamily:
      parameters['TransformationFamily'] = self.transformationFamily

    if not bkPassInfo:
      bkPassInfo = prodWorkflow.findParameter( 'BKProcessingPass' ).getValue()
    if not groupDescription:
      groupDescription = prodWorkflow.findParameter( 'groupDescription' ).getValue()

    parameters['BKCondition'] = prodWorkflow.findParameter( 'conditions' ).getValue()
    parameters['BKProcessingPass'] = bkPassInfo
    parameters['BKInputQuery'] = bkInputQuery
    parameters['groupDescription'] = groupDescription
    parameters['RequestID'] = reqID
    parameters['DerivedProduction'] = derivedProd

    result = self.getOutputLFNs( prodID, '99999999', prodXMLFile )
    if not result['OK']:
      self.LHCbJob.log.error( 'Could not create production LFNs', result )

    outputLFNs = result['Value']
    parameters['OutputLFNs'] = outputLFNs

    outputDirectories = []
    del outputLFNs['BookkeepingLFNs']  # since ProductionOutputData uses the file mask
    for i in outputLFNs.values():
      for j in i:
        outputDir = '%s%s' % ( j.split( str( prodID ) )[0], prodID )
        if not outputDir in outputDirectories:
          outputDirectories.append( outputDir )

    parameters['OutputDirectories'] = outputDirectories

    # Now for the steps of the workflow
    stepKeys = bkPassInfo.keys()
    stepKeys.sort()
    for step in stepKeys:
      info.append( '====> %s %s %s' % ( bkPassInfo[step]['ApplicationName'],
                                        bkPassInfo[step]['ApplicationVersion'],
                                        step ) )
      info.append( '%s Option Files:' % ( bkPassInfo[step]['ApplicationName'] ) )
      if bkPassInfo[step]['OptionFiles']:
        for opts in bkPassInfo[step]['OptionFiles'].split( ';' ):
          info.append( '%s' % opts )
      if bkPassInfo[step]['ExtraPackages']:
        info.append( 'ExtraPackages: %s' % ( bkPassInfo[step]['ExtraPackages'] ) )

    if parameters['BKInputQuery']:
      info.append( '\nBK Input Data Query:' )
      for n, v in parameters['BKInputQuery'].items():
        info.append( '%s= %s' % ( n, v ) )

    # BK output directories (very useful)
    bkPaths = []
    bkOutputPath = '%s/%s/%s/%s/%s' % ( parameters['configName'],
                                        parameters['configVersion'],
                                        parameters['BKCondition'],
                                        parameters['groupDescription'],
                                        parameters['eventType'] )
    fileTypes = parameters['outputDataFileMask']
    fileTypes = [a.upper() for a in fileTypes.split( ';' )]

    # Annoying that histograms are extension root
    if 'ROOT' in fileTypes:
      fileTypes.remove( 'ROOT' )
      fileTypes.append( 'HIST' )

    for f in fileTypes:
      bkPaths.append( '%s/%s' % ( bkOutputPath, f ) )
    parameters['BKPaths'] = bkPaths
    info.append( '\nBK Browsing Paths:\n%s' % ( '\n'.join( bkPaths ) ) )
    infoString = '\n'.join( info )
    parameters['DetailedInfo'] = infoString

    self.LHCbJob.log.verbose( 'Parameters that will be added: %s' % parameters )

    return parameters

  #############################################################################

  def create( self, publish = True,
              wfString = '', requestID = 0, reqUsed = 0 ):
    """ Will create the production and subsequently publish to the BK.
        Production parameters are also added at this point.

        publish = True - will add production to the production management system
                  False - does not publish the production

        The workflow XML is created regardless of the flags.
    """

    if wfString:
      self.LHCbJob.workflow = fromXMLString( wfString )
#      self.name = self.LHCbJob.workflow.getName()

    bkConditions = self.LHCbJob.workflow.findParameter( 'conditions' ).getValue()

    bkSteps = self.LHCbJob.workflow.findParameter( 'BKProcessingPass' ).getValue()

    bkDictStep = {}

    # Add the BK conditions metadata / name
    simConds = self.BKKClient.getSimConditions()
    if not simConds['OK']:
      self.LHCbJob.log.error( 'Could not retrieve conditions data from BK:\n%s' % simConds )
      return simConds
    simulationDescriptions = []
    for record in simConds['Value']:
      simulationDescriptions.append( str( record[1] ) )

    if not bkConditions in simulationDescriptions:
      self.LHCbJob.log.verbose( 'Assuming BK conditions %s are DataTakingConditions' % bkConditions )
      bkDictStep['DataTakingConditions'] = bkConditions
    else:
      self.LHCbJob.log.verbose( 'Found simulation conditions for %s' % bkConditions )
      bkDictStep['SimulationConditions'] = bkConditions

    descShort = self.LHCbJob.workflow.getDescrShort()
    descLong = self.LHCbJob.workflow.getDescription()

    prodID = 0
    if publish:

      self.setParameter( 'ProcessingType', 'JDL', str( self.prodGroup ), 'ProductionGroupOrType' )
      self.setParameter( 'Priority', 'JDL', str( self.priority ), 'UserPriority' )

      try:
        fileName = self.createWorkflow()['Value']
      except Exception, x:
        self.LHCbJob.log.error( x )
        return S_ERROR( 'Could not create workflow' )

      self.LHCbJob.log.verbose( 'Workflow XML file name is: %s' % fileName )

      workflowBody = ''
      if os.path.exists( fileName ):
        fopen = open( fileName, 'r' )
        workflowBody = fopen.read()
        fopen.close()
      else:
        return S_ERROR( 'Could not get workflow body' )

      result = self.transClient.addTransformation( fileName, descShort, descLong,
                                                   self.LHCbJob.type, self.plugin, 'Manual',
                                                   fileMask = self.inputFileMask,
                                                   transformationGroup = self.prodGroup,
                                                   groupSize = int( self.jobFileGroupSize ),
                                                   inheritedFrom = int( self.ancestorProduction ),
                                                   body = workflowBody,
                                                   bkQuery = self.inputBKSelection
                                                  )

      if not result['OK']:
        self.LHCbJob.log.error( 'Problem creating production:\n%s' % result )
        return result
      prodID = result['Value']
      self.LHCbJob.log.info( 'Production %s successfully created' % prodID )
    else:
      self.LHCbJob.log.verbose( 'Publish flag is disabled, using default production ID' )

    bkDictStep['Production'] = int( prodID )

    queryProdID = 0
    bkQuery = copy.deepcopy( self.inputBKSelection )
    if bkQuery.has_key( 'ProductionID' ):
      queryProdID = int( bkQuery['ProductionID'] )
    queryProcPass = ''
    if bkQuery.has_key( 'ProcessingPass' ):
      if not bkQuery['ProcessingPass'] == 'All':
        queryProcPass = bkQuery['ProcessingPass']

    if bkQuery:
      if queryProdID:
        inputPass = self.BKKClient.getProductionProcessingPass( queryProdID )
        if not inputPass['OK']:
          self.LHCbJob.log.error( inputPass )
          self.LHCbJob.log.error( 'Production %s was created but BK processsing pass for %s was not found' % ( prodID,
                                                                                                         queryProdID ) )
          return inputPass
        inputPass = inputPass['Value']
        self.LHCbJob.log.info( 'Setting %s as BK input production for %s with processing pass %s' % ( queryProdID,
                                                                                                      prodID,
                                                                                                      inputPass ) )
        bkDictStep['InputProductionTotalProcessingPass'] = inputPass
      elif queryProcPass:
        self.LHCbJob.log.info( 'Adding input BK processing pass for production %s from input data query: %s' % ( prodID,
                                                                                                       queryProcPass ) )
        bkDictStep['InputProductionTotalProcessingPass'] = queryProcPass

    stepList = []
    stepKeys = bkSteps.keys()
    # The BK needs an ordered list of steps
    stepKeys.sort()
    for step in stepKeys:
      stepID = bkSteps[step]['BKStepID']
      if stepID:
        stepName = bkSteps[step]['StepName']
        stepVisible = bkSteps[step]['StepVisible']
        stepList.append( {'StepId':int( stepID ), 'StepName':stepName, 'Visible':stepVisible} )

    # This is the last component necessary for the BK publishing (post reorganisation)
    bkDictStep['Steps'] = stepList

    if publish:
      self.LHCbJob.log.verbose( 'Attempting to publish production %s to the BK' % ( prodID ) )
      result = self.BKKClient.addProduction( bkDictStep )
      if not result['OK']:
        self.LHCbJob.log.error( result )
        return result

    if requestID and publish:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      reqClient = RPCClient( 'ProductionManagement/ProductionRequest', timeout = 120 )
      reqDict = {'ProductionID':long( prodID ), 'RequestID':requestID, 'Used':reqUsed, 'BkEvents':0}
      result = reqClient.addProductionToRequest( reqDict )
      if not result['OK']:
        self.LHCbJob.log.error( 'Attempt to add production %s to request %s failed: %s ' % ( prodID, requestID,
                                                                                           result['Message'] ) )
        self.LHCbJob.log.error( 'Dictionary below:\n%s' % reqDict )
      else:
        self.LHCbJob.log.info( 'Successfully added production %s to request %s with flag set to %s' % ( prodID,
                                                                                                        requestID,
                                                                                                        reqUsed ) )

    if publish:
      groupDesc = self.LHCbJob.workflow.findParameter( 'groupDescription' ).getValue(),
      paramsDict = self._getProductionParameters( prodID = prodID,
                                                  prodXMLFile = fileName,
                                                  groupDescription = groupDesc,
                                                  bkPassInfo = bkSteps,
                                                  bkInputQuery = bkQuery,
                                                  reqID = requestID,
                                                  derivedProd = self.ancestorProduction )
      for n, v in paramsDict.items():
        result = self.setProdParameter( prodID, n, v )
        if not result['OK']:
          self.LHCbJob.log.error( result['Message'] )

    return S_OK( prodID )

  #############################################################################


  def getOutputLFNs( self, prodID = '12345', prodJobID = '6789', prodXMLFile = '' ):
    """ Will construct the output LFNs for the production for visual inspection.
    """
    # TODO: fix this construction: really necessary?

    if not prodXMLFile:
      self.LHCbJob.log.verbose( 'Using workflow object to generate XML file' )
      prodXMLFile = self.createWorkflow()

    job = LHCbJob( prodXMLFile )
    result = preSubmissionLFNs( job._getParameters(), job.workflow.createCode(),
                                productionID = prodID, jobID = prodJobID )
    if not result['OK']:
      return result
    lfns = result['Value']
    self.LHCbJob.log.verbose( lfns )
    return result

  #############################################################################

  def setProdParameter( self, prodID, pname, pvalue ):
    """Set a production parameter.
    """

    if type( pvalue ) == type( [] ):
      pvalue = '\n'.join( pvalue )

    if type( pvalue ) == type( 2 ):
      pvalue = str( pvalue )
    result = self.transClient.setTransformationParameter( int( prodID ), str( pname ), str( pvalue ) )
    if not result['OK']:
      self.LHCbJob.log.error( 'Problem setting parameter %s for production %s and value: %s. Error = %s' % ( pname,
                                                                                                             prodID,
                                                                                                             pvalue,
                                                                                                  result['Message'] ) )
    return result

  #############################################################################

  def getParameters( self, prodID, pname = '' ):
    """Get a production parameter or all of them if no parameter name specified.
    """

    result = self.transClient.getTransformation( int( prodID ), True )
    if not result['OK']:
      self.LHCbJob.log.error( result )
      return S_ERROR( 'Could not retrieve parameters for production %s' % prodID )

    if not result['Value']:
      self.LHCbJob.log.info( result )
      return S_ERROR( 'No additional parameters available for production %s' % prodID )

    if pname:
      if result['Value'].has_key( pname ):
        return S_OK( result['Value'][pname] )
      else:
        self.LHCbJob.log.verbose( result )
        return S_ERROR( 'Production %s does not have parameter %s' % ( prodID, pname ) )

    return result

  #############################################################################

  def setFileMask( self, fileMask, stepMask = '' ):
    """Output data related parameters.
    """
    if type( fileMask ) == type( [] ):
      fileMask = ';'.join( fileMask )
    self.setParameter( 'outputDataFileMask', 'string', fileMask, 'outputDataFileMask' )

    if stepMask:
      if type( stepMask ) == type( [] ):
        stepMask = ';'.join( stepMask )
    self.LHCbJob._addParameter( self.LHCbJob.workflow, 'outputDataStep', 'string', stepMask, 'outputDataStep Mask' )

  #############################################################################

  def banTier1s( self ):
    """ Sets Tier1s as banned.
    """

    tier1s = []

    lcgSites = gConfig.getSections( '/Resources/Sites/LCG' )
    if not lcgSites[ 'OK' ]:
      return lcgSites

    for lcgSite in lcgSites[ 'Value' ]:

      tier = gConfig.getValue( '/Resources/Sites/LCG/%s/MoUTierLevel' % lcgSite, 2 )
      if tier in ( 0, 1 ):
        tier1s.append( lcgSite )

#    tier1s = []
#    #from DIRAC.ResourceStatusSystem.Utilities.CS import getSites, getSiteTier
#    sites = getSites()
#
#    for site in sites:
#      tier = getSiteTier( site )
#      if tier in ( 0, 1 ):
#        tier1s.append( site )

    self.LHCbJob.setBannedSites( tier1s )

  #############################################################################

  def setOutputMode( self, outputMode ):
    """ Sets output mode for all jobs, this can be 'Local' or 'Any'.
    """
    if not outputMode.lower().capitalize() in ( 'Local', 'Any' ):
      raise TypeError, 'Output mode must be Local or Any'
    self.setParameter( 'outputMode', 'string', outputMode.lower().capitalize(), 'SEResolutionPolicy' )

  #############################################################################

  def setBKParameters( self, configName, configVersion, groupDescription, conditions ):
    """ Sets BK parameters for production.
    """
    self.setParameter( 'configName', 'string', configName, 'ConfigName' )
    self.setParameter( 'configVersion', 'string', configVersion, 'ConfigVersion' )
    self.setParameter( 'groupDescription', 'string', groupDescription, 'GroupDescription' )
    self.setParameter( 'conditions', 'string', conditions, 'SimOrDataTakingCondsString' )
    self.setParameter( 'simDescription', 'string', conditions, 'SimDescription' )

  #############################################################################