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

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Workflow.Workflow import Workflow, fromXMLString
from DIRAC.Core.Utilities.List import removeEmptyElements, uniqueElements

from LHCbDIRAC.Core.Utilities.ProductionData import preSubmissionLFNs
from LHCbDIRAC.Workflow.Utilities.Utils import getStepDefinition

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
      from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
      self.LHCbJob = LHCbJob( script )

    if BKKClientIn is not None:
      self.BKKClient = BKKClientIn
    else:
      from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
      self.BKKClient = BookkeepingClient()

    if transClientIn is not None:
      self.transClient = transClientIn
    else:
      from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
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
    if not script:
      self.__setDefaults()

  #############################################################################

  def __setDefaults( self ):
    """Sets some default parameters.
    """

    self.LHCbJob.gaudiStepCount = 0
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

    #version control
    self._setParameter( 'productionVersion', 'string', __RCSID__, 'ProdAPIVersion' )

    #General workflow parameters
    self._setParameter( 'PRODUCTION_ID', 'string', '00012345', 'ProductionID' )
    self._setParameter( 'JOB_ID', 'string', '00006789', 'ProductionJobID' )
    self._setParameter( 'poolXMLCatName', 'string', 'pool_xml_catalog.xml', 'POOLXMLCatalogName' )
    self._setParameter( 'outputMode', 'string', 'Any', 'SEResolutionPolicy' )
    self._setParameter( 'outputDataFileMask', 'string', '', 'outputDataFileMask' )

    #BK related parameters
    self._setParameter( 'configName', 'string', 'MC', 'ConfigName' )
    self._setParameter( 'configVersion', 'string', '2009', 'ConfigVersion' )
    self._setParameter( 'conditions', 'string', '', 'SimOrDataTakingCondsString' )

  #############################################################################

  def setJobParameters( self, parametersDict ):
    """ Set an (LHCb)Job parameter

        The parametersDict is in the form {'parameterName': 'value'}
        Each parameter calls LHCbJob.setparameterName(value)
    """

    for parameter in parametersDict.keys():
      getattr( self.LHCbJob, 'set' + parameter )( parametersDict[parameter] )

  #############################################################################

  def _setParameter( self, name, parameterType, parameterValue, description ):
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

    for o in optionsFile:
      if re.search( 'DECFILESROOT', o ):
        self.LHCbJob.log.verbose( '%s specified, checking event type options: %s' % ( self.firstEventType, o ) )
        if re.search( '@', o ) or re.search( '%s' % self.firstEventType, o ):
          self.LHCbJob.log.verbose( 'Options: %s specify event type correctly' % ( o ) )
        else:
          raise TypeError, 'Event type options must be the event type number or workflow parameter'

    self.LHCbJob.log.verbose( 'Extra packages and event type options are correctly specified' )
    return S_OK()

  #############################################################################

  def addApplicationStep( self, stepDict, outputSE, optionsLine, inputData = None ):
    """ stepDict contains everything that is in the step, for this production, e.g.:
        {'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': 13718, 'ApplicationVersion': 'v28r3p1',
        'ExtraPackages': 'AppConfig.v3r104', 'StepName': 'Stripping14-Merging',
        'ProcessingPass': 'Merging', 'Visible': 'N', 'DDDB': 'head-20110302',
        'OptionFiles': '$APPCONFIGOPTS/Merging/DV-Stripping14-Merging.py', 'CONDDB': 'head-20110407',
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

    #a series of various checks
    if not type( appName ) == type( ' ' ) or not type( appVersion ) == type( ' ' ):
      raise TypeError, 'Expected strings for application name and version'

    if extraPackages:
      if type( extraPackages ) == type( [] ):
        extraPackages = ';'.join( extraPackages )
      if type( extraPackages ) != type( '' ):
        raise TypeError, 'extraPackages is not a string (nor a list)'
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


    #starting real stuff
    self.LHCbJob.gaudiStepCount += 1

    if 'Gaudi_App_Step' not in self.LHCbJob.workflow.step_definitions.keys():

      gaudiModules = [ 'GaudiApplication', 'AnalyseLogFile', 'AnalyseXMLSummary',
                        'ErrorLogging', 'BookkeepingReport', 'StepAccounting' ]
      gaudiPath = 'Productions/GaudiStep_Modules'
      modulesNameList = self.LHCbJob.opsHelper.getValue( gaudiPath, gaudiModules )
      #pName, pType, pValue, pDesc
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

    #create the step instance add it to the wf, and return it
    gaudiStepInstance = self.LHCbJob.workflow.createStepInstance( 'Gaudi_App_Step',
                                                                  '%s_%s' % ( appName,
                                                                              self.LHCbJob.gaudiStepCount ) )

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
      if len( fileTypesIn ) != 1:
        raise IndexError, 'More than one data type in input'
      fileTypeIn = fileTypesIn[0].upper()
      valuesToSet.append( [ 'inputDataType', fileTypeIn ] )

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
    self.__addSoftwarePackages( '%s.%s' % ( appName, appVersion ) )
    self.__addSoftwarePackages( extraPackages )

    #to construct the BK processing pass structure, starts from step '0'
    stepIDInternal = 'Step%s' % ( self.LHCbJob.gaudiStepCount - 1 )
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

    return gaudiStepInstance

  #############################################################################


  def _constructOutputFilesDict( self, filesList, outputSE, histoName = None, histoSE = None ):
    """ build list of dictionary of output files, including HIST case, and fix outputSE for file
    """

    if not histoName:
      histoName = self.histogramName

    if not histoSE:
      histoSE = self.histogramSE

    outputList = []

    for fileType in filesList:
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
        modulesNameList = self.LHCbJob.opsHelper.getValue( 'Productions/FinalizationStep_Modules',
                                                   [ 'UploadOutputData', 'UploadLogFile', 'FailoverRequest' ] )
      else:
        modulesNameList = modulesList

      jobFinalizationStepDef = getStepDefinition( 'Job_Finalization', modulesNameList = modulesNameList )
      self.LHCbJob.workflow.addStep( jobFinalizationStepDef )

    #create the step instance add it to the wf
    self.LHCbJob.workflow.createStepInstance( 'Job_Finalization', 'finalization' )

  #############################################################################
  def createWorkflow( self, name = '' ):
    """ Create XML for local testing.
    """
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

  def runLocal( self, DiracLHCb = None ):
    """
        Create XML workflow for local testing then reformulate as a job and run locally.
    """

    name = self.createWorkflow()['Value']
    # this "name" is the xml file 
    from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
    j = LHCbJob( name )
    # it makes a job (a Worklow, with Parameters), out of the xml file

    if DiracLHCb is not None:
      diracLHCb = DiracLHCb
    else:
      from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
      diracLHCb = DiracLHCb()

    return j.runLocal( diracLHCb, self.BKKClient )

  #############################################################################

  def getDetailedInfo( self, productionID ):
    """ Return detailed information for a given production.
    """
    return self.getParameters( int( productionID ), 'DetailedInfo' )

  #############################################################################

  def _getProductionParameters( self, prodXMLFile, prodID, groupDescription = '',
                                bkPassInfo = {}, bkInputQuery = {},
                                derivedProd = 0, reqID = 0 ):
    """ This method will publish production parameters.
    """

    prodWorkflow = Workflow( prodXMLFile )
    if not bkPassInfo:
      bkPassInfo = prodWorkflow.findParameter( 'BKProcessingPass' ).getValue()
    if not groupDescription:
      groupDescription = prodWorkflow.findParameter( 'groupDescription' ).getValue()

    parameters = {}

    parameters['Priority'] = prodWorkflow.findParameter( 'Priority' ).getValue()
    parameters['CondDBTag'] = prodWorkflow.findParameter( 'CondDBTag' ).getValue()
    parameters['DDDBTag'] = prodWorkflow.findParameter( 'DDDBTag' ).getValue()
    parameters['DQTag'] = prodWorkflow.findParameter( 'DQTag' ).getValue()
    parameters['configName'] = prodWorkflow.findParameter( 'configName' ).getValue()
    parameters['configVersion'] = prodWorkflow.findParameter( 'configVersion' ).getValue()
    parameters['outputDataFileMask'] = prodWorkflow.findParameter( 'outputDataFileMask' ).getValue()
    parameters['JobType'] = prodWorkflow.findParameter( 'JobType' ).getValue()
    parameters['SizeGroup'] = self.jobFileGroupSize

    if parameters['JobType'].lower() == 'mcsimulation':
      if prodWorkflow.findParameter( 'MaxNumberOfTasks' ):
        parameters['MaxNumberOfTasks'] = prodWorkflow.findParameter( 'MaxNumberOfTasks' ).getValue()

    if prodWorkflow.findParameter( 'InputData' ): #now only comes from BK query
      prodWorkflow.findParameter( 'InputData' ).setValue( '' )
      self.LHCbJob.log.verbose( 'Resetting input data for production to null, this comes from a BK query...' )
      prodXMLFile = self.createWorkflow( prodXMLFile )['Value']
      #prodWorkflow.toXMLFile(prodXMLFile)

    if prodWorkflow.findParameter( 'TransformationFamily' ):
      parameters['TransformationFamily'] = prodWorkflow.findParameter( 'TransformationFamily' ).getValue()

    parameters['BKCondition'] = prodWorkflow.findParameter( 'conditions' ).getValue()

    if not bkInputQuery and parameters['JobType'].lower() != 'mcsimulation':
      res = self.transClient.getBookkeepingQueryForTransformation( int( prodID ) )
      if not res['OK']:
        self.LHCbJob.log.error( res )
        raise ValueError, 'Could not obtain production info'
      bkInputQuery = res['Value']

    parameters['BKInputQuery'] = bkInputQuery
    parameters['BKProcessingPass'] = bkPassInfo
    parameters['groupDescription'] = groupDescription
    parameters['RequestID'] = reqID
    parameters['DerivedProduction'] = derivedProd

    dummyProdJobID = '99999999'
    result = self.getOutputLFNs( prodID, dummyProdJobID, prodXMLFile )
    if not result['OK']:
      self.LHCbJob.log.error( 'Could not create production LFNs', result )

    outputLFNs = result['Value']
    parameters['OutputLFNs'] = outputLFNs

    outputDirectories = []
    del outputLFNs['BookkeepingLFNs'] #since ProductionOutputData uses the file mask
    for i in outputLFNs.values():
      for j in i:
        outputDir = '%s%s' % ( j.split( str( prodID ) )[0], prodID )
        if not outputDir in outputDirectories:
          outputDirectories.append( outputDir )

    parameters['OutputDirectories'] = outputDirectories
    #Create detailed information string similar to ELOG entry
    #TODO: put tags per step and include other interesting parameters
    info = []
    info.append( '%s Production %s for event type %s has following parameters:\n' % ( parameters['JobType'],
                                                                                      prodID,
                                                                                      parameters['eventType'] ) )
    info.append( 'Production priority: %s' % ( parameters['Priority'] ) )
    info.append( 'BK Config Name Version: %s %s' % ( parameters['configName'], parameters['configVersion'] ) )
    info.append( 'BK Processing Pass Name: %s' % ( parameters['groupDescription'] ) )
    info.append( 'CondDB Tag: %s' % ( parameters['CondDBTag'] ) )
    info.append( 'DDDB Tag: %s\n' % ( parameters['DDDBTag'] ) )
    info.append( 'DQ Tag: %s\n' % ( parameters['DQTag'] ) )
    #info.append('Number of events: % s' %(parameters['numberOfEvents']))
    #Now for the steps of the workflow
    stepKeys = bkPassInfo.keys()
    stepKeys.sort()
    for step in stepKeys:
      info.append( '====> %s %s %s' % ( bkPassInfo[step]['ApplicationName'],
                                        bkPassInfo[step]['ApplicationVersion'],
                                        step ) )
      info.append( '%s Option Files:' % ( bkPassInfo[step]['ApplicationName'] ) )
      for opts in bkPassInfo[step]['OptionFiles'].split( ';' ):
        info.append( '%s' % opts )
      info.append( 'ExtraPackages: %s' % ( bkPassInfo[step]['ExtraPackages'] ) )

    if parameters['BKInputQuery']:
      info.append( '\nBK Input Data Query:' )
      for n, v in parameters['BKInputQuery'].items():
        info.append( '%s= %s' % ( n, v ) )

    #BK output directories (very useful)
    bkPaths = []
    bkOutputPath = '%s/%s/%s/%s/%s' % ( parameters['configName'],
                                        parameters['configVersion'],
                                        parameters['BKCondition'],
                                        parameters['groupDescription'],
                                        parameters['eventType'] )
    fileTypes = parameters['outputDataFileMask']
    fileTypes = [a.upper() for a in fileTypes.split( ';' )]

    #Annoying that histograms are extension root
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
                  False - does not publish the production, allows to check the BK script

        bkScript = True - will write a script that can be checked first before
                          adding to BK
                   False - will print BK parameters but publish the production

        transformation = True - will create a transformation to distribute the output data if bkScript is False
                         False - will not create the transformation or a transformation script in case bkScript=True

        The workflow XML is created regardless of the flags.
    """
    #Needs to be revisited in order to disentangle the many operations.

    #FIXME: requestID is the same as the parameter TransformationFamily. Why having 2? What for the Derived productions?
    if not parentRequestID:
      parentRequestID = requestID

    self.setParentRequest( parentRequestID )

    if wfString:
      self.LHCbJob.workflow = fromXMLString( wfString )
#      self.name = self.LHCbJob.workflow.getName()

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

    bkConditions = self.LHCbJob.workflow.findParameter( 'conditions' ).getValue()

    bkDict = {}
    bkSteps = self.LHCbJob.workflow.findParameter( 'BKProcessingPass' ).getValue()
    bkDict['Steps'] = bkSteps
    bkDict['GroupDescription'] = self.LHCbJob.workflow.findParameter( 'groupDescription' ).getValue()

    # After the reorganisation by steps release this stuff can be greatly simplified
    # only the stepID, stepName and stepVisible need to be tracked.
    # In the first instance I am just demonstrating the new functionality without making
    # sweeping changes
    bkDictStep = {}

    #Add the BK conditions metadata / name
    simConds = self.BKKClient.getSimConditions()
    if not simConds['OK']:
      self.LHCbJob.log.error( 'Could not retrieve conditions data from BK:\n%s' % simConds )
      return simConds
    simulationDescriptions = []
    for record in simConds['Value']:
      simulationDescriptions.append( str( record[1] ) )

    realDataFlag = False
    if not bkConditions in simulationDescriptions:
      self.LHCbJob.log.verbose( 'Assuming BK conditions %s are DataTakingConditions' % bkConditions )
      bkDict['DataTakingConditions'] = bkConditions
      bkDictStep['DataTakingConditions'] = bkConditions
      realDataFlag = True
    else:
      self.LHCbJob.log.verbose( 'Found simulation conditions for %s' % bkConditions )
      bkDict['SimulationConditions'] = bkConditions
      bkDictStep['SimulationConditions'] = bkConditions

    #Adding some MC transformation parameters if present
    maxNumberOfTasks = 0
    maxEventsPerTask = 0
    if self.LHCbJob.workflow.findParameter( 'MaxNumberOfTasks' ):
      maxNumberOfTasks = self.LHCbJob.workflow.findParameter( 'MaxNumberOfTasks' ).getValue()
    if self.LHCbJob.workflow.findParameter( 'EventsPerTask' ):
      maxEventsPerTask = self.LHCbJob.workflow.findParameter( 'EventsPerTask' ).getValue()

    descShort = self.LHCbJob.workflow.getDescrShort()
    descLong = self.LHCbJob.workflow.getDescription()

    prodID = 0
    if publish:

      self._setParameter( 'ProcessingType', 'JDL', str( self.prodGroup ), 'ProductionGroupOrType' )
      self._setParameter( 'Priority', 'JDL', str( self.priority ), 'UserPriority' )

      #This mechanism desperately needs to be reviewed
      result = self.transClient.addTransformation( fileName, descShort, descLong, self.LHCbJob.type, self.plugin, 'Manual',
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

    bkDict['Production'] = int( prodID )
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
          self.LHCbJob.log.error( 'Production %s was created but BK processsing pass for %s was not found' % ( prodID, queryProdID ) )
          return inputPass
        inputPass = inputPass['Value']
        self.LHCbJob.log.info( 'Setting %s as BK input production for %s with processing pass %s' % ( queryProdID, prodID, inputPass ) )
        bkDict['InputProductionTotalProcessingPass'] = inputPass
        bkDictStep['InputProductionTotalProcessingPass'] = inputPass
      elif queryProcPass:
        self.LHCbJob.log.info( 'Adding input BK processing pass for production %s from input data query: %s' % ( prodID, queryProcPass ) )
        bkDict['InputProductionTotalProcessingPass'] = queryProcPass
        bkDictStep['InputProductionTotalProcessingPass'] = queryProcPass

    if bkProcPassPrepend:
      self.LHCbJob.log.info( 'The following path will be prepended to the BK processing pass for this production: %s' % ( bkProcPassPrepend ) )
      bkDict['InputProductionTotalProcessingPass'] = bkProcPassPrepend
      bkDictStep['InputProductionTotalProcessingPass'] = bkProcPassPrepend

    stepList = []
    stepKeys = bkSteps.keys()
    #The BK needs an ordered list of steps
    stepKeys.sort()
    for step in stepKeys:
      stepID = bkSteps[step]['BKStepID']
      if stepID:
        stepName = bkSteps[step]['StepName']
        stepVisible = bkSteps[step]['StepVisible']
        stepList.append( {'StepId':int( stepID ), 'StepName':stepName, 'Visible':stepVisible} )

    #This is the last component necessary for the BK publishing (post reorganisation)
    bkDictStep['Steps'] = stepList

    if bkScript:
      self.LHCbJob.log.verbose( 'Writing BK publish script...' )
      self._publishProductionToBK( bkDictStep, prodID, script = True )
    else:
      for n, v in bkDictStep.items():
        self.LHCbJob.log.verbose( '%s BK parameter is: %s' % ( n, v ) )

    if publish and not bkScript:
      self._publishProductionToBK( bkDictStep, prodID, script = False )

    if requestID and publish:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      reqClient = RPCClient( 'ProductionManagement/ProductionRequest', timeout = 120 )
      reqDict = {'ProductionID':long( prodID ), 'RequestID':requestID, 'Used':reqUsed, 'BkEvents':0}
      result = reqClient.addProductionToRequest( reqDict )
      if not result['OK']:
        self.LHCbJob.log.error( 'Attempt to add production %s to request %s failed, dictionary below:\n%s' % ( prodID, requestID, reqDict ) )
      else:
        self.LHCbJob.log.info( 'Successfully added production %s to request %s with Used flag set to %s' % ( prodID, requestID, reqUsed ) )

    if publish:
      try:
        paramsDict = self._getProductionParameters( prodID = prodID,
                                                    prodXMLFile = fileName,
                                                    groupDescription = bkDict['GroupDescription'],
                                                    bkPassInfo = bkDict['Steps'],
                                                    bkInputQuery = bkQuery,
                                                    reqID = requestID,
                                                    derivedProd = self.ancestorProduction )
        for n, v in paramsDict.items():
          result = self.setProdParameter( prodID, n, v )
          if not result['OK']:
            self.LHCbJob.log.error( result['Message'] )

      except Exception, x:
        self.LHCbJob.log.error( 'Failed to set production parameters with exception\n%s\nThis can be done later...' % ( str( x ) ) )

    if transformation and not bkScript:
      if not bkQuery.has_key( 'FileType' ):
        return S_ERROR( 'BK query does not include FileType!' )
      bkFileType = bkQuery['FileType']
      result = self._createTransformation( prodID, bkFileType, transReplicas, reqID = requestID, realData = realDataFlag,
                                           prodPlugin = self.plugin, groupDescription = bkDict['GroupDescription'],
                                           parentRequestID = parentRequestID, transformationPlugin = transformationPlugin )
      if not result['OK']:
        self.LHCbJob.log.error( 'Transformation creation failed with below result, can be done later...\n%s' % ( result ) )
      else:
        self.LHCbJob.log.info( 'Successfully created transformation %s for production %s' % ( result['Value'], prodID ) )

      transID = result['Value']
      if transID and prodID:
        result = self.setProdParameter( prodID, 'AssociatedTransformation', transID )
        if not result['OK']:
          self.LHCbJob.log.error( 'Could not set AssociatedTransformation parameter to %s for %s with result %s' % ( transID, prodID, result ) )

    elif transformation:
      if not bkQuery.has_key( 'FileType' ):
        return S_ERROR( 'BK query does not include FileType!' )
      bkFileType = bkQuery['FileType']
      self.LHCbJob.log.info( 'transformation is %s, bkScript generation is %s, writing transformation script' % ( transformation, bkScript ) )
      transID = self._createTransformation( prodID, bkFileType, transReplicas, reqID = requestID, realData = realDataFlag,
                                           script = True, prodPlugin = self.plugin, groupDescription = bkDict['GroupDescription'],
                                           parentRequestID = parentRequestID, transformationPlugin = transformationPlugin )
      if not transID['OK']:
        self.LHCbJob.log.error( 'Problem writing transformation script, result was: %s' % transID )
      else:
        self.LHCbJob.log.verbose( 'Successfully created transformation script for prod %s' % prodID )
    else:
      self.LHCbJob.log.info( 'transformation is %s, bkScript generation is %s, will not write transformation script' % ( transformation, bkScript ) )

    return S_OK( prodID )

  #############################################################################
  def _createTransformation( self, inputProd, fileType, replicas, reqID = 0, realData = True,
                             script = False, prodPlugin = '', groupDescription = '',
                             parentRequestID = 0, transformationPlugin = '' ):
    """ Create a transformation to distribute the output data for a given production.
    """

    inputProd = int( inputProd )
    replicas = int( replicas )

    if transformationPlugin:
      plugin = transformationPlugin
    else:
      if realData:
        plugin = 'LHCbDSTBroadcast'
      else:
        plugin = 'LHCbMCDSTBroadcast'

    tName = '%sReplication_Prod%s' % ( fileType, inputProd )
    if reqID:
      tName = 'Request_%s_%s' % ( reqID, tName )

    if script:
      transLines = ['# Transformation publishing script created on %s by' % ( time.asctime() )]
      transLines.append( '# by %s' % self.prodVersion )
      transLines.append( 'from LHCbDIRAC.TransformationSystem.Client.Transformation import Transformation' )
      transLines.append( 'transformation=Transformation()' )
      transLines.append( 'transformation.setTransformationName("%s")' % ( tName ) )
      if type( fileType ) == type( [] ):
        transLines.append( """transformation.setBkQuery({"ProductionID":%s,"FileType":%s})""" % ( inputProd, fileType ) )
      else:
        transLines.append( 'transformation.setBkQuery({"ProductionID":%s,"FileType":"%s"})' % ( inputProd, fileType ) )
      transLines.append( 'transformation.setDescription("Replication of transformation %s output data")' % ( inputProd ) )
      transLines.append( 'transformation.setLongDescription("This transformation is to replicate the output data from transformation %s according to the computing model")' % ( inputProd ) )
      transLines.append( 'transformation.setType("Replication")' )
      transLines.append( 'transformation.setPlugin("%s")' % plugin )
      if replicas > 1:
        transLines.append( 'transformation.setDestinations(%s)' % replicas )
      transLines.append( 'transformation.addTransformation()' )
      transLines.append( 'transformation.setStatus("Active")' )
      transLines.append( 'transformation.setAgentType("Automatic")' )
      transLines.append( 'transformation.setTransformationGroup("%s")' % ( groupDescription ) )
      transLines.append( 'print transformation.getTransformationID()' )
      if os.path.exists( '%s.py' % tName ):
        shutil.move( '%s.py' % tName, '%s.py.backup' % tName )
      fopen = open( '%s.py' % tName, 'w' )
      fopen.write( '\n'.join( transLines ) + '\n' )
      fopen.close()
      return S_OK()


    self.transformation.setTransformationName( tName )
    self.transformation.setBkQuery( {'ProductionID':inputProd, 'FileType':fileType} )
    self.transformation.setDescription( 'Replication of transformation %s output data' % inputProd )
    self.transformation.setLongDescription( 'This transformation is to replicate the output data from transformation %s according to the computing model' % ( inputProd ) )
    self.transformation.setType( 'Replication' )
    self.transformation.setPlugin( plugin )
    if replicas > 1:
      self.transformation.setDestinations( replicas )
    self.transformation.setTransformationGroup( groupDescription )
    self.transformation.addTransformation()
    self.transformation.setStatus( 'Active' )
    self.transformation.setAgentType( 'Automatic' )
    transResult = self.transformation.getTransformationID()
    if not transResult['OK']:
      return transResult

    transID = transResult['Value']
    if parentRequestID:
      result = self.setProdParameter( transID, 'TransformationFamily', parentRequestID )
      if not result['OK']:
        self.LHCbJob.log.error( 'Could not set TransformationFamily parameter to %s for %s with result %s' % ( parentRequestID, transID, result ) )

    # Since other prods also have this parameter defined.
    result = self.setProdParameter( transID, 'groupDescription', groupDescription )
    if not result['OK']:
      self.LHCbJob.log.error( 'Could no set groupDescription parameter with result %s' % ( result['Message'] ) )

    # Set the detailed info parameter such that the "Show Details" portal option works for transformations.
    infoString = []
    infoString.append( 'Replication transformation %s was created for %s\nWith plugin %s' % ( transID, groupDescription, plugin ) )
    infoString.append( '\nBK Input Data Query:\n    ProductionID : %s\n    FileType     : %s' % ( inputProd, fileType ) )
    infoString = '\n'.join( infoString )
    result = self.setProdParameter( transID, 'DetailedInfo', infoString )
    if not result['OK']:
      self.LHCbJob.log.error( 'Could not set Transformation DetailedInfo parameter for %s with result %s' % ( transID, result ) )

    return transResult

  #############################################################################
  def _publishProductionToBK( self, bkDict, prodID, script = False ):
    """Publishes the production to the BK or writes a script to do so.
    """
    if script:
      bkName = 'insertBKPass%s.py' % ( prodID )
      if os.path.exists( bkName ):
        shutil.move( bkName, '%s.backup' % bkName )
      fopen = open( bkName, 'w' )
      bkLines = ['# Bookkeeping publishing script created on %s by' % ( time.asctime() )]
      bkLines.append( '# by %s' % self.prodVersion )
      bkLines.append( 'from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient' )
      bkLines.append( 'bkClient = BookkeepingClient()' )
      bkLines.append( 'bkDict = %s' % bkDict )
      bkLines.append( 'print bkClient.addProduction(bkDict)' )
      fopen.write( '\n'.join( bkLines ) + '\n' )
      fopen.close()
      return S_OK( bkName )
    self.LHCbJob.log.verbose( 'Attempting to publish production %s to the BK' % ( prodID ) )
    result = self.BKKClient.addProduction( bkDict )
    if not result['OK']:
      self.LHCbJob.log.error( result )
    return result

  #############################################################################

  def getOutputLFNs( self, prodID = '12345', prodJobID = '6789', prodXMLFile = '' ):
    """ Will construct the output LFNs for the production for visual inspection.
    """
    #TODO: fix this construction: really necessary?

    if not prodXMLFile:
      self.LHCbJob.log.verbose( 'Using workflow object to generate XML file' )
      prodXMLFile = self.createWorkflow()

    from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
    job = LHCbJob( prodXMLFile )
    result = preSubmissionLFNs( job._getParameters(), job.createCode(),
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
      self.LHCbJob.log.error( 'Problem setting parameter %s for production %s and value: %s. Error = %s' % ( pname, prodID,
                                                                                                             pvalue, result['Message'] ) )
    return result

  #############################################################################

  def getParameters( self, prodID, pname = '', printOutput = False ):
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

    if printOutput:
      for n, v in result['Value'].items():
        if not n.lower() == 'body':
          print '=' * len( n ), '\n', n, '\n', '=' * len( n )
          print v
        else:
          print '*Omitted Body from printout*'

    return result

  #############################################################################

  def setFileMask( self, fileMask, stepMask = '' ):
    """Output data related parameters.
    """
    if type( fileMask ) == type( [] ):
      fileMask = ';'.join( fileMask )
    self._setParameter( 'outputDataFileMask', 'string', fileMask, 'outputDataFileMask' )

    if stepMask:
      if type( stepMask ) == type( [] ):
        stepMask = ';'.join( stepMask )
    self.LHCbJob._addParameter( self.LHCbJob.workflow, 'outputDataStep', 'string', stepMask, 'outputDataStep Mask' )

  #############################################################################

  def banTier1s( self ):
    """ Sets Tier1s as banned.
    """
    tier1s = []
    from DIRAC.ResourceStatusSystem.Utilities.CS import getSites, getSiteTier
    sites = getSites()

    for site in sites:
      tier = getSiteTier( site )
      if tier in ( 0, 1 ):
        tier1s.append( site )

    self.LHCbJob.setBannedSites( tier1s )

  #############################################################################

  def setTargetSite( self, site ):
    """ Sets destination for all jobs.
    """
    self.LHCbJob.setDestination( site )

  #############################################################################

  def setOutputMode( self, outputMode ):
    """ Sets output mode for all jobs, this can be 'Local' or 'Any'.
    """
    if not outputMode.lower().capitalize() in ( 'Local', 'Any' ):
      raise TypeError, 'Output mode must be Local or Any'
    self._setParameter( 'outputMode', 'string', outputMode.lower().capitalize(), 'SEResolutionPolicy' )

  #############################################################################

  def setBKParameters( self, configName, configVersion, groupDescription, conditions ):
    """ Sets BK parameters for production.
    """
    self._setParameter( 'configName', 'string', configName, 'ConfigName' )
    self._setParameter( 'configVersion', 'string', configVersion, 'ConfigVersion' )
    self._setParameter( 'groupDescription', 'string', groupDescription, 'GroupDescription' )
    self._setParameter( 'conditions', 'string', conditions, 'SimOrDataTakingCondsString' )
    self._setParameter( 'simDescription', 'string', conditions, 'SimDescription' )

  #############################################################################
