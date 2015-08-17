""" ModuleBase - contains the base class for LHCb workflow modules. Defines several
    common utility methods
"""

__RCSID__ = "$Id$"

import os
import copy
import time

from DIRAC                                                    import gLogger, siteName
from DIRAC.Core.Utilities.Adler                               import fileAdler
from DIRAC.ConfigurationSystem.Client.Helpers.Operations      import Operations
from DIRAC.Resources.Catalog.PoolXMLFile                      import getGUID
from DIRAC.WorkloadManagementSystem.Client.JobReport          import JobReport
from DIRAC.TransformationSystem.Client.FileReport             import FileReport
from DIRAC.RequestManagementSystem.Client.Request             import Request
from DIRAC.RequestManagementSystem.Client.Operation           import Operation
from DIRAC.RequestManagementSystem.Client.File                import File
from DIRAC.RequestManagementSystem.private.RequestValidator   import RequestValidator
from DIRAC.DataManagementSystem.Client.DataManager            import DataManager

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient     import BookkeepingClient
from LHCbDIRAC.Core.Utilities.ProductionData                  import getLogPath, constructProductionLFNs
from LHCbDIRAC.Core.Utilities.ProdConf                        import ProdConf
from LHCbDIRAC.Workflow.Modules.ModulesUtilities              import getEventsToProduce

class ModuleBase( object ):
  """ Base class for Modules - works only within DIRAC workflows
  """

  #############################################################################

  def __init__( self, loggerIn = None, operationsHelperIn = None, bkClientIn = None, dm = None ):
    """ Initialization of module base.
    """

    if not loggerIn:
      self.log = gLogger.getSubLogger( 'ModuleBase' )
    else:
      self.log = loggerIn

    if not operationsHelperIn:
      self.opsH = Operations()
    else:
      self.opsH = operationsHelperIn

    if not bkClientIn:
      self.bkClient = BookkeepingClient()
    else:
      self.bkClient = bkClientIn

    if not dm:
      self.dataManager = DataManager()
    else:
      self.dataManager = dm

    self.requestValidator = RequestValidator()

    self.production_id = ''
    self.prod_job_id = ''
    self.jobID = 0
    self.step_number = ''
    self.step_id = ''

    self.fileReport = None
    self.jobReport = None
    self.request = None

    self.workflowStatus = None
    self.stepStatus = None
    self.workflow_commons = None
    self.step_commons = None

    self.debugSE = 'CERN-DEBUG'

    self.executable = 'gaudirun.py'
    self.applicationName = 'Unknown'
    self.applicationVersion = 'Unknown'
    self.applicationLog = None
    self.applicationType = None
    self.systemConfig = None
    self.extraPackages = None
    self.bkConfigName = None
    self.BKstepID = None
    self.condDBTag = None
    self.DDDBTag = None
    self.dqTag = None
    self.CPUe = None
    self.eventType = ''
    self.gaudiSteps = None
    self.InputData = ''
    self.inputDataList = []
    self.inputDataType = None
    self.histoName = "Hist.root"
    self.optionsFile = None
    self.optionsFormat = None
    self.optionsLine = None
    self.extraOptionsLine = None
    self.jobType = None
    self.logFilePath = None
    self.onlineCondDBTag = None
    self.onlineDDDBTag = None
    self.outputSEs = {}
    self.outputDataFileMask = None
    self.numberOfEvents = -1
    self.maxNumberOfEvents = None
    self.TCK = None
    self.mcTCK = None
    self.multicoreJob = None
    self.multicoreStep = None
    self.poolXMLCatName = 'pool_xml_catalog.xml'
    self.persistency = ''
    self.processingPass = None
    self.runNumber = 'Unknown'
    self.runTimeProjectName = None
    self.runTimeProjectVersion = None
    self.simDescription = ''
    self.siteName = None
    self.stepName = None
    self.stepInputData = None
    self.XMLSummary = None
    self.stepProcPass = None
    self.outputFilePrefix = None


  #############################################################################

  def execute( self, version = None,
               production_id = None, prod_job_id = None, wms_job_id = None,
               workflowStatus = None, stepStatus = None,
               wf_commons = None, step_commons = None,
               step_number = None, step_id = None ):
    """ Function called by all super classes
    """

    if version:
      self.log.info( '===== Executing ' + version + ' ===== ' )

    if production_id:
      self.production_id = production_id
    else:
      self.production_id = self.workflow_commons['PRODUCTION_ID']

    if prod_job_id:
      self.prod_job_id = prod_job_id
    else:
      self.prod_job_id = self.workflow_commons['JOB_ID']

    if 'JOBID' in os.environ:
      self.jobID = os.environ['JOBID']

    if wms_job_id:
      self.jobID = wms_job_id

    if workflowStatus:
      self.workflowStatus = workflowStatus

    if stepStatus:
      self.stepStatus = stepStatus

    if wf_commons:
      self.workflow_commons = wf_commons

    if step_commons:
      self.step_commons = step_commons

    if step_number:
      self.step_number = step_number
    else:
      self.step_number = self.STEP_NUMBER

    if step_id:
      self.step_id = step_id
    else:
      self.step_id = '%s_%s_%s' % ( self.production_id, self.prod_job_id, self.step_number )

    self.siteName = siteName()

  #############################################################################

  def finalize( self, version = None ):
    """ Just finalizing
    """

    self.log.flushAllMessages( 0 )

    if version:
      self.log.info( '===== Terminating ' + version + ' ===== ' )

  #############################################################################

  def setApplicationStatus( self, status, sendFlag = True ):
    """Wraps around setJobApplicationStatus of state update client
    """
    if not self._WMSJob():
      return 0  # e.g. running locally prior to submission

    if self._checkWFAndStepStatus( noPrint = True ):
      # The application status won't be updated in case the workflow or the step is failed already
      if not isinstance( status, str ):
        status = str( status )
      self.log.verbose( 'setJobApplicationStatus(%d, %s)' % ( self.jobID, status ) )
      jobStatus = self.jobReport.setApplicationStatus( status, sendFlag )
      if not jobStatus['OK']:
        self.log.warn( jobStatus['Message'] )

  #############################################################################

  def setJobParameter( self, name, value, sendFlag = True ):
    """Wraps around setJobParameter of state update client
    """
    if not self._WMSJob():
      return 0  # e.g. running locally prior to submission

    self.log.verbose( 'setJobParameter(%d,%s,%s)' % ( self.jobID, name, value ) )

    jobParam = self.jobReport.setJobParameter( str( name ), str( value ), sendFlag )
    if not jobParam['OK']:
      self.log.warn( jobParam['Message'] )

  #############################################################################

  def _resolveInputVariables( self ):
    """ By convention the module input parameters are resolved here.
    """

    self.log.verbose( "workflow_commons = ", self.workflow_commons )
    self.log.verbose( "step_commons = ", self.step_commons )

    self.fileReport = self._getFileReporter()
    self.jobReport = self._getJobReporter()
    self.request = self._getRequestContainer()

    self.__resolveInputWorkflow()

  #############################################################################

  def __resolveInputWorkflow( self ):
    """ Resolve the input variables that are in the workflow_commons
    """

    self.runNumber = self.workflow_commons.get( 'runNumber', self.runNumber )

    self.persistency = self.workflow_commons.get( 'persistency', self.persistency )

    self.jobType = self.workflow_commons.get( 'JobType', self.jobType )

    self.poolXMLCatName = self.workflow_commons.get( 'poolXMLCatName', self.poolXMLCatName )

    self.InputData = self.workflow_commons.get( 'InputData', self.InputData )

    if 'ParametricInputData' in self.workflow_commons:
      pID = copy.deepcopy( self.workflow_commons['ParametricInputData'] )
      if pID:
        if isinstance( pID, list ):
          pID = ';'.join( pID )
  #      self.InputData += ';' + pID
        self.InputData = pID
        self.InputData = self.InputData.rstrip( ';' )

    if self.InputData == ';':
      self.InputData = ''

    self.inputDataList = [lfn.strip( 'LFN:' ) for lfn in self.InputData.split( ';' ) if lfn]

    # only required until the stripping is the same for MC / data
    self.bkConfigName = self.workflow_commons.get( 'configName', self.bkConfigName )

    self.simDescription = self.workflow_commons.get( 'simDescription', self.simDescription )

    if 'runMetadata' in self.workflow_commons:
      runMetadataDict = eval( self.workflow_commons['runMetadata'] )
      self.onlineDDDBTag = runMetadataDict['DDDB']
      self.onlineCondDBTag = runMetadataDict['CondDb']
      self.TCK = runMetadataDict['TCK']

    if 'outputDataFileMask' in self.workflow_commons:
      self.outputDataFileMask = self.workflow_commons['outputDataFileMask']
      if not isinstance( self.outputDataFileMask, list ):
        self.outputDataFileMask = [i.lower().strip() for i in self.outputDataFileMask.split( ';' )]

    self.gaudiSteps = self.workflow_commons.get( 'gaudiSteps', self.gaudiSteps )

    if 'CPUe' in self.workflow_commons:
      self.CPUe = int( round( float( self.workflow_commons['CPUe'] ) ) )

    self.multicoreJob = self.workflow_commons.get( 'multicore', self.multicoreJob )

    self.processingPass = self.workflow_commons.get( 'processingPass', self.processingPass )

    self.logFilePath = self.workflow_commons.get( 'LogFilePath', self.logFilePath )
    if isinstance( self.logFilePath, list ):
      self.logFilePath = self.logFilePath[0]
    else:
      if 'PRODUCTION_ID' and 'JOB_ID' and 'configVersion' and 'configName' in self.workflow_commons:
        self.log.info( 'LogFilePath parameter not found, creating on the fly' )
        result = getLogPath( self.workflow_commons, self.bkClient )
        if not result['OK']:
          self.log.error( 'Could not create LogFilePath', result['Message'] )
          raise RuntimeError( result['Message'] )
        self.logFilePath = result['Value']['LogFilePath'][0]

    if 'maxNumberOfEvents' in self.workflow_commons:
      self.maxNumberOfEvents = int( self.workflow_commons[ 'maxNumberOfEvents'] )

    self.eventType = self.workflow_commons.get( 'eventType', self.eventType )

    self.numberOfEvents = int( self.workflow_commons.get( 'numberOfEvents', self.numberOfEvents ) )

    if 'outputSEs' in self.workflow_commons:
      self.outputSEs = self.workflow_commons[ 'outputSEs' ]
    else:
      # this is here for backward compatibility
      histogramSE = self.opsH.getValue( 'Productions/HistogramSE', 'CERN-HIST' )
      histoTypes = self.opsH.getValue( 'Productions/HistogramTypes', ['HIST', 'BRUNELHIST', 'DAVINCIHIST',
                                                                      'GAUSSHIST'] )
      self.outputSEs = dict( ( ht, histogramSE ) for ht in histoTypes )
    # for older productions we construct it based on what should be found in the steps
    if 'listoutput' in self.step_commons:
      listOutputStep = self.step_commons['listoutput']
      for lOutput in listOutputStep:
        try:
          for outputDataType in lOutput['outputDataType'].split( ';' ):
            if outputDataType:
              self.outputSEs.setdefault( outputDataType.upper(), lOutput['outputDataSE'] )
        except KeyError:
          continue
        self.workflow_commons['outputSEs'] = self.outputSEs

  #############################################################################

  def _resolveInputStep( self ):
    """ Resolve the input variables for an application step
    """

    self.stepName = self.step_commons['STEP_INSTANCE_NAME']

    self.executable = self.step_commons.get( 'executable', self.executable )

    self.applicationName = self.step_commons.get( 'applicationName', self.applicationName )

    self.applicationVersion = self.step_commons.get( 'applicationVersion', self.applicationVersion )

    self.applicationLog = self.step_commons.get( 'applicationLog', self.applicationLog )

    self.XMLSummary = self.step_commons.get( 'XMLSummary', self.XMLSummary )

    self.BKstepID = self.step_commons.get( 'BKStepID', self.BKstepID )

    self.stepProcPass = self.step_commons.get( 'StepProcPass', self.stepProcPass )

    self.outputFilePrefix = self.step_commons.get( 'outputFilePrefix', self.outputFilePrefix )

    self.inputDataType = self.step_commons.get( 'inputDataType', self.inputDataType )

    self.histoName = self.step_commons.get( 'HistogramName', self.histoName )

    self.applicationType = self.step_commons.get( 'applicationType', self.applicationType )

    self.optionsFile = self.step_commons.get( 'optionsFile', self.optionsFile )

    self.optionsLine = self.step_commons.get( 'optionsLine', self.optionsLine )

    self.extraOptionsLine = self.step_commons.get( 'extraOptionsLine', self.extraOptionsLine )

    if 'runTimeProjectName' in self.step_commons:
      self.runTimeProjectName = self.step_commons['runTimeProjectName']
      self.runTimeProjectVersion = self.step_commons['runTimeProjectVersion']

    if 'extraPackages' in self.step_commons:
      self.extraPackages = self.step_commons['extraPackages']
      if not self.extraPackages == '':
        if not isinstance( self.extraPackages, list ):
          self.extraPackages = self.extraPackages.split( ';' )

    stepInputData = []
    if 'inputData' in self.step_commons:
      if self.step_commons['inputData']:
        stepInputData = self.step_commons['inputData']
    elif self.InputData:
      stepInputData = copy.deepcopy( self.InputData )
    if stepInputData:
      stepInputData = self._determineStepInputData( stepInputData, )
      self.stepInputData = [sid.strip( 'LFN:' ) for sid in stepInputData]

    self.optionsFormat = self.step_commons.get( 'optionsFormat', self.optionsFormat )

    self.multicoreStep = self.step_commons.get( 'multiCore', self.multicoreStep )

    self.systemConfig = self.step_commons.get( 'SystemConfig', self.systemConfig )

    self.mcTCK = self.step_commons.get( 'mcTCK', self.mcTCK )

    self.DDDBTag = self.step_commons.get( 'DDDBTag', self.DDDBTag )

    self.condDBTag = self.step_commons.get( 'CondDBTag', self.condDBTag )

    self.dqTag = self.step_commons.get( 'DQTag', self.dqTag )

  #############################################################################

  def _determineOutputs( self ):
    """ Determines the correct outputs.
        For merging jobs the output has to be the same as the input.
        For the others, we use what is in the step definition
        We always remove the 'HIST'(s), when present
    """

    if self.jobType.lower() == 'merge':
      res = self.bkClient.getFileMetadata( self.stepInputData )
      if not res['OK']:
        raise RuntimeError( res['Message'] )

      outputTypes = []
      if len( res['Value']['Successful'] ) != len( self.stepInputData ):
        self.log.warn( "Some inputs are not in BKK, trying to parse the file names" )
        for sid in self.stepInputData:
          fType = '.'.join( os.path.basename( sid ).split( '.' )[1:] ).lower()
          if fType not in outputTypes:
            outputTypes.append( fType )
      else:
        for mdDict in res['Value']['Successful'].values():
          if mdDict['FileType'] not in outputTypes:
            outputTypes.append( mdDict['FileType'] )

      if len( outputTypes ) > 1:
        raise ValueError( "Not all input files have the same type" )
      outputType = outputTypes[0].lower()
      stepOutTypes = [outputType.lower()]
      stepOutputs = [{'outputDataName': self.step_id + '.' + outputType.lower(),
                      'outputDataType': outputType.lower(),
                      'outputBKType': outputType.upper()}]


    else:
      stepOutputs = self.step_commons['listoutput']

      stepOutputsT = [x['outputDataType'] for x in stepOutputs]
      stepOutTypes = []
      for fts in stepOutputsT:
        for ft in fts.split( ';' ):
          if ft and ft not in stepOutTypes:
            stepOutTypes.append( ft.lower() )


    histogram = False
    for hist in self.opsH.getValue( 'Productions/HistogramTypes', ['HIST', 'BRUNELHIST', 'DAVINCIHIST', 'GAUSSHIST' ] ):
      try:
        stepOutTypes.remove( hist )
        histogram = True
      except ValueError:
        pass
      try:
        stepOutTypes.remove( hist.lower() )
        histogram = True
      except ValueError:
        pass

    return stepOutputs, stepOutTypes, histogram

  #############################################################################

  def _getJobReporter( self ):
    """ just return the job reporter (object, always defined by dirac-jobexec)
    """

    if 'JobReport' in self.workflow_commons:
      return self.workflow_commons['JobReport']
    else:
      jobReport = JobReport( self.jobID )
      self.workflow_commons['JobReport'] = jobReport
      return jobReport

  #############################################################################

  def _getFileReporter( self ):
    """ just return the file reporter (object)
    """

    if 'FileReport' in self.workflow_commons:
      return self.workflow_commons['FileReport']
    else:
      fileReport = FileReport()
      self.workflow_commons['FileReport'] = fileReport
      return fileReport

  #############################################################################

  def _getRequestContainer( self ):
    """ just return the Request reporter (object)
    """

    if 'Request' in self.workflow_commons:
      return self.workflow_commons['Request']
    else:
      request = Request()
      self.workflow_commons['Request'] = request
      return request

  #############################################################################

  def getCandidateFiles( self, outputList, outputLFNs, fileMask = '', stepMask = '' ):
    """ Returns list of candidate files to upload, check if some outputs are missing.

        outputList has the following structure:
          [ {'outputDataType':'', 'outputDataName':''} , {...} ]

        outputLFNs is the list of output LFNs for the job

        fileMask is the output file extensions to restrict the outputs to

        returns dictionary containing type, SE and LFN for files restricted by mask
    """
    fileInfo = {}

    for outputFile in outputList:
      if 'outputDataType' in outputFile and 'outputDataName' in outputFile:
        fname = outputFile['outputDataName']
        fileType = outputFile['outputDataType']
        fileInfo[fname] = {'type':fileType}
      else:
        self.log.error( 'Ignoring mal-formed output data specification', str( outputFile ) )

    for lfn in outputLFNs:
      if os.path.basename( lfn ) in fileInfo.keys():
        fileInfo[os.path.basename( lfn )]['lfn'] = lfn
        self.log.verbose( "Found LFN %s for file %s" % ( lfn, os.path.basename( lfn ) ) )
      elif os.path.basename(lfn).split('_')[-1] in fileInfo.keys():
        fileInfo[os.path.basename( lfn ).split('_')[-1]]['lfn'] = lfn
        self.log.verbose( "Found LFN %s for file %s" % ( lfn, os.path.basename( lfn ).split( '_' )[-1] ) )

    # check local existance
    self._checkLocalExistance( fileInfo.keys() )

    # Select which files have to be uploaded: in principle all
    candidateFiles = self._applyMask( fileInfo, fileMask, stepMask )

    # Sanity check all final candidate metadata keys are present
    self._checkSanity( candidateFiles )

    # Adding the SEs
    for candidateFile in candidateFiles:
      try:
        fType = candidateFiles[candidateFile]['type']
        for fType in [fType, fType.lower(), fType.upper(), fType.capitalize()]:
          try:
            wfSE = self.outputSEs[fType]
            candidateFiles[candidateFile]['workflowSE'] = wfSE
            break
          except KeyError:
            continue
      except AttributeError:
        break

    return candidateFiles

  #############################################################################

  def _checkLocalExistance( self, fileList ):
    """ Check that the list of output files are present locally
    """

    notPresentFiles = []

    for fileName in fileList:
      if not os.path.exists( fileName ):
        notPresentFiles.append( fileName )

    if notPresentFiles:
      self.log.error( 'Output data file list %s does not exist locally' % notPresentFiles )
      raise os.error( "Output data not found" )

  #############################################################################

  def _applyMask( self, candidateFilesIn, fileMask, stepMask ):
    """ Select which files have to be uploaded: in principle all
    """
    candidateFiles = copy.deepcopy( candidateFilesIn )

    if fileMask and not isinstance( fileMask, list ):
      fileMask = [fileMask]
    if isinstance( stepMask, int ):
      stepMask = str( stepMask )
    if stepMask and not isinstance( stepMask, list ):
      stepMask = [stepMask]

    if fileMask and fileMask != ['']:
      for fileName, metadata in candidateFiles.items():
        if metadata['type'].lower() not in fileMask:  # and ( fileName.split( '.' )[-1] not in fileMask ) ):
          del candidateFiles[fileName]
          self.log.info( 'Output file %s was produced but will not be treated (fileMask is %s)' % ( fileName,
                                                                                                    ', '.join( fileMask ) ) )
    else:
      self.log.info( 'No outputDataFileMask provided, the files with all the extensions will be considered' )

    if stepMask and stepMask != ['']:
      for fileName, metadata in candidateFiles.items():
        if fileName.lower().replace( metadata['type'], '' ).split( '_' )[-1].split( '.' )[0] not in stepMask:
          del candidateFiles[fileName]
          self.log.info( 'Output file %s was produced but will not be treated (stepMask is %s)' % ( fileName,
                                                                                                    ', '.join( stepMask ) ) )
    else:
      self.log.info( 'No outputDataStep provided, the files output of all the steps will be considered' )

    return candidateFiles

  #############################################################################

  def _checkSanity( self, candidateFiles ):
    """ Sanity check all final candidate metadata keys are present
    """

    notPresentKeys = []

    mandatoryKeys = ['type', 'lfn']  # filedict is used for requests
    for fileName, metadata in candidateFiles.items():
      for key in mandatoryKeys:
        if not key in metadata:
          notPresentKeys.append( ( fileName, key ) )

    if notPresentKeys:
      for fileName_keys in notPresentKeys:
        self.log.error( "File %s has missing %s" % ( fileName_keys[0], fileName_keys[1] ) )
      raise ValueError( "Missing requested fileName keys" )

  #############################################################################

  def getFileMetadata( self, candidateFiles ):
    """ Returns the candidate file dictionary with associated metadata.

        The input candidate files dictionary has the structure:
        {'foo_1.txt': {'lfn': '/lhcb/MC/2010/DST/00012345/0001/foo_1.txt',
                       'type': 'txt',
                       'workflowSE': SE1},
        'bar_2.py': {'lfn': '/lhcb/MC/2010/DST/00012345/0001/bar_2.py',
                     'type': 'py',
                     'workflowSE': 'SE2'},
        }

        this also assumes the files are in the current working directory.
    """
    # Retrieve the POOL File GUID(s) for any final output files
    self.log.info( 'Will search for POOL GUIDs for: %s' % ( ', '.join( candidateFiles.keys() ) ) )
    pfnGUID = getGUID( candidateFiles.keys() )
    if not pfnGUID['OK']:
      self.log.error( '''PoolXMLFile failed to determine POOL GUID(s) for output file list,
      these will be generated by the DataManager''', pfnGUID['Message'] )
      for fileName in candidateFiles.keys():
        candidateFiles[fileName]['guid'] = ''
    elif pfnGUID['generated']:
      self.log.warn( 'PoolXMLFile generated GUID(s) for the following files ', ', '.join( pfnGUID['generated'] ) )
    else:
      self.log.info( 'GUIDs found for all specified POOL files: %s' % ( ', '.join( candidateFiles.keys() ) ) )

    for pfn, guid in pfnGUID['Value'].items():
      candidateFiles[pfn]['guid'] = guid

    # Get all additional metadata about the file necessary for requests
    final = {}
    for fileName, metadata in candidateFiles.items():
      fileDict = {}
      fileDict['LFN'] = metadata['lfn']
      fileDict['Size'] = os.path.getsize( fileName )
      fileDict['Checksum'] = fileAdler( fileName )
      fileDict['ChecksumType'] = 'ADLER32'
      fileDict['GUID'] = metadata['guid']
      fileDict['Status'] = 'Waiting'

      final[fileName] = metadata
      final[fileName]['filedict'] = fileDict
      final[fileName]['localpath'] = '%s/%s' % ( os.getcwd(), fileName )

    # Sanity check all final candidate metadata keys are present (return S_ERROR if not)
    mandatoryKeys = ['guid', 'filedict']  # filedict is used for requests (this method adds guid and filedict)
    for fileName, metadata in final.items():
      for key in mandatoryKeys:
        if not key in metadata:
          raise RuntimeError( "File %s has missing %s" % ( fileName, key ) )

    return final

  #############################################################################

  def _determineStepInputData( self, inputData ):
    """ determine the input data for the step
    """
    if inputData == 'previousStep':
      stepIndex = self.gaudiSteps.index( self.stepName )
      previousStep = self.gaudiSteps[stepIndex - 1]

      stepInputData = []
      for outputF in self.workflow_commons['outputList']:
        try:
          if outputF['stepName'] == previousStep and outputF['outputBKType'].lower() == self.inputDataType.lower():
            stepInputData.append( outputF['outputDataName'] )
        except KeyError:
          raise RuntimeError( "Can't find output of step %s" % previousStep )

      return stepInputData

    else:
      return [x.strip( 'LFN:' ) for x in inputData.split( ';' )]

  #############################################################################

  def _manageAppOutput( self, outputs ):
    """ Calls self._findOutputs to find what's produced, then creates the LFNs
    """

    if not outputs:
      self.log.warn( 'Step outputs are not defined (normal user jobs. Not normal in productions and SAM jobs)' )
      return
    else:
      finalOutputs, _bkFileTypes = self._findOutputs( outputs )

    self.log.info( 'Final step outputs are: %s' % ( finalOutputs ) )
    self.step_commons['listoutput'] = finalOutputs

    if 'outputList' in self.workflow_commons:
      for outFile in finalOutputs:
        if outFile not in self.workflow_commons['outputList']:
          self.workflow_commons['outputList'].append( outFile )
    else:
      self.workflow_commons['outputList'] = finalOutputs

    if 'PRODUCTION_ID' and 'JOB_ID' and 'configVersion' and 'configName' in self.workflow_commons:
      self.log.info( 'Attempting to recreate the production output LFNs...' )
      result = constructProductionLFNs( self.workflow_commons, self.bkClient )
      if not result['OK']:
        raise IOError( "Could not create production LFNs: %s" % result['Message'] )
      self.workflow_commons['BookkeepingLFNs'] = result['Value']['BookkeepingLFNs']
      self.workflow_commons['LogFilePath'] = result['Value']['LogFilePath']
      self.workflow_commons['ProductionOutputData'] = result['Value']['ProductionOutputData']

  #############################################################################

  def _findOutputs( self, stepOutput ):
    """ Find which outputs of those in stepOutput (what are expected to be produced) are effectively produced.
        stepOutput, as called here, corresponds to step_commons['listoutput']

        stepOutput =
        [{'outputDataType': 'bhadron.dst', 'outputBKType': 'BHADRON.DST',
        'outputDataName': '00012345_00012345_2.BHADRON.DST'},
        {'outputDataType': 'calibration.dst','outputDataType': 'CALIBRATION.DST',
        'outputDataName': '00012345_00012345_2.CALIBRATION.DST'},

    """

    bkFileTypes = []
    finalOutputs = []

    filesFound = []

    for output in stepOutput:

      found = False
      fileOnDisk = None

      for fileOnDisk in os.listdir( '.' ):
        if output['outputDataName'].lower() == fileOnDisk.lower():
          found = True
          break

      if found and fileOnDisk:
        self.log.info( 'Found output file %s matching %s (case is not considered)' % ( fileOnDisk,
                                                                                       output['outputDataName'] ) )
        output['outputDataName'] = fileOnDisk
        filesFound.append( output )
      else:
        self.log.error( '%s not found' % output['outputDataName'] )
        raise IOError( "OutputData not found" )

    for fileFound in filesFound:
      bkFileTypes.append( fileFound['outputDataType'].upper() )
      finalOutputs.append( {'outputDataName': fileFound['outputDataName'],
                            'outputDataType': fileFound['outputDataType'].lower(),
                            'outputBKType': fileFound['outputDataType'].upper(),
                            'stepName':self.stepName} )

    return ( finalOutputs, bkFileTypes )

  #############################################################################

  def _WMSJob( self ):
    """ Check if this job is running via WMS
    """
    return True if self.jobID else False

  #############################################################################

  def _enableModule( self ):
    """ Enable module if it's running via WMS
    """
    if not self._WMSJob():
      self.log.info( 'No WMS JobID found, disabling module via control flag' )
      return False
    else:
      self.log.verbose( 'Found WMS JobID = %d' % self.jobID )
      return True

  #############################################################################

  def _checkWFAndStepStatus( self, noPrint = False ):
    """ Check the WF and Step status
    """
    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      if not noPrint:
        self.log.info( 'Skip this module, failure detected in a previous step :' )
        self.log.info( 'Workflow status : %s' % ( self.workflowStatus ) )
        self.log.info( 'Step Status : %s' % ( self.stepStatus ) )
      return False
    else:
      return True

  #############################################################################

  def _disableWatchdogCPUCheck( self ):
    """ just writes a file to disable the watchdog
    """
    self.log.info( "Creating DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK in order to disable the Watchdog" )
    fopen = open( 'DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK', 'w' )
    fopen.write( '%s' % time.asctime() )
    fopen.close()

  #############################################################################

  def generateFailoverFile( self ):
    """ Retrieve the accumulated reporting request, and produce a JSON file that is consumed by the JobWrapper
    """
    reportRequest = None
    result = self.jobReport.generateForwardDISET()
    if not result['OK']:
      self.log.warn( "Could not generate Operation for job report with result:\n%s" % ( result ) )
    else:
      reportRequest = result['Value']
    if reportRequest:
      self.log.info( "Populating request with job report information" )
      self.request.addOperation( reportRequest )

    if len( self.request ):
      try:
        optimized = self.request.optimize()
      except AttributeError:
        optimized = {'OK':True}
      if not optimized['OK']:
        self.log.error( "Could not optimize: %s" % optimized['Message'] )
        self.log.error( "Not failing the job because of that, keep going" )
      isValid = self.requestValidator.validate( self.request )
      if not isValid['OK']:
        raise RuntimeError( "Failover request is not valid: %s" % isValid['Message'] )
      else:
        requestJSON = self.request.toJSON()
        if requestJSON['OK']:
          self.log.info( "Creating failover request for deferred operations for job %d" % self.jobID )
          request_string = str( requestJSON['Value'] )
          self.log.debug( request_string )
          # Write out the request string
          fname = '%s_%s_request.json' % ( self.production_id, self.prod_job_id )
          jsonFile = open( fname, 'w' )
          jsonFile.write( request_string )
          jsonFile.close()
          self.log.info( "Created file containing failover request %s" % fname )
          result = self.request.getDigest()
          if result['OK']:
            self.log.info( "Digest of the request: %s" % result['Value'] )
          else:
            self.log.warn( "No digest? That's not sooo important, anyway: %s" % result['Message'] )
        else:
          raise RuntimeError( requestJSON['Message'] )

    accountingReport = None
    if 'AccountingReport' in self.workflow_commons:
      accountingReport = self.workflow_commons['AccountingReport']
    if accountingReport:
      result = accountingReport.commit()
      if not result['OK']:
        self.log.error( "!!! Both accounting and RequestDB are down? !!!" )
        self.log.error( "accountingReport result: %s" % result['Message'] )
        self.log.error( "Anyway, the job won't fail for this reason, because this is \"just\" the accounting report" )

  #############################################################################

  def setBKRegistrationRequest( self, lfn, error = '',
                                metaData = {'Checksum':'justSomething',
                                            'ChecksumType': 'ADLER32',
                                            'GUID': 'aGUID'} ):
    """ Set a BK registration request for changing the replica flag.  Uses the global request object.
    """
    if error:
      self.log.info( 'BK registration for %s failed with message: "%s" setting failover request' % ( lfn, error ) )
    else:
      self.log.info( 'Setting BK registration request for %s' % ( lfn ) )

    regFile = Operation()
    regFile.Type = 'RegisterFile'
    regFile.Catalog = 'BookkeepingDB'

    bkFile = File()
    bkFile.LFN = lfn
    # this should NOT be needed... but RMS complains!
    bkFile.PFN = lfn
    bkFile.GUID = metaData['GUID']
    bkFile.Checksum = metaData['Checksum']
    bkFile.ChecksumType = metaData['ChecksumType']

    regFile.addFile( bkFile )
    res = self.request.addOperation( regFile )
    if not res['OK']:
      raise RuntimeError( res['Message'] )

  #############################################################################

  def createProdConfFile( self, stepOutputTypes, histogram, runNumberGauss, firstEventNumberGauss ):
    """ Utility that creates a ProdConf file, used mostly as input for gaudirun jobs
    """
    # Creating ProdConf file
    prodConfFileName = 'prodConf_%s_%s_%s_%s.py' % ( self.applicationName,
                                                     self.production_id,
                                                     self.prod_job_id,
                                                     self.step_number )
    optionsDict = {}

    optionsDict['Application'] = self.applicationName

    optionsDict['AppVersion'] = self.applicationVersion

    if self.optionsFormat:
      optionsDict['OptionFormat'] = self.optionsFormat

    if self.stepInputData:
      optionsDict['InputFiles'] = ['LFN:' + sid for sid in self.stepInputData]
    else:
      if self.applicationName.lower() != "gauss":
        raise RuntimeError( "No MC, but no input data" )

    if self.outputFilePrefix:
      optionsDict['OutputFilePrefix'] = self.outputFilePrefix

    optionsDict['OutputFileTypes'] = stepOutputTypes

    optionsDict['XMLSummaryFile'] = self.XMLSummary

    optionsDict['XMLFileCatalog'] = self.poolXMLCatName

    if histogram:
      optionsDict['HistogramFile'] = self.histoName

    if self.DDDBTag:
      if self.DDDBTag.lower() == 'online':
        try:
          optionsDict['DDDBTag'] = self.onlineDDDBTag
          self.log.debug( 'Set the online DDDB tag' )
        except NameError, e:
          self.log.error( 'Could not find an online DDDb Tag: ', e )
          raise RuntimeError( "Could not find an online DDDb Tag" )
      else:
        optionsDict['DDDBTag'] = self.DDDBTag

    if self.condDBTag:
      if self.condDBTag.lower() == 'online':
        optionsDict['CondDBTag'] = self.onlineCondDBTag
        self.log.debug( 'Set the online CondDB tag' )
      else:
        optionsDict['CondDBTag'] = self.condDBTag

    if self.dqTag:
      optionsDict['DQTag'] = self.dqTag

    if self.applicationName.lower() == 'gauss':
      if self.CPUe and self.maxNumberOfEvents:
        # Here we set maxCPUTime to 1 day, which seems reasonable
        eventsToProduce = getEventsToProduce( self.CPUe,
                                              maxNumberOfEvents = self.maxNumberOfEvents,
                                              maxCPUTime = 86400 )
      else:
        eventsToProduce = self.numberOfEvents
    else:
      eventsToProduce = self.numberOfEvents
    optionsDict['NOfEvents'] = eventsToProduce

    if runNumberGauss:
      optionsDict['RunNumber'] = runNumberGauss

    if self.runNumber:
      if self.runNumber != 'Unknown':
        optionsDict['RunNumber'] = self.runNumber

    if firstEventNumberGauss:
      optionsDict['FirstEventNumber'] = firstEventNumberGauss

    # TCK: can't have both set!
    if self.TCK and self.mcTCK:
      raise RuntimeError( "%s step: TCK set in step, and should't be!" % self.applicationName )
    if self.TCK or self.mcTCK:
      optionsDict['TCK'] = self.TCK if self.TCK else self.mcTCK

    if self.processingPass:
      optionsDict['ProcessingPass'] = self.processingPass

    prodConfFile = ProdConf( prodConfFileName )
    self.log.debug( optionsDict )
    prodConfFile.putOptionsIn( optionsDict )

    return prodConfFileName

  #############################################################################

  # properties
  def set_jobID( self, value ):
    if isinstance( value, str ):
      if value:
        value = int( value )
      else:
        value = 0
    self._jobID = value
  def get_jobID( self ):
    return self._jobID
  jobID = property( get_jobID, set_jobID )
