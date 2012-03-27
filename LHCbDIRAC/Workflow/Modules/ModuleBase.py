########################################################################
# $HeadURL$
########################################################################

""" ModuleBase - base class for LHCb workflow modules. Defines several
    common utility methods

"""

__RCSID__ = "$Id$"

import os, copy

import DIRAC
from DIRAC  import S_OK, S_ERROR

class ModuleBase( object ):

  #############################################################################

  def __init__( self, loggerIn = None ):
    """ Initialization of module base.
    """

    if not loggerIn:
      from DIRAC import gLogger
      self.log = gLogger.getSubLogger( 'ModuleBase' )
    else:
      self.log = loggerIn

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

    if not production_id:
      self.production_id = self.PRODUCTION_ID
    else:
      self.production_id = production_id

    if not prod_job_id:
      self.prod_job_id = self.JOB_ID
    else:
      self.prod_job_id = prod_job_id

    if not wms_job_id:
      self.jobID = ''
      if os.environ.has_key( 'JOBID' ):
        self.jobID = os.environ['JOBID']
    else:
      self.jobID = wms_job_id

    if not workflowStatus:
      self.workflowStatus = self.workflowStatus
    else:
      self.workflowStatus = workflowStatus
    if not stepStatus:
      self.stepStatus = self.stepStatus
    else:
      self.stepStatus = stepStatus
    if not wf_commons:
      self.workflow_commons = self.workflow_commons
    else:
      self.workflow_commons = wf_commons
    if not step_commons:
      self.step_commons = self.step_commons
    else:
      self.step_commons = step_commons
    if not step_number:
      self.step_number = self.STEP_NUMBER
    else:
      self.step_number = step_number
    if not step_id:
      self.step_id = '%s_%s_%s' % ( self.production_id, self.prod_job_id, self.step_number )
    else:
      self.step_id = step_id

  #############################################################################

  def finalize( self, version = None ):
    """ Just finalizing
    """

    self.log.flushAllMessages( 0 )

    if version:
      self.log.info( '===== Terminating ' + version + ' ===== ' )

  #############################################################################

  def setApplicationStatus( self, status, sendFlag = True, jr = None ):
    """Wraps around setJobApplicationStatus of state update client
    """
    if not self._WMSJob():
      return S_OK( 'JobID not defined' ) # e.g. running locally prior to submission

    self.log.verbose( 'setJobApplicationStatus(%s, %s)' % ( self.jobID, status ) )

    if not jr:
      jr = self._getJobReporter()

    jobStatus = jr.setApplicationStatus( status, sendFlag )
    if not jobStatus['OK']:
      self.log.warn( jobStatus['Message'] )

    return jobStatus

  #############################################################################

  def sendStoredStatusInfo( self, jr = None ):
    """Wraps around sendStoredStatusInfo of state update client
    """
    if not self._WMSJob():
      return S_OK( 'JobID not defined' ) # e.g. running locally prior to submission

    if not jr:
      jr = self._getJobReporter()

    sendStatus = jr.sendStoredStatusInfo()
    if not sendStatus['OK']:
      self.log.error( sendStatus['Message'] )

    return sendStatus

  #############################################################################

  def setJobParameter( self, name, value, sendFlag = True, jr = None ):
    """Wraps around setJobParameter of state update client
    """
    if not self._WMSJob():
      return S_OK( 'JobID not defined' ) # e.g. running locally prior to submission

    self.log.verbose( 'setJobParameter(%s,%s,%s)' % ( self.jobID, name, value ) )

    if not jr:
      jr = self._getJobReporter()

    jobParam = jr.setJobParameter( str( name ), str( value ), sendFlag )
    if not jobParam['OK']:
      self.log.warn( jobParam['Message'] )

    return jobParam

  #############################################################################

  def sendStoredJobParameters( self, jr = None ):
    """Wraps around sendStoredJobParameters of state update client
    """
    if not self._WMSJob():
      return S_OK( 'JobID not defined' ) # e.g. running locally prior to submission

    if not jr:
      jr = self.__getJobReporter()

    sendStatus = jr.sendStoredJobParameters()
    if not sendStatus['OK']:
      self.log.error( sendStatus['Message'] )

    return sendStatus

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

    if self.workflow_commons.has_key( 'SystemConfig' ):
      self.systemConfig = self.workflow_commons['SystemConfig']

    if self.workflow_commons.has_key( 'runNumber' ):
      self.runNumber = self.workflow_commons['runNumber']
    else:
      self.runNumber = 'Unknown'

    if self.workflow_commons.has_key( 'JobType' ):
      self.jobType = self.workflow_commons['JobType']

    if self.workflow_commons.has_key( 'poolXMLCatName' ):
      self.poolXMLCatName = self.workflow_commons['poolXMLCatName']

    self.InputData = ''
    if self.workflow_commons.has_key( 'InputData' ):
      if self.workflow_commons['InputData']:
        self.InputData = self.workflow_commons['InputData']

    if self.workflow_commons.has_key( 'ParametricInputData' ):
      pID = copy.deepcopy( self.workflow_commons['ParametricInputData'] )
      if pID:
        if type( pID ) == type( [] ):
          pID = ';'.join( pID )
  #      self.InputData += ';' + pID
        self.InputData = pID
        self.InputData = self.InputData.rstrip( ';' )

    if self.InputData == ';':
      self.InputData = ''

    #only required until the stripping is the same for MC / data
    if self.workflow_commons.has_key( 'configName' ):
      self.bkConfigName = self.workflow_commons['configName']

    if self.workflow_commons.has_key( 'simDescription' ):
      self.simDescription = self.workflow_commons['simDescription']

    if self.workflow_commons.has_key( 'DDDBTag' ):
      self.DDDBTag = self.workflow_commons['DDDBTag']

    if self.workflow_commons.has_key( 'CondDBTag' ):
      self.CondDBTag = self.workflow_commons['CondDBTag']

    if self.workflow_commons.has_key( 'DQTag' ):
      self.DQTag = self.workflow_commons['DQTag']

  #############################################################################

  def _resolveInputStep( self ):
    """ Resolve the input variables for an application step
    """

    if self.step_commons.has_key( 'applicationName' ):
      self.applicationName = self.step_commons['applicationName']

    if self.step_commons.has_key( 'applicationVersion' ):
      self.applicationVersion = self.step_commons['applicationVersion']

    if self.step_commons.has_key( 'applicationLog' ):
      self.applicationLog = self.step_commons['applicationLog']

    if self.step_commons.has_key( 'XMLSummary' ):
      self.XMLSummary = self.step_commons['XMLSummary']

    if self.step_commons.has_key( 'BKStepID' ):
      self.BKstepID = self.step_commons['BKStepID']

    if self.step_commons.has_key( 'StepProcPass' ):
      self.stepProcPass = self.step_commons['StepProcPass']

    if self.step_commons.has_key( 'outputFilePrefix' ):
      self.outputFilePrefix = self.step_commons['outputFilePrefix']

    if self.step_commons.has_key( 'numberOfEvents' ):
      self.numberOfEvents = self.step_commons['numberOfEvents']

    if self.step_commons.has_key( 'eventType' ):
      self.eventType = self.step_commons['eventType']

    if self.step_commons.has_key( 'inputDataType' ):
      self.inputDataType = self.step_commons['inputDataType']

    if self.step_commons.has_key( 'HistogramName' ):
      self.histoName = self.step_commons[ 'HistogramName' ]

    if self.step_commons.has_key( 'applicationType' ):
      self.applicationType = self.step_commons['applicationType']

    if self.step_commons.has_key( 'numberOfEvents' ):
      self.numberOfEvents = self.step_commons['numberOfEvents']

    if self.step_commons.has_key( 'optionsFile' ):
      self.optionsFile = self.step_commons['optionsFile']

    if self.step_commons.has_key( 'optionsLine' ):
      self.optionsLine = self.step_commons['optionsLine']

    if self.step_commons.has_key( 'extraOptionsLine' ):
      self.extraOptionsLine = self.step_commons['extraOptionsLine']

    if self.step_commons.has_key( 'generatorName' ):
      self.generator_name = self.step_commons['generatorName']

    if self.step_commons.has_key( 'runTimeProjectName' ):
      self.runTimeProjectName = self.step_commons['runTimeProjectName']
      self.runTimeProjectVersion = self.step_commons['runTimeProjectVersion']

    if self.step_commons.has_key( 'extraPackages' ):
      self.extraPackages = self.step_commons['extraPackages']
      if not self.extraPackages == '':
        if type( self.extraPackages ) != type( [] ):
          self.extraPackages = self.extraPackages.split( ';' )

    if self.step_commons.has_key( 'inputData' ):
      if self.step_commons['inputData']:
        self.stepInputData = self.step_commons['inputData']

    if self.step_commons.has_key( 'listoutput' ):
      self.stepOutputs = self.step_commons['listoutput']
      self.stepOutputsType = [x['outputDataType'] for x in self.stepOutputs]

    if self.step_commons.has_key( 'optionsFormat' ):
      self.optionsFormat = self.step_commons['optionsFormat']

  #############################################################################

  def _getJobReporter( self ):
    """ just return the job reporter (object, always defined by dirac-jobexec)
    """

    if self.workflow_commons.has_key( 'JobReport' ):
      return self.workflow_commons['JobReport']
    else:
      from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport
      jobReport = JobReport( self.jobID )
      self.workflow_commons['JobReport'] = jobReport
      return jobReport

  #############################################################################

  def _getFileReporter( self ):
    """ just return the file reporter (object)
    """

    if self.workflow_commons.has_key( 'FileReport' ):
      return self.workflow_commons['FileReport']
    else:
      from DIRAC.TransformationSystem.Client.FileReport import FileReport
      fileReport = FileReport()
      self.workflow_commons['FileReport'] = fileReport
      return fileReport

  #############################################################################

  def _getRequestContainer( self ):
    """ just return the RequestContainer reporter (object)
    """

    if self.workflow_commons.has_key( 'Request' ):
      return self.workflow_commons['Request']
    else:
      from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
      request = RequestContainer()
      self.workflow_commons['Request'] = request
      return request

  #############################################################################

  def setFileStatus( self, production, lfn, status, fileReport = None ):
    """ set the file status for the given production in the Production Database
    """
    self.log.verbose( 'setFileStatus(%s,%s,%s)' % ( production, lfn, status ) )

    if not fileReport:
      fileReport = self._getFileReporter()

    fileReport.setFileStatus( production, lfn, status )

  #############################################################################

  def getCandidateFiles( self, outputList, outputLFNs, fileMask, stepMask = '' ):
    """ Returns list of candidate files to upload, check if some outputs are missing.
        
        outputList has the following structure:
          [ {'outputDataType':'','outputDataSE':'','outputDataName':''} , {...} ] 
          
        outputLFNs is the list of output LFNs for the job
        
        fileMask is the output file extensions to restrict the outputs to
        
        returns dictionary containing type, SE and LFN for files restricted by mask
    """
    fileInfo = {}

    for outputFile in outputList:
      if outputFile.has_key( 'outputDataType' ) and outputFile.has_key( 'outputDataSE' ) and outputFile.has_key( 'outputDataName' ):
        fname = outputFile['outputDataName']
        fileSE = outputFile['outputDataSE']
        fileType = outputFile['outputDataType']
        fileInfo[fname] = {'type':fileType, 'workflowSE':fileSE}
      else:
        self.log.error( 'Ignoring malformed output data specification', str( outputFile ) )

    for lfn in outputLFNs:
      if os.path.basename( lfn ) in fileInfo.keys():
        fileInfo[os.path.basename( lfn )]['lfn'] = lfn
        self.log.verbose( 'Found LFN %s for file %s' % ( lfn, os.path.basename( lfn ) ) )

    # check local existance
    try:
      self._checkLocalExistance( fileInfo.keys() )
    except OSError:
      return S_ERROR( 'Output Data Not Found' )

    #Select which files have to be uploaded: in principle all
    candidateFiles = self._applyMask( fileInfo, fileMask, stepMask )

    #Sanity check all final candidate metadata keys are present (return S_ERROR if not)
    try:
      self._checkSanity( candidateFiles )
    except ValueError:
      return S_ERROR( 'Missing requested fileName keys' )

    return S_OK( candidateFiles )

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
      raise os.error

  #############################################################################

  def _applyMask( self, candidateFilesIn, fileMask, stepMask ):
    """ Select which files have to be uploaded: in principle all
    """
    candidateFiles = copy.deepcopy( candidateFilesIn )

    if type( fileMask ) != type( [] ):
      fileMask = [fileMask]

    if fileMask and fileMask != ['']:
      for fileName, metadata in candidateFiles.items():
        if ( ( metadata['type'].lower() not in fileMask ) ):#and ( fileName.split( '.' )[-1] not in fileMask ) ):
          del( candidateFiles[fileName] )
          self.log.info( 'Output file %s was produced but will not be treated (fileMask is %s)' % ( fileName, ', '.join( fileMask ) ) )
    else:
      self.log.info( 'No outputDataFileMask provided, the files with all the extensions will be considered' )

    if stepMask:
      for fileName, metadata in candidateFiles.items():
        if fileName.split( '_' )[-1].split( '.' )[0] not in stepMask:
          del( candidateFiles[fileName] )
          self.log.info( 'Output file %s was produced but will not be treated (stepMask is %s)' % ( fileName, ', '.join( stepMask ) ) )
    else:
      self.log.info( 'No outputDataStep provided, the files output of all the steps will be considered' )

    return candidateFiles

  #############################################################################

  def _checkSanity( self, candidateFiles ):
    """ Sanity check all final candidate metadata keys are present
    """

    notPresentKeys = []

    mandatoryKeys = ['type', 'workflowSE', 'lfn'] #filedict is used for requests
    for fileName, metadata in candidateFiles.items():
      for key in mandatoryKeys:
        if not metadata.has_key( key ):
          notPresentKeys.append( ( fileName, key ) )

    if notPresentKeys:
      for fileName_keys in notPresentKeys:
        self.log.error( 'File %s has missing %s' % ( fileName_keys[0], fileName_keys[1] ) )
      raise ValueError

  #############################################################################

  def getFileMetadata( self, candidateFiles ):
    """ Returns the candidate file dictionary with associated metadata.
    
        The input candidate files dictionary has the structure:
        {'lfn':'','type':'','workflowSE':''}
       
        this also assumes the files are in the current working directory.
    """
    from DIRAC.Resources.Catalog.PoolXMLFile import getGUID
    from DIRAC.Core.Utilities.Adler import fileAdler

    #Retrieve the POOL File GUID(s) for any final output files
    self.log.info( 'Will search for POOL GUIDs for: %s' % ( ', '.join( candidateFiles.keys() ) ) )
    pfnGUID = getGUID( candidateFiles.keys() )
    if not pfnGUID['OK']:
      self.log.error( 'PoolXMLFile failed to determine POOL GUID(s) for output file list, these will be generated by the ReplicaManager',
                      pfnGUID['Message'] )
      for fileName in candidateFiles.keys():
        candidateFiles[fileName]['guid'] = ''
    elif pfnGUID['generated']:
      self.log.verbose( 'PoolXMLFile generated GUID(s) for the following files ', ', '.join( pfnGUID['generated'] ) )
    else:
      self.log.info( 'GUIDs found for all specified POOL files: %s' % ( ', '.join( candidateFiles.keys() ) ) )

    for pfn, guid in pfnGUID['Value'].items():
      candidateFiles[pfn]['guid'] = guid

    #Get all additional metadata about the file necessary for requests
    final = {}
    for fileName, metadata in candidateFiles.items():
      fileDict = {}
      fileDict['LFN'] = metadata['lfn']
      fileDict['Size'] = os.path.getsize( fileName )
      fileDict['Addler'] = fileAdler( fileName )
      fileDict['GUID'] = metadata['guid']
      fileDict['Status'] = "Waiting"

      final[fileName] = metadata
      final[fileName]['filedict'] = fileDict
      final[fileName]['localpath'] = '%s/%s' % ( os.getcwd(), fileName )

    #Sanity check all final candidate metadata keys are present (return S_ERROR if not)
    mandatoryKeys = ['guid', 'filedict'] #filedict is used for requests (this method adds guid and filedict)
    for fileName, metadata in final.items():
      for key in mandatoryKeys:
        if not metadata.has_key( key ):
          return S_ERROR( 'File %s has missing %s' % ( fileName, key ) )

    return S_OK( final )

  #############################################################################

  def _WMSJob( self ):
    """ Check if this job is running via WMS
    """
    if not self.jobID:
      return False
    else:
      return True

  #############################################################################

  def _enableModule( self ):
    """ Enable module if it's running via WMS  
    """
    if not self._WMSJob():
      self.log.info( 'No WMS JobID found, disabling module via control flag' )
      return False
    else:
      self.log.verbose( 'Found WMS JobID = %s' % self.jobID )
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

