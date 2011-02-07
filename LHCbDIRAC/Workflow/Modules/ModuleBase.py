########################################################################
# $HeadURL$
########################################################################

""" ModuleBase - base class for LHCb workflow modules. Defines several
    common utility methods

"""

__RCSID__ = "$Id$"

from DIRAC  import S_OK, S_ERROR, gLogger
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.RequestManagementSystem.Client.DISETSubRequest import DISETSubRequest
from DIRAC.Core.Security.Misc import getProxyInfoAsString
from DIRAC.Resources.Catalog.PoolXMLFile import getGUID
from DIRAC.Core.Utilities.Adler import fileAdler
from DIRAC.TransformationSystem.Client.FileReport import FileReport
import DIRAC
import os, string, copy

class ModuleBase( object ):

  #############################################################################
  def __init__( self ):
    """ Initialization of module base.
    """
    self.log = gLogger.getSubLogger( 'ModuleBase' )
    # FIXME: do we need to do this for every module?
    result = getProxyInfoAsString()
    if not result['OK']:
      self.log.error( 'Could not obtain proxy information in module environment with message:\n', result['Message'] )
    else:
      self.log.verbose( 'Payload proxy information:\n', result['Value'] )

  #############################################################################
  def setApplicationStatus( self, status, sendFlag = True ):
    """Wraps around setJobApplicationStatus of state update client
    """
    if not self.jobID:
      return S_OK( 'JobID not defined' ) # e.g. running locally prior to submission

    self.log.verbose( 'setJobApplicationStatus(%s,%s)' % ( self.jobID, status ) )

    if self.workflow_commons.has_key( 'JobReport' ):
      self.jobReport = self.workflow_commons['JobReport']

    if not self.jobReport:
      return S_OK( 'No reporting tool given' )
    jobStatus = self.jobReport.setApplicationStatus( status, sendFlag )
    if not jobStatus['OK']:
      self.log.warn( jobStatus['Message'] )

    return jobStatus

  #############################################################################
  def sendStoredStatusInfo( self ):
    """Wraps around sendStoredStatusInfo of state update client
    """
    if not self.jobID:
      return S_OK( 'JobID not defined' ) # e.g. running locally prior to submission

    if self.workflow_commons.has_key( 'JobReport' ):
      self.jobReport = self.workflow_commons['JobReport']

    if not self.jobReport:
      return S_OK( 'No reporting tool given' )

    sendStatus = self.jobReport.sendStoredStatusInfo()
    if not sendStatus['OK']:
      self.log.error( sendStatus['Message'] )

    return sendStatus

  #############################################################################
  def setJobParameter( self, name, value, sendFlag = True ):
    """Wraps around setJobParameter of state update client
    """
    if not self.jobID:
      return S_OK( 'JobID not defined' ) # e.g. running locally prior to submission

    self.log.verbose( 'setJobParameter(%s,%s,%s)' % ( self.jobID, name, value ) )

    if self.workflow_commons.has_key( 'JobReport' ):
      self.jobReport = self.workflow_commons['JobReport']

    if not self.jobReport:
      return S_OK( 'No reporting tool given' )
    jobParam = self.jobReport.setJobParameter( str( name ), str( value ), sendFlag )
    if not jobParam['OK']:
      self.log.warn( jobParam['Message'] )

    return jobParam

  #############################################################################
  def sendStoredJobParameters( self ):
    """Wraps around sendStoredJobParameters of state update client
    """
    if not self.jobID:
      return S_OK( 'JobID not defined' ) # e.g. running locally prior to submission

    if self.workflow_commons.has_key( 'JobReport' ):
      self.jobReport = self.workflow_commons['JobReport']

    if not self.jobReport:
      return S_OK( 'No reporting tool given' )

    sendStatus = self.jobReport.sendStoredJobParameters()
    if not sendStatus['OK']:
      self.log.error( sendStatus['Message'] )

    return sendStatus

  #############################################################################
  def setFileStatus( self, production, lfn, status ):
    """ set the file status for the given production in the Production Database
    """
    self.log.verbose( 'setFileStatus(%s,%s,%s)' % ( production, lfn, status ) )

    if not self.workflow_commons.has_key( 'FileReport' ):
      fileReport = FileReport( 'ProductionManagement/ProductionManager' )
      self.workflow_commons['FileReport'] = fileReport

    fileReport = self.workflow_commons['FileReport']
    result = fileReport.setFileStatus( production, lfn, status )
    if not result['OK']:
      self.log.warn( result['Message'] )

    self.workflow_commons['FileReport'] = fileReport

    return result

  #############################################################################
  def setReplicaProblematic( self, lfn, se, pfn = '', reason = 'Access failure' ):
    """ Set replica status to Problematic in the File Catalog
    """

    rm = ReplicaManager()
    source = "Job %d at %s" % ( self.jobID, DIRAC.siteName() )
    result = rm.setReplicaProblematic( ( lfn, pfn, se, reason ), source )
    if not result['OK'] or result['Value']['Failed']:
      # We have failed the report, let's attempt the Integrity DB faiover
      integrityDB = RPCClient( 'DataManagement/DataIntegrity', timeout = 120 )
      fileMetadata = {'Prognosis':reason, 'LFN':lfn, 'PFN':pfn, 'StorageElement':se}
      result = integrityDB.insertProblematic( source, fileMetadata )
      if not result['OK']:
        # Add it to the request
        if self.workflow_commons.has_key( 'Request' ):
          request = self.workflow_commons['Request']
          subrequest = DISETSubRequest( result['rpcStub'] ).getDictionary()
          request.addSubRequest( subrequest, 'integrity' )

    return S_OK()

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
      if outputFile.has_key( 'abandonOutput' ):
        self.log.info( 'Abandoning output file %s as specified by the workflow' % ( outputFile ) )
      elif outputFile.has_key( 'outputDataType' ) and outputFile.has_key( 'outputDataSE' ) and outputFile.has_key( 'outputDataName' ):
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
          self.log.info( 'Output file %s was produced but will not be treated (fileMask is %s)' % ( fileName, string.join( fileMask, ', ' ) ) )
    else:
      self.log.info( 'No outputDataFileMask provided, the files with all the extensions will be considered' )

    if stepMask:
      for fileName, metadata in candidateFiles.items():
        if fileName.split( '_' )[-1].split( '.' )[0] not in stepMask:
          del( candidateFiles[fileName] )
          self.log.info( 'Output file %s was produced but will not be treated (stepMask is %s)' % ( fileName, string.join( stepMask, ', ' ) ) )
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
    #Retrieve the POOL File GUID(s) for any final output files
    self.log.info( 'Will search for POOL GUIDs for: %s' % ( string.join( candidateFiles.keys(), ', ' ) ) )
    pfnGUID = getGUID( candidateFiles.keys() )
    if not pfnGUID['OK']:
      self.log.error( 'PoolXMLFile failed to determine POOL GUID(s) for output file list, these will be generated by the ReplicaManager', pfnGUID['Message'] )
      for fileName in candidateFiles.keys():
        candidateFiles[fileName]['guid'] = ''
    elif pfnGUID['generated']:
      self.log.verbose( 'PoolXMLFile generated GUID(s) for the following files ', string.join( pfnGUID['generated'], ', ' ) )
    else:
      self.log.info( 'GUIDs found for all specified POOL files: %s' % ( string.join( candidateFiles.keys(), ', ' ) ) )

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
