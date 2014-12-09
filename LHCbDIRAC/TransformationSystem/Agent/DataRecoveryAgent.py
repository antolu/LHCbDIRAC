""" In general for data processing productions we need to completely abandon the 'by hand'
    reschedule operation such that accidental reschedulings don't result in data being processed twice.

    For all above cases the following procedure should be used to achieve 100%:

    - Starting from the data in the Production DB for each transformation
      look for files in the following status:
         Assigned
         MaxReset
      some of these will correspond to the final WMS status 'Failed'.

    For files in MaxReset and Assigned:
    - Discover corresponding job WMS ID
    - Check that there are no outstanding requests for the job
      o wait until all are treated before proceeding
    - Check that none of the job input data has BK descendants for the current production
      o if the data has a replica flag it means all was uploaded successfully - should be investigated by hand
      o if there is no replica flag can proceed with file removal from LFC / storage (can be disabled by flag)
    - Mark the recovered input file status as 'Unused' in the ProductionDB
"""

__RCSID__ = '$Id:  $'

import datetime

from DIRAC                                                       import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                                 import AgentModule
from DIRAC.Core.Utilities.Time                                   import dateTime
from DIRAC.ConfigurationSystem.Client.Helpers.Operations         import Operations
from DIRAC.RequestManagementSystem.Client.ReqClient              import ReqClient

from LHCbDIRAC.DataManagementSystem.Client.ConsistencyChecks     import ConsistencyChecks
from LHCbDIRAC.TransformationSystem.Client.TransformationClient  import TransformationClient

AGENT_NAME = 'Transformation/DataRecoveryAgent'

class DataRecoveryAgent( AgentModule ):
  """ Standard DIRAC agent class
  """

  def __init__( self, *args, **kwargs ):
    """ c'tor
    """
    AgentModule.__init__( self, *args, **kwargs )

    self.transClient = None
    self.reqClient = None
    self.cc = None

    self.enableFlag = True
    self.transformationTypes = []

  #############################################################################

  def initialize( self ):
    """Sets defaults
    """
    self.am_setOption( 'shifterProxy', 'ProductionManager' )

    self.transClient = TransformationClient()
    self.reqClient = ReqClient()
    self.cc = ConsistencyChecks( interactive = False, transClient = self.transClient )

    self.transformationTypes = Operations().getValue( 'Transformations/DataProcessing', [] )
    self.transformationTypes = list( set( self.transformationTypes ) - set( ['MCSimulation', 'Simulation'] ) )

    return S_OK()

  #############################################################################
  def execute( self ):
    """ The main execution method.
    """
    # Configuration settings
    self.enableFlag = self.am_getOption( 'EnableFlag', True )
    self.log.verbose( 'Enable flag is %s' % self.enableFlag )

    transformationStatus = self.am_getOption( 'TransformationStatus', ['Active', 'Completing'] )
    fileSelectionStatus = self.am_getOption( 'FileSelectionStatus', ['Assigned', 'MaxReset'] )
    updateStatus = self.am_getOption( 'FileUpdateStatus', 'Unused' )
    wmsStatusList = self.am_getOption( 'WMSStatus', ['Failed'] )

    # only worry about files > 12hrs since last update
    selectDelay = self.am_getOption( 'SelectionDelay', 1 )  # hours

    transformationDict = {}
    for transStatus in transformationStatus:
      result = self._getEligibleTransformations( transStatus, self.transformationTypes )
      if not result['OK']:
        self.log.error( "Could not obtain eligible transformations", "Status '%s': %s" % ( transStatus, result['Message'] ) )
        return result

      if not result['Value']:
        self.log.info( 'No "%s" transformations of types %s to process.' % ( transStatus,
                                                                             ', '.join( self.transformationTypes ) ) )
        continue

      transformationDict.update( result['Value'] )

    self.log.info( 'Selected %s transformations of types %s' % ( len( transformationDict.keys() ),
                                                                 ', '.join( self.transformationTypes ) ) )
    self.log.verbose( 'Transformations selected %s:\n%s' % ( ', '.join( self.transformationTypes ),
                                                             ', '.join( transformationDict.keys() ) ) )


    for transformation, typeName in transformationDict.items():

      self.log.info( '=' * len( 'Looking at transformation %s type %s:' % ( transformation, typeName ) ) )

      result = self._selectTransformationFiles( transformation, fileSelectionStatus )
      if not result['OK']:
        self.log.error( 'Could not select files for transformation', '%s: %s' % ( transformation, result['Message'] ) )
        continue

      if not result['Value']:
        self.log.info( 'No files in status %s selected for transformation %s' % ( ', '.join( fileSelectionStatus ),
                                                                                  transformation ) )
        continue

      fileDict = result['Value']
      result = self._obtainWMSJobIDs( transformation, fileDict, selectDelay, wmsStatusList )
      if not result['OK']:
        self.log.error( "Could not obtain WMS jobIDs for files of transformation" "%s: %s" % ( transformation, result['Message'] ) )
        continue

      if not result['Value']:
        self.log.info( 'No eligible WMS jobIDs found for %s files in list:\n%s ...' % ( len( fileDict.keys() ),
                                                                                        fileDict.keys()[0] ) )
        continue

      jobFileDict = result['Value']
      self.log.verbose( "Looking at WMS jobs %s" % str( jobFileDict ) )
      fileCount = 0
      for job, lfnList in jobFileDict.items():
        fileCount += len( lfnList )

      if not fileCount:
        self.log.verbose( 'No files were selected for transformation %s after examining WMS jobs.' % transformation )
        continue

      self.log.info( '%s files are selected after examining related WMS jobs' % ( fileCount ) )

      result = self._checkOutstandingRequests( jobFileDict )
      if not result['OK']:
        self.log.error( result )
        continue

      if not result['Value']:
        self.log.info( 'No WMS jobs without pending requests to process.' )
        continue

      jobFileNoRequestsDict = result['Value']
      fileCount = 0
      for job, lfnList in jobFileNoRequestsDict.items():
        fileCount += len( lfnList )

      self.log.info( '%s files are selected after removing any relating to jobs with pending requests' % ( fileCount ) )
      jobsThatDidntProduceOutputs, jobsThatProducedOutputs = self._checkdescendants( transformation,
                                                                                     jobFileNoRequestsDict )

      self.log.info( '====> Transformation %s total jobs that can be updated now: %s' % ( transformation,
                                                                                          len( jobsThatDidntProduceOutputs ) ) )
      self.log.info( '====> Transformation %s total jobs with descendants: %s' % ( transformation,
                                                                                   len( jobsThatProducedOutputs ) ) )

      filesToUpdate = []
      filesWithdescendantsInBK = []
      for job, fileList in jobFileNoRequestsDict.items():
        if job in jobsThatDidntProduceOutputs:
          filesToUpdate += fileList
        elif job in jobsThatProducedOutputs:
          filesWithdescendantsInBK += fileList

      if filesToUpdate:
        result = self._updateFileStatus( transformation, filesToUpdate, updateStatus )
        if not result['OK']:
          self.log.error( 'Recoverable files were not updated with result:\n%s' % ( result['Message'] ) )
          continue

      if filesWithdescendantsInBK:
        self.log.warn( '!!!!!!!! Note that transformation %s has descendants with \
        BK replica flags for files that are not marked as processed !!!!!!!!' % ( transformation ) )
        self.log.warn( 'Files: %s' % ';'.join( filesWithdescendantsInBK ) )

    return S_OK()

  #############################################################################
  def _getEligibleTransformations( self, status, typeList ):
    """ Select transformations of given status and type.
    """
    res = self.transClient.getTransformations( condDict = {'Status':status, 'Type':typeList} )
    self.log.debug( res )
    if not res['OK']:
      return res
    transformations = {}
    for prod in res['Value']:
      prodID = prod['TransformationID']
      transformations[str( prodID )] = prod['Type']
    return S_OK( transformations )

  #############################################################################
  def _selectTransformationFiles( self, transformation, statusList ):
    """ Select files, production jobIDs in specified file status for a given transformation.
    """
    # Until a query for files with timestamp can be obtained must rely on the
    # WMS job last update
    res = self.transClient.getTransformationFiles( condDict = {'TransformationID':transformation, 'Status':statusList} )
    self.log.debug( res )
    if not res['OK']:
      return res
    resDict = {}
    for fileDict in res['Value']:
      if not fileDict.has_key( 'LFN' ) or not fileDict.has_key( 'TaskID' ) or not fileDict.has_key( 'LastUpdate' ):
        self.log.verbose( 'LFN, %s and LastUpdate are mandatory, >=1 are missing for:\n%s' % ( 'TaskID', fileDict ) )
        continue
      lfn = fileDict['LFN']
      jobID = fileDict['TaskID']
#      lastUpdate = fileDict['LastUpdate']
      resDict[lfn] = jobID
    if resDict:
      self.log.info( 'Selected %s files overall for transformation %s' % ( len( resDict.keys() ), transformation ) )
    return S_OK( resDict )

  #############################################################################
  def _obtainWMSJobIDs( self, transformation, fileDict, selectDelay, wmsStatusList ):
    """ Group files by the corresponding WMS jobIDs, check the corresponding
        jobs have not been updated for the delay time.  Can't get into any
        mess because we start from files only in MaxReset / Assigned and check
        corresponding jobs.  Mixtures of files for jobs in MaxReset and Assigned
        statuses only possibly include some files in Unused status (not Processed
        for example) that will not be touched.
    """
    prodJobIDs = list( set( fileDict.values() ) )
    self.log.verbose( "The following %s production jobIDs apply to the selected files:\n%s" % ( len( prodJobIDs ),
                                                                                                prodJobIDs ) )

    jobFileDict = {}
    condDict = {'TransformationID':transformation, 'TaskID':prodJobIDs}
    delta = datetime.timedelta( hours = selectDelay )
    now = dateTime()
    olderThan = now - delta

    res = self.transClient.getTransformationTasks( condDict = condDict,
                                                   older = olderThan,
                                                   timeStamp = 'LastUpdateTime',
                                                   inputVector = True )
    self.log.debug( res )
    if not res['OK']:
      self.log.error( "getTransformationTasks returned an error", '%s' % res['Message'] )
      return res

    for jobDict in res['Value']:
      missingKey = False
      for key in ['TaskID', 'ExternalID', 'LastUpdateTime', 'ExternalStatus', 'InputVector']:
        if not jobDict.has_key( key ):
          self.log.info( 'Missing key %s for job dictionary, the following is available:\n%s' % ( key, jobDict ) )
          missingKey = True
          continue

      if missingKey:
        continue

      job = jobDict['TaskID']
      wmsID = jobDict['ExternalID']
      lastUpdate = jobDict['LastUpdateTime']
      wmsStatus = jobDict['ExternalStatus']
      jobInputData = jobDict['InputVector']
      jobInputData = [lfn.replace( 'LFN:', '' ) for lfn in jobInputData.split( ';' )]

      if not int( wmsID ):
        self.log.verbose( 'Prod job %s status is %s (ID = %s) so will not recheck with WMS' % ( job,
                                                                                                wmsStatus, wmsID ) )
        continue

      self.log.info( 'Job %s, prod job %s last update %s, production management system status %s' % ( wmsID,
                                                                                                      job,
                                                                                                      lastUpdate,
                                                                                                      wmsStatus ) )
      # Exclude jobs not having appropriate WMS status - have to trust that production management status is correct
      if not wmsStatus in wmsStatusList:
        self.log.verbose( 'Job %s is in status %s, not %s so will be ignored' % ( wmsID, wmsStatus,
                                                                                  ', '.join( wmsStatusList ) ) )
        continue

      finalJobData = []
      # Must map unique files -> jobs in expected state
      for lfn, prodID in fileDict.items():
        if int( prodID ) == int( job ):
          finalJobData.append( lfn )

      self.log.info( 'Found %s files for job %s' % ( len( finalJobData ), job ) )
      jobFileDict[wmsID] = finalJobData

    return S_OK( jobFileDict )

  #############################################################################

  def _checkOutstandingRequests( self, jobFileDict ):
    """ Before doing anything check that no outstanding requests are pending for the set of WMS jobIDs.
    """
    jobs = jobFileDict.keys()

    result = self.reqClient.getRequestNamesForJobs( jobs )
    if not result['OK']:
      return result

    if not result['Value']['Successful']:
      self.log.verbose( 'None of the jobs have pending requests' )
      return S_OK( jobFileDict )

    for jobID, requestName in result['Value']['Successful'].items():
      res = self.reqClient.getRequestStatus( requestName )
      if not res['OK']:
        self.log.error( 'Failed to get Status for Request', '%s:%s' % ( requestName, res['Message'] ) )
      else:
        if res['Value'] == 'Done':
          continue

      # If we fail to get the Status or it is not Done, we must wait, so remove the job from the list.
      del jobFileDict[str( jobID )]
      self.log.verbose( 'Removing jobID %s from consideration until requests are completed' % ( jobID ) )

    return S_OK( jobFileDict )

  #############################################################################
  def _checkdescendants( self, transformation, jobFileDict ):
    """ Check BK descendants for input files, prepare list of actions to be
        taken for recovery.
    """

    jobsThatDidntProduceOutputs = []
    jobsThatProducedOutputs = []

    self.cc.prod = transformation
    for job, fileList in jobFileDict.items():
      result = self.cc.getDescendants( fileList )
      filesWithDesc = result[0]
      filesWithMultipleDesc = result[2]
      if filesWithDesc or filesWithMultipleDesc:
        jobsThatProducedOutputs.append( job )
      else:
        jobsThatDidntProduceOutputs.append( job )

    return jobsThatDidntProduceOutputs, jobsThatProducedOutputs

  ############################################################################
  def _updateFileStatus( self, transformation, fileList, fileStatus ):
    """ Update file list to specified status.
    """
    if not self.enableFlag:
      self.log.verbose( "Enable flag is False, would have updated %d files to '%s' status for %s" % ( len( fileList ),
                                                                                                      fileStatus,
                                                                                                      transformation ) )
      return S_OK()

    self.log.info( "Updating %s files to '%s' status for %s" % ( len( fileList ),
                                                                 fileStatus,
                                                                 transformation ) )
    result = self.transClient.setFileStatusForTransformation( int( transformation ),
                                                              fileStatus,
                                                              fileList,
                                                              force = False )
    self.log.debug( result )
    return result
