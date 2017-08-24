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

__RCSID__ = "$Id$"

import datetime

from DIRAC                                                       import S_OK
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
    self.consChecks = None

    self.enableFlag = True
    self.transformationTypes = []

  #############################################################################

  def initialize( self ):
    """Sets defaults
    """
    self.am_setOption( 'shifterProxy', 'ProductionManager' )

    self.transClient = TransformationClient()
    self.reqClient = ReqClient()
    self.consChecks = ConsistencyChecks( interactive = False, transClient = self.transClient )

    transformationTypes = Operations().getValue( 'Transformations/DataProcessing', [] )
    self.transformationTypes = list( set( transformationTypes ) - {'MCSimulation', 'Simulation'} )


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
      result = self.__getEligibleTransformations( transStatus, self.transformationTypes )
      if not result['OK']:
        self.log.error( "Could not obtain eligible transformations", "Status '%s': %s" % ( transStatus, result['Message'] ) )
        return result

      if not result['Value']:
        self.log.info( 'No "%s" transformations of types %s to process.' % ( transStatus,
                                                                             ', '.join( self.transformationTypes ) ) )
        continue

      transformationDict.update( result['Value'] )

    self.log.info( 'Selected %d transformations of types %s' % ( len( transformationDict ),
                                                                 ', '.join( self.transformationTypes ) ) )
    self.log.verbose( 'Transformations selected:\n%s' % ( ', '.join( transformationDict ) ) )


    for transformation, typeName in transformationDict.iteritems():
      self.log.info( '=' * len( 'Looking at transformation %s type %s:' % ( transformation, typeName ) ) )

      result = self.__selectTransformationFiles( transformation, fileSelectionStatus )
      if not result['OK']:
        self.log.error( 'Could not select files for transformation', '%s: %s' % ( transformation, result['Message'] ) )
        continue
      fileDict = result['Value']
      if not fileDict:
        self.log.info( 'No files in status %s selected for transformation %s' % ( ', '.join( fileSelectionStatus ),
                                                                                  transformation ) )
        continue

      result = self.__obtainWMSJobIDs( transformation, fileDict, selectDelay, wmsStatusList )
      if not result['OK']:
        self.log.error( "Could not obtain WMS jobIDs for files of transformation" "%s: %s" % ( transformation, result['Message'] ) )
        continue
      if not result['Value']:
        self.log.info( 'No eligible WMS jobIDs found for %s files in list:\n%s ...' % ( len( fileDict ),
                                                                                        fileDict.keys()[0] ) )
        continue

      jobFileDict = result['Value']
      self.log.verbose( "Looking at WMS jobs %s" % ','.join( str( jobID ) for jobID in jobFileDict ) )

      fileCount = sum( len( lfnList ) for lfnList in jobFileDict.itervalues() )
      if not fileCount:
        self.log.verbose( 'No files were selected for transformation %s after examining WMS jobs.' % transformation )
        continue
      self.log.info( '%s files are selected after examining related WMS jobs' % ( fileCount ) )

      result = self.__removePendingRequestsJobs( jobFileDict )
      if not result['OK']:
        self.log.error( result )
        continue
      # This method modifies the input dictionary
      if not jobFileDict:
        self.log.info( 'No WMS jobs without pending requests to process.' )
        continue

      fileCount = sum( len( lfnList ) for lfnList in jobFileDict.itervalues() )
      if not fileCount:
        self.log.verbose( 'No files were selected for transformation %s after examining WMS jobs.' % transformation )
        continue
      self.log.info( '%s files are selected after removing any job with pending requests' % ( fileCount ) )

      jobsThatDidntProduceOutputs, jobsThatProducedOutputs = self.__checkdescendants( transformation,
                                                                                     jobFileDict )

      self.log.info( '====> Transformation %s total jobs that can be updated now: %s' % ( transformation,
                                                                                          len( jobsThatDidntProduceOutputs ) ) )
      if jobsThatProducedOutputs:
        self.log.info( '====> Transformation %s: %d jobs have descendants' %
                       ( transformation, len( jobsThatProducedOutputs ) ) )
      else:
        self.log.info( '====> Transformation %s: no jobs have descendants' % transformation )

      filesToUpdate = []
      filesWithDescendants = []
      for job, fileList in jobFileDict.iteritems():
        if job in jobsThatDidntProduceOutputs:
          filesToUpdate += fileList
        elif job in jobsThatProducedOutputs:
          filesWithDescendants += fileList

      if filesToUpdate:
        result = self.__updateFileStatus( transformation, filesToUpdate, updateStatus )
        if not result['OK']:
          self.log.error( 'Recoverable files were not updated with result:\n%s' % ( result['Message'] ) )
          continue

      if filesWithDescendants:
        self.log.warn( '!!!!!!!! Note that transformation %s has descendants for files that are not marked as processed !!!!!!!!' %
                       ( transformation ) )
        self.log.warn( 'Files: %s' % ';'.join( filesWithDescendants ) )

    return S_OK()

  #############################################################################
  def __getEligibleTransformations( self, status, typeList ):
    """ Select transformations of given status and type.
    """
    res = self.transClient.getTransformations( condDict = {'Status':status, 'Type':typeList} )
    self.log.debug( res )
    if not res['OK']:
      return res
    transformations = dict( ( str( prod['TransformationID'] ), prod['Type'] ) for prod in res['Value'] )
    return S_OK( transformations )

  #############################################################################
  def __selectTransformationFiles( self, transformation, statusList ):
    """ Select files, production jobIDs in specified file status for a given transformation.
    """
    # Until a query for files with timestamp can be obtained must rely on the
    # WMS job last update
    res = self.transClient.getTransformationFiles( condDict = {'TransformationID':transformation, 'Status':statusList} )
    if not res['OK']:
      return res
    resDict = {}
    mandatoryKeys = {'LFN', 'TaskID', 'LastUpdate'}
    for fileDict in res['Value']:
      missingKeys = mandatoryKeys - set( fileDict )
      if missingKeys:
        for key in missingKeys:
          self.log.verbose( '%s is mandatory, but missing for:\n\t%s' % ( key, str( fileDict ) ) )
      else:
        resDict[fileDict['LFN']] = fileDict['TaskID']
    if resDict:
      self.log.info( 'Selected %s files overall for transformation %s' % ( len( resDict ), transformation ) )
    return S_OK( resDict )

  #############################################################################
  def __obtainWMSJobIDs( self, transformation, fileDict, selectDelay, wmsStatusList ):
    """ Group files by the corresponding WMS jobIDs, check the corresponding
        jobs have not been updated for the delay time.  Can't get into any
        mess because we start from files only in MaxReset / Assigned and check
        corresponding jobs.  Mixtures of files for jobs in MaxReset and Assigned
        statuses only possibly include some files in Unused status (not Processed
        for example) that will not be touched.
    """
    taskIDList = sorted( set( fileDict.values() ) )
    self.log.verbose( "The following %d task IDs correspond to the selected files:\n%s" %
                      ( len( taskIDList ), ', '.join( str( taskID ) for taskID in taskIDList ) ) )

    jobFileDict = {}
    olderThan = dateTime() - datetime.timedelta( hours = selectDelay )

    res = self.transClient.getTransformationTasks( condDict = {'TransformationID':transformation, 'TaskID':taskIDList},
                                                   older = olderThan,
                                                   timeStamp = 'LastUpdateTime' )
    if not res['OK']:
      self.log.error( "getTransformationTasks returned an error", '%s' % res['Message'] )
      return res

    mandatoryKeys = {'TaskID', 'ExternalID', 'LastUpdateTime', 'ExternalStatus', 'InputVector' }
    for taskDict in res['Value']:
      missingKey = mandatoryKeys - set( taskDict )
      if missingKey:
        for key in missingKey:
          self.log.info( 'Missing key %s for job dictionary:\n\t%s' % ( key, str( taskDict ) ) )
        continue

      taskID = taskDict['TaskID']
      wmsID = taskDict['ExternalID']
      wmsStatus = taskDict['ExternalStatus']

      if not int( wmsID ):
        self.log.verbose( 'TaskID %s: status is %s (jobID = %s) so will not recheck with WMS' %
                          ( taskID, wmsStatus, wmsID ) )
        continue

      # Exclude jobs not having appropriate WMS status - have to trust that production management status is correct
      if wmsStatus not in wmsStatusList:
        self.log.verbose( 'Job %s is in status %s, not in %s so will be ignored' %
                          ( wmsID, wmsStatus, ', '.join( wmsStatusList ) ) )
        continue
      self.log.info( 'Job %s, taskID %s, last update %s, WMS status %s' %
                     ( wmsID, taskID, taskDict['LastUpdateTime'], wmsStatus ) )

      # Must map unique files -> jobs in expected state
      jobFileDict[wmsID] = [lfn for lfn, tID in fileDict.iteritems() if int( tID ) == int( taskID )]

      self.log.info( 'Found %d files for taskID %s, jobID %s' % ( len( jobFileDict[wmsID] ), taskID, wmsID ) )

    return S_OK( jobFileDict )

  #############################################################################

  def __removePendingRequestsJobs( self, jobFileDict ):
    """ Before doing anything check that no outstanding requests are pending for the set of WMS jobIDs.
    """
    jobs = jobFileDict.keys()

    result = self.reqClient.getRequestIDsForJobs( jobs )
    if not result['OK']:
      return result

    if not result['Value']['Successful']:
      self.log.verbose( 'None of the jobs have pending requests' )
      return S_OK()

    for jobID, requestID in result['Value']['Successful'].iteritems():
      res = self.reqClient.getRequestStatus( requestID )
      if not res['OK']:
        self.log.error( 'Failed to get Status for Request', '%s:%s' % ( requestID, res['Message'] ) )
      elif res['Value'] != 'Done':
        # If we fail to get the Status or it is not Done, we must wait, so remove the job from the list.
        del jobFileDict[str( jobID )]
        self.log.verbose( 'Removing jobID %s from consideration until requests are completed' % ( jobID ) )

    return S_OK()

  #############################################################################
  def __checkdescendants( self, transformation, jobFileDict ):
    """ Check BK descendants for input files, prepare list of actions to be
        taken for recovery.
    """

    jobsThatDidntProduceOutputs = []
    jobsThatProducedOutputs = []

    self.consChecks.prod = transformation
    for job, fileList in jobFileDict.iteritems():
      result = self.consChecks.getDescendants( fileList )
      filesWithDesc = result[0]
      filesWithMultipleDesc = result[2]
      if filesWithDesc or filesWithMultipleDesc:
        jobsThatProducedOutputs.append( job )
      else:
        jobsThatDidntProduceOutputs.append( job )

    return jobsThatDidntProduceOutputs, jobsThatProducedOutputs

  ############################################################################
  def __updateFileStatus( self, transformation, fileList, fileStatus ):
    """ Update file list to specified status.
    """
    if not self.enableFlag:
      self.log.info( "Enable flag is False, would have updated %d files to '%s' status for %s" % ( len( fileList ),
                                                                                                      fileStatus,
                                                                                                      transformation ) )
      return S_OK()

    self.log.info( "Updating %s files to '%s' status for %s" % ( len( fileList ),
                                                                 fileStatus,
                                                                 transformation ) )
    return self.transClient.setFileStatusForTransformation( int( transformation ),
                                                              fileStatus,
                                                              fileList,
                                                              force = False )
