""" Module to upload specified job output files according to the parameters
    defined in the user workflow.
"""

__RCSID__ = "$Id$"

import os, random, time, re

import DIRAC
from DIRAC                                                    import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.Utilities                                     import List
from DIRAC.Core.Utilities.File                                import getGlobbedFiles
from DIRAC.DataManagementSystem.Client.FailoverTransfer       import FailoverTransfer
from DIRAC.Core.Security.ProxyInfo                            import getProxyInfo

from LHCbDIRAC.Core.Utilities.ProductionData                  import constructUserLFNs
from LHCbDIRAC.Workflow.Modules.ModuleBase                    import ModuleBase
from LHCbDIRAC.Core.Utilities.ResolveSE                       import getDestinationSEList


class UserJobFinalization( ModuleBase ):

  #############################################################################
  def __init__( self, bkClient = None, dm = None ):
    """Module initialization.
    """

    self.log = gLogger.getSubLogger( "UserJobFinalization" )
    super( UserJobFinalization, self ).__init__( self.log, bkClientIn = bkClient, dm = dm )

    self.version = __RCSID__
    self.enable = True
    self.defaultOutputSE = gConfig.getValue( '/Resources/StorageElementGroups/Tier1-USER', [] )
    self.failoverSEs = gConfig.getValue( '/Resources/StorageElementGroups/Tier1-Failover', [] )
    # List all parameters here
    self.userFileCatalog = 'LcgFileCatalogCombined'
    self.request = None
    self.lastStep = False
    # Always allow any files specified by users
    self.outputDataFileMask = ''
    self.userOutputData = []
    self.userOutputSE = ''
    self.userOutputPath = ''
    self.failoverTransfer = None

  #############################################################################
  def _resolveInputVariables( self ):
    """ By convention the module parameters are resolved here.
    """
    super( UserJobFinalization, self )._resolveInputVariables()

    # Use LHCb utility for local running via dirac-jobexec
    if self.workflow_commons.has_key( 'UserOutputData' ):
      userOutputData = self.workflow_commons[ 'UserOutputData' ]
      if not isinstance( userOutputData, list ):  # type( userOutputData ) == type( [] ):
        userOutputData = [ i.strip() for i in userOutputData.split( ';' ) ]
      self.userOutputData = userOutputData

    if self.workflow_commons.has_key( 'UserOutputSE' ):
      specifiedSE = self.workflow_commons['UserOutputSE']
      if not type( specifiedSE ) == type( [] ):
        self.userOutputSE = [i.strip() for i in specifiedSE.split( ';' )]
    else:
      self.log.verbose( 'No UserOutputSE specified, using default value: %s' % ( ', '.join( self.defaultOutputSE ) ) )
      self.userOutputSE = []

    if self.workflow_commons.has_key( 'UserOutputPath' ):
      self.userOutputPath = self.workflow_commons['UserOutputPath']

  #############################################################################

  def execute( self, production_id = None, prod_job_id = None, wms_job_id = None,
               workflowStatus = None, stepStatus = None,
               wf_commons = None, step_commons = None,
               step_number = None, step_id = None ):
    """ Main execution function.
    """
    # Have to work out if the module is part of the last step i.e.
    # user jobs can have any number of steps and we only want
    # to run the finalization once.

    try:

      super( UserJobFinalization, self ).execute( self.version, production_id, prod_job_id, wms_job_id,
                                                  workflowStatus, stepStatus,
                                                  wf_commons, step_commons, step_number, step_id )

      if int( self.step_number ) == int( self.workflow_commons['TotalSteps'] ):
        self.lastStep = True
      else:
        self.log.debug( 'Current step = %s, total steps of workflow = %s, \
        UserJobFinalization will enable itself only at the last workflow step.' % ( self.step_number,
                                                                                    self.workflow_commons['TotalSteps'] ) )
      if not self.lastStep:
        return S_OK()

      self._resolveInputVariables()

      # Earlier modules may have populated the report objects
      self.request.RequestName = 'job_%d_request.xml' % self.jobID
      self.request.JobID = self.jobID
      self.request.SourceComponent = "Job_%d" % self.jobID

      if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
        self.log.verbose( 'Workflow status = %s, step status = %s' % ( self.workflowStatus['OK'],
                                                                       self.stepStatus['OK'] ) )
        self.log.error( "Workflow status is not ok, will not overwrite application status." )
        return S_ERROR( "Workflow failed, UserJobFinalization module completed" )

      if not self.userOutputData:
        self.log.info( "No user output data is specified for this job, nothing to do" )
        return S_OK( "No output data to upload" )

      self.log.info( "User specified output file list is: %s" % ( ', '.join( self.userOutputData ) ) )

      globList = []
      for i in self.userOutputData:
        if re.search( '\*', i ):
          globList.append( i )

      # Check whether list of userOutputData is a globbable pattern
      if globList:
        for i in globList:
          self.userOutputData.remove( i )

        globbedOutputList = List.uniqueElements( getGlobbedFiles( globList ) )
        if globbedOutputList:
          self.log.info( 'Found a pattern in the output data file list, \
          extra files to upload are: %s' % ( ', '.join( globbedOutputList ) ) )
          self.userOutputData += globbedOutputList
        else:
          self.log.info( "No files were found on the local disk for the following patterns: %s" % ( ', '.join( globList ) ) )

      self.log.info( "Final list of files to upload are: %s" % ( ', '.join( self.userOutputData ) ) )

      # Determine the final list of possible output files for the
      # workflow and all the parameters needed to upload them.
      outputList = []
      for i in self.userOutputData:
        outputList.append( {'outputDataType':( '.'.split( i )[-1] ).upper(),
                            'outputDataSE':self.userOutputSE,
                            'outputDataName':os.path.basename( i )} )

      userOutputLFNs = []
      if self.userOutputData:
        self.log.info( "Constructing user output LFN(s) for %s" % ( ', '.join( self.userOutputData ) ) )

        owner = self.getCurrentOwner()

        userOutputLFNs = constructUserLFNs( self.jobID, owner, self.userOutputData, self.userOutputPath )

      self.log.verbose( "Calling getCandidateFiles( %s, %s, %s)" % ( outputList, userOutputLFNs,
                                                                     self.outputDataFileMask ) )
      try:
        fileDict = self.getCandidateFiles( outputList, userOutputLFNs, self.outputDataFileMask )
      except os.error, e:
        self.setApplicationStatus( e )
        return S_OK()

      try:
        fileMetadata = self.getFileMetadata( fileDict )
      except RuntimeError, e:
        self.setApplicationStatus( e )
        return S_OK()

      if not fileMetadata:
        self.log.info( "No output data files were determined to be uploaded for this workflow" )
        self.setApplicationStatus( 'No Output Data Files To Upload' )
        return S_OK()

      # First get the local (or assigned) SE to try first for upload and others in random fashion
      localSEs = set( getDestinationSEList( 'Tier1-USER', DIRAC.siteName(), outputmode = 'local' ) )
      self.log.verbose( "Site Local SE for user outputs is: %s" % ( localSEs ) )
      userSEs = set( self.userOutputSE )
      otherSEs = set( self.defaultOutputSE ) - localSEs - userSEs
      # If a user SE is  local set it first
      topSEs = userSEs & localSEs
      # reordered user SEs, setting local first
      userSEs = list( topSEs ) + list( userSEs - topSEs )
      localSEs = list( localSEs - topSEs )
      if len( userSEs ) < 2 and localSEs:
        # Set a local SE first
        orderedSEs = localSEs[0:1] + userSEs + localSEs[1:]
      else:
        orderedSEs = userSEs + localSEs
      orderedSEs += List.randomize( list( otherSEs ) )

      self.log.info( "Ordered list of output SEs is: %s" % ( ', '.join( orderedSEs ) ) )
      final = {}
      for fileName, metadata in fileMetadata.items():
        final[fileName] = metadata
        final[fileName]['resolvedSE'] = orderedSEs

      # At this point can exit and see exactly what the module will upload
      if not self._enableModule():
        self.log.info( "Module disabled would have attempted to upload the files %s" % ', '.join( final.keys() ) )
        for fileName, metadata in final.items():
          self.log.info( '--------%s--------' % fileName )
          for n, v in metadata.items():
            self.log.info( '%s = %s' % ( n, v ) )

        return S_OK( 'Module is disabled by control flag' )

      # Disable the watchdog check in case the file uploading takes a long time
      self.log.info( "Creating DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK in order to disable the Watchdog prior to upload" )
      fopen = open( 'DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK', 'w' )
      fopen.write( '%s' % time.asctime() )
      fopen.close()

      # Instantiate the failover transfer client with the global request object
      if not self.failoverTransfer:
        self.failoverTransfer = FailoverTransfer( self.request )

      # One by one upload the files with failover if necessary
      replication = {}
      failover = {}
      uploaded = []
      for fileName, metadata in final.items():
        self.log.info( "Attempting to store file %s to the following SE(s):\n%s" % ( fileName,
                                                                                ', '.join( metadata['resolvedSE'] ) ) )
        fileMetaDict = { 'Size': metadata['filedict']['Size'],
                         'LFN' : metadata['filedict']['LFN'],
                         'GUID' : metadata['filedict']['GUID'] }
        result = self.failoverTransfer.transferAndRegisterFile( fileName = fileName,
                                                                localPath = metadata['localpath'],
                                                                lfn = metadata['filedict']['LFN'],
                                                                destinationSEList = metadata['resolvedSE'],
                                                                fileMetaDict = fileMetaDict,
                                                                fileCatalog = self.userFileCatalog )
        if not result['OK']:
          self.log.error( "Could not transfer and register %s with metadata:\n %s" % ( fileName, metadata ) )
          failover[fileName] = metadata
        else:
          # Only attempt replication after successful upload
          lfn = metadata['lfn']
          uploaded.append( lfn )
          seList = metadata['resolvedSE']
          replicateSE = ''
          uploadedSE = result['Value'].get( 'uploadedSE', '' )
          if uploadedSE:
            for se in seList:
              if not se == uploadedSE:
                replicateSE = se
                break

          if replicateSE and lfn:
            self.log.info( "Will attempt to replicate %s to %s" % ( lfn, replicateSE ) )
            replication[lfn] = ( uploadedSE, replicateSE )

      cleanUp = False
      for fileName, metadata in failover.items():
        random.shuffle( self.failoverSEs )
        targetSE = metadata['resolvedSE'][0]
        if len( metadata['resolvedSE'] ) > 1:
          replicateSE = metadata['resolvedSE'][1]
        else:
          replicateSE = ''
        metadata['resolvedSE'] = self.failoverSEs
        fileMetaDict = { 'Size': metadata['filedict']['Size'],
                         'LFN' : metadata['filedict']['LFN'],
                         'GUID' : metadata['filedict']['GUID'] }
        result = self.failoverTransfer.transferAndRegisterFileFailover( fileName,
                                                                        metadata['localpath'],
                                                                        metadata['lfn'],
                                                                        targetSE,
                                                                        metadata['resolvedSE'],
                                                                        fileMetaDict = fileMetaDict,
                                                                        fileCatalog = self.userFileCatalog )
        if not result['OK']:
          self.log.error( 'Could not transfer and register %s with metadata:\n %s' % ( fileName, metadata ) )
          cleanUp = True
          continue  # for users can continue even if one completely fails
        else:
          lfn = metadata['lfn']
          uploaded.append( lfn )
          # Even when using Failover, one needs to replicate to a second SE
          if replicateSE:
            replication[lfn] = ( targetSE, replicateSE )

      # For files correctly uploaded must report LFNs to job parameters
      if uploaded:
        report = ', '.join( uploaded )
        self.setJobParameter( 'UploadedOutputData', report )

      # Now after all operations, retrieve potentially modified request object
      self.request = self.failoverTransfer.request

      # If some or all of the files failed to be saved to failover
      if cleanUp:
        self.workflow_commons['Request'] = self.request
        # Leave any uploaded files just in case it is useful for the user
        # do not try to replicate any files.
        return S_ERROR( 'Failed To Upload Output Data' )

      # If there is now at least one replica for uploaded files can trigger replication
      self.log.info( 'Sleeping for 10 seconds before attempting replication of recently uploaded files' )
      time.sleep( 10 )
      for lfn in replication:
        uploadedSE , repSE = replication[lfn]
        result = self.rm.replicateAndRegister( lfn, repSE, catalog = self.userFileCatalog )
        if not result['OK']:
          self.log.info( "Replication failed: %s. Now adding request" % ( result['Message'] ) )
          self.failoverTransfer._setFileReplicationRequest( lfn, repSE, fileMetaDict, uploadedSE )
          self.request = self.failoverTransfer.request

      self.workflow_commons['Request'] = self.request

      self.generateFailoverFile()

      self.setApplicationStatus( 'Job Finished Successfully' )

      return S_OK( 'Output data uploaded' )

    except Exception, e:
      self.log.exception( e )
      self.setApplicationStatus( e )
      return S_ERROR( e )

    finally:
      super( UserJobFinalization, self ).finalize( self.version )

  #############################################################################

  def getCurrentOwner( self ):
    """Simple function to return current DIRAC username.
    """
    result = getProxyInfo()

    if not result['OK']:
      if not self._enableModule():
        return 'testUser'
      raise RuntimeError( 'Could not obtain proxy information' )

    if not result['Value'].has_key( 'username' ):
      raise RuntimeError( 'Could not get username from proxy' )

    return result['Value']['username']

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
