""" Module to upload specified job output files according to the parameters
    defined in the production workflow.
"""

__RCSID__ = "$Id$"

import os, random, time, glob, copy
import DIRAC

from DIRAC                                                    import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.Utilities                                     import DEncode
from DIRAC.DataManagementSystem.Client.FailoverTransfer       import FailoverTransfer
from DIRAC.RequestManagementSystem.Client.Operation           import Operation
from DIRAC.RequestManagementSystem.Client.File                import File

from LHCbDIRAC.Core.Utilities.ResolveSE                       import getDestinationSEList
from LHCbDIRAC.Core.Utilities.ProductionData                  import constructProductionLFNs
from LHCbDIRAC.DataManagementSystem.Client.ConsistencyChecks  import getFileDescendants

from LHCbDIRAC.Workflow.Modules.ModuleBase                    import ModuleBase

class UploadOutputData( ModuleBase ):

  #############################################################################
  def __init__( self, bkClient = None, rm = None ):
    """Module initialization.
    """

    self.log = gLogger.getSubLogger( "UploadOutputData" )
    super( UploadOutputData, self ).__init__( self.log, bkClientIn = bkClient, rm = rm )

    self.version = __RCSID__
    self.commandTimeOut = 10 * 60
    self.jobID = ''
    self.failoverSEs = gConfig.getValue( '/Resources/StorageElementGroups/Tier1-Failover', [] )
    self.existingCatalogs = []
    result = gConfig.getSections( '/Resources/FileCatalogs' )
    if result['OK']:
      self.existingCatalogs = result['Value']

    # List all parameters here
    self.outputDataFileMask = ''
    self.outputMode = 'Any'  # or 'Local' for reco case
    self.outputList = []
    self.outputDataStep = ''
    self.request = None
    self.failoverTransfer = None

  #############################################################################
  def _resolveInputVariables( self ):
    """ By convention the module parameters are resolved here.
    """

    super( UploadOutputData, self )._resolveInputVariables()

    if self.workflow_commons.has_key( 'outputDataStep' ):
      self.outputDataStep = self.workflow_commons['outputDataStep']

    if self.workflow_commons.has_key( 'outputList' ):
      self.outputList = self.workflow_commons['outputList']

    if self.workflow_commons.has_key( 'outputMode' ):
      self.outputMode = self.workflow_commons['outputMode']

    # Use LHCb utility for local running via jobexec
    if self.workflow_commons.has_key( 'ProductionOutputData' ):
      self.prodOutputLFNs = self.workflow_commons['ProductionOutputData']
      if not type( self.prodOutputLFNs ) == type( [] ):
        self.prodOutputLFNs = [i.strip() for i in self.prodOutputLFNs.split( ';' )]
    else:
      self.log.info( 'ProductionOutputData parameter not found, creating on the fly' )
      result = constructProductionLFNs( self.workflow_commons, self.bkClient )
      if not result['OK']:
        self.log.error( 'Could not create production LFNs', result['Message'] )
        return result
      self.prodOutputLFNs = result['Value']['ProductionOutputData']

  #############################################################################

  def execute( self, production_id = None, prod_job_id = None, wms_job_id = None,
               workflowStatus = None, stepStatus = None,
               wf_commons = None, step_commons = None,
               step_number = None, step_id = None,
               SEs = None, fileDescendants = None ):
    """ Main execution function.
    """

    try:

      super( UploadOutputData, self ).execute( self.version, production_id, prod_job_id, wms_job_id,
                                               workflowStatus, stepStatus,
                                               wf_commons, step_commons, step_number, step_id )

      self._resolveInputVariables()

      if not self._checkWFAndStepStatus():
        return S_OK( 'No output data upload attempted' )

      # Determine the final list of possible output files for the workflow and all the parameters needed to upload them.
      self.log.verbose( 'Getting the list of candidate files' )
      fileDict = self.getCandidateFiles( self.outputList, self.prodOutputLFNs,
                                       self.outputDataFileMask, self.outputDataStep )

      fileMetadata = self.getFileMetadata( fileDict )

      if not fileMetadata:
        self.log.info( 'No output data files were determined to be uploaded for this workflow' )
        return S_OK()

      # Get final, resolved SE list for files
      final = {}

      for fileName, metadata in fileMetadata.items():
        if not SEs:
          resolvedSE = getDestinationSEList( metadata['workflowSE'], DIRAC.siteName(), self.outputMode )
        else:
          resolvedSE = SEs
        final[fileName] = metadata
        final[fileName]['resolvedSE'] = resolvedSE

      self.log.info( 'The following files will be uploaded: %s' % ( ', '.join( final.keys() ) ) )
      for fileName, metadata in final.items():
        self.log.info( '--------%s--------' % fileName )
        for n, v in metadata.items():
          self.log.info( '%s = %s' % ( n, v ) )

      # At this point can exit and see exactly what the module would have uploaded
      if not self._enableModule():
        self.log.info( 'Would have attempted to upload the following files %s' % ', '.join( final.keys() ) )
        return S_OK()

      # Prior to uploading any files must check (for productions with input data) that no descendent files
      # already exist with replica flag in the BK.

      if self.inputDataList:
        if fileDescendants != None:
          result = fileDescendants
        else:
          result = getFileDescendants( self.production_id, self.inputDataList, rm = self.rm, bkClient = self.bkClient )
        if not result:
          self.log.info( "No descendants found, outputs can be uploaded" )
        else:
          self.log.error( "Found descendants!!! Outputs won't be uploaded" )
          return S_ERROR( 'Input Data Already Processed' )

      # Disable the watchdog check in case the file uploading takes a long time
      self.log.info( 'Creating DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK in order to disable the Watchdog prior to upload' )
      fopen = open( 'DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK', 'w' )
      fopen.write( '%s' % time.asctime() )
      fopen.close()

      # Instantiate the failover transfer client with the global request object
      if not self.failoverTransfer:
        self.failoverTransfer = FailoverTransfer( self.request )

      # Track which files are successfully uploaded (not to failover) via
      performBKRegistration = []
      # Failover replicas are always added to the BK when they become available

      # One by one upload the files with failover if necessary
      registrationFailure = False
      failover = {}
      for fileName, metadata in final.items():
        targetSE = metadata['resolvedSE']
        self.log.info( "Attempting to store file %s to the following SE(s):\n%s" % ( fileName,
                                                                                     ', '.join( targetSE ) ) )
        fileMetaDict = { 'Size'         : metadata['filedict']['Size'],
                         'LFN'          : metadata['filedict']['LFN'],
                         'GUID'         : metadata['filedict']['GUID'],
                         'Checksum'     : metadata['filedict']['Checksum'],
                         'ChecksumType' : metadata['filedict']['ChecksumType'] }

        result = self.failoverTransfer.transferAndRegisterFile( fileName = fileName,
                                                                localPath = metadata['localpath'],
                                                                lfn = metadata['filedict']['LFN'],
                                                                destinationSEList = targetSE,
                                                                fileMetaDict = fileMetaDict,
                                                                fileCatalog = 'LcgFileCatalogCombined' )
        if not result['OK']:
          self.log.error( 'Could not transfer and register %s with metadata:\n %s' % ( fileName, metadata ) )
          failover[fileName] = metadata
        else:
          if result['Value'].has_key( 'registration' ):
            self.log.info( 'File %s was put to the SE but the catalog registration \
            will be set as an asynchronous request' % ( fileName ) )
            registrationFailure = True
          else:
            self.log.info( '%s uploaded successfully, will be registered in BK if \
            all files uploaded for job' % ( fileName ) )

          performBKRegistration.append( metadata )

      cleanUp = False
      for fileName, metadata in failover.items():
        self.log.info( 'Setting default catalog for failover transfer to LcgFileCatalogCombined' )
        random.shuffle( self.failoverSEs )
        targetSE = metadata['resolvedSE'][0]
        metadata['resolvedSE'] = self.failoverSEs

        fileMetaDict = { 'Size'         : metadata['filedict']['Size'],
                         'LFN'          : metadata['filedict']['LFN'],
                         'GUID'         : metadata['filedict']['GUID'],
                         'Checksum'     : metadata['filedict']['Checksum'],
                         'ChecksumType' : metadata['filedict']['ChecksumType'] }

        result = self.failoverTransfer.transferAndRegisterFileFailover( fileName = fileName,
                                                                        localPath = metadata['localpath'],
                                                                        lfn = metadata['filedict']['LFN'],
                                                                        targetSE = targetSE,
                                                                        failoverSEList = metadata['resolvedSE'],
                                                                        fileMetaDict = fileMetaDict,
                                                                        fileCatalog = 'LcgFileCatalogCombined' )
        if not result['OK']:
          self.log.error( 'Could not transfer and register %s in failover with metadata:\n %s' % ( fileName,
                                                                                                   metadata ) )
          cleanUp = True
          break  # no point continuing if one completely fails

      # Now after all operations, retrieve potentially modified request object
      self.request = self.failoverTransfer.request

      # If some or all of the files failed to be saved to failover
      if cleanUp:
        lfns = []
        for fileName, metadata in final.items():
          lfns.append( metadata['lfn'] )

        self.__cleanUp( lfns )
        self.workflow_commons['Request'] = self.request
        return S_ERROR( 'Failed to upload output data' )

      # Now double-check prior to final BK replica flag setting that the input files are still not processed
      if self.inputDataList:
        if fileDescendants != None:
          result = fileDescendants
        else:
          result = getFileDescendants( self.production_id, self.inputDataList, rm = self.rm, bkClient = self.bkClient )
        if not result:
          self.log.info( "No descendants found, outputs can be uploaded" )
        else:
          self.log.error( "Input files for this job were marked as processed during the upload. Cleaning up..." )
          self.__cleanUp( lfns )
          self.workflow_commons['Request'] = self.request
          return S_ERROR( 'Input Data Already Processed' )

      # Finally can send the BK records for the steps of the job
      bkFileExtensions = ['bookkeeping*.xml']
      bkFiles = []
      for ext in bkFileExtensions:
        self.log.debug( 'Looking at BK record wildcard: %s' % ext )
        globList = glob.glob( ext )
        for check in globList:
          if os.path.isfile( check ):
            self.log.verbose( 'Found locally existing BK file record: %s' % check )
            bkFiles.append( check )

      # Unfortunately we depend on the file names to order the BK records
      bkFiles.sort()
      self.log.info( 'The following BK records will be sent: %s' % ( ', '.join( bkFiles ) ) )
      for bkFile in bkFiles:
        fopen = open( bkFile, 'r' )
        bkXML = fopen.read()
        fopen.close()
        self.log.info( 'Sending BK record:\n%s' % ( bkXML ) )
        result = self.bkClient.sendXMLBookkeepingReport( bkXML )
        self.log.verbose( result )
        if result['OK']:
          self.log.info( 'Bookkeeping report sent for %s' % bkFile )
        else:
          self.log.error( "Could not send Bookkeeping XML file to server: %s" % result['Message'] )
          self.log.info( "Preparing DISET request for", bkFile )
          bkDISETReq = Operation()
          bkDISETReq.Type = "ForwardDISET"
          bkDISETReq.Arguments = DEncode.encode( result['rpcStub'] )
          self.request.addOperation( bkDISETReq )
          self.workflow_commons['Request'] = self.request  # update each time, just in case

      # Can now register the successfully uploaded files in the BK i.e. set the BK replica flags
      if not performBKRegistration:
        self.log.info( 'There are no files to perform the BK registration for, all could be saved to failover' )
      else:
        if registrationFailure:
          self.log.info( 'Catalog registration failures when upload files: preparing BK registration requests' )
          for lfnMetadata in performBKRegistration:
            self.setBKRegistrationRequest( lfnMetadata['lfn'], metaData = lfnMetadata['filedict'] )
        else:
          result = self.rm.addCatalogFile( [lfnMeta['lfn'] for lfnMeta in performBKRegistration],
                                           catalogs = ['BookkeepingDB'] )
          self.log.verbose( result )
          if not result['OK']:
            self.log.error( result )
            return S_ERROR( 'Could Not Perform BK Registration' )
          if result['Value']['Failed']:
            for lfn, error in result['Value']['Failed'].items():
              lfnMetadata = {}
              for lfnMD in performBKRegistration:
                if lfnMD['lfn'] == lfn:
                  lfnMetadata = lfnMD['filedict']
                  break
              self.setBKRegistrationRequest( lfn, error = error, metaData = lfnMetadata )

      self.workflow_commons['Request'] = self.request

      return S_OK( 'Output data uploaded' )

    except Exception, e:
      self.log.exception( e )
      self.setApplicationStatus( e )
      return S_ERROR( e )

    finally:
      super( UploadOutputData, self ).finalize( self.version )


  #############################################################################

  def __cleanUp( self, lfnList ):
    """ Clean up uploaded data for the LFNs in the list
    """
    for op in self.request:
      if op.Type in ['PutAndRegister', 'ReplicateAndRegister', 'RegisterFile', 'RegisterReplica']:
        for files in op:
          if files.LFN in lfnList:
            del op

    # Set removal requests just in case
    removeFiles = Operation()
    removeFiles.Type = 'RemoveFile'
    for lfn in lfnList:
      removedFile = File()
      removedFile.LFN = lfn
      removeFiles.addFile( removedFile )
    self.request.addOperation( removeFiles )

    return S_OK()

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
