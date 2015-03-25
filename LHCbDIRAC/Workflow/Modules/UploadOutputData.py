""" Module to upload specified job output files according to the parameters defined in the production workflow.
"""

__RCSID__ = "$Id$"

import os
import random
import glob
from operator import itemgetter

from DIRAC                                                    import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.Utilities                                     import DEncode
from DIRAC.DataManagementSystem.Client.FailoverTransfer       import FailoverTransfer
from DIRAC.RequestManagementSystem.Client.Request             import Request
from DIRAC.RequestManagementSystem.Client.Operation           import Operation
from DIRAC.RequestManagementSystem.Client.File                import File
from DIRAC.Resources.Catalog.FileCatalog                      import FileCatalog

from LHCbDIRAC.Core.Utilities.ResolveSE                       import getDestinationSEList
from LHCbDIRAC.Core.Utilities.ProductionData                  import constructProductionLFNs
from LHCbDIRAC.DataManagementSystem.Client.ConsistencyChecks  import getFileDescendants

from LHCbDIRAC.Workflow.Modules.ModuleBase                    import ModuleBase

class UploadOutputData( ModuleBase ):
  """ Module to upload specified job output files according to the parameters defined in the production workflow.
  """

  #############################################################################
  def __init__( self, bkClient = None, dm = None ):
    """ Module initialization.
    """

    self.log = gLogger.getSubLogger( "UploadOutputData" )
    super( UploadOutputData, self ).__init__( self.log, bkClientIn = bkClient, dm = dm )

    self.version = __RCSID__
    self.commandTimeOut = 10 * 60
    self.jobID = ''
    self.failoverSEs = gConfig.getValue( '/Resources/StorageElementGroups/Tier1-FAILOVER', [] )
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
    self.prodOutputLFNs = []

  #############################################################################
  def _resolveInputVariables( self ):
    """ By convention the module parameters are resolved here.
    """

    super( UploadOutputData, self )._resolveInputVariables()

    if self.workflow_commons.has_key( 'outputDataStep' ):
      self.outputDataStep = [str( ds ) for ds in self.workflow_commons['outputDataStep'].split( ';' )]

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
      self.log.info( "ProductionOutputData parameter not found, creating on the fly" )
      result = constructProductionLFNs( self.workflow_commons, self.bkClient )
      if not result['OK']:
        self.log.error( "Could not create production LFNs", result['Message'] )
        return result
      self.prodOutputLFNs = result['Value']['ProductionOutputData']

  #############################################################################

  def execute( self, production_id = None, prod_job_id = None, wms_job_id = None,
               workflowStatus = None, stepStatus = None,
               wf_commons = None, step_commons = None,
               step_number = None, step_id = None,
               SEs = None, fileDescendants = None ):
    """ Main execution function.

        1. Determine the final list of possible output files for the workflow and all the parameters needed to upload them.
        2. Verifying that the input files have no descendants (and exiting with error, otherwise)
        3. Sending the BK records for the steps of the job
        4. Transfer output files in their destination, register in the FC (with failover)
        5. Registering the output files in the Bookkeeping
    """

    try:

      super( UploadOutputData, self ).execute( self.version, production_id, prod_job_id, wms_job_id,
                                               workflowStatus, stepStatus,
                                               wf_commons, step_commons, step_number, step_id )

      self._resolveInputVariables()

      if not self._checkWFAndStepStatus():
        return S_OK( "Failures detected in previous steps: no output data upload attempted" )

      # ## 1. Determine the final list of possible output files for the workflow and all the parameters needed to upload them.
      # ##

      self.log.verbose( "Getting the list of candidate files" )
      fileDict = self.getCandidateFiles( self.outputList, self.prodOutputLFNs,
                                         self.outputDataFileMask, self.outputDataStep )

      fileMetadata = self.getFileMetadata( fileDict )

      if not fileMetadata:
        self.log.info( "No output data files were determined to be uploaded for this workflow" )
        return S_OK()

      # Get final, resolved SE list for files
      final = {}

      for fileName, metadata in fileMetadata.items():
        if not SEs:
          resolvedSE = getDestinationSEList( metadata['workflowSE'], self.siteName, self.outputMode )
        else:
          resolvedSE = SEs
        final[fileName] = metadata
        final[fileName]['resolvedSE'] = resolvedSE

      self.log.info( "The following files will be uploaded: %s" % ( ', '.join( final.keys() ) ) )
      for fileName, metadata in final.items():
        self.log.info( '--------%s--------' % fileName )
        for name, val in metadata.items():
          self.log.info( '%s = %s' % ( name, val ) )

      if not self._enableModule():
        # At this point can exit and see exactly what the module would have uploaded
        self.log.info( "Would have attempted to upload the following files %s" % ', '.join( final.keys() ) )
        return S_OK()


      # ## 2. Prior to uploading any files must check (for productions with input data) that no descendant files
      # ## already exist with replica flag in the BK.
      # ##

      if self.inputDataList:
        if fileDescendants != None:
          result = fileDescendants
        else:
          result = getFileDescendants( self.production_id, self.inputDataList,
                                       dm = self.dataManager, bkClient = self.bkClient )
        if not result:
          self.log.info( "No descendants found, outputs can be uploaded" )
        else:
          self.log.error( "Found descendants!!! Outputs won't be uploaded" )
          self.log.info( "Files with descendants: %s" ', '.join( result ) )
          return S_ERROR( "Input Data Already Processed" )


      # ## 3. Sending the BK records for the steps of the job
      # ##

      bkFileExtensions = ['bookkeeping*.xml']
      bkFiles = []
      for ext in bkFileExtensions:
        self.log.debug( "Looking at BK record wildcard: %s" % ext )
        globList = glob.glob( ext )
        for check in globList:
          if os.path.isfile( check ):
            self.log.verbose( "Found locally existing BK file record: %s" % check )
            bkFiles.append( check )

      # Unfortunately we depend on the file names to order the BK records
      bkFilesListTuples = []
      for bk in bkFiles:
        bkFilesListTuples.append( ( bk, int( bk.split( '_' )[-1].split( '.' )[0] ) ) )
      bkFiles = [bk[0] for bk in sorted( bkFilesListTuples, key = itemgetter( 1 ) )]

      self.log.info( "The following BK records will be sent: %s" % ( ', '.join( bkFiles ) ) )
      if self._enableModule():
        for bkFile in bkFiles:
          fopen = open( bkFile, 'r' )
          bkXML = fopen.read()
          fopen.close()
          self.log.info( "Sending BK record:\n%s" % ( bkXML ) )
          result = self.bkClient.sendXMLBookkeepingReport( bkXML )
          self.log.verbose( result )
          if result['OK']:
            self.log.info( "Bookkeeping report sent for %s" % bkFile )
          else:
            self.log.error( "Could not send Bookkeeping XML file to server: %s" % result['Message'] )
            self.log.info( "Preparing DISET request for", bkFile )
            bkDISETReq = Operation()
            bkDISETReq.Type = 'ForwardDISET'
            bkDISETReq.Arguments = DEncode.encode( result['rpcStub'] )
            self.request.addOperation( bkDISETReq )
            self.workflow_commons['Request'] = self.request  # update each time, just in case
      else:
        self.log.info( "Would have attempted to send bk records, but module is disabled" )


      # ## 4. Transfer output files in their destination, register in the FC (with failover)
      # ##

      # Disable the watchdog check in case the file uploading takes a long time
      self._disableWatchdogCPUCheck()

      # Instantiate the failover transfer client with the global request object
      if not self.failoverTransfer:
        self.failoverTransfer = FailoverTransfer( self.request )

      # Track which files are successfully uploaded (not to failover) via
      performBKRegistration = []
      # Failover replicas are always added to the BK when they become available (actually, added to all the catalogs)

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
                                                                masterCatalogOnly = True )
        if not result['OK']:
          self.log.error( "Could not transfer and register %s with metadata:\n %s" % ( fileName, metadata ) )
          failover[fileName] = metadata
        else:
          self.log.info( "%s uploaded, will be registered in BK if all files uploaded for job" % fileName )

          # if the files are uploaded in the SE, independently if the registration in the FC is done,
          # then we have to register all of them in the BKK
          performBKRegistration.append( metadata )

      cleanUp = False
      for fileName, metadata in failover.items():
        self.log.info( "Setting default catalog for failover transfer registration to master catalog" )
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
                                                                        masterCatalogOnly = True )
        if not result['OK']:
          self.log.error( "Could not transfer and register %s in failover with metadata:\n %s" % ( fileName,
                                                                                                   metadata ) )
          cleanUp = True
          break  # no point continuing if one completely fails

      # Now after all operations, retrieve potentially modified request object
      self.request = self.failoverTransfer.request

      # If some or all of the files failed to be saved even to failover
      if cleanUp:
        self._cleanUp( final )
        self.workflow_commons['Request'] = self.request
        return S_ERROR( 'Failed to upload output data' )

      # For files correctly uploaded must report LFNs to job parameters
      if final:
        report = ', '.join( final.keys() )
        self.setJobParameter( 'UploadedOutputData', report )

      # ## 5. Can now register the successfully uploaded files in the BK i.e. set the BK replica flags
      # ##

      if not performBKRegistration:
        self.log.info( "There are no files to perform the BK registration for, all are in failover" )
      else:
        result = FileCatalog( catalogs = ['BookkeepingDB'] ).addFile( [lfnMeta['lfn'] for lfnMeta in performBKRegistration] )
        self.log.verbose( "BookkeepingDB.addFile: %s" % result )
        if not result['OK']:
          self.log.error( result )
          return S_ERROR( "Could Not Perform BK Registration" )
        if 'Failed' in result['Value'] and result['Value']['Failed']:
          for lfn, error in result['Value']['Failed'].items():
            lfnMetadata = {}
            for lfnMD in performBKRegistration:
              if lfnMD['lfn'] == lfn:
                lfnMetadata = lfnMD['filedict']
                break
            self.setBKRegistrationRequest( lfn, error = error, metaData = lfnMetadata )

      self.workflow_commons['Request'] = self.request

      return S_OK( "Output data uploaded" )

    except Exception, e:
      self.log.exception( e )
      self.setApplicationStatus( e )
      return S_ERROR( e )

    finally:
      super( UploadOutputData, self ).finalize( self.version )


  #############################################################################

  def _cleanUp( self, final ):
    """ Clean up uploaded data for the LFNs in the list
    """
    lfnList = []
    for _fileName, metadata in final.items():
      lfnList.append( metadata['lfn'] )

    self.log.verbose( "Cleaning up the request, for LFNs: %s" % ', '.join( lfnList ) )

    newRequest = Request()

    for op in self.request:
      add = True
      if op.Type in ['PutAndRegister', 'ReplicateAndRegister', 'RegisterFile', 'RegisterReplica']:
        for files in op:
          if files.LFN in lfnList:
            add = False
      if add:
        newRequest.addOperation( op )

    self.request = newRequest

    self.log.verbose( "And adding RemoveFile operation for LFNs: %s, just in case" % ', '.join( lfnList ) )

    removeFiles = Operation()
    removeFiles.Type = 'RemoveFile'
    for lfn in lfnList:
      removedFile = File()
      removedFile.LFN = lfn
      removeFiles.addFile( removedFile )
    self.request.addOperation( removeFiles )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
