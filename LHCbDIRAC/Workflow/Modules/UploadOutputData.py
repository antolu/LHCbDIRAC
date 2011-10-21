########################################################################
# $Id$
########################################################################
""" Module to upload specified job output files according to the parameters
    defined in the production workflow.
"""

__RCSID__ = "$Id$"

from LHCbDIRAC.Core.Utilities.ProductionData import constructProductionLFNs
from LHCbDIRAC.Core.Utilities.ResolveSE import getDestinationSEList
from LHCbDIRAC.Workflow.Modules.ModuleBase import ModuleBase

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
import DIRAC

import string, os, random, time, glob

class UploadOutputData( ModuleBase ):

  #############################################################################
  def __init__( self ):
    """Module initialization.
    """

    self.log = gLogger.getSubLogger( "UploadOutputData" )
    super( UploadOutputData, self ).__init__( self.log )

    self.version = __RCSID__
    self.commandTimeOut = 10 * 60
    self.jobID = ''
    self.jobType = ''
    self.failoverSEs = gConfig.getValue( '/Resources/StorageElementGroups/Tier1-Failover', [] )
    self.existingCatalogs = []
    result = gConfig.getSections( '/Resources/FileCatalogs' )
    if result['OK']:
      self.existingCatalogs = result['Value']

    #List all parameters here
    self.inputData = []
    self.outputDataFileMask = ''
    self.outputMode = 'Any' #or 'Local' for reco case
    self.outputList = []
    self.outputDataStep = ''
    self.request = None
    self.PRODUCTION_ID = None

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

    if self.workflow_commons.has_key( 'outputDataFileMask' ):
      self.outputDataFileMask = self.workflow_commons['outputDataFileMask']
      if not type( self.outputDataFileMask ) == type( [] ):
        self.outputDataFileMask = [i.lower().strip() for i in self.outputDataFileMask.split( ';' )]

    #Use LHCb utility for local running via jobexec
    if self.workflow_commons.has_key( 'ProductionOutputData' ):
        self.prodOutputLFNs = self.workflow_commons['ProductionOutputData']
        if not type( self.prodOutputLFNs ) == type( [] ):
          self.prodOutputLFNs = [i.strip() for i in self.prodOutputLFNs.split( ';' )]
    else:
      self.log.info( 'ProductionOutputData parameter not found, creating on the fly' )
      result = constructProductionLFNs( self.workflow_commons )
      if not result['OK']:
        self.log.error( 'Could not create production LFNs', result['Message'] )
        return result
      self.prodOutputLFNs = result['Value']['ProductionOutputData']

    if self.workflow_commons.has_key( 'InputData' ):
      self.inputData = self.workflow_commons['InputData']

    if self.inputData:
      if type( self.inputData ) != type( [] ):
        self.inputData = self.inputData.split( ';' )

    if self.workflow_commons.has_key( 'JobType' ):
      self.jobType = self.workflow_commons['JobType']

  #############################################################################

  def execute( self, production_id = None, prod_job_id = None, wms_job_id = None,
               workflowStatus = None, stepStatus = None,
               wf_commons = None, step_commons = None,
               step_number = None, step_id = None,
               rm = None, ft = None, bk = None ):
    """ Main execution function.
    """

    try:

      super( UploadOutputData, self ).execute( self.version, production_id, prod_job_id, wms_job_id,
                                               workflowStatus, stepStatus,
                                               wf_commons, step_commons, step_number, step_id )

      self._resolveInputVariables()

      self.request.setRequestName( 'job_%s_request.xml' % self.jobID )
      self.request.setJobID( self.jobID )
      self.request.setSourceComponent( "Job_%s" % self.jobID )

      if not bk:
        from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
        bkClient = BookkeepingClient()
      else:
        bkClient = bk

      if not self._checkWFAndStepStatus():
        return S_OK( 'No output data upload attempted' )

      #Determine the final list of possible output files for the
      #workflow and all the parameters needed to upload them.
      self.log.verbose( 'Getting the list of candidate files' )
      result = self.getCandidateFiles( self.outputList, self.prodOutputLFNs,
                                       self.outputDataFileMask, self.outputDataStep )
      if not result['OK']:
        self.setApplicationStatus( result['Message'] )
        return result

      fileDict = result['Value']
      result = self.getFileMetadata( fileDict )
      if not result['OK']:
        self.setApplicationStatus( result['Message'] )
        return result

      if not result['Value']:
        self.log.info( 'No output data files were determined to be uploaded for this workflow' )
        return S_OK()

      fileMetadata = result['Value']

      #Get final, resolved SE list for files
      final = {}
      for fileName, metadata in fileMetadata.items():
        result = getDestinationSEList( metadata['workflowSE'], DIRAC.siteName(), self.outputMode )
        if not result['OK']:
          self.log.error( 'Could not resolve output data SE: ', result['Message'] )
          self.setApplicationStatus( 'Failed To Resolve OutputSE' )
          return result

        resolvedSE = result['Value']
        final[fileName] = metadata
        final[fileName]['resolvedSE'] = resolvedSE

      self.log.info( 'The following files will be uploaded: %s' % ( string.join( final.keys(), ', ' ) ) )
      for fileName, metadata in final.items():
        self.log.info( '--------%s--------' % fileName )
        for n, v in metadata.items():
          self.log.info( '%s = %s' % ( n, v ) )

      #At this point can exit and see exactly what the module would have uploaded
      if not self._enableModule():
        self.log.info( 'Would have attempted to upload the following files %s' % string.join( final.keys(), ', ' ) )
        return S_OK()

      #Prior to uploading any files must check (for productions with input data) that no descendent files
      #already exist with replica flag in the BK.  The result structure is:
      #{'OK': True, 
      # 'Value': {
      #'Successful': {'/lhcb/certification/2009/SIM/00000048/0000/00000048_00000013_1.sim': ['/lhcb/certification/2009/DST/00000048/0000/00000048_00000013_3.dst']}, 
      #'Failed': [], 'NotProcessed': []}}

      result = self.checkInputsNotAlreadyProcessed( self.inputData, self.production_id, bkClient )
      if not result['OK']:
        return result

      #Disable the watchdog check in case the file uploading takes a long time
      self.log.info( 'Creating DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK in order to disable the Watchdog prior to upload' )
      fopen = open( 'DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK', 'w' )
      fopen.write( '%s' % time.asctime() )
      fopen.close()

      #Instantiate the failover transfer client with the global request object
      if not ft:
        from DIRAC.DataManagementSystem.Client.FailoverTransfer import FailoverTransfer
        failoverTransfer = FailoverTransfer( self.request )
      else:
        failoverTransfer = ft

      #Track which files are successfully uploaded (not to failover) via
      performBKRegistration = []
      #Failover replicas are always added to the BK when they become available

      #One by one upload the files with failover if necessary
      registrationFailure = False
      failover = {}
      for fileName, metadata in final.items():
        self.log.info( "Attempting to store file %s to the following SE(s):\n%s" % ( fileName, string.join( metadata['resolvedSE'], ', ' ) ) )
        result = failoverTransfer.transferAndRegisterFile( fileName, metadata['localpath'], metadata['lfn'],
                                                           metadata['resolvedSE'], fileGUID = metadata['guid'],
                                                           fileCatalog = 'LcgFileCatalogCombined' )
        if not result['OK']:
          self.log.error( 'Could not transfer and register %s with metadata:\n %s' % ( fileName, metadata ) )
          failover[fileName] = metadata
        else:
          if result['Value'].has_key( 'registration' ):
            self.log.info( 'File %s was put to the SE but the catalog registration will be set as an asynchronous request' % ( fileName ) )
            registrationFailure = True
          else:
            self.log.info( '%s uploaded successfully, will be registered in BK if all files uploaded for job' % ( fileName ) )

          lfn = metadata['lfn']
          performBKRegistration.append( lfn )

      cleanUp = False
      for fileName, metadata in failover.items():
        self.log.info( 'Setting default catalog for failover transfer to LcgFileCatalogCombined' )
        random.shuffle( self.failoverSEs )
        targetSE = metadata['resolvedSE'][0]
        metadata['resolvedSE'] = self.failoverSEs
        result = failoverTransfer.transferAndRegisterFileFailover( fileName, metadata['localpath'], metadata['lfn'],
                                                                   targetSE, metadata['resolvedSE'], fileGUID = metadata['guid'],
                                                                   fileCatalog = 'LcgFileCatalogCombined' )
        if not result['OK']:
          self.log.error( 'Could not transfer and register %s with metadata:\n %s' % ( fileName, metadata ) )
          cleanUp = True
          break #no point continuing if one completely fails

      #Now after all operations, retrieve potentially modified request object
      result = failoverTransfer.getRequestObject()
      if not result['OK']:
        self.log.error( result )
        return S_ERROR( 'Could not retrieve modified request' )

      self.request = result['Value']

      #If some or all of the files failed to be saved to failover
      if cleanUp:
        lfns = []
        for fileName, metadata in final.items():
          lfns.append( metadata['lfn'] )

        self.__cleanUp( lfns )
        self.workflow_commons['Request'] = self.request
        return S_ERROR( 'Failed to upload output data' )

      #Now double-check prior to final BK replica flag setting that the input files are still not processed 
      result = self.checkInputsNotAlreadyProcessed( self.inputData, self.production_id, bkClient )
      if not result['OK']:
        lfns = []
        self.log.error( 'Input files for this job were marked as processed during the upload of this job\'s outputs! Cleaning up...' )
        for fileName, metadata in final.items():
          lfns.append( metadata['lfn'] )

        self.__cleanUp( lfns )
        self.workflow_commons['Request'] = self.request
        return result

      #Finally can send the BK records for the steps of the job
      bkFileExtensions = ['bookkeeping*.xml']
      bkFiles = []
      for ext in bkFileExtensions:
        self.log.debug( 'Looking at BK record wildcard: %s' % ext )
        globList = glob.glob( ext )
        for check in globList:
          if os.path.isfile( check ):
            self.log.verbose( 'Found locally existing BK file record: %s' % check )
            bkFiles.append( check )

      #Unfortunately we depend on the file names to order the BK records
      bkFiles.sort()
      self.log.info( 'The following BK records will be sent: %s' % ( string.join( bkFiles, ', ' ) ) )

      for bkFile in bkFiles:
        fopen = open( bkFile, 'r' )
        bkXML = fopen.read()
        fopen.close()
        self.log.info( 'Sending BK record %s:\n%s' % ( bkFile, bkXML ) )
        result = bkClient.sendBookkeeping( bkFile, bkXML )
        self.log.verbose( result )
        if result['OK']:
          self.log.info( 'Bookkeeping report sent for %s' % bkFile )
        else:
          self.log.error( 'Could not send Bookkeeping XML file to server, preparing DISET request for', bkFile )
          self.request.setDISETRequest( result['rpcStub'], executionOrder = 0 )
          self.workflow_commons['Request'] = self.request  # update each time, just in case

      #Can now register the successfully uploaded files in the BK i.e. set the BK replica flags
      if not performBKRegistration:
        self.log.info( 'There are no files to perform the BK registration for, all could be saved to failover' )
      elif registrationFailure:
        self.log.info( 'There were catalog registration failures during the upload of files for this job, BK registration requests are being prepared' )
        for lfn in performBKRegistration:
          result = self.setBKRegistrationRequest( lfn )
          if not result['OK']:
            return result
      else:
        if not rm:
          from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
          rm = ReplicaManager()
        result = rm.addCatalogFile( performBKRegistration, catalogs = ['BookkeepingDB'] )
        self.log.verbose( result )
        if not result['OK']:
          self.log.error( result )
          return S_ERROR( 'Could Not Perform BK Registration' )
        if result['Value']['Failed']:
          for lfn, error in result['Value']['Failed'].items():
            result = self.setBKRegistrationRequest( lfn, error )
            if not result['OK']:
              return result

      self.workflow_commons['Request'] = self.request

      return S_OK( 'Output data uploaded' )

    except Exception, e:
      self.log.exception( e )
      return S_ERROR( e )

    finally:
      super( UploadOutputData, self ).finalize( self.version )


  #############################################################################

  def checkInputsNotAlreadyProcessed( self, inputData, productionID, bkClient ):
    """ Checks that the input files for the job were not already processed by
        another job i.e. that there are no other descendent files for the 
        current productionID having a BK replica flag.  
    """
    if not inputData:
      self.log.verbose( 'This job has no input data to check for descendents in the BK' )
      return S_OK()

    prodID = str( productionID )
    prodID = prodID.lstrip( '0' )
    self.log.info( 'Will check BK descendents for input data of job prior to uploading outputs' )
    start = time.time()
    result = bkClient.getAllDescendents( inputData, depth = 9, production = int( prodID ), checkreplica = True )
    timing = time.time() - start
    self.log.info( 'BK Descendents Lookup Time: %.2f seconds ' % ( timing ) )
    if not result['OK']:
      self.log.error( 'Would have uploaded output data for job but could not check for descendents of input data from BK with result:\n%s' % ( result ) )
      return S_ERROR( 'Could Not Contact BK To Check Descendents' )
    if result['Value']['Failed']:
      self.log.error( 'BK getAllDescendents returned an error for some files:\n%s\nwill exit to avoid uploading outputs that have already been processed' % ( result['Value']['Failed'] ) )
      return S_ERROR( 'BK Descendents Check Was Not Complete' )

    inputDataDescDict = result['Value']['Successful']
    failed = False
    for inputDataFile, descendents in inputDataDescDict.items():
      if descendents:
        failed = True
        self.log.error( 'Input files: \n%s \nDescendents: %s' % ( string.join( inputData, '\n' ), string.join( descendents, '\n' ) ) )
    if failed:
      self.log.error( '!!!!Found descendent files for production %s with BK replica flag for an input file of this job!!!!' % ( prodID ) )
      return S_ERROR( 'Input Data Already Processed' )

    return S_OK( 'Outputs can be uploaded' )

  #############################################################################

  def setBKRegistrationRequest( self, lfn, error = '' ):
    """ Set a BK registration request for changing the replica flag.  Uses the
        global request object.  
    """
    if error:
      self.log.info( 'BK registration for %s failed with message: "%s" setting failover request' % ( lfn, error ) )
    else:
      self.log.info( 'Setting BK registration request for %s' % ( lfn ) )

    result = self.request.addSubRequest( {'Attributes':{'Operation':'registerFile', 'ExecutionOrder':2, 'Catalogue':'BookkeepingDB'}}, 'register' )
    if not result['OK']:
      self.log.error( 'Could not set registerFile request:\n%s' % result )
      return S_ERROR( 'Could Not Set BK Registration Request' )
    fileDict = {'LFN':lfn, 'Status':'Waiting'}
    index = result['Value']
    self.request.setSubRequestFiles( index, 'register', [fileDict] )
    return S_OK()

  #############################################################################

  def __cleanUp( self, lfnList ):
    """ Clean up uploaded data for the LFNs in the list
    """
    # Clean up the current request
    for req_type in ['transfer', 'register']:
      for lfn in lfnList:
        result = self.request.getNumSubRequests( req_type )
        if result['OK']:
          nreq = result['Value']
          if nreq:
            # Go through subrequests in reverse order in order not to spoil the numbering
            ind_range = [0]
            if nreq > 1:
              ind_range = range( nreq - 1, -1, -1 )
            for i in ind_range:
              result = self.request.getSubRequestFiles( i, req_type )
              if result['OK']:
                fileList = result['Value']
                if fileList[0]['LFN'] == lfn:
                  result = self.request.removeSubRequest( i, req_type )

    # Set removal requests just in case
    for lfn in lfnList:
      result = self.request.addSubRequest( {'Attributes':{'Operation':'removeFile', 'TargetSE':'', 'ExecutionOrder':1}}, 'removal' )
      index = result['Value']
      fileDict = {'LFN':lfn, 'PFN':'', 'Status':'Waiting'}
      self.request.setSubRequestFiles( index, 'removal', [fileDict] )

    return S_OK()

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
