""" Module to upload specified job output files according to the parameters
    defined in the user workflow.
"""

__RCSID__ = "$Id$"

import os, random, time, re

import DIRAC
from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.Utilities import List
from DIRAC.Core.Utilities.File import getGlobbedFiles
from DIRAC.DataManagementSystem.Client.FailoverTransfer import FailoverTransfer

from LHCbDIRAC.Core.Utilities.ProductionData import constructUserLFNs
from LHCbDIRAC.Workflow.Modules.ModuleBase import ModuleBase
from LHCbDIRAC.Core.Utilities.ResolveSE import getDestinationSEList


class UserJobFinalization( ModuleBase ):

  #############################################################################
  def __init__( self, bkClient = None, rm = None ):
    """Module initialization.
    """

    self.log = gLogger.getSubLogger( "UserJobFinalization" )
    super( UserJobFinalization, self ).__init__( self.log, bkClientIn = bkClient, rm = rm )

    self.version = __RCSID__
    self.enable = True
    self.defaultOutputSE = gConfig.getValue( '/Resources/StorageElementGroups/Tier1-USER', [] )
    self.failoverSEs = gConfig.getValue( '/Resources/StorageElementGroups/Tier1-Failover', [] )
    #List all parameters here
    self.userFileCatalog = 'LcgFileCatalogCombined'
    self.request = None
    self.lastStep = False
    #Always allow any files specified by users
    self.outputDataFileMask = ''
    self.userOutputData = ''
    self.userOutputSE = ''
    self.userOutputPath = ''
    self.jobReport = None

  #############################################################################
  def _resolveInputVariables( self ):
    """ By convention the module parameters are resolved here.
    """
    super( UserJobFinalization, self )._resolveInputVariables()

    #Use LHCb utility for local running via dirac-jobexec
    if self.workflow_commons.has_key( 'UserOutputData' ):
      self.userOutputData = self.workflow_commons['UserOutputData']
      if not type( self.userOutputData ) == type( [] ):
        self.userOutputData = [i.strip() for i in self.userOutputData.split( ';' )]

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
               step_number = None, step_id = None,
               ft = None ):
    """ Main execution function.
    """
    #Have to work out if the module is part of the last step i.e.
    #user jobs can have any number of steps and we only want
    #to run the finalization once.

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

      #Earlier modules may have populated the report objects
      self.request.setRequestName( 'job_%s_request.xml' % self.jobID )
      self.request.setJobID( self.jobID )
      self.request.setSourceComponent( "Job_%s" % self.jobID )

      if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
        self.log.verbose( 'Workflow status = %s, step status = %s' % ( self.workflowStatus['OK'],
                                                                       self.stepStatus['OK'] ) )
        self.log.error( 'Workflow status is not ok, will not overwrite application status.' )
        return S_ERROR( 'Workflow failed, UserJobFinalization module completed' )

      if not self.userOutputData:
        self.log.info( 'No user output data is specified for this job, nothing to do' )
        return S_OK( 'No output data to upload' )

      self.log.info( 'User specified output file list is: %s' % ( ', '.join( self.userOutputData ) ) )

      globList = []
      for i in self.userOutputData:
        if re.search( '\*', i ):
          globList.append( i )

      #Check whether list of userOutputData is a globbable pattern
      if globList:
        for i in globList:
          self.userOutputData.remove( i )

        globbedOutputList = List.uniqueElements( getGlobbedFiles( globList ) )
        if globbedOutputList:
          self.log.info( 'Found a pattern in the output data file list, \
          extra files to upload are: %s' % ( ', '.join( globbedOutputList ) ) )
          self.userOutputData += globbedOutputList
        else:
          self.log.info( 'No files were found on the local disk for the following patterns: %s' % ( ', '.join( globList ) ) )

      self.log.info( 'Final list of files to upload are: %s' % ( ', '.join( self.userOutputData ) ) )

      #Determine the final list of possible output files for the
      #workflow and all the parameters needed to upload them.
      outputList = []
      for i in self.userOutputData:
        outputList.append( {'outputDataType':( '.'.split( i )[-1] ).upper(),
                            'outputDataSE':self.userOutputSE,
                            'outputDataName':os.path.basename( i )} )

      userOutputLFNs = []
      if self.userOutputData:
        self.log.info( 'Constructing user output LFN(s) for %s' % ( ', '.join( self.userOutputData ) ) )
        if not self.jobID:
          self.jobID = 12345
        owner = ''

        if self.workflow_commons.has_key( 'OwnerName' ):
          owner = self.workflow_commons['OwnerName']
        else:
          owner = self.getCurrentOwner()
          if not owner['OK']:
            return owner
          owner = owner['Value']

        result = constructUserLFNs( int( self.jobID ), owner, self.userOutputData, self.userOutputPath )
        if not result['OK']:
          self.log.error( 'Could not create production LFNs', result['Message'] )
          return result
        userOutputLFNs = result['Value']

      self.log.verbose( 'Calling getCandidateFiles( %s, %s, %s)' % ( outputList, userOutputLFNs,
                                                                     self.outputDataFileMask ) )
      result = self.getCandidateFiles( outputList, userOutputLFNs, self.outputDataFileMask )
      if not result['OK']:
        self.setApplicationStatus( result['Message'] )
        return S_OK()

      fileDict = result['Value']
      result = self.getFileMetadata( fileDict )
      if not result['OK']:
        self.setApplicationStatus( result['Message'] )
        return S_OK()

      if not result['Value']:
        self.log.info( 'No output data files were determined to be uploaded for this workflow' )
        self.setApplicationStatus( 'No Output Data Files To Upload' )
        return S_OK()

      fileMetadata = result['Value']

      #First get the local (or assigned) SE to try first for upload and others in random fashion
      result = getDestinationSEList( 'Tier1-USER', DIRAC.siteName(), outputmode = 'local' )
      if not result['OK']:
        self.log.error( 'Could not resolve output data SE', result['Message'] )
        self.setApplicationStatus( 'Failed To Resolve OutputSE' )
        return result

      localSE = result['Value']
      self.log.verbose( 'Site Local SE for user outputs is: %s' % ( localSE ) )
      orderedSEs = self.defaultOutputSE
      for se in localSE:
        if se in orderedSEs:
          orderedSEs.remove( se )
      for se in self.userOutputSE:
        if se in orderedSEs:
          orderedSEs.remove( se )

      orderedSEs = localSE + List.randomize( orderedSEs )
      if self.userOutputSE:
        prependSEs = []
        for userSE in self.userOutputSE:
          if not userSE in orderedSEs:
            prependSEs.append( userSE )
        orderedSEs = prependSEs + orderedSEs

      self.log.info( 'Ordered list of output SEs is: %s' % ( ', '.join( orderedSEs ) ) )
      final = {}
      for fileName, metadata in fileMetadata.items():
        final[fileName] = metadata
        final[fileName]['resolvedSE'] = orderedSEs

      #At this point can exit and see exactly what the module will upload
      if not self._enableModule():
        self.log.info( 'Module is disabled by control flag, \
        would have attempted to upload the following files %s' % ', '.join( final.keys() ) )
        for fileName, metadata in final.items():
          self.log.info( '--------%s--------' % fileName )
          for n, v in metadata.items():
            self.log.info( '%s = %s' % ( n, v ) )

        return S_OK( 'Module is disabled by control flag' )

      #Disable the watchdog check in case the file uploading takes a long time
      self.log.info( 'Creating DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK in order to disable the Watchdog prior to upload' )
      fopen = open( 'DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK', 'w' )
      fopen.write( '%s' % time.asctime() )
      fopen.close()

      #Instantiate the failover transfer client with the global request object
      if not ft:
        ft = FailoverTransfer( self.request )

      #One by one upload the files with failover if necessary
      replication = {}
      failover = {}
      uploaded = []
      for fileName, metadata in final.items():
        self.log.info( "Attempting to store file %s to the following SE(s):\n%s" % ( fileName,
                                                                                     ', '.join( metadata['resolvedSE'] ) ) )
        result = ft.transferAndRegisterFile( fileName, metadata['localpath'],
                                             metadata['lfn'],
                                             metadata['resolvedSE'],
                                             fileGUID = metadata['guid'],
                                             fileCatalog = self.userFileCatalog )
        if not result['OK']:
          self.log.error( 'Could not transfer and register %s with metadata:\n %s' % ( fileName, metadata ) )
          failover[fileName] = metadata
        else:
          #Only attempt replication after successful upload
          lfn = metadata['lfn']
          uploaded.append( lfn )
          seList = metadata['resolvedSE']
          replicateSE = ''
          if result['Value'].has_key( 'uploadedSE' ):
            uploadedSE = result['Value']['uploadedSE']
            for se in seList:
              if not se == uploadedSE:
                replicateSE = se
                break

          if replicateSE and lfn:
            self.log.info( 'Will attempt to replicate %s to %s' % ( lfn, replicateSE ) )
            replication[lfn] = replicateSE

      cleanUp = False
      for fileName, metadata in failover.items():
        random.shuffle( self.failoverSEs )
        targetSE = metadata['resolvedSE'][0]
        metadata['resolvedSE'] = self.failoverSEs
        result = ft.transferAndRegisterFileFailover( fileName,
                                                     metadata['localpath'],
                                                     metadata['lfn'],
                                                     targetSE,
                                                     metadata['resolvedSE'],
                                                     fileGUID = metadata['guid'],
                                                     fileCatalog = self.userFileCatalog )
        if not result['OK']:
          self.log.error( 'Could not transfer and register %s with metadata:\n %s' % ( fileName, metadata ) )
          cleanUp = True
          continue #for users can continue even if one completely fails
        else:
          lfn = metadata['lfn']
          uploaded.append( lfn )

      #For files correctly uploaded must report LFNs to job parameters
      if uploaded:
        report = ', '.join( uploaded )
        self.jobReport.setJobParameter( 'UploadedOutputData', report )

      #Now after all operations, retrieve potentially modified request object
      result = ft.getRequestObject()
      if not result['OK']:
        self.log.error( result )
        return S_ERROR( 'Could Not Retrieve Modified Request' )

      self.request = result['Value']

      #If some or all of the files failed to be saved to failover
      if cleanUp:
        self.workflow_commons['Request'] = self.request
        #Leave any uploaded files just in case it is useful for the user
        #do not try to replicate any files.
        return S_ERROR( 'Failed To Upload Output Data' )

      #If there is now at least one replica for uploaded files can trigger replication
      self.log.info( 'Sleeping for 10 seconds before attempting replication of recently uploaded files' )
      time.sleep( 10 )
      self.log.verbose( 'Setting all non-CERN LFC mirrors to "InActive" before replication' )
      cfgRoot = '/Resources/FileCatalogs/LcgFileCatalogCombined'
      result = gConfig.getSections( cfgRoot )
      if not result['OK']:
        self.log.info( 'Could not get %s section to turn off mirrors' )
      else:
        for tier in result['Value']:
          if not tier == 'LCG.CERN.ch':
            tierCfg = '%s/%s/Status' % ( cfgRoot, tier )
            self.log.verbose( 'Setting "%s" to "InActive" in local configuration' % tierCfg )
            gConfig.setOptionValue( tierCfg, 'InActive' )

      for lfn, repSE in replication.items():
        result = self.rm.replicateAndRegister( lfn, repSE, catalog = self.userFileCatalog )
        if not result['OK']:
          self.log.info( 'Replication failed with below error\
           but file already exists in Grid storage with at least one replica:\n%s' % ( result ) )

      self.workflow_commons['Request'] = self.request

      #Now must ensure if any pending requests are generated that these are propagated to the job wrapper
      reportRequest = None
      if self.jobReport:
        result = self.jobReport.generateRequest()
        if not result['OK']:
          self.log.warn( 'Could not generate request for job report with result:\n%s' % ( result ) )
        else:
          reportRequest = result['Value']
      if reportRequest:
        self.log.info( 'Populating request with job report information' )
        self.request.update( reportRequest )

      if not self.request.isEmpty()['Value']:
        request_string = self.request.toXML()['Value']
        # Write out the request string
        fname = 'user_job_%s_request.xml' % ( self.jobID )
        xmlfile = open( fname, 'w' )
        xmlfile.write( request_string )
        xmlfile.close()
        self.log.info( 'Creating failover request for deferred operations for job %s:' % self.jobID )
        result = self.request.getDigest()
        if result['OK']:
          digest = result['Value']
          self.log.info( digest )

      self.setApplicationStatus( 'Job Finished Successfully' )

      return S_OK( 'Output data uploaded' )

    except Exception, e:
      self.log.exception( e )
      return S_ERROR( e )

    finally:
      super( UserJobFinalization, self ).finalize( self.version )

  #############################################################################

  def getCurrentOwner( self ):
    """Simple function to return current DIRAC username.
    """
    from DIRAC.Core.Security.ProxyInfo import getProxyInfo
    result = getProxyInfo()

    if not result['OK']:
      return S_ERROR( 'Could not obtain proxy information' )

    if not result['Value'].has_key( 'username' ):
      return S_ERROR( 'Could not get username from proxy' )

    username = result['Value']['username']

    return S_OK( username )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
