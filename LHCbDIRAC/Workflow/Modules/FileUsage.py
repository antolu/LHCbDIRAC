''' Module to upload specified job output files according to the parameters
    defined in the user workflow.
'''

__RCSID__ = "$Id$"

import os, copy

from DIRAC                                                 import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.RequestManagementSystem.Client.Request          import Operation
from DIRAC.Core.Utilities                                  import DEncode
from LHCbDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from LHCbDIRAC.DataManagementSystem.Client.DataUsageClient import DataUsageClient

class FileUsage( ModuleBase ):

  #############################################################################

  def __init__( self, bkClient = None, rm = None ):
    '''Module initialization.
    '''
    self.log = gLogger.getSubLogger( "FileUsage" )
    super( FileUsage, self ).__init__( self.log, bkClientIn = bkClient, rm = rm )
    self.version = __RCSID__
    self.dataUsageClient = DataUsageClient()

  #############################################################################

  def _resolveInputVariables( self ):
    ''' By convention the module parameters are resolved here.
    '''
    super( FileUsage, self )._resolveInputVariables()

    inputDataList = ''
    if self.InputData:
      inputDataList = copy.deepcopy( self.InputData )
      if type( inputDataList ) != type( [] ):
        inputDataList = inputDataList.split( ';' )

    dirDict = {}
    if inputDataList:
      for inputFile in inputDataList:
        if inputFile:
          baseName = os.path.basename( inputFile )
          in_f = copy.deepcopy( inputFile )
          strippedDir = in_f[0:in_f.find( baseName )].strip( 'LFN:' )
          if not strippedDir:
            self.log.error( 'Dataset unknown for file %s, probably file specified without path! ' % ( in_f ) )
          else:
            if strippedDir in dirDict:
              dirDict[strippedDir] += 1
            else:
              dirDict[strippedDir] = 1
    else:
      self.log.info( 'No input data specified for this job' )

    self.log.info( 'dirDict = ', dirDict )

    return S_OK( dirDict )

  #############################################################################

  def execute( self, production_id = None, prod_job_id = None, wms_job_id = None,
               workflowStatus = None, stepStatus = None,
               wf_commons = None, step_commons = None,
               step_id = None, step_number = None ):
    ''' Main execution function.
    '''

    try:

      super( FileUsage, self ).execute( self.version, production_id, prod_job_id, wms_job_id,
                                        workflowStatus, stepStatus,
                                        wf_commons, step_commons, step_number, step_id )

      result = self._resolveInputVariables()
      if not result['OK']:
        self.log.error( result['Message'] )
        return S_OK()
      dirDict = result['Value']

      self.request.RequestName = 'job_%d_request.xml' % self.jobID
      self.request.JobID = self.jobID
      self.request.SourceComponent = "Job_%d" % self.jobID

      if dirDict:
        result = self._reportFileUsage( dirDict )
        if not result['OK']:
          self.log.error( result['Message'] )
          return S_OK()
        self.log.info( "Reporting input file usage successful!" )
      else:
        self.log.info( "No input data usage to report!" )

      if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
        self.log.verbose( 'Workflow status = %s, step status = %s' % ( self.workflowStatus['OK'],
                                                                       self.stepStatus['OK'] ) )
        self.log.error( 'Workflow status is not ok, will not overwrite application status.' )
        return S_ERROR( 'Workflow failed, FileUsage module completed' )

      return S_OK( 'File Usage reported successfully' )
    except Exception, e:
      self.log.exception( e )
      return S_ERROR( e )

    finally:
      super( FileUsage, self ).finalize( self.version )

  #############################################################################

  def _reportFileUsage( self, dirDict ):
    '''Send the data usage report (SE,dirDict) where dirDict = {'Dataset':NumberOfHits}
    example: {'/lhcb/certification/test/ALLSTREAMS.DST/00000002/0000/': 1,
    '/lhcb/LHCb/Collision11/BHADRON.DST/00012957/0000/': 2}
    '''
    self.log.verbose( 'FileUsage._reportFileUsage' )
    self.log.verbose( 'Reporting input file usage:' )
    for entry in dirDict:
      self.log.verbose( '%s:%s' % ( entry, dirDict[entry] ) )
    #dataUsageClient = RPCClient( 'DataManagement/DataUsage', timeout = 120 )
    localSEList = gConfig.getValue( '/LocalSite/LocalSE', '' )
    if not localSEList:
      self.log.error( 'FileUsage._reportFileUsage: Could not get value from CS for option /LocalSite/LocalSE' )
      localSEList = "UNKNOWN"
    self.log.verbose( 'Using /LocalSite/LocalSE: %s' % ( localSEList ) )
    # example LocalSEList = 'SARA-RAW, SARA-RDST, SARA-ARCHIVE, SARA-DST, SARA_M-DST, SARA-USER'

    # we only care about the site, so strip the SE list
    localSE = localSEList
    cutoff = min( localSEList.find( '_' ), localSEList.find( '-' ) )
    if cutoff != -1:
      localSE = localSE[0:cutoff]

    if self._enableModule():
      usageStatus = self.dataUsageClient.sendDataUsageReport( localSE, dirDict )
      if not usageStatus['OK']:
        self.log.error( 'Could not send data usage report, preparing a DISET failover request object' )
        self.log.verbose( usageStatus['rpcStub'] )
        forwardDISETOp = Operation()
        forwardDISETOp.Type = "ForwardDISET"
        forwardDISETOp.Arguments = DEncode.encode( usageStatus['rpcStub'] )
        self.request.addOperation( forwardDISETOp )
        self.workflow_commons['Request'] = self.request
    else:
      self.log.info( 'Would have attempted to report %s at %s' % ( dirDict, localSE ) )
      return S_OK()

    return S_OK()

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
