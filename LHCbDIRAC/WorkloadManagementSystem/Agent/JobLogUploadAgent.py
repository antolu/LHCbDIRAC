########################################################################
# $HeadURL$
########################################################################
"""  JobLogUploadAgent uploads log and other auxilliary files of the given job
     to the long term lo storage
"""
__RCSID__ = "$Id$"

from DIRAC  import gLogger, gConfig, gMonitor, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                              import AgentModule
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient import SandboxStoreClient
from DIRAC.DataManagementSystem.Client.ReplicaManager    import ReplicaManager
from DIRAC.RequestManagementSystem.Agent.RequestAgentMixIn import RequestAgentMixIn

import time, os, re, shutil
from types import *

AGENT_NAME = 'WorkloadManagement/JobLogUploadAgent'

class JobLogUploadAgent( AgentModule, RequestAgentMixIn ):

  def initialize( self ):
    self.RequestDBClient = RequestClient()
    self.rm = ReplicaManager()

    gMonitor.registerActivity( "Iteration", "Agent Loops", "JobLogUploadAgent", "Loops/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "Attempted", "Request Processed", "JobLogUploadAgent", "Requests/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "Successful", "Request Forward Successful", "JobLogUploadAgent", "Requests/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "Failed", "Request Forward Failed", "JobLogUploadAgent", "Requests/min", gMonitor.OP_SUM )

    self.workDir = self.am_getWorkDirectory()
    self.am_setOption( 'shifterProxy', 'ProductionManager' )
    self.local = PathFinder.getServiceURL( "RequestManagement/localURL" )
    if not self.local:
      errStr = 'The RequestManagement/localURL option must be defined.'
      gLogger.fatal( errStr )
      return S_ERROR( errStr )
    return S_OK()

  def execute( self ):
    """ Takes the DISET requests and forwards to destination service
    """
    gMonitor.addMark( "Iteration", 1 )

    work_done = True

    while work_done:
      work_done = False
      res = self.RequestDBClient.getRequest( 'logupload', url = self.local )
      if not res['OK']:
        gLogger.error( "JobLogUploadAgent.execute: Failed to get request from database.", self.local )
        return S_OK()
      elif not res['Value']:
        gLogger.info( "JobLogUploadAgent.execute: No requests to be executed found." )
        return S_OK()

      gMonitor.addMark( "Attempted", 1 )
      requestString = res['Value']['RequestString']
      requestName = res['Value']['RequestName']
      jobID = res['Value']['JobID']
      try:
        jobID = int( res['Value']['JobID'] )
      except:
        jobID = 0
      gLogger.info( "JobLogUploadAgent.execute: Obtained request %s" % requestName )

      result = self.RequestDBClient.getCurrentExecutionOrder( requestName, self.local )
      if result['OK']:
        currentOrder = result['Value']
      else:
        gLogger.warn( 'Can not get the request execution order' )
        continue

      oRequest = RequestContainer( request = requestString )
      requestAttributes = oRequest.getRequestAttributes()['Value']

      ################################################
      # Find the number of sub-requests from the request
      res = oRequest.getNumSubRequests( 'logupload' )
      if not res['OK']:
        errStr = "JobLogUploadAgent.execute: Failed to obtain number of logupload subrequests."
        gLogger.error( errStr, res['Message'] )
        continue

      gLogger.info( "JobLogUploadAgent.execute: Found %s sub requests for job %s" % ( res['Value'], jobID ) )
      ################################################
      # For all the sub-requests in the request
      modified = False
      for ind in range( res['Value'] ):
        subRequestAttributes = oRequest.getSubRequestAttributes( ind, 'logupload' )['Value']
        subExecutionOrder = int( subRequestAttributes['ExecutionOrder'] )
        subStatus = subRequestAttributes['Status']
        targetSE = subRequestAttributes['TargetSE']
        subRequestFiles = oRequest.getSubRequestFiles( ind, 'logupload' )['Value']
        path = subRequestFiles[0]['LFN']
        gLogger.info( "JobLogUploadAgent.execute: Processing sub-request %s with execution order %d" % ( ind, subExecutionOrder ) )
        if subStatus == 'Waiting' and subExecutionOrder <= currentOrder:
          res = self.uploadLogsForJob( jobID, targetSE, path )
          if res['OK']:
            gLogger.info( "JobLogUploadAgent.execute: Successfully uploaded." )
            oRequest.setSubRequestStatus( ind, 'logupload', 'Done' )
            gMonitor.addMark( "Successful", 1 )
            modified = True
            work_done = True
          else:
            oRequest.setSubRequestError( ind, 'logupload', res['Message'] )
            gLogger.error( "JobLogUploadAgent.execute: Failed to forward request.", res['Message'] )
        else:
          gLogger.info( "JobLogUploadAgent.execute: Sub-request %s is status '%s' and  not to be executed." % ( ind, subRequestAttributes['Status'] ) )

      ################################################
      #  Generate the new request string after operation
      requestString = oRequest.toXML()['Value']
      res = self.RequestDBClient.updateRequest( requestName, requestString, self.local )
      if res['OK']:
        gLogger.info( "JobLogUploadAgent.execute: Successfully updated request." )
      else:
        gLogger.error( "JobLogUploadAgent.execute: Failed to update request to", self.central )

      if modified and jobID:
        result = self.finalizeRequest( requestName, jobID, self.local )

    return S_OK()

  def uploadLogsForJob( self, jobID, targetSE, path ):
    """ Upload log files for a given job
    """

    workDir = self.workDir + '/' + str( jobID )
    if os.path.exists( workDir ):
      shutil.rmtree( workDir )
    os.makedirs( workDir )

    # 1. Output sandbox
    sandboxClient = SandboxStoreClient()
    result = sandboxClient.downloadSandboxForJob( jobID, 'Output', workDir )
    if not result['OK']:
      if result['Message'].find( 'No Output sandbox registered for job' ) == -1:
        shutil.rmtree( workDir )
        return result
    if os.path.exists( workDir + '/std.out' ):
      os.rename( workDir + '/std.out', workDir + '/jobstd.out' )
    if os.path.exists( workDir + '/std.err' ):
      os.rename( workDir + '/std.err', workDir + '/jobstd.err' )

    # 2. Job Logging history
    loggingClient = RPCClient( 'WorkloadManagement/JobMonitoring' )
    result = loggingClient.getJobLoggingInfo( jobID )
    if not result['OK']:
      shutil.rmtree( workDir )
      return result

    logfile = open( workDir + '/jobLoggingInfo', 'w' )
    loggingTupleList = result['Value']
    headers = ( 'Status', 'MinorStatus', 'ApplicationStatus', 'DateTime', 'Source' )

    line = ''.join( [ x.ljust( 30 ) for x in headers] )
    logfile.write( line + '\n' )

    for i in loggingTupleList:
      line = ''.join( [ x.ljust( 30 ) for x in list( i )] )
      logfile.write( line + '\n' )

    logfile.close()

    # 3. Job Parameters
    monitoringClient = RPCClient( 'WorkloadManagement/JobMonitoring' )
    result = monitoringClient.getJobParameters( jobID )
    if not result['OK']:
      shutil.rmtree( workDir )
      return result

    parDict = result['Value']
    parfile = open( workDir + '/jobParameters', 'w' )
    for name, value in parDict.items():
      parfile.write( name.ljust( 25 ) + ': ' + value + '\n' )
    parfile.close()

    # 4. Upload files
    files = os.listdir( workDir )
    for f in files:
      result = self.rm.put( path + '/' + f, workDir + '/' + f, targetSE )
      if not result['OK']:
        shutil.rmtree( workDir )
        return result

    shutil.rmtree( workDir )
    return S_OK()
