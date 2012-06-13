"""  LogUploadAgent uploads log and other auxilliary files of the given job
     to the long term log storage
"""
__RCSID__ = "$Id$"

import os, shutil

from DIRAC  import gLogger, gMonitor, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient import SandboxStoreClient
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.RequestManagementSystem.Agent.RequestAgentMixIn import RequestAgentMixIn

AGENT_NAME = 'DataManagement/LogUploadAgent'

class LogUploadAgent( AgentModule, RequestAgentMixIn ):

  def initialize( self ):
    self.RequestDBClient = RequestClient()
    self.rm = ReplicaManager()

    gMonitor.registerActivity( "Iteration", "Agent Loops", "JobLogUploadAgent", "Loops/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "Attempted", "Request Processed", "JobLogUploadAgent", "Requests/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "Successful", "Request Forward Successful", "JobLogUploadAgent", "Requests/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "Failed", "Request Forward Failed", "JobLogUploadAgent", "Requests/min", gMonitor.OP_SUM )

#    self.workDir = self.am_getWorkDirectory()
    self.am_setOption( 'shifterProxy', 'ProductionManager' )
    self.local = PathFinder.getServiceURL( "RequestManagement/localURL" )
    if not self.local:
      errStr = 'The RequestManagement/localURL option must be defined.'
      gLogger.fatal( errStr )
      return S_ERROR( errStr )
    return S_OK()

  def execute( self ):
    """ Takes the logupload requests and forwards to destination service
    """
    gMonitor.addMark( "Iteration", 1 )

    work_done = True

    while work_done:
      work_done = False
      res = self.RequestDBClient.getRequest( 'logupload', url = self.local )
      if not res['OK']:
        gLogger.error( "Failed to get request from database.", self.local )
        return S_OK()
      elif not res['Value']:
        gLogger.info( "No requests to be executed found." )
        return S_OK()

      gMonitor.addMark( "Attempted", 1 )
      requestString = res['Value']['RequestString']
      requestName = res['Value']['RequestName']
      jobID = res['Value']['JobID']
      try:
        jobID = int( res['Value']['JobID'] )
      except:
        jobID = 0
      gLogger.info( "Obtained request %s" % requestName )

      result = self.RequestDBClient.getCurrentExecutionOrder( requestName, self.local )
      if result['OK']:
        currentOrder = result['Value']
      else:
        gLogger.warn( 'Can not get the request execution order' )
        continue

      oRequest = RequestContainer( request = requestString )
#      requestAttributes = oRequest.getRequestAttributes()['Value']

      ################################################
      # Find the number of sub-requests from the request
      res = oRequest.getNumSubRequests( 'logupload' )
      if not res['OK']:
        errStr = "Failed to obtain number of logupload subrequests."
        gLogger.error( errStr, res['Message'] )
        continue

      gLogger.info( "Found %s sub requests for job %s" % ( res['Value'], jobID ) )
      ################################################
      # For all the sub-requests in the request
      modified = False
      for ind in range( res['Value'] ):
        subRequestAttributes = oRequest.getSubRequestAttributes( ind, 'logupload' )['Value']
        subExecutionOrder = int( subRequestAttributes['ExecutionOrder'] )
        subStatus = subRequestAttributes['Status']
        targetSE = subRequestAttributes['TargetSE']
        subRequestFiles = oRequest.getSubRequestFiles( ind, 'logupload' )['Value']
        lfn = subRequestFiles[0]['LFN']
        gLogger.info( "Processing sub-request %s with execution order %d" % ( ind, subExecutionOrder ) )
        if subStatus == 'Waiting' and subExecutionOrder <= currentOrder:
          res = self.rm.replicate( lfn, targetSE )
          if res['OK']:
            gLogger.info( "Successfully uploaded %s to %s for job %s." % ( lfn, targetSE, jobID ) )
            oRequest.setSubRequestStatus( ind, 'logupload', 'Done' )
            gMonitor.addMark( "Successful", 1 )
            modified = True
            work_done = True
          else:
            gMonitor.addMark( "Failed", 1 )
            gLogger.error( "Failed to upload log file: ", res['Message'] )
        else:
          gLogger.info( "Sub-request %s is status '%s' and  not to be executed." % ( ind, subRequestAttributes['Status'] ) )

      ################################################
      #  Generate the new request string after operation
      requestString = oRequest.toXML()['Value']
      res = self.RequestDBClient.updateRequest( requestName, requestString, self.local )
      if res['OK']:
        gLogger.info( "JobLogUploadAgent.execute: Successfully updated request." )
      else:
        gLogger.error( "JobLogUploadAgent.execute: Failed to update request to ", self.local )

      if modified and jobID:
        result = self.finalizeRequest( requestName, jobID, self.local )

    return S_OK()
