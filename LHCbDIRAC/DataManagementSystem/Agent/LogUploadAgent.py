########################################################################
# $HeadURL $
# File: LogUploadAgent.py
########################################################################

"""  
    :mod: LogUplaodAgent
    ====================
 
    .. module: LogUplaodAgent
    :synopsis: LogUploadAgent uploads log and other auxilliary files of the given job
     to the long term log storage.
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

"""

__RCSID__ = "$Id$"

import os

from DIRAC  import gMonitor, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager

__RCSID__ = "$Id$"

AGENT_NAME = 'DataManagement/LogUploadAgent'

class LogUploadAgent( AgentModule ):
  """
  .. class:: LogUploadAgent
  
  :param RequestClient requestClient: RequestClient instance
  :param ReplicaManager replicaManager: ReplicaManager instance
  :param str local: local URL to RequestClient
  """
  requestClient = None
  replicaManager = None 
  local = None

  def initialize( self ):
    """ agent initalisation

    :param self: self reference
    """
    gMonitor.registerActivity( "Iteration", "Agent Loops", 
                               "JobLogUploadAgent", "Loops/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "Attempted", "Request Processed", 
                               "JobLogUploadAgent", "Requests/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "Successful", "Request Forward Successful", 
                               "JobLogUploadAgent", "Requests/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "Failed", "Request Forward Failed", 
                               "JobLogUploadAgent", "Requests/min", gMonitor.OP_SUM )

    self.requestClient = RequestClient()
    self.replicaManager = ReplicaManager()

    self.am_setOption( 'shifterProxy', 'ProductionManager' )
    self.local = PathFinder.getServiceURL( "RequestManagement/localURL" )
    if not self.local:
      errStr = 'The RequestManagement/localURL option must be defined.'
      self.log.fatal( errStr )
      return S_ERROR( errStr )
    return S_OK()

  def execute( self ):
    """ Takes the logupload requests and forwards to destination service.

    :param self: self reference
    """
    gMonitor.addMark( "Iteration", 1 )

    work_done = True

    while work_done:
      work_done = False
      res = self.requestClient.getRequest( 'logupload', url = self.local )
      if not res['OK']:
        self.log.error( "Failed to get request from database.", self.local )
        return S_OK()
      elif not res['Value']:
        self.log.info( "No requests to be executed found." )
        return S_OK()

      gMonitor.addMark( "Attempted", 1 )
      requestString = res['Value']['RequestString']
      requestName = res['Value']['RequestName']
      jobID = res['Value']['JobID']
      try:
        jobID = int( res['Value']['JobID'] )
      except (TypeError, ValueError):
        jobID = 0
      self.log.info( "Obtained request %s" % requestName )

      result = self.requestClient.getCurrentExecutionOrder( requestName, self.local )
      if result['OK']:
        currentOrder = result['Value']
      else:
        self.log.warn( 'Can not get the request execution order' )
        continue

      oRequest = RequestContainer( request = requestString )

      ################################################
      # Find the number of sub-requests from the request
      res = oRequest.getNumSubRequests( 'logupload' )
      if not res['OK']:
        errStr = "Failed to obtain number of logupload subrequests."
        self.log.error( errStr, res['Message'] )
        continue

      self.log.info( "Found %s subrequests for job %s" % ( res['Value'], jobID ) )
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
        self.log.info( "Processing subrequest %s with execution order %d" % ( ind, subExecutionOrder ) )
        if subStatus == 'Waiting' and subExecutionOrder <= currentOrder:
          destination = '/'.join( lfn.split( '/' )[0:-1] ) + \
          '/' + ( os.path.basename( lfn ) ).split( '_' )[1].split( '.' )[0]
          res = self.replicaManager.replicate( lfn, targetSE, destPath = destination )
          if res['OK']:
            self.log.info( "Successfully uploaded %s to %s for job %s." % ( lfn, targetSE, jobID ) )
            oRequest.setSubRequestStatus( ind, 'logupload', 'Done' )
            gMonitor.addMark( "Successful", 1 )
            modified = True
            work_done = True
          else:
            gMonitor.addMark( "Failed", 1 )
            self.log.error( "Failed to upload log file: ", res['Message'] )
        else:
          self.log.info( "Subrequest %s status is '%s', skipping." % ( ind, 
                                                                       subRequestAttributes['Status'] ) )

      ################################################
      #  Generate the new request string after operation
      requestString = oRequest.toXML()['Value']
      res = self.requestClient.updateRequest( requestName, requestString, self.local )
      if res['OK']:
        self.log.info( "Successfully updated request '%s' to %s." % ( requestName, self.local ) )
      else:
        self.log.error( "Failed to update request '%s' to %s: %s" % ( requestName, self.local, res["Message"] ) )
        return res
    
      if modified and jobID:
        result = self.requestClient.finalizeRequest( requestName, jobID, self.local )
        if not result["OK"]:
          self.log.error( "Failed to finalize request '%s': %s" % ( requestName, result["Message"] ) )
          return result
        else:
          self.log.info("Request '%s' has been finalised." % requestName )

    return S_OK()
