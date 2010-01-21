########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/ProductionManagementSystem/Agent/ReplicationSubmissionAgent.py $
########################################################################

"""  The Replication Submission Agent takes replication tasks created in the transformation database and submits the replication requests to the transfer management system. """

__RCSID__ = "$Id: ReplicationSubmissionAgent.py 20001 2010-01-20 12:47:38Z acsmith $"

from DIRAC                                                          import S_OK, S_ERROR, gConfig, gMonitor, gLogger, rootPath
from DIRAC.Core.Base.AgentModule                                    import AgentModule
from DIRAC.Core.DISET.RPCClient                                     import RPCClient
from LHCbDIRAC.ProductionManagementSystem.Client.ProductionClient   import ProductionClient
from DIRAC.RequestManagementSystem.Client.RequestContainer          import RequestContainer
from DIRAC.RequestManagementSystem.Client.RequestClient             import RequestClient
import os, time, string, datetime, re

#from LHCbDIRAC.Interfaces.API.DiracProduction             import DiracProduction

AGENT_NAME = 'ProductionManagement/ReplicationSubmissionAgent'

class ReplicationSubmissionAgent(AgentModule):

  #############################################################################
  def initialize(self):
    """Sets defaults
    """
    self.am_setModuleParam('shifter','ProductionManager')
    self.am_setModuleParam("shifterProxyLocation","%s/runit/%s/proxy" % (rootPath,AGENT_NAME))
    gMonitor.registerActivity("SubmittedTasks","Automatically submitted tasks","Transformation Monitoring","Tasks", gMonitor.OP_ACUM)

    self.transClient = ProductionClient()
    self.requestClient = RequestClient()
    return S_OK()

  #############################################################################
  def execute(self):
    """The ReplicationSubmissionAgent execution method. """

    # Determine whether the agent is to be executed
    enableFlag = self.am_getOption('EnableFlag','True')
    if not enableFlag == 'True':
      gLogger.info("%s.execute: Agent is disabled by configuration option %s/EnableFlag" % (AGENT_NAME,self.section))
      return S_OK()

    # Determine whether to update tasks stuck in Reserved status
    checkReserved = self.am_getOption('CheckReservedTasks','True')
    if checkReserved == 'True':
      res = self.checkReservedTasks()
      if not res['OK']:
        gLogger.warn('%s.execute: Failed to check Reserved tasks' % AGENT_NAME, res['Message'])

    # Obtain the transformations to be submitted
    submitType = self.am_getOption('TransformationType',['Replication'])
    submitStatus = self.am_getOption('SubmitStatus',['Active'])
    submitAgentType = self.am_getOption('SubmitAgentType',['Automatic'])
    selectCond = {'Type' : submitType, 'Status' : submitStatus, 'AgentType' : submitAgentType}
    res = self.transClient.getTransformations(condDict=selectCond)
    if not res['OK']:
      gLogger.error("%s.execute: Failed to get transformations for submission." % AGENT_NAME,res['Message'])
      return res
    if not res['Value']:
      gLogger.info("%s.execute: No transformations found for submission." % AGENT_NAME)
      return res

    for transformation in res['Value']:
      transID = transformation['TransformationID']
      gLogger.info("%s.execute: Attempting to submit tasks for transformation %d" % (AGENT_NAME,transID))
      res = self.submitTasks(transID)
      if not res['OK']:
        gLogger.error("%s.execute: Failed to submit tasks for transformation" % AGENT_NAME,transID)
    return S_OK()

  def submitTasks(self,transID):
    tasksPerLoop = self.am_getOption('TasksPerLoop',50)
    res = self.transClient.getTasksToSubmit(transID,tasksPerLoop)
    if not res['OK']:
      gLogger.error("%s.submitTasks: Failed to obtain tasks from transformation database" % AGENT_NAME, res['Message'])
      return res
    tasks = res['Value']['JobDictionary']
    if not tasks:
      gLogger.info("%s.submitTasks: No tasks found for submission" % AGENT_NAME)
      return S_OK()
    submitted = 0
    startTime = time.time()
    for taskID, taskDict in tasks.items():
      oRequest = RequestContainer(init=False)
      subRequestIndex = oRequest.initiateSubRequest('transfer')['Value']
      attributeDict = {'Operation':'replicateAndRegister','TargetSE':taskDict['TargetSE']}
      oRequest.setSubRequestAttributes(subRequestIndex,'transfer',attributeDict)
      files = []
      for lfn in taskDict['InputData'].split(';'):
        files.append({'LFN':lfn})     
      oRequest.setSubRequestFiles(subRequestIndex,'transfer',files)
      requestName = str(transID).zfill(8)+'_'+str(taskID).zfill(8)
      requestString = oRequest.toXML()['Value']
      res = self.requestClient.setRequest(requestName,requestString)
      if not res['OK']:
        gLogger.error("%s.submitTasks: Failed to set replication request" % AGENT_NAME, "%s %s" % (requestName, res['Message']))
        gLogger.debug("%s.submitTasks: %s" % (AGENT_NAME,requestString))
        res = self.transClient.setTaskStatus(transID,taskID,'Created')
        if not res['OK']:
          gLogger.warn("%s.submitTasks: Failed to update task status after submission failure" % AGENT_NAME, "%s %s" % (requestName,res['Message']))
      else:
        requestID = res['Value']
        gLogger.verbose("%s.submitTasks: Successfully set replication request" % AGENT_NAME, requestName)
        res = self.transClient.setTaskStatusAndWmsID(transID,taskID,'Submitted',str(requestID))
        if not res['OK']:
          gLogger.warn("%s.submitTasks: Failed to update task status after submission" % AGENT_NAME, "%s %s" % (requestName,res['Message']))
        gMonitor.addMark("SubmittedTasks",1)
        submitted+=1
    gLogger.info('%s.submitTasks: Transformation %d submission time: %.2f seconds for %d tasks' % (AGENT_NAME,transID,time.time()-startTime,submitted))
    return S_OK()

  def checkReservedTasks(self):
    """ Check if there are tasks in Reserved state for more than an hour, verify that there were not submitted and reset them to Created """
    gLogger.info("%s.checkReservedTasks: Checking Reserved tasks" % AGENT_NAME)

    # Get the transformations which should be checked
    submitType = self.am_getOption('TransformationType',['Replication'])
    submitStatus = self.am_getOption('SubmitStatus',['Active','Stopped'])
    selectCond = {'Type' : submitType, 'Status' : submitStatus}
    res = self.transClient.getTransformations(condDict=selectCond)
    if not res['OK']:
      gLogger.error("%s.checkReservedTasks: Failed to get transformations." % AGENT_NAME,res['Message'])
      return res
    if not res['Value']:
      gLogger.info("%s.checkReservedTasks: No transformations found." % AGENT_NAME)
      return res
    transIDs = []
    for transformation in res['Value']:
      transIDs.append(transformation['TransformationID'])

    # Select the tasks which have been in Reserved status for more than 1 hour for selected transformations
    condDict = {"TransformationID":transIDs,"WmsStatus":'Reserved'}
    time_stamp_older = str(datetime.datetime.utcnow() - datetime.timedelta(hours=1))
    time_stamp_newer = str(datetime.datetime.utcnow() - datetime.timedelta(days=7))
    res = self.transClient.getTransformationTasks(condDict=condDict,older=time_stamp_older,newer=time_stamp_newer, timeStamp='LastUpdateTime')
    if not res['OK']:
      gLogger.error("%s.checkReservedTasks: Failed to get Reserved tasks" % AGENT_NAME, res['Message'])
      return S_OK()
    if not res['Value']:
      gLogger.info("%s.checkReservedTasks: No Reserved tasks found" % AGENT_NAME)
      return S_OK()
    taskNameList = []
    for taskDict in res['Value']:
      transID = taskDict['TransformationID']
      taskID = taskDict['JobID']
      taskName = str(transID).zfill(8)+'_'+str(taskID).zfill(8)
      taskNameList.append(taskName)

    # Determine the requestID for the Reserved tasks from the request names
    taskNameIDs = {}
    noTasks = []
    for taskName in taskNameList:
      res = self.requestClient.getRequestInfo(taskName,'RequestManagement/RequestManager')
      if res['OK']:
        taskNameIDs[taskName] = res['Value'][0]
      elif re.search("Failed to retreive RequestID for Request", res['Message']):
        noTasks.append(taskName)
      else:
        gLogger.warn("Failed to get requestID for request", res['Message'])

    # For the tasks with no assoicated request found re-set the status of the task in the transformationDB
    for taskName in noTasks:
      transID,taskID = taskName.split('_')
      gLogger.info("%s.checkReservedTasks: Resetting status of %s to Reserved as no associated task found" % (AGENT_NAME,taskName))
      res = self.transClient.setTaskStatus(int(transID),int(taskID),'Created')
      if not res['OK']:
        gLogger.warn("%s.checkReservedTasks: Failed to update task status and ID after recovery" % AGENT_NAME, "%s %s" % (taskName,res['Message']))

    # For the tasks for which an assoicated request was found update the task details in the transformationDB
    for taskName,extTaskID in taskNameIDs.items():
      transID,taskID = taskName.split('_')
      gLogger.info("%s.checkReservedTasks: Resetting status of %s to Created with ID %s" % (AGENT_NAME,taskName,extTaskID))
      res = self.transClient.setTaskStatusAndWmsID(int(transID),int(taskID),'Submitted',str(extTaskID))
      if not res['OK']:
        gLogger.warn("%s.checkReservedTasks: Failed to update task status and ID after recovery" % AGENT_NAME, "%s %s" % (taskName,res['Message']))

    return S_OK()
