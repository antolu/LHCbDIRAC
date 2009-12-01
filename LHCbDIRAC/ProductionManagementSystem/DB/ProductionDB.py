# $Id$

""" DIRAC ProductionDB class is a front-end to the LHCb production specific database """

__RCSID__ = "$Revision: 1.65 $"

from DIRAC                                                  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.DB                                     import DB
from LHCbDIRAC.TransformationSystem.DB.TransformationDB     import TransformationDB

class ProductionDB(TransformationDB):

  def __init__( self, maxQueueSize=10 ):
    """ Constructor
    """
    TransformationDB.__init__(self,'ProductionDB', 'ProductionManagement/ProductionDB', maxQueueSize)

################ WORKFLOW SECTION ####################################

  def publishWorkflow(self, wf_name, wf_parent, wf_descr, wf_descr_long, wf_body, authorDN, authorGroup, update=False):

    if not self._isWorkflowExists(wf_name):
      # workflow is not exists
      result = self._insert('Workflows', [ 'WFName', 'WFParent', 'Description', 'LongDescription','AuthorDN', 'AuthorGroup', 'Body' ],
                            [wf_name, wf_parent, wf_descr, wf_descr_long, authorDN, authorGroup, wf_body])
      if result['OK']:
        gLogger.verbose('Workflow "%s" published by DN="%s"' % (wf_name, authorDN))
      else:
        error = 'Workflow "%s" FAILED to be published by DN="%s" with message "%s"' % (wf_name, authorDN, result['Message'])
        gLogger.error(error)
        return S_ERROR( error )
    else: # workflow already exists
      if update: # we were asked to update

        # KGG tentative code !!!! have to check (seems working)
        result = self._escapeString( wf_body )
        if not result['OK']: return result
        wf_body_esc = result['Value'][1:len(result['Value'])-1] # we have to remove trailing " left by self._escapeString()
        result = self._escapeString( wf_descr_long )
        if not result['OK']: return result
        wf_descr_long_esc = result['Value'][1:len(result['Value'])-1] # we have to remove trailing " left by self._escapeString()
        # end of tentative code

        cmd = "UPDATE Workflows Set WFParent='%s', Description='%s', LongDescription='%s', AuthorDN='%s', AuthorGroup='%s', Body='%s' WHERE WFName='%s' " \
              % (wf_parent, wf_descr, wf_descr_long_esc, authorDN, authorGroup, wf_body_esc, wf_name)

        result = self._update( cmd )
        if result['OK']:
          gLogger.verbose( 'Workflow "%s" updated by DN="%s"' % (wf_name, authorDN) )
        else:
          error = 'Workflow "%s" FAILED on update by DN="%s"' % (wf_name, authorDN)
          gLogger.error( error )
          return S_ERROR( error )
      else: # update was not requested
        error = 'Workflow "%s" is exist in the repository, it was published by DN="%s"' % (wf_name, authorDN)
        gLogger.error( error )
        return S_ERROR( error )
    return result

  def getListWorkflows(self):
    cmd = "SELECT  WFName, WFParent, Description, LongDescription, AuthorDN, AuthorGroup, PublishingTime from Workflows;"
    result = self._query(cmd)
    if result['OK']:
      newres=[] # repacking
      for pr in result['Value']:
        newres.append({'WFName':pr[0], 'WFParent':pr[1], 'Description':pr[2], 'LongDescription':pr[3],
                       'AuthorDN':pr[4], 'AuthorGroup':pr[5], 'PublishingTime':pr[6].isoformat(' ')})
      return S_OK(newres)
    return result

  def getWorkflow(self, name):
    cmd = "SELECT Body from Workflows WHERE WFName='%s'" % name
    result = self._query(cmd)
    if result['OK']:
      return S_OK(result['Value'][0][0]) # we
    else:
      return S_ERROR("Failed to retrive Workflow with name '%s' " % name)

  def _isWorkflowExists(self, name):
    cmd = "SELECT COUNT(*) from Workflows WHERE WFName='%s'" % name
    result = self._query(cmd)
    if not result['OK']:
      gLogger.error("Failed to check if Workflow of name %s exists %s" % (name, result['Message']))
      return False
    elif result['Value'][0][0] > 0:
        return True
    return False

  def deleteWorkflow(self, name):
    if not self._isWorkflowExists(name):
      return S_ERROR("There is no Workflow with the name '%s' in the repository" % (name))

    cmd = "DELETE FROM Workflows WHERE WFName=\'%s\'" % (name)
    result = self._update(cmd)
    if not result['OK']:
      return S_ERROR("Failed to delete Workflow '%s' with the message %s" % (name, result['Message']))
    return result

  def getWorkflowInfo(self, name):
    cmd = "SELECT  WFName, WFParent, Description, LongDescription, AuthorDN, AuthorGroup, PublishingTime from Workflows WHERE WFname='%s'" % name
    result = self._query(cmd)
    if result['OK']:
      pr=result['Value'][0]

      newres = {'WFName':pr[0], 'WFParent':pr[1], 'Description':pr[2], 'LongDescription':pr[3],
                'AuthorDN':pr[4], 'AuthorGroup':pr[5], 'PublishingTime':pr[6].isoformat(' ')}
      return S_OK(newres)
    else:
      return S_ERROR('Failed to retrive Workflow with the name=%s' % (name, result['Message']) )

######################## Web monitoring ############################

  def getJobStats(self,productionID):
    """ Returns dictionary with number of jobs per status in the given production. 
        The status is one of the following:
        Created - this counter returns the total number of jobs already created;
        Submitted - jobs submitted to the WMS;
        Waiting - jobs being checked or waiting for execution in the WMS;
        Running - jobs being executed in the WMS;
        Stalled - jobs stalled in the WMS;
        Done - jobs completed in the WMS;
        Failed - jobs completed with errors in the WMS;
    """
    res  = self.__getTransformationID(productionID)
    if not res['OK']:
      gLogger.error("Failed to get ID for transformation",res['Message'])
      return res
    productionID = res['Value']
    if productionID:
      resultStats = self.getCounters('Jobs',['WmsStatus'],{'TransformationID':productionID}) 
    else:
      resultStats = self.getCounters('Jobs',['WmsStatus','TransformationID'],{})   
    if not resultStats['OK']:
      return resultStats
    if not resultStats['Value']:
      return S_ERROR('No records found')
    statusList = {}
    for s in ['Created','Submitted','Waiting','Running','Stalled','Done','Failed']:
      statusList[s] = 0
    for attrDict,count in resultStats['Value']:
      status = attrDict['WmsStatus']
      stList = ['Created']
      # Choose the status to report
      if not status == "Created" and not status == "Reserved":
        if not "Submitted" in stList:
          stList.append("Submitted")
      if status == "Waiting" or status == "Received" or status == "Checking" or \
         status == "Matched" or status == "Submitted" or status == "Staging":
        if not "Waiting" in stList:
          stList.append("Waiting")
      if status == "Done" or status == "Completed":
        if not "Done" in stList:
          stList.append("Done")
      if status == "Failed" or status == "Stalled" or status == "Running":
        stList.append(status)
      for st in stList:
        statusList[st] += count
    return S_OK(statusList)
