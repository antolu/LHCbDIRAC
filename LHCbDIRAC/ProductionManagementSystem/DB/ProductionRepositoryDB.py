# $Id: ProductionRepositoryDB.py,v 1.27 2007/11/16 18:54:49 gkuznets Exp $
"""
    DIRAC ProductionRepositoryDB class is a front-end to the pepository database containing
    Workflow (templates) Productions and vectors to create jobs.

    The following methods are provided for public usage:

    publishWorkflow()
    getWorkflow()
    getWorkglowsList()
    getWorkflowInfo()

"""
__RCSID__ = "$Revision: 1.27 $"

from DIRAC.Core.Base.DB import DB
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC  import gLogger, S_OK, S_ERROR
from DIRAC.Core.Workflow.WorkflowReader import *

class ProductionRepositoryDB(DB):

  def __init__( self, maxQueueSize=4 ):
    """ Constructor
    """

    DB.__init__(self,'ProductionRepositoryDB', 'ProductionManagement/ProductionRepositoryDB', maxQueueSize)


  def publishWorkflow(self, wf_type, wf_body, publisherDN, update=False):
    # KGG WE HAVE TO CHECK IS WORKFLOW EXISTS
    result = self.getWorkflowInfo(wf_type)
    #print "KGG", result
    if result['OK']:
      # workflow is not exists
      if result['Value'] == ():
        result = self._insert('Workflows', [ 'WFType', 'PublisherDN', 'Body' ], [wf_type, publisherDN, wf_body])
        if result['OK']:
          gLogger.verbose('Workflow Type "%s" published by DN="%s"' % (wf_type, publisherDN))
        else:
          error = 'Workflow Type "%s" FAILED to be published by DN="%s" with message "%s"' % (wf_type, publisherDN, result['Message'])
          gLogger.error(error)
          return S_ERROR( error )
      else: # workflow already exists
        if update: # we were asked to update
          result = self._escapeString( wf_body )
          if not result['OK']: return result
          wf_body_esc = result['Value']

          cmd = "UPDATE Workflows Set PublisherDN='%s', Body='%s' WHERE WFType='%s' " \
                % (publisherDN, wf_body_esc, wf_type)

          result = self._update( cmd )
          if result['OK']:
            gLogger.info( 'Workflow Type "%s" updated by DN="%s"' % (wf_type, publisherDN) )
          else:
            error = 'Workflow Type "%s" FAILED on update by DN="%s"' % (wf_type, publisherDN)
            gLogger.error( error )
            return S_ERROR( error )
        else: # update was not requested
          error = 'Workflow "%s" is exist in the repository, it was published by DN="%s"' % (wf_type, publisherDN)
          gLogger.error( error )
          return S_ERROR( error )
    else:
      error = 'Workflow Type "%s" FAILED to be published by DN="%s" with message "%s"' % (wf_type, publisherDN)
      gLogger.error( error )
      return S_ERROR( error )
    return result

  def getWorkflow(self, wf_type):
    cmd = "SELECT WFType, PublisherDN, PublishingTime, Body from Workflows WHERE WFType='%s'" % wf_type
    result = self._query(cmd)
    if result['OK']:
      return S_OK(result['Value'][0][3]) # we
    else:
      return S_ERROR("Failed to retrive Workflow with name '%s' " % wf_type)

  # KGG NEED TO BE INCORPORATED!!!!
  def _isWorkflowExists(self, wf_type):
    cmd = "SELECT  WFType from Workflows WHERE WFType='%s'" % wf_type
    result = self._query(cmd)
    if not result['OK']:
      gLogger.error("Failed to check if Workflow of type %s exists %s" % (wf_type, result['Message']))
      return False
    elif result['Value'] == () :
      gLogger.debug("No workflow with the name '%s' in the Production Repository" % wf_type)
      return False
    return True

  def _isProductionExists(self, pr_name):
    cmd = "SELECT PRName from Productions WHERE PRName='%s'" % pr_name
    result = self._query(cmd)
    if not result['OK']:
      gLogger.error("Failed to check if Production with name %s exists %s" % (pr_name, result['Message']))
      return False
    elif result['Value'] == () :
      gLogger.debug("No Production with the name '%s' in the Production Repository" % pr_name)
      return False
    return True

  def _isProductionIDExists(self, id):
    cmd = "SELECT ProductionID from Productions WHERE ProductionID='%d'" % id
    result = self._query(cmd)
    if not result['OK']:
      gLogger.error("Failed to check if Production with ID %d exists %s" % (id, result['Message']))
      return False
    elif result['Value'] == () :
      gLogger.debug("No Production with the ID '%d' in the Production Repository" % id)
      return False
    return True

  def deleteWorkflow(self, wf_type):
    cmd = "SELECT  WFType from Workflows WHERE WFType='%s'" % wf_type
    result = self._query(cmd)

    if not result['OK']:
      return S_ERROR("Failed to check if Workflow of type %s exists %s" % (wf_type, result['Message']))
    elif result['Value'] == () :
      return S_ERROR("No workflow with the name '%s' in the Production Repository" % wf_type)

    cmd = "DELETE FROM Workflows WHERE WFType=\'%s\'" % (wf_type)
    result = self._update(cmd)
    if not result['OK']:
      return S_ERROR("Failed to delete Workflow '%s' with the message %s" % (wf_type, result['Message']))
    return result

  def getListWorkflows(self):
    cmd = "SELECT  WFType, PublisherDN, PublishingTime from Workflows;"
    return self._query(cmd)

  def getWorkflowInfo(self, wf_type):
    cmd = "SELECT  WFType, PublisherDN, PublishingTime from Workflows WHERE WFType='%s'" % wf_type
    result = self._query(cmd)
    if result['OK']:
      return result
    else:
      return S_ERROR('Failed to retrive Workflow with the name=%s' % (wf_type, result['Message']) )

  def getProductionInfo(self, pr_name):
    cmd = "SELECT  ProductionID, PRName, Status, PRParent, JobsTotal, JobsSubmitted, LastSubmittedJob, PublishingTime, PublisherDN, Comment from Productions WHERE PRName='%s'" % pr_name
    result = self._query(cmd)
    if result['OK']:
      return result
    else:
      return S_ERROR('Failed to retrive Production with the name=%s message=%s' % (pr_name, result['Message']))

  def publishProduction(self, pr_name, pr_parent, pr_comment, pr_body, publisherDN, update=False):
    # KGG WE HAVE TO CHECK IS WORKFLOW EXISTS
    if not self._isProductionExists(pr_name): # workflow is not exists
        result = self._insert('Productions', [ 'PRName','PRParent','PublisherDN','Status','Comment', 'Body' ], \
                              [pr_name, pr_parent, publisherDN, 'NEW', pr_comment, pr_body])
        if result['OK']:
          gLogger.info('Production "%s" published by DN="%s"' % (pr_name, publisherDN))
          return result
        else:
          error = 'Production "%s" FAILED to be published by DN="%s" with message "%s"' % (pr_name, publisherDN, result['Message'])
          gLogger.error(error)
          return S_ERROR( error )
    else: # workflow already exists
        if update: # we were asked to update
          result = self._escapeString( pr_body )
          if not result['OK']: return result
          pr_body_esc = result['Value']

          cmd = "UPDATE Productions Set PRParent='%s', PublisherDN='%s', Comment='%s', Body='%s' WHERE PRName='%s' " \
                % (pr_parent, publisherDN, pr_comment, pr_body_esc, pr_name)

          result = self._update( cmd )
          if result['OK']:
            gLogger.info( 'Production "%s" updated by DN="%s"' % (pr_name, publisherDN) )
            return result
          else:
            error = 'Production "%s" FAILED on update by DN="%s" with the message="s"' % (pr_name, publisherDN, result['Message'])
            gLogger.error( error )
            return S_ERROR( error )
        else: # update was not requested
          error = 'Production "%s" is exist in the repository, it was published by DN="%s"' % (pr_name, publisherDN)
          gLogger.error( error )
          return S_ERROR( error )

  def getListProductions(self):
    cmd = "SELECT  ProductionID, PRName, PRParent, PublisherDN, PublishingTime, JobsTotal, JobsSubmitted, LastSubmittedJob,  Status, Comment from Productions;"
    result = self._query(cmd)
    if result['OK']:
      newres=[] # repacking
      for pr in result['Value']:
        #newres.append(dict.fromkeys(('ProductionID', 'PRName', 'PRParent','PublisherDN', 'PublishingTime',
        #                    'JobsTotal', 'JobsSubmitted', 'LastSubmittedJob', 'Status', 'Comment'),pr))
        newres.append({'ProductionID':pr[0], 'PRName':pr[1], 'PRParent':pr[2],'PublisherDN':pr[3], 'PublishingTime':pr[4],
                            'JobsTotal':pr[5], 'JobsSubmitted':pr[6], 'LastSubmittedJob':pr[7], 'Status':pr[8], 'Comment':pr[9]})
      return S_OK(newres)
    return result

  def deleteProduction(self, pr_name):
    if self._isProductionExists(pr_name):
      cmd = "DELETE FROM Productions WHERE PRName=\'%s\'" % (pr_name)
      result = self._update(cmd)
      if not result['OK']:
        return S_ERROR("Failed to delete Production '%s' with the message %s" % (pr_name, result['Message']))
      return result
    return S_ERROR("No production with the name '%s' in the Production Repository" % pr_name)

  def deleteProductionID(self, id):
    if self._isProductionIDExists(id):
      cmd = "DELETE FROM Productions WHERE ProductionID=\'%d\'" % (id)
      result = self._update(cmd)
      if not result['OK']:
        return S_ERROR("Failed to delete Production with ID '%d' with the message %s" % (id, result['Message']))
      return result
    return S_ERROR("No production with the ID '%d' in the Production Repository" % id)

  def getProductionID(self, id):
    cmd = "SELECT Body from Productions WHERE ProductionID='%d'" % id
    result = self._query(cmd)
    if result['OK']:
      return S_OK(result['Value'][0][0]) # we
    else:
      return S_ERROR("Failed to retrive Production with ID '%d' " % id)

  def getProduction(self, pr_name):
    cmd = "SELECT Body from Productions WHERE PRName='%s'" % pr_name
    result = self._query(cmd)
    if result['OK']:
      return S_OK(result['Value'][0][0]) # we
    else:
      return S_ERROR("Failed to retrive Production with name '%s' " % pr_name)
