# $Id: ProductionRepositoryDB.py,v 1.26 2007/11/13 20:35:05 gkuznets Exp $
"""
    DIRAC ProductionRepositoryDB class is a front-end to the pepository database containing
    Workflow (templates) Productions and vectors to create jobs.

    The following methods are provided for public usage:

    publishWorkflow()
    getWorkflow()
    getWorkglowsList()
    getWorkflowInfo()

"""
__RCSID__ = "$Revision: 1.26 $"

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
  def isWorkflowExists(selfself, wf_type):
    cmd = "SELECT  WFType from Workflows WHERE WFType='%s'" % wf_type
    result = self._query(cmd)
    if not result['OK']:
      return S_ERROR("Failed to check if Workflow of type %s exists %s" % (wf_type, result['Message']))
    elif result['Value'] == () :
      return S_ERROR("No workflow with the name '%s' in the Production Repository" % wf_type)


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
    #KGG we need to adjust code for the empty list!!!!
    cmd = "SELECT  WFType, PublisherDN, PublishingTime from Workflows;"
    result = self._query(cmd)
    return result

  def getWorkflowInfo(self, wf_type):
    cmd = "SELECT  WFType, PublisherDN, PublishingTime from Workflows WHERE WFType='%s'" % wf_type
    result = self._query(cmd)
    if result['OK']:
      return result
    else:
      return S_ERROR('Failed to retrive Workflow with the name '+wf_type)

  def getProductionInfo(self, pr_name):
    cmd = "SELECT  PRName, PRParent, PublisherDN, PublishingTime, JobsTotal, JobsSubmitted, LastSubmittedJob, Status, Comment from Productions WHERE PRName='%s'" % pr_name
    result = self._query(cmd)
    if result['OK']:
      return result
    else:
      return S_ERROR('Failed to retrive Production with the name ' + pr_name)

  def publishProduction(self, pr_name, pr_parent, pr_comment, pr_body, publisherDN, update=False):
    # KGG WE HAVE TO CHECK IS WORKFLOW EXISTS
    result = self.getProductionInfo(pr_name)
    #print "KGG", result
    if result['OK']:
      # workflow is not exists
      if result['Value'] == ():
        result = self._insert('Productions', [ 'PRName','PRParent','PublisherDN','Status','Comment', 'Body' ], \
                              [pr_name, pr_parent, publisherDN, 'NEW', pr_comment, pr_body])
        if result['OK']:
          gLogger.info('Production "%s" published by DN="%s"' % (pr_name, publisherDN))
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
          else:
            error = 'Production "%s" FAILED on update by DN="%s"' % (pr_name, publisherDN)
            gLogger.error( error )
            return S_ERROR( error )
        else: # update was not requested
          error = 'Production "%s" is exist in the repository, it was published by DN="%s"' % (pr_name, publisherDN)
          gLogger.error( error )
          return S_ERROR( error )
    else:
      error = 'Production "%s" FAILED to be published by DN="%s" with message "%s"' % (pr_name, publisherDN)
      gLogger.error( error )
      return S_ERROR( error )
    return result

  def getListProductions(self):
    #KGG we need to adjust code for the empty list!!!!
    cmd = "SELECT  ProductionID, PRName, Status, PRParent, JobsTotal, JobsSubmitted, LastSubmittedJob, PublishingTime, PublisherDN, Comment from Productions;"
    result = self._query(cmd)
    return result

