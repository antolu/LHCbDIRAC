# $Id: ProductionRepositoryDB.py,v 1.32 2008/01/22 14:15:12 gkuznets Exp $
"""
    DIRAC ProductionRepositoryDB class is a front-end to the pepository database containing
    Workflow (templates) Productions and vectors to create jobs.

    The following methods are provided for public usage:

    publishWorkflow()
    getWorkflow()
    getWorkglowsList()
    getWorkflowInfo()

"""
__RCSID__ = "$Revision: 1.32 $"

from DIRAC.Core.Base.DB import DB
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC  import gLogger, S_OK, S_ERROR
from DIRAC.Core.Workflow.WorkflowReader import *

class ProductionRepositoryDB(DB):

  def __init__( self, maxQueueSize=4 ):
    """ Constructor
    """

    DB.__init__(self,'ProductionRepositoryDB', 'ProductionManagement/ProductionRepositoryDB', maxQueueSize)

################ WORKFLOW SECTION ####################################

  def submitWorkflow(self, wf_name, wf_parent, wf_descr, wf_body, publisherDN, update=False):

    if not self._isWorkflowExists(wf_name):
      # workflow is not exists
      result = self._insert('Workflows', [ 'WFName', 'WFParent', 'Description', 'PublisherDN', 'Body' ], [wf_name, wf_parent, wf_descr, publisherDN, wf_body])
      if result['OK']:
        gLogger.verbose('Workflow "%s" published by DN="%s"' % (wf_name, publisherDN))
      else:
        error = 'Workflow "%s" FAILED to be published by DN="%s" with message "%s"' % (wf_name, publisherDN, result['Message'])
        gLogger.error(error)
        return S_ERROR( error )
    else: # workflow already exists
      if update: # we were asked to update
        result = self._escapeString( wf_body )
        if not result['OK']: return result
        wf_body_esc = result['Value'][1:len(result['Value'])-1] # we have to remove trailing " left by self._escapeString()

        cmd = "UPDATE Workflows Set WFParent='%s', Description='%s', PublisherDN='%s', Body='%s' WHERE WFName='%s' " \
              % (wf_parent, wf_descr, publisherDN, wf_body_esc, wf_name)

        result = self._update( cmd )
        if result['OK']:
          gLogger.verbose( 'Workflow "%s" updated by DN="%s"' % (wf_name, publisherDN) )
        else:
          error = 'Workflow "%s" FAILED on update by DN="%s"' % (wf_name, publisherDN)
          gLogger.error( error )
          return S_ERROR( error )
      else: # update was not requested
        error = 'Workflow "%s" is exist in the repository, it was published by DN="%s"' % (wf_name, publisherDN)
        gLogger.error( error )
        return S_ERROR( error )
    return result

  def getListWorkflows(self):
    cmd = "SELECT  WFName, WFParent, Description, PublisherDN, PublishingTime from Workflows;"
    result = self._query(cmd)
    if result['OK']:
      newres=[] # repacking
      for pr in result['Value']:
        newres.append({'WFName':pr[0], 'WFParent':pr[1], 'Description':pr[2], 'PublisherDN':pr[3], 'PublishingTime':pr[4].isoformat(' ')})
      return S_OK(newres)
    return result


  def getWorkflow(self, wf_name):
    cmd = "SELECT Body from Workflows WHERE WFName='%s'" % wf_name
    result = self._query(cmd)
    if result['OK']:
      return S_OK(result['Value'][0][0]) # we
    else:
      return S_ERROR("Failed to retrive Workflow with name '%s' " % wf_name)

  # KGG NEED TO BE INCORPORATED!!!!
  def _isWorkflowExists(self, wf_name):
    cmd = "SELECT COUNT(*) from Workflows WHERE WFName='%s'" % wf_name
    result = self._query(cmd)
    if not result['OK']:
      gLogger.error("Failed to check if Workflow of name %s exists %s" % (wf_name, result['Message']))
      return False
    elif result['Value'][0][0] > 0:
        return True
    return False

  def deleteWorkflow(self, wf_name):

    if not self._isWorkflowExists(wf_name):
      return S_ERROR("There is no Workflow with the name '%s' in the repository" % (wf_name))

    cmd = "DELETE FROM Workflows WHERE WFName=\'%s\'" % (wf_name)
    result = self._update(cmd)
    if not result['OK']:
      return S_ERROR("Failed to delete Workflow '%s' with the message %s" % (wf_name, result['Message']))
    return result

  def getWorkflowInfo(self, wf_name):
    cmd = "SELECT  WFName, WFParent, Description, PublisherDN, PublishingTime from Workflows WHERE WFname='%s'" % wf_name
    result = self._query(cmd)
    if result['OK']:
      pr=result['Value']
      newres = {'WFName':pr[0], 'WFParent':pr[1], 'Description':pr[2], 'PublisherDN':pr[3], 'PublishingTime':pr[4].isoformat(' ')}
      return S_OK(newres)
    else:
      return S_ERROR('Failed to retrive Workflow with the name=%s' % (wf_name, result['Message']) )


################ PRODUCTION SECTION ####################################

  def _isProductionExists(self, pr_name):
    cmd = "SELECT COUNT(*) from Productions WHERE PRName='%s'" % pr_name
    result = self._query(cmd)
    if not result['OK']:
      gLogger.error("Failed to check if Production with name %s exists %s" % (pr_name, result['Message']))
      return False
    elif result['Value'][0][0] > 0:
        return True
    return False

  def _isProductionIDExists(self, id):
    cmd = "SELECT COUNT(*) from Productions WHERE ProductionID='%d'" % id
    result = self._query(cmd)
    if not result['OK']:
      gLogger.error("Failed to check if Production with ID %d exists %s" % (id, result['Message']))
      return False
    elif result['Value'][0][0] > 0:
        return True
    return False

  def submitProduction(self, pr_name, pr_parent, pr_description, pr_body, publisherDN, update=False):
    # KGG WE HAVE TO CHECK IS WORKFLOW EXISTS
    if not self._isProductionExists(pr_name): # workflow is not exists
        #result = self._escapeString( pr_body )
        #if not result['OK']: return result
        #pr_body_esc = result['Value'][1:len(result['Value'])-1] # we have to remove trailing " left by self._escapeString()
        result = self._insert('Productions', [ 'PRName','PRParent','PublisherDN','Status','Description', 'Body' ], \
                              [pr_name, pr_parent, publisherDN, 'NEW', pr_description, pr_body])
        if result['OK']:
          gLogger.info('Production "%s" published by DN="%s"' % (pr_name, publisherDN))
          return result
        else:
          error = 'Production "%s" FAILED to be published by DN="%s" with message "%s"' % (pr_name, publisherDN, result['Message'])
          gLogger.error(error)
          return S_ERROR( error )
    else: # workflow already exists
        if update: # we were asked to update
          #result = self._escapeString( pr_body )
          #if not result['OK']: return result
          #pr_body_esc = result['Value'][1:len(result['Value'])-1] # we have to remove trailing " left by self._escapeString()

          cmd = "UPDATE Productions Set PRParent='%s', PublisherDN='%s', Description='%s', Body='%s' WHERE PRName='%s' " \
                % (pr_parent, publisherDN, pr_description, pr_body, pr_name)

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
    cmd = "SELECT  ProductionID, PRName, PRParent, PublisherDN, PublishingTime, JobsTotal, JobsSubmitted, LastSubmittedJob,  Status, Description from Productions;"
    result = self._query(cmd)
    if result['OK']:
      newres=[] # repacking
      for pr in result['Value']:
        #newres.append(dict.fromkeys(('ProductionID', 'PRName', 'PRParent','PublisherDN', 'PublishingTime',
        #                    'JobsTotal', 'JobsSubmitted', 'LastSubmittedJob', 'Status', 'Description'),pr))
        newres.append({'ProductionID':pr[0], 'PRName':pr[1], 'PRParent':pr[2],'PublisherDN':pr[3], 'PublishingTime':pr[4].isoformat(' '),
                            'JobsTotal':pr[5], 'JobsSubmitted':pr[6], 'LastSubmittedJob':pr[7], 'Status':pr[8], 'Description':pr[9]})
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

  def getProductionInfo(self, pr_name):
    cmd = "SELECT  ProductionID, PRName, Status, PRParent, JobsTotal, JobsSubmitted, LastSubmittedJob, PublishingTime, PublisherDN, Description from Productions WHERE PRName='%s'" % pr_name
    result = self._query(cmd)
    if result['OK']:
      pr=result['Value'][0]
      print "KGG", pr
      newres = {'ProductionID':pr[0], 'PRName':pr[1], 'PRParent':pr[3],'PublisherDN':pr[8], 'PublishingTime':pr[7].isoformat(' '),
                            'JobsTotal':pr[4], 'JobsSubmitted':pr[5], 'LastSubmittedJob':pr[6], 'Status':pr[2], 'Description':pr[9]}
      return S_OK(newres)
    else:
      return S_ERROR('Failed to retrive Production with the name=%s message=%s' % (pr_name, result['Message']))

  def getProductionInfoID(self, id):
    cmd = "SELECT  ProductionID, PRName, Status, PRParent, JobsTotal, JobsSubmitted, LastSubmittedJob, PublishingTime, PublisherDN, Description from Productions WHERE ProductionID='%s'" % id
    result = self._query(cmd)
    print "KGG", result
    if result['OK']:
      pr=result['Value'][0]
      newres = {'ProductionID':pr[0], 'PRName':pr[1], 'PRParent':pr[3],'PublisherDN':pr[8], 'PublishingTime':pr[7].isoformat(' '),
                            'JobsTotal':pr[4], 'JobsSubmitted':pr[5], 'LastSubmittedJob':pr[6], 'Status':pr[2], 'Description':pr[9]}
      return S_OK(newres)
    else:
      return S_ERROR('Failed to retrive Production with the id=%s message=%s' % (id, result['Message']))
