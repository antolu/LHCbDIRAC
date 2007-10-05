# $Id: ProductionRepositoryDB.py,v 1.24 2007/10/05 14:39:37 gkuznets Exp $
"""
    DIRAC ProductionRepositoryDB class is a front-end to the pepository database containing
    Workflow (templates) Productions and vectors to create jobs.

    The following methods are provided for public usage:

    publishWorkflow()
    getWorkflow()
    getWorkglowsList()
    getWorkflowInfo()

"""
__RCSID__ = "$Revision: 1.24 $"

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
    result = self._insert('Workflows', [ 'WFType', 'PublisherDN', 'Body' ], [wf_type, publisherDN, wf_body])
    if result['OK']:
      gLogger.verbose('Workflow Type "%s" published by DN="%s"' % (wf_type, publisherDN))
    else:
      errKey = 'Workflow Type "%s" FAILED to be published by DN="%s"' % (wf_type, publisherDN)
      errExpl = " because %s" % (result['Message'])
      gLogger.error(errKey, errExpl)
    return result

  def publishWorkflow_(self, wf_type, wf_body, publisherDN, update=False):
    # KGG WE HAVE TO CHECK IS WORKFLOW EXISTS
    #result = self.getWorkflowInfo(wf_type)
    #print "KGG", result
    if result['OK']:
      # workflow already exists
      if result['Value'] == ():
        # it is a new workflow
        #cmd = "INSERT INTO `Workflows` ( `WFType`, `PublisherDN`, `PublishingTime`, `Body` ) VALUES ( 'TotalSumm', '/C=UK/O=eScience/OU=CLRC/L=RAL/CN=gennady kuznetsov', 'NEW()', '<Workflow>'"
        #cmd = 'INSERT INTO Workflows ( WFType, PublisherDN, PublishingTime, Body ) VALUES ' \
        #        '(\'%s\', \'%s\', NOW(), \'%s\')' % (wf_type, publisherDN, wf_body)

        result = self._insert('Workflows', [ 'WFType', 'PublisherDN', 'Body' ], [wf_type, publisherDN, wf_body])
        #result = self._update(cmd)
        if result['OK']:
          self.log.info( 'Workflow Type "%s" published by DN="%s"' % (wf_type, publisherDN) )
        else:
          error = 'Workflow Type "%s" FAILED to be published by DN="%s"' % (wf_type, publisherDN)
          self.log.error( error )
          return S_ERROR( error )
      else:
        if update: # we were asked to update
          cmd = "UPDATE Workflows set PublisherDN='%s', Body='%s' WHERE WFType='%s'" \
                % (publisherDN, wf_body, wf_type)
          result = self._update( cmd )
          if result['OK']:
            self.log.info( 'Workflow Type "%s" updated by DN="%s"' % (wf_type, publisherDN) )
          else:
            error = 'Workflow Type "%s" FAILED on update by DN="%s"' % (wf_type, publisherDN)
            self.log.error( error )
            return S_ERROR( error )
        else: # update was not requested
          error = 'Workflow "%s" is exist in the repository, it was published by DN="%s"' % (wf_type, publisherDN)
          self.log.error( error )
          return S_ERROR( error )
    else:
      error = 'Workflow Type "%s" FAILED to be published by DN="%s"' % (wf_type, publisherDN)
      self.log.error( error )
      return S_ERROR( error )
    return S_OK()

  def getWorkflow(self, wf_type):
    cmd = "SELECT WFType, PublisherDN, PublishingTime, Body from Workflows WHERE WFType='%s'" % wf_type
    result = self._query(cmd)
    if result['OK']:
      return S_OK(result['Value'])
    else:
      return S_ERROR('Failed to retrive Workflow of type '+wf_type)

  def getListWorkflows(self):
    #KGG we need to adjust code for the empty list!!!!
    cmd = "SELECT  WFType, PublisherDN, PublishingTime from Workflows;"
    result = self._query(cmd)
    print "===KGG ", result
    #if result['OK']:
    return result

  def getWorkflowInfo(self, wf_type):
    cmd = "SELECT  WFType, PublisherDN, PublishingTime from Workflows WHERE WFType='%s'" % wf_type
    result = self._query(cmd)
    if result['OK']:
      return S_OK(result['Value'])
    else:
      return S_ERROR('Failed to retrive Workflow with the name '+wf_type) #KGG need to check logic

