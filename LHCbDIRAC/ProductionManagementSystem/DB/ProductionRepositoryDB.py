# $Id: ProductionRepositoryDB.py,v 1.19 2007/06/29 15:08:46 gkuznets Exp $
"""
    DIRAC ProductionRepositoryDB class is a front-end to the pepository database containing
    Workflow (templates) Productions and vectors to create jobs.

    The following methods are provided for public usage:

    publishWorkflow()
    getWorkflow()
    getWorkglowsList()
    getWorkflowInfo()

"""
__RCSID__ = "$Revision: 1.19 $"

from DIRAC.Core.Base.DB import DB
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC  import gLogger, S_OK, S_ERROR
from DIRAC.Core.Workflow.WorkflowReader import *

class ProductionRepositoryDB(DB):

  def __init__( self, maxQueueSize=4 ):
    """ Constructor
    """

    DB.__init__(self,'ProductionRepositoryDB', 'ProductionManagement/ProductionRepositoryDB', maxQueueSize)

  def publishWorkflow(self, wf_body, publisherDN, update=False):
    wf = fromXMLString(wf_body)
    wf_type = wf.getType()
    # KGG WE HAVE TO CHECK IS WORKFLOW EXISTS
    result = self.getWorkflowInfo(wf_type)
    if result['OK']:
      # workflow already exists
      if result['Value'] == ():
        # it is a new workflow
        #cmd = 'INSERT INTO Workflows ( WFType, PublisherDN, PublishingTime, Body ) VALUES ' \
        #        '(\'%s\', \'%s\', NOW(), \'%s\')' % (wf_type, publisherDN, wf_body)

        result = self._insert('Workflows', [ 'WFType', 'PublisherDN', 'PublishingTime', 'Body' ], [wf_type, publisherDN, dateTime().toString(),wf_body])
        if result['OK']:
          self.log.info( 'Workflow Type "%s" published by DN="%s"' % (wf_type, publisherDN) )
        else:
          error = 'Workflow Type "%s" FAILED to be published by DN="%s"' % (wf_type, publisherDN)
          self.log.error( error )
          return S_ERROR( error )
      else:
        if update: # we were asked to update
          cmd = "UPDATE Workflows set WFType='%s', PublisherDN='%s', PublishingTime=NOW(), Body='%s' WHERE WFName='%s'" \
                % (wf_type, publisherDN, wf_body, wf_name)
          result = self._update( cmd )
          if result['OK']:
            self.log.info( 'Workflow "%s" Type "%s" updated by DN="%s"' % (wf_name, wf_type, publisherDN) )
          else:
            error = 'Workflow "%s" Type "%s" FAILED on update by DN="%s"' % (wf_name, wf_type, publisherDN)
            self.log.error( error )
            return S_ERROR( error )
        else: # update was not requested
          error = 'Workflow "%s" is exist in the repository, it was published by DN="%s"' % (wf_name, wf_type, publisherDN)
          self.log.error( error )
          return S_ERROR( error )
    else:
      error = 'Workflow Type "%s" FAILED to be published by DN="%s"' % (wf_type, publisherDN)
      self.log.error( error )
      return S_ERROR( error )
    return S_OK()

  def getWorkflow(self, wf_type):
    cmd = "SELECT WFType, PublisherDN, PublishingTime Body from Workflows WHERE WFType='%s'" % wf_type
    result = self._query(cmd)
    if result['OK']:
      return S_OK(result['Value'])
    else:
      return S_ERROR('Failed to retrive Workflow of type '+wf_type)

  def getWorkflowsList(self):
    #KGG we need to adjust code for the empty list!!!!
    cmd = "SELECT  WFType, PublisherDN, PublishingTime from Workflows"
    result = self._query(cmd)
    if not result['OK']:
      return result
    try:
      wf_list = result['Value']
      result_list = [ (x[0],x[1],x[2],x[3]) for x in wf_list]
      return S_OK(result_list)
    except:
      return S_ERROR('Failed to get Workflow list from the Production Repository')

  def getWorkflowInfo(self, wf_type):
    cmd = "SELECT  WFType, PublisherDN, PublishingTime from Workflows WHERE WFType='%s'" % wf_type
    result = self._query(cmd)
    if result['OK']:
      return S_OK(result['Value'])
    else:
      return S_ERROR('Failed to retrive Workflow with the name '+wf_name) #KGG need to check logic

