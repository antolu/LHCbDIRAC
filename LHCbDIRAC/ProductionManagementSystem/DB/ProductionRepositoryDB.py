# $Id: ProductionRepositoryDB.py,v 1.1 2007/05/16 13:30:21 gkuznets Exp $
"""
    DIRAC ProductionRepositoryDB class is a front-end to the pepository database containing
    Workflow (templates) Productions and vectors to create jobs.

    The following methods are provided for public usage:

    publishWF()

"""
__RCSID__ = "$Revision: 1.1 $"

from DIRAC.Core.Base.DB import DB
from DIRAC  import gLogger, gConfig, S_OK, S_ERROR

class ProductionRepositoryDB(BaseDB):

  def __init__( self, maxQueueSize=10 ):
    """ Constructor
    """

    BaseDB.__init__(self,'ProductionRepositoryDB', 'ProductionManagement/ProductionrepositoryDB', maxQueueSize)

  def publishWF(self, wf, userDN):
    wfName = wf.getName()
    wfType = wf.getType()
    cmd = 'INSERT INTO Workflows ( WFName, WFType, PublisherDN, PublishingTime, Body ) VALUES ' \
            '(\'%s\', \'%s\', \'%s\', NOW(), \'%s\')' % (wfName, wfType, publisherDN, wf.toXMLString())
    result = self._update( cmd )
    if result['OK']:
      self.gLogger.info( 'Workflow "%s" Type "%s" published by DN="%s"' % (wfName, wfType, publisherDN) )
    else:
      self.gLogger.error( 'Workflow "%s" Type "%s" FAILED to be published by DN="%s"' % (wfName, wfType, publisherDN) )
      return S_ERROR('Failed to publish Workflow')
    return S_OK()

  def getWorkflow(self, wfName):
    cmd = "SELECT body from Workflows WHERE WFName='%s'" % wfName
    result = self._query(cmd)
    if result['OK']:
      return S_OK(result['Value'])
    else:
      return S_ERROR('Failed to retrive Workflow with the name '+wfName)
