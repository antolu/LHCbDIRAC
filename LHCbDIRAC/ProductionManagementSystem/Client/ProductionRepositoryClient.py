# $Id: ProductionRepositoryClient.py,v 1.11 2007/11/12 17:12:54 gkuznets Exp $
__RCSID__ = "$Revision: 1.11 $"

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC import gLogger, S_OK, S_ERROR

#KGG !!! I must add checking of the retrived value

class ProductionRepositoryClient:

  def __init__(self, *args, **kwargs):
    self.productionRepositoryUrl = gConfig.getValue('/Systems/ProductionManagement/Development/URLs/ProductionRepository')
    self.rpcClient=RPCClient(self.productionRepositoryUrl)

  def publishWorkflow(self, path, update=False):
    fd = file( path )
    body = fd.read()
    fd.close()
    return self.rpcClient.publishWorkflow(body, update)

  def publishWorkflowString(self, body, update=False):
    return self.rpcClient.publishWorkflow(body, update)

  def deleteWorkflow(self, name):
    return self.rpcClient.deleteWorkflow(name)

  def getListWorkflows(self):
    return self.rpcClient.getListWorkflows()

