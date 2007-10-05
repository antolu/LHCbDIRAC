# $Id: ProductionRepositoryClient.py,v 1.10 2007/10/05 14:39:37 gkuznets Exp $
__RCSID__ = "$Revision: 1.10 $"

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC import gLogger, S_OK, S_ERROR

class ProductionRepositoryClient:

  def __init__(self, *args, **kwargs):
    self.productionRepositoryUrl = gConfig.getValue('/Systems/ProductionManagement/Development/URLs/ProductionRepository')
    self.rpcClient=RPCClient(self.productionRepositoryUrl)

  def publishWorkflow(self, path):
    fd = file( path )
    body = fd.read()
    fd.close()
    retVal = self.rpcClient.publishWorkflow(body)
    print retVal

  def getListWorkflows(self):
    retVal = self.rpcClient.getListWorkflows()
    print retVal
