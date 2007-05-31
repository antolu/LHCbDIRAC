# $Id: ProductionRepositoryClient.py,v 1.4 2007/05/31 13:26:23 gkuznets Exp $
__RCSID__ = "$Revision: 1.4 $"

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC import gLogger, S_OK, S_ERROR

class ProductionRepositoryClient(RPCClient):

  def __init__(self, *args, **kwargs):
    RPCClient.__init__(self, "ProductionManagementSystem/ProductionRepository/")
    print gConfigurationData.getMasterServer()

  def publishWorkflow(self, path):
    print path
