# $Id: ProductionRepositoryClient.py,v 1.5 2007/06/01 12:49:02 gkuznets Exp $
__RCSID__ = "$Revision: 1.5 $"

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC import gLogger, S_OK, S_ERROR

class ProductionRepositoryClient(RPCClient):

  def __init__(self, *args, **kwargs):
    print "KGG"
    RPCClient.__init__(self, "dips://volhcb03.cern.ch:9131/ProductionManagement/ProductionRepository")

  def publishWorkflow(self, path):
    print path
