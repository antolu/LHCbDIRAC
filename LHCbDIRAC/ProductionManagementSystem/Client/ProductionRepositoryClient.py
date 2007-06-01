# $Id: ProductionRepositoryClient.py,v 1.7 2007/06/01 13:11:18 gkuznets Exp $
__RCSID__ = "$Revision: 1.7 $"

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC import gLogger, S_OK, S_ERROR

class ProductionRepositoryClient:

  def __init__(self, *args, **kwargs):
    print "KGG"
    client=RPCClient("dips://volhcb03.cern.ch:9131/ProductionManagement/ProductionRepository")
    print "KGG2"

  def publishWorkflow(self, path):
    print path
