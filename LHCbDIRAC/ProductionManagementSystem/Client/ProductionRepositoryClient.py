# $Id: ProductionRepositoryClient.py,v 1.8 2007/06/01 13:58:58 gkuznets Exp $
__RCSID__ = "$Revision: 1.8 $"

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC import gLogger, S_OK, S_ERROR

class ProductionRepositoryClient:

  def __init__(self, *args, **kwargs):
    print "KGG"
    #client=RPCClient("dips://volhcb03.cern.ch:9131/ProductionManagement/ProductionRepository")
    #print gConfig.getValue( "/DIRAC/Setup", "Production" )
    #print "KGG2"

  def publishWorkflow(self, path):
    print path
