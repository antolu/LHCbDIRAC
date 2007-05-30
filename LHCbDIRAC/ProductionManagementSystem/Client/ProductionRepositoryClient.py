# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ProductionManagementSystem/Client/Attic/ProductionRepositoryClient.py,v 1.1 2007/05/30 17:22:11 gkuznets Exp $
__RCSID__ = "$Id: ProductionRepositoryClient.py,v 1.1 2007/05/30 17:22:11 gkuznets Exp $"

import dirac
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC import gLogger, gConfig, S_OK, S_ERROR

class ProductionRepositoryClient(RPCClient):

  def __init__(self, *args, **kwargs):
    RPCClient.__init__( *args, **kwargs )
