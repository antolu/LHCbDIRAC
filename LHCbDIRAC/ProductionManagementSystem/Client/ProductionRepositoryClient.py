# $Id: ProductionRepositoryClient.py,v 1.3 2007/05/31 09:57:02 gkuznets Exp $
__RCSID__ = "$Revision: 1.3 $"

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC import gLogger, S_OK, S_ERROR

class ProductionRepositoryClient(RPCClient):

  def __init__(self, *args, **kwargs):
    RPCClient.__init__(self, *args, **kwargs )
