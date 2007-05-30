# $Id: ProductionRepositoryClient.py,v 1.2 2007/05/30 17:25:15 gkuznets Exp $
__RCSID__ = "$Revision: 1.2 $"

import dirac
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC import gLogger, gConfig, S_OK, S_ERROR

class ProductionRepositoryClient(RPCClient):

  def __init__(self, *args, **kwargs):
    RPCClient.__init__( *args, **kwargs )
