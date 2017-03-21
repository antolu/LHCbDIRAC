import sys
from DIRAC import gLogger
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
gConfigurationData.setOptionInCFG( '/DIRAC/Security/UseServerCertificate', 'true' )
gLogger.setLevel('FATAL')
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Interfaces.API.Dirac import Dirac
dApi = Dirac()

if __name__ == '__main__':
  
  print dApi.ping(None, None, url = 'dips://localhost:%s'%sys.argv[1])
#  print doServerPing(sys.argv[1], sys.argv[2], sys.argv[3])
