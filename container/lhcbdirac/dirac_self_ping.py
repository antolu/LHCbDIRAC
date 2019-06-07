###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
# Perform a DIPS ping on a given target and exit with the return code.
# The target is specified as ""<port>/System/Service"

import sys
import os
import time

with open(os.devnull, 'w') as redirectStdout, open(os.devnull, 'w') as redirectStderr:
  #sys.stdout = redirectStdout
  #sys.stderr = redirectStderr
  from DIRAC import gLogger
  from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
  gConfigurationData.setOptionInCFG( '/DIRAC/Security/UseServerCertificate', 'true' )
  gLogger.setLevel('FATAL')
  from DIRAC.Core.DISET.RPCClient import RPCClient
  #from DIRAC.Interfaces.API.Dirac import Dirac
  #dApi = Dirac()
  if __name__ == '__main__':
    rpc = RPCClient('dips://localhost:%s'%sys.argv[1])
    res = rpc.ping()
    time.sleep(0.1)
    #res = dApi.ping(None, None, url = 'dips://localhost:%s'%sys.argv[1])
    if not res['OK']:
      sys.exit(1)
    else:
      sys.exit(0)
