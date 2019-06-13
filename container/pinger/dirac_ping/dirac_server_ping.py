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
import sys
from DIRAC import gLogger
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
gConfigurationData.setOptionInCFG( '/DIRAC/Security/UseServerCertificate', 'true' )
gLogger.setLevel('FATAL')
from DIRAC.Interfaces.API.Dirac import Dirac
dApi = Dirac()

if __name__ == '__main__':
  
  print dApi.ping(None, None, url = 'dips://localhost:%s'%sys.argv[1])
#  print doServerPing(sys.argv[1], sys.argv[2], sys.argv[3])
