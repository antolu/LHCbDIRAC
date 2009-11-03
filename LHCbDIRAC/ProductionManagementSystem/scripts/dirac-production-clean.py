#!/usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ProductionManagementSystem/scripts/dirac-production-clean.py,v 1.2 2009/11/03 10:38:22 acsmith Exp $
########################################################################
__RCSID__   = "$Id: dirac-production-clean.py,v 1.2 2009/11/03 10:38:22 acsmith Exp $"
__VERSION__ = "$Revision: 1.2 $"

import sys
if len(sys.argv) < 2:
  print 'Usage: dirac-production-clean prodID'
  sys.exit()
else:
  prodID = int(sys.argv[1])

from DIRAC.ProductionManagementSystem.Agent.ProductionCleaningAgent import ProductionCleaningAgent
from DIRAC.ProductionManagementSystem.Client.ProductionClient       import ProductionClient
from DIRAC import gLogger
import DIRAC

client = ProductionClient()
res = client.getProductionStatus(prodID)
if not res['OK']:
  gLogger.error("Failed to determine production status")
  gLogger.error(res['Message'])
  DIRAC.exit(-1)
status = res['Value']
if not status in ['Deleted','Cleaning','Archived']:
  gLogger.error("The production is in %s status and can not be cleaned" % status)
  DIRAC.exit(-1)
agent = ProductionCleaningAgent('ProductionManagement/ProductionCleaningAgent','dirac-production-clean-production')
agent.initialize()
agent.cleanProduction(prodID)
