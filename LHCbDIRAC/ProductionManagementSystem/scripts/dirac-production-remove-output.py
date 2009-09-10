#!/usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ProductionManagementSystem/scripts/dirac-production-remove-output.py,v 1.1 2009/09/10 12:16:52 acsmith Exp $
########################################################################
__RCSID__   = "$Id: dirac-production-remove-output.py,v 1.1 2009/09/10 12:16:52 acsmith Exp $"
__VERSION__ = "$Revision: 1.1 $"

import sys
if len(sys.argv) < 2:
  print 'Usage: dirac-production-remove-output prodID'
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
if not status in ['RemovingOutput','ValidatingInput']:
  gLogger.error("The production is in %s status and the outputs can not be removed" % status)
  DIRAC.exit(-1)
agent = ProductionCleaningAgent('ProductionManagement/ProductionCleaningAgent','dirac-production-remove-output')
agent.initialize()
agent.removeProductionOutput(prodID)
