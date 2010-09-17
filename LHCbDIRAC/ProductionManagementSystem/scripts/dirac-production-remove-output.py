#!/usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
########################################################################
# $HeadURL$
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.2 $"

import sys
if len(sys.argv) < 2:
  print 'Usage: dirac-production-remove-output prodID'
  sys.exit()
else:
  prodIDs = [int(arg) for arg in sys.argv[1:]]

from LHCbDIRAC.ProductionManagementSystem.Agent.ProductionCleaningAgent  import ProductionCleaningAgent
from LHCbDIRAC.ProductionManagementSystem.Client.ProductionClient        import ProductionClient
from DIRAC                                                               import gLogger
import DIRAC

agent = ProductionCleaningAgent('ProductionManagement/ProductionCleaningAgent','dirac-production-remove-output')
agent.initialize()

client = ProductionClient()
for prodID in prodIDs:
  res = client.getTransformationParameters(prodID,['Status'])
  if not res['OK']:
    gLogger.error("Failed to determine production status")
    gLogger.error(res['Message'])
    continue
  status = res['Value']
  if not status in ['RemovingFiles','RemovingOutput','ValidatingInput','Active']:
    gLogger.error("The production is in %s status and the outputs can not be removed" % status)
    continue
  agent.removeProductionOutput(prodID)
