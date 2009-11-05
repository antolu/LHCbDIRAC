#!/usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
########################################################################
# $HeadURL$
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.10 $"

import sys
if len(sys.argv) < 2:
  print 'Usage: dirac-production-verify-outputdata prodID'
  sys.exit()
else:
  prodID = int(sys.argv[1])

from DIRAC.ProductionManagementSystem.Agent.ValidateOutputData      import ValidateOutputData
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
if not status in ['ValidatingOutput','WaitingIntegrity']:
  gLogger.error("The production is in %s status and can not be validated" % status)
  DIRAC.exit(-1)
agent = ValidateOutputData('ProductionManagement/ValidateOutputData','dirac-production-verify-outputdata')
agent.initialize()
agent.checkProductionIntegrity(prodID)
