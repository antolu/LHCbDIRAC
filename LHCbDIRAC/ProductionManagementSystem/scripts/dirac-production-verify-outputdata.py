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

from LHCbDIRAC.ProductionManagementSystem.Agent.ValidateOutputDataAgent import ValidateOutputDataAgent
from LHCbDIRAC.ProductionManagementSystem.Client.ProductionClient       import ProductionClient
from DIRAC import gLogger
import DIRAC

client = ProductionClient()
res = client.getTransformationParameters(prodID,['Status'])
if not res['OK']:
  gLogger.error("Failed to determine production status")
  gLogger.error(res['Message'])
  DIRAC.exit(-1)
status = res['Value']
if not status in ['ValidatingOutput','WaitingIntegrity','Active','Completed']:
  gLogger.error("The production is in %s status and can not be validated" % status)
  DIRAC.exit(-1)
agent = ValidateOutputDataAgent('ProductionManagement/ValidateOutputDataAgent','dirac-production-verify-outputdata')
agent.initialize()
agent.checkProductionIntegrity(prodID)
