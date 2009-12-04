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
  print 'Usage: dirac-production-clean prodID'
  sys.exit()
else:
  prodID = int(sys.argv[1])

from DIRAC import gLogger

from LHCbDIRAC.ProductionManagementSystem.Agent.ProductionCleaningAgent import ProductionCleaningAgent
from LHCbDIRAC.ProductionManagementSystem.Client.ProductionClient       import ProductionClient

import DIRAC

client = ProductionClient()
res = client.getTransformationParameters(prodID,['Status'])
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
