#!/usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
########################################################################
# $HeadURL$
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.1 $"

import sys
if len(sys.argv) < 2:
  print 'Usage: dirac-production-archive prodID'
  sys.exit()
else:
  prodID = int(sys.argv[1])

from DIRAC                                                              import gLogger

from LHCbDIRAC.ProductionManagementSystem.Agent.ProductionCleaningAgent import ProductionCleaningAgent
from LHCbDIRAC.ProductionManagementSystem.Client.ProductionClient       import ProductionClient

import DIRAC

client = ProductionClient()
res = client.getProductionStatus(prodID)
if not res['OK']:
  gLogger.error("Failed to determine production status")
  gLogger.error(res['Message'])
  DIRAC.exit(-1)
status = res['Value']
if not status in ['Completed']:
  gLogger.error("The production is in %s status and can not be archived" % status)
  DIRAC.exit(-1)
agent = ProductionCleaningAgent('ProductionManagement/ProductionCleaningAgent','dirac-production-archive')
agent.initialize()
agent.archiveProduction(prodID)
