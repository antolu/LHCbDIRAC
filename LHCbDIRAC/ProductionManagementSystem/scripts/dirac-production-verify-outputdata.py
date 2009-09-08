#!/usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ProductionManagementSystem/scripts/dirac-production-verify-outputdata.py,v 1.9 2009/09/08 14:11:19 acsmith Exp $
########################################################################
__RCSID__   = "$Id: dirac-production-verify-outputdata.py,v 1.9 2009/09/08 14:11:19 acsmith Exp $"
__VERSION__ = "$Revision: 1.9 $"

import sys
if len(sys.argv) < 2:
  print 'Usage: dirac-production-verify-outputdata.py prodID'
  sys.exit()
else:
  prodID = int(sys.argv[1])


from DIRAC.ProductionManagementSystem.Agent.ValidateOutputData import ValidateOutputData
agent = ValidateOutputData('ProductionManagement/ValidateOutputData','dirac-production-verify-outputdata')
agent.initialize()
agent.checkProductionIntegrity(prodID)
