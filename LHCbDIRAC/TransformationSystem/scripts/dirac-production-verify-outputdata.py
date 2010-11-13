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
  print 'Usage: dirac-production-verify-outputdata transID [transID] [transID]'
  sys.exit()
else:
  transIDs = [int(arg) for arg in sys.argv[1:]]

from LHCbDIRAC.TransformationSystem.Agent.ValidateOutputDataAgent       import ValidateOutputDataAgent
from LHCbDIRAC.TransformationSystem.Client.TransformationClient         import TransformationClient
from DIRAC import gLogger
import DIRAC

agent = ValidateOutputDataAgent('Transformation/ValidateOutputDataAgent','dirac-production-verify-outputdata')
agent.initialize()

client = TransformationClient()
for transID in transIDs:
  res = client.getTransformationParameters(transID,['Status'])
  if not res['OK']:
    gLogger.error("Failed to determine transformation status")
    gLogger.error(res['Message'])
    continue
  status = res['Value']
  if not status in ['ValidatingOutput','WaitingIntegrity','Active','Completed']:
    gLogger.error("The transformation is in %s status and can not be validated" % status)
    continue
  agent.checkTransformationIntegrity(transID)
