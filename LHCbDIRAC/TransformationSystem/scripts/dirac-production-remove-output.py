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
  print 'Usage: dirac-production-remove-output transID [transID] [transID]'
  sys.exit()
else:
  transIDs = [int(arg) for arg in sys.argv[1:]]

from LHCbDIRAC.TransformationSystem.Agent.TransformationCleaningAgent     import TransformationCleaningAgent
from LHCbDIRAC.TransformationSystem.Client.TransformationDBClient         import TransformationClient
from DIRAC                                                                import gLogger
import DIRAC

agent = TransformationCleaningAgent('Transformation/TransformationCleaningAgent','dirac-production-remove-output')
agent.initialize()

client = TransformationDBClient()
for transID in transIDs:
  res = client.getTransformationParameters(transID,['Status'])
  if not res['OK']:
    gLogger.error("Failed to determine transformation status")
    gLogger.error(res['Message'])
    continue
  status = res['Value']
  if not status in ['RemovingFiles','RemovingOutput','ValidatingInput','Active']:
    gLogger.error("The transformation is in %s status and the outputs can not be removed" % status)
    continue
  agent.removeTransformationOutput(prodID)
