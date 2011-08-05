#!/usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
########################################################################
# $HeadURL$
########################################################################
__RCSID__ = "$Id$"

import sys
if len( sys.argv ) < 2:
  print 'Usage: dirac-production-archive transID [transID] [transID]'
  sys.exit()
else:
  transIDs = [int( arg ) for arg in sys.argv[1:]]


from LHCbDIRAC.TransformationSystem.Agent.TransformationCleaningAgent     import TransformationCleaningAgent
from LHCbDIRAC.TransformationSystem.Client.TransformationClient           import TransformationClient
from DIRAC                                                                import gLogger
import DIRAC

agent = TransformationCleaningAgent( 'Transformation/TransformationCleaningAgent', 'dirac-production-archive' )
agent.initialize()

client = TransformationClient()
for transID in transIDs:
  res = client.getTransformationParameters( transID, ['Status'] )
  if not res['OK']:
    gLogger.error( "Failed to determine transformation status" )
    gLogger.error( res['Message'] )
    continue
  status = res['Value']
  if not status in ['Completed']:
    gLogger.error( "The transformation is in %s status and can not be archived" % status )
    continue
  agent.archiveTransformation( transID )
