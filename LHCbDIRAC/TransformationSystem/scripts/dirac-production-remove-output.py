#!/usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
import DIRAC
parseCommandLine()
__RCSID__ = "$Id$"

import sys
if len( sys.argv ) < 2:
  print 'Usage: dirac-production-remove-output transID [transID] [transID]'
  DIRAC.exit( 1 )
else:
  try:
    transIDs = [int( arg ) for arg in sys.argv[1:]]
  except:
    print 'Invalid list of productions'
    DIRAC.exit( 1 )

from LHCbDIRAC.TransformationSystem.Agent.TransformationCleaningAgent     import TransformationCleaningAgent
from LHCbDIRAC.ProductionManagementSystem.Client.ProductionsClient           import ProductionsClient
from DIRAC                                                                import gLogger
import DIRAC

agent = TransformationCleaningAgent( 'Transformation/TransformationCleaningAgent',
                                     'Transformation/TransformationCleaningAgent',
                                     'dirac-production-remove-output' )
agent.initialize()

client = ProductionsClient()
for transID in transIDs:
  res = client.getTransformationParameters( transID, ['Status'] )
  if not res['OK']:
    gLogger.error( "Failed to determine transformation status" )
    gLogger.error( res['Message'] )
    continue
  status = res['Value']
  if not status in ['RemovingFiles', 'RemovingOutput', 'ValidatingInput', 'Active']:
    gLogger.error( "The transformation is in %s status and the outputs cannot be removed" % status )
    continue
  agent.removeTransformationOutput( transID )
