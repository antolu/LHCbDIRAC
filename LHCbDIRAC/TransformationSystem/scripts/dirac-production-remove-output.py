#!/usr/bin/env python
""" remove output of production
"""

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import exit as DIRACExit

__RCSID__ = "$Id$"

import sys
if len( sys.argv ) < 2:
  print 'Usage: dirac-production-remove-output transID [transID] [transID]'
  DIRACExit( 1 )
else:
  try:
    transIDs = [int( arg ) for arg in sys.argv[1:]]
  except:
    print 'Invalid list of productions'
    DIRACExit( 1 )

import DIRAC
from DIRAC                                                                import gLogger
from LHCbDIRAC.TransformationSystem.Agent.TransformationCleaningAgent     import TransformationCleaningAgent
from LHCbDIRAC.TransformationSystem.Client.TransformationClient           import TransformationClient

agent = TransformationCleaningAgent( 'Transformation/TransformationCleaningAgent',
                                     'Transformation/TransformationCleaningAgent',
                                     'dirac-production-remove-output' )
agent.initialize()

client = TransformationClient()
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
