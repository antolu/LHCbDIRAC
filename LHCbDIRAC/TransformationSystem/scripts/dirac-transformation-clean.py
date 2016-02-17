#!/usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
import DIRAC
parseCommandLine()

__RCSID__ = "$Id$"

import sys
if len( sys.argv ) < 2:
  print 'Usage: dirac-transformation-clean transID [transID] [transID]'
  DIRAC.exit( 1 )
else:
  try:
    transIDs = [int( arg ) for arg in sys.argv[1:]]
  except:
    print 'Invalid list of transformations'
    DIRAC.exit( 1 )

from LHCbDIRAC.TransformationSystem.Agent.TransformationCleaningAgent     import TransformationCleaningAgent

agent = TransformationCleaningAgent( 'Transformation/TransformationCleaningAgent',
                                     'Transformation/TransformationCleaningAgent',
                                     'dirac-transformation-clean' )
agent.initialize()

for transID in transIDs:
  agent.cleanTransformation( transID )
