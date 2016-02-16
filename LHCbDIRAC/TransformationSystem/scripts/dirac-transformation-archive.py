#!/usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
import DIRAC

__RCSID__ = "$Id: dirac-transformation-archive.py 69359 2013-08-08 13:57:13Z phicharp $"

import sys
if len( sys.argv ) < 2:
  print 'Usage: dirac-transformation-archive transID [transID] [transID]'
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
                                     'dirac-transformation-archive' )
agent.initialize()

for transID in transIDs:
  agent.archiveTransformation( transID )
