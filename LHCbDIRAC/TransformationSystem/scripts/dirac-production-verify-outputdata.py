#!/usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
__RCSID__ = "$Id$"

import sys
if len( sys.argv ) < 2:
  print 'Usage: dirac-production-verify-outputdata transID [transID] [transID]'
  sys.exit()
else:
  transIDs = [int( arg ) for arg in sys.argv[1:]]

from LHCbDIRAC.TransformationSystem.Agent.ValidateOutputDataAgent       import ValidateOutputDataAgent
from LHCbDIRAC.TransformationSystem.Client.TransformationClient         import TransformationClient
from DIRAC import gLogger
import DIRAC

agent = ValidateOutputDataAgent( 'Transformation/ValidateOutputDataAgent',
                                 'Transformation/ValidateOutputDataAgent',
                                 'dirac-production-verify-outputdata' )
agent.initialize()

client = TransformationClient()
for transID in transIDs:
  agent.checkTransformationIntegrity( transID )
