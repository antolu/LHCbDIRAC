#!/usr/bin/env python
###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
import DIRAC

__RCSID__ = "$Id$"

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
