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

"""  Check the space token usage at the site and report the space usage from several sources:
      File Catalogue, Storage dumps, StorageElement interface
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
import DIRAC

if __name__ == "__main__":

  unit = 'TB'
  Script.registerSwitch("u:", "Unit=", "   Unit to use [%s] (MB,GB,TB,PB)" % unit)
  Script.registerSwitch("S:", "Sites=", "  Sites to consider [ALL] (space or comma separated list, e.g. LCG.CNAF.it")

  Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                    'Usage:',
                                    '  %s [option|cfgfile] ...' %
                                    Script.scriptName, ]))

  Script.parseCommandLine(ignoreErrors=False)

  from LHCbDIRAC.DataManagementSystem.Client.SpaceTokenUsage import combinedResult
  DIRAC.exit(combinedResult(unit))
