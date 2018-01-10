#!/usr/bin/env python

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
