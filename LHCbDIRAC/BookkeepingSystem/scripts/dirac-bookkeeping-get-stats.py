#! /usr/bin/env python
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
"""
   Get statistical information on a dataset
"""

__RCSID__ = "$Id$"

from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, Script

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerBKSwitches()
  dmScript.registerFileSwitches()

  Script.registerSwitch( '', 'TriggerRate', '   For RAW files, returns the trigger rate' )
  Script.registerSwitch( '', 'ListRuns', '   Give a list of runs (to be used with --Trigger)' )
  Script.registerSwitch( '', 'ListFills', '   Give a list of fills (to be used with --Trigger)' )
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile]' % Script.scriptName, ] ) )

  Script.parseCommandLine( ignoreErrors = False )

  from LHCbDIRAC.BookkeepingSystem.Client.ScriptExecutors import executeGetStats
  executeGetStats( dmScript )
