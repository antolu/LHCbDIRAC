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

"""
  Retrieve an access URL for an LFN replica given a valid DIRAC SE.
"""

__RCSID__ = "$Id$"

from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, Script

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerFileSwitches()
  dmScript.registerSiteSwitches()
  Script.registerSwitch('', 'Protocol=',
                        '   Define the protocol for which a tURL is requested (default:root)')
  Script.setUsageMessage('\n'.join(__doc__.split('\n') + [
      'Usage:',
      '  %s [option|cfgfile] ... [LFN[,LFN2[,LFN3...]]] SE[,SE2...]' % Script.scriptName,
      'Arguments:',
      '  LFN:      Logical File Name or file containing LFNs',
      '  SE:       Valid DIRAC SE']))
  Script.parseCommandLine(ignoreErrors=True)

  from LHCbDIRAC.DataManagementSystem.Client.ScriptExecutors import executeAccessURL
  from DIRAC import exit
  exit(executeAccessURL(dmScript))
