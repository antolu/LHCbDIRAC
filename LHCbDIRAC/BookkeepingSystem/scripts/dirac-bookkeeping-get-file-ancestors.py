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
########################################################################
# File :    dirac-bookkeeping-get-file-ancestors
# Author :  Zoltan Mathe
########################################################################
"""
  returns ancestors for a (list of) LFN(s) 
"""
__RCSID__ = "$Id$"

from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, Script

if __name__ == "__main__":
  dmScript = DMScript()
  dmScript.registerFileSwitches()
  level = 1
  Script.registerSwitch('', 'All', 'Do not restrict to ancestors with replicas')
  Script.registerSwitch('', 'Full', 'Get full metadata information on ancestors')
  Script.registerSwitch('', 'Depth=', 'Number of processing levels (default:%d)' % level)
  Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                    'Usage:',
                                    '  %s [option|cfgfile] ... [LFN|File] [Level]' % Script.scriptName,
                                    'Arguments:',
                                    '  LFN:      Logical File Name',
                                    '  File:     Name of the file with a list of LFNs',
                                    '  Level:    Number of levels to search (default: %d)' % level]))

  Script.parseCommandLine(ignoreErrors=True)

  from LHCbDIRAC.BookkeepingSystem.Client.ScriptExecutors import executeFileAncestors
  executeFileAncestors(dmScript, level)
