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
# File :    dirac-dms-get-file
# Author :  Stuart Paterson
########################################################################
"""
  Retrieve a single file or list of files from Grid storage to the current directory.
"""
__RCSID__ = "$Id$"
import os

from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, Script

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerFileSwitches()
  dmScript.registerNamespaceSwitches('download to (default = %s)' % os.path.realpath('.'))

  Script.setUsageMessage('\n'.join([__doc__,
                                    'Usage:',
                                    '  %s [option|cfgfile] [<LFN>] [<LFN>...] [SourceSE]' % Script.scriptName, ]))

  Script.parseCommandLine(ignoreErrors=False)

  from LHCbDIRAC.DataManagementSystem.Client.ScriptExecutors import executeGetFile
  from DIRAC import exit
  exit(executeGetFile(dmScript))
