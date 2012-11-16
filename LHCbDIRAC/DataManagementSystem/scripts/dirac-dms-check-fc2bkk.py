#!/usr/bin/env python
'''
   1) If --Directory is used: get files in FC directories, check if they are in BKK and if the replica flag is set
   2) If a BK query is used (--BKQuery, --Production), gets files in BK and check if they are in the FC
   If --FixIt is set, take actions:
     1) LFC->BK
       Missing files: remove from SE and LFC
       No replica flag: set it
     2) BK->LFC
       Removes the replica flag in the BK for files not in the LFC
'''

#Script initialization
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__,
                                     'Usage:',
                                     '  %s [option|cfgfile] [values]' % Script.scriptName, ] ) )
Script.registerSwitch( '', 'FixIt', '   Take action to fix the catalogs' )
Script.parseCommandLine( ignoreErrors = True )

#imports
import sys, os, time
import DIRAC
from DIRAC import gLogger
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript
from LHCbDIRAC.DataManagementSystem.Client.ConsistencyChecks import ConsistencyChecks

#Code
if __name__ == '__main__':
  dmScript = DMScript()
  dmScript.registerNamespaceSwitches()
  dmScript.registerFileSwitches()
  dmScript.registerBKSwitches()

  fixIt = False
  switches = Script.getUnprocessedSwitches()
  for opt, val in switches:
    if opt == 'FixIt':
      fixIt = True

  directories = dmScript.getOption( "Directory", [] )
  lfns = dmScript.getOption( "LFNs", [] )
