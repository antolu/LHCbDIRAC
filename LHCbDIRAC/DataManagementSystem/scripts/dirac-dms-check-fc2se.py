#!/usr/bin/env python
'''
    Check if the files are in the BK, the LFC and the SEs they are supposed to be in.

    Uses the DM script switches, and, unless a list of LFNs is provided:
    1) If --Directory is used: get files in FC directories
    2) If --Production or --BK options is used get files in the FC directories from the BK

    If --FixIt is set, takes actions:
      Missing files: remove from SE and FC
      No replica flag: set it (in the BK)
      Not existing in SE: remove replica or file from the catalog
      Bad checksum: remove replica or file from SE and catalogs if no good replica
'''
__RCSID__ = "$Id: dirac-dms-check-fc2se.py 69455 2013-08-14 08:42:29Z phicharp $"



if __name__ == '__main__':

  # Script initialization
  from DIRAC.Core.Base import Script
  from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       '  %s [option|cfgfile] [values]' % Script.scriptName, ] ) )
  dmScript = DMScript()
  dmScript.registerNamespaceSwitches()  # Directory
  dmScript.registerFileSwitches()  # File, LFNs
  dmScript.registerBKSwitches()
  Script.registerSwitch( '', 'FixIt', '   Take action to fix the catalogs and storage' )
  Script.registerSwitch( '', 'NoReplace', '   Do not replace bad or missing replicas' )
  Script.registerSwitch( '', 'NoBK', '   Do not check with BK' )
  Script.parseCommandLine( ignoreErrors = True )

  fixIt = False
  bkCheck = True
  noReplace = True
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'FixIt':
      fixIt = True
    elif switch[0] == 'NoBK':
      bkCheck = False
    elif switch[0] == 'NoReplace':
      noreplace = True

  # imports
  from DIRAC import gLogger
  from LHCbDIRAC.DataManagementSystem.Client.ConsistencyChecks import ConsistencyChecks
  cc = ConsistencyChecks()
  cc.directories = dmScript.getOption( 'Directory', [] )
  cc.lfns = dmScript.getOption( 'LFNs', [] ) + [lfn for arg in Script.getPositionalArgs() for lfn in arg.split( ',' )]
  bkQuery = dmScript.getBKQuery( visible = 'All' )
  if bkQuery:
    bkQuery.setOption( 'ReplicaFlag', 'All' )
    cc.bkQuery = bkQuery

  from LHCbDIRAC.DataManagementSystem.Client.CheckExecutors import doCheckFC2SE
  doCheckFC2SE( cc, bkCheck, fixIt, noReplace )
