#!/usr/bin/env python
'''
    Uses the DM script switches, and, unless a list of LFNs is provided:

    1) If --Directory is used: get files in FC directories, check if they are in BK and if the replica flag is set
    2) If --Production is used get files in the FC directories used, and proceed as with --Directory

    If --FixIt is set, take actions:
      Missing files: remove from SE and FC
      No replica flag: set it (in the BK)
'''
__RCSID__ = "$Id$"

# Code



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
  Script.registerSwitch( '', 'FixIt', '   Take action to fix the catalogs' )
  Script.registerSwitch( '', 'AffectedRuns', '   List the runs affected by the encountered problem' )
  Script.parseCommandLine( ignoreErrors = True )

  fixIt = False
  listAffectedRuns = False
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'FixIt':
      fixIt = True
    elif switch[0] == 'AffectedRuns':
      listAffectedRuns = True

  # imports
  from DIRAC import gLogger
  from LHCbDIRAC.DataManagementSystem.Client.ConsistencyChecks import ConsistencyChecks
  cc = ConsistencyChecks()
  cc.directories = dmScript.getOption( 'Directory', [] )
  cc.lfns = dmScript.getOption( 'LFNs', [] ) + [lfn for arg in Script.getPositionalArgs() for lfn in arg.split( ',' )]
  productions = dmScript.getOption( 'Productions', [] )
  runs = dmScript.getOption( 'Runs', [] )

  from LHCbDIRAC.DataManagementSystem.Client.CheckExecutors import doCheckFC2BK
  if productions and not runs:
    fileType = dmScript.getOption( 'FileType', [] )
    if fileType:
      cc.fileType = fileType
    for prod in productions:
      cc.prod = prod
      gLogger.always( "Processing production %d" % cc.prod )
      doCheckFC2BK( cc, fixIt, listAffectedRuns )
      gLogger.always( "Processed production %d" % cc.prod )
  else:
    bkQuery = dmScript.getBKQuery( visible = 'All' )
    if bkQuery:
      bkQuery.setOption( 'ReplicaFlag', 'All' )
      cc.bkQuery = bkQuery
    doCheckFC2BK( cc, fixIt, listAffectedRuns )
