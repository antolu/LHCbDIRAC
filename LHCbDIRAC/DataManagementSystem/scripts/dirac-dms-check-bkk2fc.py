#!/usr/bin/env python
'''
    Uses the DM script switches, and, unless a list of LFNs is provided:

    1) If --BKQuery is used: get files in BK directories, check if they are in FC
    2) If --Production is used get files using the bk query of the given production

    Then check if files registered as having a replica in the BK are also in the FC.

    If --FixIt is set, take actions:
      - add files to the BK if they exist in the FC, but have replica = NO in the BK
      - set replicaFlag = No in the BK for those files that are not in the FC
'''

__RCSID__ = "$Id$"

# Code
def doCheck( checkAll ):
  """
  Method actually calling for the the check using ConsistencyChecks module
  It prints out results and calls corrective actions if required
  """
  cc.checkBK2FC( checkAll )
  maxPrint = 20

  if checkAll:
    if cc.existLFNsBKRepNo:
      nFiles = len( cc.existLFNsBKRepNo )
      if nFiles <= maxPrint:
        comment = str( cc.existLFNsBKRepNo )
      else:
        comment = ' (first %d LFNs) : %s' % ( maxPrint, str( cc.existLFNsBKRepNo[:maxPrint] ) )
      if fixIt:
        gLogger.always( "Setting the replica flag to %d files: %s" % ( nFiles, comment ) )
        res = bk.addFiles( cc.existLFNsBKRepNo )
        if not res['OK']:
          gLogger.error( "Something wrong: %s" % res['Message'] )
        else:
          gLogger.always( "Successfully set replica flag to %d files" % nFiles )
      else:
        gLogger.error( "%d LFNs exist in the FC but have replicaFlag = No: %s" % ( nFiles, comment ) )
        gLogger.always( "Use option --FixIt to fix it (set the replica flag)" )
    else:
      gLogger.always( "No LFNs exist in the FC but have replicaFlag = No in the BK -> OK!" )

  if cc.absentLFNsBKRepYes:
    nFiles = len( cc.absentLFNsBKRepYes )
    if nFiles <= maxPrint:
      comment = str( cc.absentLFNsBKRepYes )
    else:
      comment = ' (first %d LFNs) : %s' % ( maxPrint, str( cc.absentLFNsBKRepYes[:maxPrint] ) )

    if fixIt:
      gLogger.always( "Removing the replica flag to %d files: %s" % ( nFiles, comment ) )
      res = bk.removeFiles( cc.absentLFNsBKRepYes )
      if not res['OK']:
        gLogger.error( "Something wrong: %s" % res['Message'] )
      else:
        gLogger.always( "Successfully removed replica flag to %d files" % nFiles )
    else:
      gLogger.error( "%d files have replicaFlag = Yes but are not in FC: %s" % ( nFiles, comment ) )
      gLogger.always( "Use option --FixIt to fix it (remove the replica flag)" )
  else:
    gLogger.always( "No LFNs have replicaFlag = Yes but are not in the FC -> OK!" )


if __name__ == '__main__':

  # Script initialization
  from DIRAC.Core.Base import Script
  from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript
  from DIRAC import gLogger

  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       '  %s [option|cfgfile] [values]' % Script.scriptName, ] ) )
  dmScript = DMScript()
  dmScript.registerBKSwitches()
  dmScript.registerFileSwitches()
  Script.registerSwitch( '', 'FixIt', '   Take action to fix the catalogs' )
  Script.registerSwitch( '', 'CheckAllFlags', '   Consider also files with replica flag NO' )
  Script.parseCommandLine( ignoreErrors = True )

  fixIt = False
  checkAll = False
  production = 0
  switches = Script.getUnprocessedSwitches()
  for opt, val in switches:
    if opt == 'FixIt':
      fixIt = True
    elif opt == 'CheckAllFlags':
      checkAll = True

  # imports
  from DIRAC.DataManagementSystem.Client.DataManager import DataManager
  from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
  from LHCbDIRAC.DataManagementSystem.Client.ConsistencyChecks import ConsistencyChecks

  gLogger.setLevel( 'INFO' )
  dm = DataManager()
  bk = BookkeepingClient()

  cc = ConsistencyChecks( dm = dm, bkClient = bk )
  bkQuery = dmScript.getBKQuery( visible = 'All' )
  cc.bkQuery = bkQuery
  cc.lfns = dmScript.getOption( 'LFNs', [] )
  prods = dmScript.getOption( 'Productions', [] )

  if prods:
    for prod in prods:
      cc.prod = prod
      gLogger.always( "Processing production %d" % cc.prod )
      doCheck( checkAll )
      gLogger.always( "Processed production %d" % cc.prod )
  else:
    doCheck( checkAll )
