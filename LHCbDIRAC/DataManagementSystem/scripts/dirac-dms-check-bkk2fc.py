#!/usr/bin/env python
'''
    Uses the DM script switches, and, unless a list of LFNs is provided:

    1) If --BKQuery is used: get files in BKK directories, check if they are in FC
    2) If --Production is used get files using the bk query of the given production

    Then check if files registered as having a replica in the BKK are also in the FC.

    If --FixIt is set, take actions:
      - add files to the BKK if they exist in the FC, but have replica = NO in the BKK
      - set replicaFlag = No in the BKK for those files that are not in the FC
'''

#Script initialization
from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

Script.setUsageMessage( '\n'.join( [ __doc__,
                                     'Usage:',
                                     '  %s [option|cfgfile] [values]' % Script.scriptName, ] ) )
dmScript = DMScript()
dmScript.registerBKSwitches()
Script.registerSwitch( '', 'FixIt', '   Take action to fix the catalogs' )
Script.parseCommandLine( ignoreErrors=True )

#imports
import sys, os, time
import DIRAC
from DIRAC import gLogger
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.DataManagementSystem.Client.ConsistencyChecks import ConsistencyChecks

#Code
def doCheck():
  cc.checkBKK2FC()
  maxPrint = 20

  if cc.existingLFNsWithBKKReplicaNO:
    nFiles = len( cc.existingLFNsWithBKKReplicaNO )
    if nFiles <= maxPrint:
      comment = str( cc.existingLFNsWithBKKReplicaNO )
    else:
      comment = ' (first %d LFNs) : %s' % ( maxPrint, str( cc.existingLFNsWithBKKReplicaNO[:maxPrint] ) )
    if fixIt:
      gLogger.always( "Setting the replica flag to %d files: %s" % ( nFiles, comment ) )
      res = bk.addFiles( cc.existingLFNsWithBKKReplicaNO )
      if not res['OK']:
        gLogger.error( "Something wrong: %s" % res['Message'] )
      else:
        gLogger.always( "Successfully set replica flag to %d files" % nFiles )
    else:
      gLogger.error( "%d LFNs exist in the FC but have replicaFlag = No: %s" % ( nFiles, comment ) )
      gLogger.always( "Use option --FixIt to fix it" )
  else:
    gLogger.always( "No LFNs exist in the FC but have replicaFlag = No in the BKK -> OK!" )

  if cc.nonExistingLFNsWithBKKReplicaYES:
    nFiles = len( cc.nonExistingLFNsWithBKKReplicaYES )
    if nFiles <= maxPrint:
      comment = str( cc.nonExistingLFNsWithBKKReplicaYES )
    else:
      comment = ' (first %d LFNs) : %s' % ( maxPrint, str( cc.nonExistingLFNsWithBKKReplicaYES[:maxPrint] ) )

    if fixIt:
      gLogger.always( "Removing the replica flag to %d files: %s" % ( nFiles, comment ) )
      res = bk.removeFiles( cc.nonExistingLFNsWithBKKReplicaYES )
      if not res['OK']:
        gLogger.error( "Something wrong: %s" % res['Message'] )
      else:
        gLogger.always( "Successfully removed replica flag to %d files" % nFiles )
    else:
      gLogger.error( "%d LFNs have replicaFlag = Yes but are not in FC: %s" % ( nFiles, comment ) )
      gLogger.always( "Use option --FixIt to fix it" )
  else:
    gLogger.always( "No LFNs have replicaFlag = Yes but are not in the FC -> OK!" )


if __name__ == '__main__':

  fixIt = False
  production = 0
  switches = Script.getUnprocessedSwitches()
  for opt, val in switches:
    if opt == 'FixIt':
      fixIt = True

  rm = ReplicaManager()
  bk = BookkeepingClient()
  gLogger.setLevel( 'INFO' )

  cc = ConsistencyChecks( rm=rm, bkClient=bk )
  cc.bkQuery = dmScript.getBKQuery()

  doCheck()
