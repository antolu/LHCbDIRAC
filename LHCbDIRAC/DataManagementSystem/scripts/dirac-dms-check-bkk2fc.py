#!/usr/bin/env python
'''
    Uses the DM script switches, and, unless a list of LFNs is provided:

    1) If --BKQuery is used: get files in BKK directories, check if they are in FC
    2) If --Production is used get files using the bk query of the given production

    If --FixIt is set, take actions:

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
Script.parseCommandLine( ignoreErrors = True )

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

  if cc.existingLFNsWithBKKReplicaNO:
    gLogger.error( "%d LFNs have replicaFlag = No: %s" % ( len( cc.existingLFNsWithBKKReplicaNO ),
                                                           str( cc.existingLFNsWithBKKReplicaNO ) ) )

    if fixIt:
      gLogger.always( "Setting the replica flag to %d files: %s" % ( len( cc.existingLFNsWithBKKReplicaNO ),
                                                                     str( cc.existingLFNsWithBKKReplicaNO ) ) )
      res = bkClient.addFiles( cc.existingLFNsWithBKKReplicaNO )
      if not res['OK']:
        gLogger.error( "Something wrong: %s" % res['Message'] )
  else:
    gLogger.always( "No LFNs have Replica = NO -> OK!" )

  if cc.nonExistingLFNsWithBKKReplicaYES:
    gLogger.error( "%d LFNs have replicaFlag = Yes, but are not in FC: %s" % ( len( cc.existingLFNsWithBKKReplicaNO ),
                                                                               str( cc.existingLFNsWithBKKReplicaNO ) ) )

    if fixIt:
      gLogger.always( "Un-Setting the replica flag to %d files: %s" % ( len( cc.nonExistingLFNsWithBKKReplicaYES ),
                                                                        str( cc.nonExistingLFNsWithBKKReplicaYES ) ) )
      res = bkClient.removeFiles( cc.nonExistingLFNsWithBKKReplicaYES )
      if not res['OK']:
        gLogger.error( "Something wrong: %s" % res['Message'] )
  else:
    gLogger.always( "No File have Replica = Yes, but are not in FC -> OK!" )


if __name__ == '__main__':

  fixIt = False
  production = 0
  switches = Script.getUnprocessedSwitches()
  for opt, val in switches:
    if opt == 'FixIt':
      fixIt = True

  rm = ReplicaManager()
  bk = BookkeepingClient()

  cc = ConsistencyChecks( rm = rm, bkClient = bk )
  cc.bkQuery = dmScript.getOption( 'BKPath', {} )
  cc.runsList = dmScript.getOption( 'Runs' )
  prods = dmScript.getOption( 'Productions', [] )

  if prods:
    for prod in prods:
      cc.prod = prod
      doCheck()
      gLogger.always( "Processed production %d" % cc.prod )
  else:
    doCheck()
