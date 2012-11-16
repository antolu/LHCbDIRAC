#!/usr/bin/env python
'''
    Uses the DM script switches, and, unless a list of LFNs is provided:

    1) If --Directory is used: get files in FC directories, check if they are in BKK and if the replica flag is set
    2) If --Production is used get files in the FC directories used, and proceed as with --Directory

    If --FixIt is set, take actions:
      Missing files: remove from SE and FC
      No replica flag: set it (in the BKK)
'''

#Script initialization
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__,
                                     'Usage:',
                                     '  %s [option|cfgfile] [values]' % Script.scriptName, ] ) )
Script.registerSwitch( '', 'Production', '   Set a production from which to start' )
Script.registerSwitch( '', 'FixIt', '   Take action to fix the catalogs' )
Script.parseCommandLine( ignoreErrors = True )

#imports
import sys, os, time
import DIRAC
from DIRAC import gLogger
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript
from LHCbDIRAC.DataManagementSystem.Client.ConsistencyChecks import ConsistencyChecks

#Code
if __name__ == '__main__':
  dmScript = DMScript()
  dmScript.registerNamespaceSwitches() #Directory
  dmScript.registerFileSwitches() #File, LFNs

  fixIt = False
  production = 0
  switches = Script.getUnprocessedSwitches()
  for opt, val in switches:
    if opt == 'FixIt':
      fixIt = True
    if opt == 'Production':
      production = val

  rm = ReplicaManager()
  bk = BookkeepingClient()

  cc = ConsistencyChecks( rm = rm, bkClient = bk )
  cc.directories = dmScript.getOption( 'Directory', [] )
  cc.lfns = dmScript.getOption( 'LFNs', [] )
  cc.prod = production

  cc.checkFC2BKK()

  if cc.existingLFNsWithBKKReplicaNO:
    gLogger.error( "%d files are in the FC but have replica = NO in BKK: %s" % ( len( cc.existingLFNsWithBKKReplicaNO ),
                                                                                 str( cc.existingLFNsWithBKKReplicaNO ) ) )
    if fixIt:
      gLogger.always( "Going to fix them, setting the replica flag" )
      res = bk.addFiles( cc.existingLFNsWithBKKReplicaNO )
      if res['OK']:
        gLogger.always( "\tSuccessfully added replica flag" )
      else:
        gLogger.error( res['Message'] )
  else:
    gLogger.always( "No files in FC with replica = NO in BKK -> OK!" )

  if cc.existingLFNsNotInBKK:
    gLogger.error( "%d files are in the FC but are NOT in BKK: %s" % ( len( cc.existingLFNsNotInBKK ),
                                                                       str( cc.existingLFNsNotInBKK ) ) )
    if fixIt:
      gLogger.always( "Going to fix them, by removing from the FC and storage" )
      res = rm.removeFile( cc.existingLFNsNotInBKK )
      if res['OK']:
        success = len( res['Value']['Successful'] )
        failures = len( res['Value']['Failed'] )
        gLogger.always( "\t%d success, %d failures" % ( success, failures ) )
  else:
    gLogger.always( "No files in FC not in BKK -> OK!" )
