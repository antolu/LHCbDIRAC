#!/usr/bin/env python
'''
    Uses the DM script switches, and, unless a list of LFNs is provided:

    1) If --Directory is used: get files in FC directories, check if they are in BKK and if the replica flag is set
    2) If --Production is used get files in the FC directories used, and proceed as with --Directory

    If --FixIt is set, take actions:
      Missing files: remove from SE and FC
      No replica flag: set it (in the BKK)
'''

#Code
def doCheck():
  cc.checkFC2BKK()

  maxFiles = 20
  if cc.existingLFNsWithBKKReplicaNO:
    affectedRuns = []
    for run in cc.existingLFNsWithBKKReplicaNO.values():
      if run not in affectedRuns:
        affectedRuns.append( str( run ) )
    if len( cc.existingLFNsWithBKKReplicaNO ) > maxFiles:
      prStr = ' (first %d)' % maxFiles
    else:
      prStr = ''
    gLogger.error( "%d files are in the FC but have replica = NO in BKK%s:\nAffected runs: %s\n%s" %
                   ( len( cc.existingLFNsWithBKKReplicaNO ), prStr, ','.join( affectedRuns ),
                     '\n'.join( sorted( cc.existingLFNsWithBKKReplicaNO )[0:maxFiles] ) ) )
    if fixIt:
      gLogger.always( "Going to fix them, setting the replica flag" )
      res = bk.addFiles( cc.existingLFNsWithBKKReplicaNO )
      if res['OK']:
        gLogger.always( "\tSuccessfully added replica flag" )
      else:
        gLogger.error( res['Message'] )
    else:
      gLogger.always( "Use --FixIt to fix it" )
  else:
    gLogger.always( "No files in FC with replica = NO in BKK -> OK!" )

  if cc.existingLFNsNotInBKK:
    if len( cc.existingLFNsNotInBKK ) > maxFiles:
      prStr = ' (first %d)' % maxFiles
    else:
      prStr = ''
    gLogger.error( "%d files are in the FC but are NOT in BKK%s:\n%s" %
                   ( len( cc.existingLFNsNotInBKK ), prStr,
                     '\n'.join( sorted( cc.existingLFNsNotInBKK[0:maxFiles] ) ) ) )
    if fixIt:
      gLogger.always( "Going to fix them, by removing from the FC and storage" )
      res = rm.removeFile( cc.existingLFNsNotInBKK )
      if res['OK']:
        success = len( res['Value']['Successful'] )
        failures = len( res['Value']['Failed'] )
        errors = {}
        for reason in res['Value']['Failed'].values():
          reason = str( reason )
          errors[reason] = errors.setdefault( reason, 0 ) + 1
        gLogger.always( "\t%d success, %d failures%s" % ( success, failures, ':' if failures else '' ) )
        if failures:
          for reason in errors:
            gLogger.always( '\t%s : %d' % ( reason, errors[reason] ) )
    else:
      gLogger.always( "Use --FixIt to fix it" )
  else:
    gLogger.always( "No files in FC not in BKK -> OK!" )



if __name__ == '__main__':

  #Script initialization
  from DIRAC.Core.Base import Script
  from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       '  %s [option|cfgfile] [values]' % Script.scriptName, ] ) )
  dmScript = DMScript()
  dmScript.registerNamespaceSwitches() #Directory
  dmScript.registerFileSwitches() #File, LFNs
  Script.registerSwitch( "P:", "Productions=",
                         "   Production ID to search (comma separated list)", dmScript.setProductions )
  Script.registerSwitch( '', 'FixIt', '   Take action to fix the catalogs' )
  Script.parseCommandLine( ignoreErrors = True )

  #imports
  import sys, os, time
  import DIRAC
  from DIRAC import gLogger
  from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
  from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
  from LHCbDIRAC.DataManagementSystem.Client.ConsistencyChecks import ConsistencyChecks

  fixIt = False
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'FixIt':
      fixIt = True

  rm = ReplicaManager()
  bk = BookkeepingClient()

  cc = ConsistencyChecks( rm = rm, bkClient = bk )
  cc.directories = dmScript.getOption( 'Directory', [] )
  cc.lfns = dmScript.getOption( 'LFNs', [] )
  productions = dmScript.getOption( 'Productions', [] )

  if productions:
    for prod in productions:
      cc.prod = prod
      gLogger.always( "Processing production %d" % cc.prod )
      doCheck()
      gLogger.always( "Processed production %d" % cc.prod )
  else:
    doCheck()
