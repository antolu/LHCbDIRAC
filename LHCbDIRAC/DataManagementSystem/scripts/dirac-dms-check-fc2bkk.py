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
def doCheck():
  """
  Method actually calling for the the check using ConsistencyChecks module
  It prints out results and calls corrective actions if required
  """
  cc.checkFC2BK()

  maxFiles = 20
  if cc.existLFNsBKRepNo:
    affectedRuns = []
    for run in cc.existLFNsBKRepNo.values():
      if run not in affectedRuns:
        affectedRuns.append( str( run ) )
    if len( cc.existLFNsBKRepNo ) > maxFiles:
      prStr = ' (first %d)' % maxFiles
    else:
      prStr = ''
    gLogger.error( "%d files are in the FC but have replica = NO in BK%s:\nAffected runs: %s\n%s" %
                   ( len( cc.existLFNsBKRepNo ), prStr, ','.join( affectedRuns ),
                     '\n'.join( sorted( cc.existLFNsBKRepNo )[0:maxFiles] ) ) )
    if fixIt:
      gLogger.always( "Going to fix them, setting the replica flag" )
      res = bk.addFiles( cc.existLFNsBKRepNo.keys() )
      if res['OK']:
        gLogger.always( "\tSuccessfully added replica flag" )
      else:
        gLogger.error( 'Failed to set the replica flag', res['Message'] )
    else:
      gLogger.always( "Use --FixIt to fix it (set the replica flag)" )
  else:
    gLogger.always( "No files in FC with replica = NO in BK -> OK!" )

  if cc.existingLFNsNotInBK:
    if len( cc.existingLFNsNotInBK ) > maxFiles:
      prStr = ' (first %d)' % maxFiles
    else:
      prStr = ''
    gLogger.error( "%d files are in the FC but are NOT in BK%s:\n%s" %
                   ( len( cc.existingLFNsNotInBK ), prStr,
                     '\n'.join( sorted( cc.existingLFNsNotInBK[0:maxFiles] ) ) ) )
    if fixIt:
      gLogger.always( "Going to fix them, by removing from the FC and storage" )
      res = rm.removeFile( cc.existingLFNsNotInBK )
      if res['OK']:
        success = len( res['Value']['Successful'] )
        failures = 0
        errors = {}
        for reason in res['Value']['Failed'].values():
          reason = str( reason )
          if reason != "{'BookkeepingDB': 'File does not exist'}":
            errors[reason] = errors.setdefault( reason, 0 ) + 1
            failures += 1
        gLogger.always( "\t%d success, %d failures%s" % ( success, failures, ':' if failures else '' ) )
        for reason in errors:
          gLogger.always( '\tError %s : %d files' % ( reason, errors[reason] ) )
    else:
      gLogger.always( "Use --FixIt to fix it (remove from FC and storage)" )
  else:
    gLogger.always( "No files in FC not in BK -> OK!" )



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
  Script.registerSwitch( "P:", "Productions=",
                         "   Production ID to search (comma separated list)", dmScript.setProductions )
  Script.registerSwitch( '', 'FixIt', '   Take action to fix the catalogs' )
  Script.parseCommandLine( ignoreErrors = True )

  # imports
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
