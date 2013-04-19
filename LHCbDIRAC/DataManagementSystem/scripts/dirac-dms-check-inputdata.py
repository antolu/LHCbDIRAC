#!/usr/bin/env python
"""
Check input files availability for a (list of) jobs
"""

__RCSID__ = "$transID: dirac-transformation-debug.py 61232 2013-01-28 16:29:21Z phicharp $"


def inaccessibleReplicas( lfn, se ):
  if type( se ) == type( '' ):
    seList = [se]
  else:
    seList = se
  failed = {}
  for se in seList:
    res = rm.getReplicaMetadata( lfn, se )
    if not res['OK']:
      gLogger.always( 'Error getting metadata of %s at %s' % ( lfn, se ) )
      continue
    if lfn in res['Value']['Failed']:
      failed[se] = res['Value']['Failed'][lfn]
  return failed

def prettyMsg( msg, msgList ):
  areIs = 's are' if len( msgList ) > 1 else ' is'
  gLogger.always( 'The following file%s %s:\n%s' % ( areIs, msg, '\n'.join( msgList ) ) )

#====================================
if __name__ == "__main__":

  from DIRAC.Core.Base import Script
  import sys
  from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript
  dmScript = DMScript()

  Script.registerSwitch( 'v', 'Verbose', '   Set verbose mode' )

  Script.parseCommandLine( ignoreErrors = True )
  import DIRAC
  from DIRAC import gLogger, gConfig
  from DIRAC.Core.DISET.RPCClient import RPCClient

  verbose = False
  for opt, val in Script.getUnprocessedSwitches():
    if opt in ( 'v', 'Verbose' ):
      verbose = True

  args = Script.getPositionalArgs()
  try:
    jobs = [int( job ) for job in args[0].split( ',' )]
  except:
    gLogger.fatal( "Invalid list of jobIDs" )
    DIRAC.exit( 2 )

  from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
  rm = ReplicaManager()

  monitoring = RPCClient( 'WorkloadManagement/JobMonitoring' )
  res = monitoring.getJobsSites( jobs )
  if not res['OK']:
    gLogger.fatal( 'Error getting job sites', res['Message'] )
    DIRAC.exit( 2 )
  gLogger.setLevel( 'FATAL' )
  jobSites = res['Value']
  sep = ''
  for jobID in jobs:
    try:
      pbFound = False
      gLogger.always( '%sJob %d' % ( sep, jobID ) )
      sep = '=====================================\n'
      res = monitoring.getJobJDL( jobID )
      if not res['OK']:
        gLogger.always( 'Error getting job %d JDL' % jobID, res['Message'] )
        continue
      jdl = res['Value'].splitlines()
      ind = 0
      found = 0
      for l in jdl:
        if 'InputData =' in l:
          found = ind
        if ind == found + 1:
          if '{' in l:
            found = ind + 1
          else:
            end = ind
        if found and '}' in l:
          end = ind
          break
        ind += 1
      if not found:
        gLogger.always( 'No InputData field found in JDL for job %d' % jobID )
        continue
      if end == found + 1:
        inputData = dmScript.getLFNsFromList( jdl[found].split( '"' )[1] )
      else:
        inputData = dmScript.getLFNsFromList( eval( '[' + ''.join( jdl[found:end] ) + ']' ) )
      inputData.sort()
      if verbose:
        gLogger.always( 'Input Data for job %d\n%s' % ( jobID, '\n'.join( inputData ) ) )
      site = jobSites.get( jobID, {} ).get( 'Site', 'Unknown' )
      if verbose:
        gLogger.always( 'Site: %s........' % site )
      res = gConfig.getOptionsDict( '/Resources/Sites/LCG/%s' % site )
      seList = []
      if res['OK'] and type( res['Value'] ) == type( {} ) and 'SE' in res['Value']:
        seList = res['Value']['SE'].replace( ' ', '' ).split( ',' )
      if verbose:
        gLogger.always( 'SEs: %s' % str( seList ) )

      res = rm.getReplicas( inputData )
      if not res['OK']:
        gLogger.always( "Error getting replicas for %d files" % len( inputData ), res['Message'] )
        continue
      notInFC = res['Value']['Failed']
      if notInFC:
        pbFound = True
        prettyMsg( 'not in the FC', notInFC )
      replicas = res['Value']['Successful']
      notFoundReplicas = replicas.keys()
      missingReplicas = []
      accessibleReplicas = []
      for lfn in [l for l in inputData if l in replicas]:
        for se in replicas[lfn]:
          if se in seList:
            # Found a replica at the site
            if lfn in notFoundReplicas:
              notFoundReplicas.remove( lfn )
            inaccessible = inaccessibleReplicas( lfn, se )
            if se in inaccessible:
              reason = inaccessible[se]
              otherReplicas = [s for s in replicas[lfn] if s != se]
              inaccessible = inaccessibleReplicas( lfn, otherReplicas )
              accessible = [s for s in otherReplicas if s not in inaccessible]
              missingReplicas.append( ( lfn, se, reason, 'Accessible at %s' % str( accessible ) if accessible else 'No other accessible replicas' ) )
              prStr = 'not '
            else:
              prStr = ''
            if verbose:
              gLogger.always( '%s is %s accessible at %s' % ( lfn, prStr, se ) )
      if missingReplicas:
        pbFound = True
        msgList = ['%s at %s: %s\n\t%s' % x for x in missingReplicas]
        prettyMsg( 'not accessible', msgList )
      if notFoundReplicas:
        pbFound = True
        prettyMsg( 'not found at %s' % site, notFoundReplicas )
    except Exception, e:
      gLogger.always( '%s' % e )
      pass
    finally:
      if not pbFound:
        gLogger.always( 'No particular problem was found with %d input files at %s' % ( len( inputData ), site ) )
