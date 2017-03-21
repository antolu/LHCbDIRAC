#!/usr/bin/env python
'''
    Check if the files are in the BK, the FC and the SEs they are supposed to be in.

    Uses the DM script switches, and, unless a list of LFNs is provided:
    1) If --Directory is used: get files in FC directories
    2) If --Production or --BK options is used get files in the FC directories from the BK

    If --FixIt is set, takes actions:
      Missing files: remove from SE and FC
      No replica flag: set it (in the BK)
      Not existing in SE: remove replica or file from the catalog
      Bad checksum: remove replica or file from SE and catalogs if no good replica
'''
__RCSID__ = "$Id$"

from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers

def __getSEsFromOptions( dmScript ):
  seList = dmScript.getOption( 'SEs', [] )
  sites = dmScript.getOption( 'Sites', [] )
  if sites:
    siteSEs = []
    dmsHelper = DMSHelpers()
    for site in sites:
      siteSEs += dmsHelper.getSEsForSite( site ).get( 'Value', [] )
    if seList and siteSEs:
      seList = list( set( seList ) & set( siteSEs ) )
    else:
      seList += siteSEs
  return seList

if __name__ == '__main__':

  # Script initialization
  from DIRAC.Core.Base import Script
  from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       '  %s [option|cfgfile] [values]' % Script.scriptName, ] ) )
  dmScript = DMScript()
  dmScript.registerDMSwitches()
  Script.registerSwitch( '', 'FixIt', '   Take action to fix the catalogs and storage' )
  Script.registerSwitch( '', 'Replace', '   Replace bad or missing replicas (default=False)' )
  Script.registerSwitch( '', 'NoBK', '   Do not check with BK' )
  Script.registerSwitch( '', 'Verbose', '   Set logging mode to INFO' )
  Script.parseCommandLine( ignoreErrors = True )

  fixIt = False
  bkCheck = True
  replace = False
  verbose = False
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'FixIt':
      fixIt = True
    elif switch[0] == 'NoBK':
      bkCheck = False
    elif switch[0] == 'Replace':
      replace = True
    elif switch[0] == 'Verbose':
      verbose = True

  # imports
  from LHCbDIRAC.DataManagementSystem.Client.ConsistencyChecks import ConsistencyChecks
  from DIRAC import gLogger
  if verbose:
    gLogger.setLevel( 'INFO' )
  cc = ConsistencyChecks()
  cc.directories = dmScript.getOption( 'Directory', [] )
  cc.lfns = dmScript.getOption( 'LFNs', [] ) + [lfn for arg in Script.getPositionalArgs() for lfn in arg.split( ',' )]
  bkQuery = dmScript.getBKQuery( visible = 'All' )
  if bkQuery:
    bkQuery.setOption( 'ReplicaFlag', 'All' )
    cc.bkQuery = bkQuery

  cc.seList = __getSEsFromOptions( dmScript )
  from LHCbDIRAC.DataManagementSystem.Client.CheckExecutors import doCheckFC2SE
  try:
    doCheckFC2SE( cc, bkCheck = bkCheck, fixIt = fixIt, replace = replace )
  except RuntimeError as e:
    gLogger.fatal( str( e ) )
  except Exception as e:
    gLogger.exception( 'Exception', lException = e )
