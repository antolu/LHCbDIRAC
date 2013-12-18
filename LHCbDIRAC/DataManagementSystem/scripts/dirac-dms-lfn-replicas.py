#!/usr/bin/env python
########################################################################
# File :   dirac-admin-lfn-replicas
# Author : Stuart Paterson
########################################################################
"""
    Show replicas for a (set of) LFNs
"""
__RCSID__ = "$Id$"
__VERSION__ = "$Revision$"

from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, printDMResult
import DIRAC

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerFileSwitches()
  Script.registerSwitch( 'a', "All", "  Also show inactive replicas" )

  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       '  %s [option|cfgfile] [<LFN>] [<LFN>...]' % Script.scriptName, ] ) )

  Script.parseCommandLine( ignoreErrors = False )
  for lfn in Script.getPositionalArgs():
    dmScript.setLFNsFromFile( lfn )
  lfnList = dmScript.getOption( 'LFNs', [] )

  active = True
  switches = Script.getUnprocessedSwitches()
  for switch in switches:
    if switch[0] in ( "a", "All" ):
      active = False

  if not lfnList or len( lfnList ) < 1:
    Script.showHelp()

  # from DIRAC.Interfaces.API.Dirac                         import Dirac
  # dirac = Dirac()

  from DIRAC.DataManagementSystem.Client.ReplicaManager                  import ReplicaManager
  from DIRAC import gLogger
  rm = ReplicaManager()

  while True:
    res = rm.getActiveReplicas( lfnList ) if active else rm.getReplicas( lfnList )
    if not res['OK']:
      break
    if active and not res['Value']['Successful'] and not res['Value']['Failed']:
      active = False
    else:
      break
  if res['OK']:
    if active:
      res = rm.checkActiveReplicas( res['Value'] )
      value = res['Value']
    else:
      lfns = []
      replicas = res['Value']['Successful']
      value = {'Failed': res['Value']['Failed'], 'Successful' : {}}
      for lfn in sorted( replicas ):
        for se in sorted( replicas[lfn] ):
          res = rm.getCatalogReplicaStatus( {lfn:se} )
          if not res['OK']:
            value['Failed'][lfn] = "Can't get replica status"
          else:
            value['Successful'].setdefault( lfn, {} )[se] = "(%s) %s" % ( res['Value']['Successful'][lfn], replicas[lfn][se] )
      res = DIRAC.S_OK( value )
  # DIRAC.exit( printDMResult( dirac.getReplicas( lfnList, active=active, printOutput=False ),
  #                           empty="No allowed SE found", script="dirac-dms-lfn-replicas" ) )
  DIRAC.exit( printDMResult( res,
                             empty = "No allowed replica found", script = "dirac-dms-lfn-replicas" ) )
