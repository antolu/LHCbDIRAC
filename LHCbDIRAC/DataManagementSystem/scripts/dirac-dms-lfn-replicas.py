#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/tags/DIRAC/Interfaces/if_2010110301/scripts/dirac-dms-lfn-replicas.py $
# File :   dirac-admin-lfn-replicas
# Author : Stuart Paterson
########################################################################
"""
    Show replicas for a (set of) LFNs
"""
__RCSID__   = "$Id: dirac-dms-lfn-replicas.py 22056 2010-02-23 18:41:29Z rgracian $"
__VERSION__ = "$Revision: 1.1 $"

from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, printDMResult
import DIRAC

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerFileSwitches()
  Script.registerSwitch( 'a', "All", "  Also show inactive replicas" )

  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] [<LFN>] [<LFN>...]' % Script.scriptName, ] ) )

  Script.parseCommandLine( ignoreErrors = False )
  lfns = dmScript.getOption('LFNs', [])
  lfns += Script.getPositionalArgs()
  lfnList = []
  for lfn in lfns:
    try:
      f = open(lfn, 'r')
      lfnList += f.read().splitlines()
      f.close()
    except:
      lfnList.append(lfn)

  active = True
  switches = Script.getUnprocessedSwitches()
  for switch in switches:
    opt = switch[0].lower()
    if opt in ( "a", "all" ):
      active = False

  if not lfnList or len(lfnList) < 1:
    Script.showHelp()
    DIRAC.exit(2)

  from DIRAC.Interfaces.API.Dirac                         import Dirac
  dirac = Dirac()
  exitCode = 0

  DIRAC.exit( printDMResult( dirac.getReplicas( lfnList, active = active, printOutput = False ),
                             empty="No allowed SE found", script="dirac-dms-lfn-replicas") )
