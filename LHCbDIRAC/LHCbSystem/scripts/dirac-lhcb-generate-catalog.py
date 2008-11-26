#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/scripts/dirac-lhcb-generate-catalog.py,v 1.1 2008/11/26 11:50:05 paterson Exp $
# File :   dirac-lhcb-generate-catalog.py
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-lhcb-generate-catalog.py,v 1.1 2008/11/26 11:50:05 paterson Exp $"
__VERSION__ = "$Revision: 1.1 $"

from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch( "n:", "Site=", "DIRAC Site Name" )
Script.registerSwitch( "f:", "Catalog=", "Catalogue File Name e.g. can be /path/to/catalog/file.xml, defaults to pool_xml_catalog.xml in PWD" )
Script.parseCommandLine( ignoreErrors = True )

from DIRAC.Interfaces.API.Dirac                              import Dirac

args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <LFN> |[<LFN>]  --Site=<DIRAC Site Name> --Catalog=<Catalog File Name>' %(Script.scriptName)
  print 'Try --help, -h for more information.'
  DIRAC.exit(2)

if not args or not Script.getUnprocessedSwitches():
  usage()

dirac=Dirac()
exitCode = 0

siteName = ''
catalogName = 'pool_xml_catalog.xml'
lfns = args

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() in ('n','site'):
    siteName=switch[1]
  elif switch[0].lower() in ('f','catalog'):
    catalogName=switch[1]

result = dirac.getInputDataCatalog(lfns,siteName,catalogName)
if not result['OK']:
  exitCode=2
  print 'ERROR: %s' %result['Message']
  DIRAC.exit(exitCode)

DIRAC.exit(exitCode)