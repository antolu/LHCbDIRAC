#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/scripts/dirac-lhcb-generate-catalog.py,v 1.3 2008/12/02 18:05:48 paterson Exp $
# File :   dirac-lhcb-generate-catalog.py
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-lhcb-generate-catalog.py,v 1.3 2008/12/02 18:05:48 paterson Exp $"
__VERSION__ = "$Revision: 1.3 $"

from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch( "n:", "site=", "DIRAC Site Name" )
Script.registerSwitch( "f:", "catalog=", "Catalogue File Name e.g. can be /path/to/catalog/file.xml, defaults to pool_xml_catalog.xml in PWD" )
Script.registerSwitch( "d:", "depth=", "Optional ancestor depth to be queried from the Bookkeeping system" )
Script.parseCommandLine( ignoreErrors = True )

from DIRAC.Interfaces.API.Dirac                              import Dirac
from DIRAC.LHCbSystem.Utilities.AncestorFiles                import getAncestorFiles

args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <LFN> |[<LFN>]  --site=<DIRAC Site Name> --catalog=<Catalog File Name>' %(Script.scriptName)
  print 'Try --help, -h for more information.'
  DIRAC.exit(2)

if not args:
  usage()

dirac=Dirac()
exitCode = 0

siteName = ''
catalogName = 'pool_xml_catalog.xml'
lfns = args
ancestorDepth = 0

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() in ('n','site'):
    siteName=switch[1]
  elif switch[0].lower() in ('f','catalog'):
    catalogName=switch[1]
  elif switch[0].lower() in ('d','depth'):
    ancestorDepth=switch[1]

if ancestorDepth:
  try:
    ancestorDepth = int(ancestorDepth)
  except Exception,x:
    print 'ERROR: ancestor depth must be an integer %s' %x
    DIRAC.exit(2)
  result = getAncestorFiles(lfns,ancestorDepth)
  if not result:
    print 'ERROR: null result from getAncestorFiles() call'
    DIRAC.exit(2)
  if not result['OK']:
    print 'ERROR: problem during getAncestorFiles() call\n%s' %result['Message']
    DIRAC.exit(2)
  lfns = result['Value']

result = dirac.getInputDataCatalog(lfns,siteName,catalogName)
if not result['OK']:
  exitCode=2
  print 'ERROR: %s' %result['Message']
  DIRAC.exit(exitCode)

DIRAC.exit(exitCode)