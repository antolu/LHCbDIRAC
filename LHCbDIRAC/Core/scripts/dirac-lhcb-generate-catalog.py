#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-lhcb-generate-catalog.py
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.5 $"

import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch( "n:", "site=", "DIRAC Site Name" )
Script.registerSwitch( "f:", "catalog=", "Catalogue File Name e.g. can be /path/to/catalog/file.xml, defaults to pool_xml_catalog.xml in PWD" )
Script.registerSwitch( "d:", "depth=", "Optional ancestor depth to be queried from the Bookkeeping system" )
Script.registerSwitch( "i", "ignore", "Optional flag to ignore missing files" )
Script.parseCommandLine( ignoreErrors = True )

from DIRAC.Interfaces.API.Dirac                              import Dirac
from LHCbDIRAC.BookkeepingSystem.Client.AncestorFiles        import getAncestorFiles

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
ignore = False
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
  elif switch[0].lower() in ('i','ignore'):
    ignore = True

if ancestorDepth:
  try:
    ancestorDepth = int(ancestorDepth)
  except Exception,x:
    print 'ERROR: ancestor depth must be an integer %s' %x
    DIRAC.exit(2)
  if ancestorDepth:
    result = getAncestorFiles(lfns,ancestorDepth)
    if not result:
      print 'ERROR: null result from getAncestorFiles() call'
      DIRAC.exit(2)
    if not result['OK']:
      print 'ERROR: problem during getAncestorFiles() call\n%s' %result['Message']
      DIRAC.exit(2)
    lfns = result['Value']

result = dirac.getInputDataCatalog(lfns,siteName,catalogName,ignoreMissing=ignore)
if not result['OK']:
  exitCode=2
  print 'ERROR: %s' %result['Message']
  DIRAC.exit(exitCode)

DIRAC.exit(exitCode)
