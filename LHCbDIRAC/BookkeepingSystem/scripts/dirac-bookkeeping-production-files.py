#!/usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/BookkeepingSystem/scripts/dirac-bookkeeping-production-files.py,v 1.1 2009/02/11 11:42:40 acsmith Exp $
# File :   dirac-bookkeeping-get-production-files.py  
########################################################################
__RCSID__   = "$Id: dirac-bookkeeping-production-files.py,v 1.1 2009/02/11 11:42:40 acsmith Exp $"
__VERSION__ = "$ $"

from DIRAC import gLogger
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.List import sortList
from DIRAC.ConfigurationSystem.Client import PathFinder
import sys

if not len(sys.argv) == 3:
  print 'Usage: getProductionLFNs.py prodID <ALL,DST,SIM,DIGI,RDST,MDF>'
  sys.exit()
else:
  prodID = int(sys.argv[1])
  type = sys.argv[2]

client = RPCClient('Bookkeeping/BookkeepingManager')
res = client.getProductionFiles(prodID,type)
if not res['OK']:
  print 'Failed to retrieve production files: %s' % res['Message']
else:
  if not res['Value']:
    print 'No files found for production %s with type %s' % (prodID,type)
  else:
    print  '%s %s %s %s' % ('FileName'.ljust(100),'Size'.ljust(10),'GUID'.ljust(40),'Replica'.ljust(8))
    for lfn in sortList(res['Value'].keys()):
      size = res['Value'][lfn]['FilesSize']
      guid = res['Value'][lfn]['GUID']
      hasReplica = res['Value'][lfn]['GotReplica']
      #print lfn,dict['GotReplica'],dict['FilesSize'],dict['GUID']
      print '%s %s %s %s' % (lfn.ljust(100),str(size).ljust(10),guid.ljust(40),hasReplica.ljust(8))

