#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/BookkeepingSystem/scripts/dirac-bookkeeping-run-informations.py,v 1.2 2009/02/12 11:04:52 zmathe Exp $
# File :   dirac-bookkeeping-run-informations
# Author : Zoltan Mathe
########################################################################
__RCSID__   = "$Id: dirac-bookkeeping-run-informations.py,v 1.2 2009/02/12 11:04:52 zmathe Exp $"
__VERSION__ = "$ $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <Run number> ' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 1:
  usage()
  
exitCode = 0

from DIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()
run=long(args[0])

res = bk.getRunInformations(int(run))

if res['OK']:
    val = res['Value']
    print "Run  Informations: "
    print "  Configuration Name:",val['Configuration Name']
    print "  Configuration Version:",val['Configuration Version']
    print "  FillNumber:",val['FillNumber']
    print "  PhysicStat:",val['PhysicStat']
    print "  Data taking description:",val['DataTakingDescription']
    print "  Processing pass:",val['ProcessingPass']
    print "  Number of events",val['Number of events']
    print "  Number of file:",val['Number of file']
    print "  File size:",val['File size']
    
else:
    print "ERROR %s: %s" % (str(run),res['Message'] )
    exitCode = 2
DIRAC.exit(exitCode)
