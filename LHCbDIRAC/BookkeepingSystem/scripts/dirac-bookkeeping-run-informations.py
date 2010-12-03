#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/branches/LHCbDIRAC/BookkeepingSystem/bk_2010101501/scripts/dirac-bookkeeping-run-informations.py $
# File :   dirac-bookkeeping-run-informations
# Author : Zoltan Mathe
########################################################################
__RCSID__   = "$Id: dirac-bookkeeping-run-informations.py 25383 2010-05-26 14:39:53Z zmathe $"
__VERSION__ = "$ $"
import DIRAC
from DIRAC.Core.Base import Script

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <Run number> ' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 1:
  usage()
  
exitCode = 0

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()
run=long(args[0])

res = bk.getRunInformations(int(run))

if res['OK']:
    val = res['Value']
    print "Run  Informations: "
    print "Run Start:".ljust(50),str(val['RunStart'])
    print "Run End:".ljust(50),str(val['RunEnd'])
    print "  Configuration Name:".ljust(50),val['Configuration Name']
    print "  Configuration Version:".ljust(50),val['Configuration Version']
    print "  FillNumber:".ljust(50),val['FillNumber']
    print "  Data taking description: ".ljust(50),val['DataTakingDescription']
    print "  Processing pass: ".ljust(50),val['ProcessingPass']
    print "  Stream: ".ljust(50),val['Stream']
    print "  FullStat: ".ljust(50)+str(val['FullStat']).ljust(50)+" Total: ".ljust(10)+str(sum(val['FullStat']))
    print "  Number of events: ".ljust(50) +str(val['Number of events']).ljust(50)+" Total:".ljust(10)+str(sum(val['Number of events']))
    print "  Number of file: ".ljust(50)+str(val['Number of file']).ljust(50) + " Total: ".ljust(10)+str(sum(val['Number of file']))
    print "  File size: ".ljust(50) + str(val['File size']).ljust(50)+ " Total: ".ljust(10) +str(sum(val['File size']))
    
else:
    print "ERROR %s: %s" % (str(run),res['Message'] )
    exitCode = 2
DIRAC.exit(exitCode)
