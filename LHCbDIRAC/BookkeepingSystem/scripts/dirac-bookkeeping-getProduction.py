#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-bookkeeping-getProduction
# Author : Zoltan Mathe
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$ $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s ProgramName=Boole ProgramVersion=v13r3 Evt=ALL' %(Script.scriptName)
  print 'OR'
  print 'Usage: %s ProgramName=ALL ProgramVersion=ALL Evt=13463000' %(Script.scriptName)
  print 'OR'
  print 'Usage: %s ProgramName=ALL ProgramVersion=v13r3 Evt=ALL' %(Script.scriptName)
  print 'OR'
  print 'Usage: %s ProgramName=Brunel ProgramVersion=ALL Evt=ALL' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 3:
  usage()
  
exitCode = 0

from DIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()

prgn = ''
prgv = ''
evt = ''
for i in range(0,len(args)):
  arg = args[i]
  values = arg.split('=')
  if len(values) == 1:
    print 'Wrong format!'
    DIRAC.exit(2)
  if values[0]=='ProgramName':
    prgn = values[1]
  elif values[0]=='ProgramVersion':
    prgv = values[1]
  elif values[0]=='Evt':
    evt = values[1]
  
res = bk.getProductionsWithPrgAndEvt(prgn,prgv,evt)
if not res['OK']:
  print 'ERROR',res['Message']
else:
  values = res['Value']
  print  '%s %s %s' % ('EventTypeId'.ljust(20),'Description'.ljust(50),'Production'.ljust(40))
  for record in values:
    eid = record[0]
    desc = record[1]
    prod = record[2]
    print '%s %s %s' % (str(eid).ljust(20),desc.ljust(50),str(prod).ljust(40))