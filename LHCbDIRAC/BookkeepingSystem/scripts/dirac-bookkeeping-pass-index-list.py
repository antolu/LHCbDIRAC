#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-bookkeeping-pass-index-list.py
# Author : Zoltan Mathe
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$ $"

import sys,string,re
import DIRAC
from DIRAC.Core.Base import Script

Script.parseCommandLine( ignoreErrors = True )

from DIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()

args = Script.getPositionalArgs()
list = False
insert = False


exitCode = 0


width=20
res=bk.getPass_index()
if res['OK']:
  dbresult = res['Value']
  for record in dbresult:
    print 'PassId:'
    print '   '+str(record[0]).ljust(15)
    print '        Group description:'+str(record[1])
    print '        Step 0:'+str(record[2])+'-'+str(record[3])
    print '          Option files:'+str(record[4])
    print '          DDDb:'+str(record[5])
    print '          ConDDb:'+str(record[6])
      
    if record[7] != None:
      print '        Step 1:'+str(record[7])+'-'+str(record[8])
      print '          Option files:'+str(record[9])
      print '          DDDb:'+str(record[10])
      print '          ConDDb:'+str(record[11])
      
    if record[12] != None:
      print '        Step 2:'+str(record[12])+'-'+str(record[13])
      print '          Option files:'+str(record[14])
      print '          DDDb:'+str(record[15])
      print '          ConDDb:'+str(record[16])
      
    if record[17] != None:
      print '        Step 3:'+str(record[17])+'-'+str(record[18])
      print '          Option files:'+str(record[19])
      print '          DDDb:'+str(record[20])
      print '          ConDDb:'+str(record[21])
      
    if record[22] != None:
      print '        Step 4:'+str(record[22])+'-'+str(record[23])
      print '          Option files:'+str(record[24])
      print '          DDDb:'+str(record[25])
      print '          ConDDb:'+str(record[26])
      
    if record[27] != None:
      print '        Step 5:'+str(record[27])+'-'+str(record[28])
      print '          Option files:'+str(record[29])
      print '          DDDb:'+str(record[30])
      print '          ConDDb:'+str(record[31])
    
    if record[32] != None:
      print '        Step 6:'+str(record[32])+'-'+str(record[33])
      print '          Option files:'+str(record[34])
      print '          DDDb:'+str(record[35])
      print '          ConDDb:'+str(record[36])
    
      