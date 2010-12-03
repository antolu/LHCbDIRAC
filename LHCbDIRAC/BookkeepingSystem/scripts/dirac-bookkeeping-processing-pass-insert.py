#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/branches/LHCbDIRAC/BookkeepingSystem/bk_2010101501/scripts/dirac-bookkeeping-processing-pass-insert.py $
# File :   dirac-bookkeeping-processing-pass-insert.py
# Author : Zoltan Mathe
########################################################################
__RCSID__   = "$Id: dirac-bookkeeping-processing-pass-insert.py 18177 2009-11-11 14:02:57Z zmathe $"
__VERSION__ = "$ $"

import sys,string,re
import DIRAC
from DIRAC.Core.Base import Script

Script.parseCommandLine( ignoreErrors = True )

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()


production = raw_input("Production: ")
passid = raw_input("PassId: ")
inputprod = raw_input("Total processing pass for input production: ")
simcond = raw_input('Simulation description: ')

print 'Do you want to add this new pass_index conditions? (yes or no)'
value = raw_input('Choice: ')
choice=value.lower()
if choice in ['yes','y']:
  res = bk.insertProcessing(production, passid, inputprod, simcond)
  if res['OK']:
    print 'The processing pass added successfully!'
  else:
    print "Error discovered!",res['Message']
elif choice in ['no','n']:
  print 'Aborded!'


exitCode = 0
