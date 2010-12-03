#!/usr/bin/env python
########################################################################
# $Id$ 
# File :   dirac-bookkeeping-set-production-visibility
# Author : Zoltan Mathe
########################################################################

__RCSID__   = "$Id: $"
__VERSION__ = "$ $"

import sys,string,re
import DIRAC
from DIRAC.Core.Base import Script

from DIRAC.Interfaces.API.DiracAdmin                     import DiracAdmin
from DIRAC                                               import gConfig

Script.parseCommandLine( ignoreErrors = True )

diracAdmin = DiracAdmin()

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()

exitCode = 0

prod = raw_input("Production: " )
visible = raw_input("Visible(True,False):")
print 'Do you want to change the visibility(yes or no)?'
value = raw_input('Choice:')
choice=value.lower()
if choice in ['yes','y']:
  if visible.upper() == 'TRUE':
    res = bk.setProductionVisible({'Production':int(prod), 'Visible':True})
    if res["OK"]:
      print 'The production visible!'
    else:
      print res['Message']
      DIRAC.exit(255)
  else:
    res = bk.setProductionVisible({'Production':int(prod), 'Visible':False})
    if res["OK"]:
      print 'The production invisible!'
    else:
      print res['Message']
      DIRAC.exit(255)
else:
  print 'Unespected choice:',value

DIRAC.exit(exitCode)