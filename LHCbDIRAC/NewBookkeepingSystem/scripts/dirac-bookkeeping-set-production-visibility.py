#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-bookkeeping-set-production-visibility
# Author :  Zoltan Mathe
########################################################################
"""
  Change visibility of a Production in the Bookkeeping
"""
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ...' % Script.scriptName ] ) )
Script.parseCommandLine( ignoreErrors = True )

from DIRAC.Interfaces.API.DiracAdmin                     import DiracAdmin
diracAdmin = DiracAdmin()

from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()

exitCode = 0

prod = raw_input( "Production: " )
visible = raw_input( "Visible(True,False):" )
print 'Do you want to change the visibility(yes or no)?'
value = raw_input( 'Choice:' )
choice = value.lower()
if choice in ['yes', 'y']:
  if visible.upper() == 'TRUE':
    res = bk.setProductionVisible( {'Production':int( prod ), 'Visible':True} )
    if res["OK"]:
      print 'The production visible!'
    else:
      print 'ERROR:', res['Message']
      exitCode = 255
  else:
    res = bk.setProductionVisible( {'Production':int( prod ), 'Visible':False} )
    if res["OK"]:
      print 'The production invisible!'
    else:
      print 'ERROR:', res['Message']
      exitCode = 255
else:
  print 'Unexpected choice:', value
  exitCode = 2

DIRAC.exit( exitCode )
