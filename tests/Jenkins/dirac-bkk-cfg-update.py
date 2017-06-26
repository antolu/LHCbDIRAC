#!/usr/bin/env python
""" update local cfg file for the bkk database
"""

from DIRAC.Core.Base import Script
from DIRAC import gLogger

Script.registerSwitch( 'p:', 'passwd=', '   <passwd>, is the password of the accounts' )
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... passwd' % Script.scriptName,
                                     'Arguments:',
                                     '  passwd: password' ] ) )

Script.parseCommandLine()
passwd = ''
for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() in ( 'p', 'passwd' ):
    passwd = switch[1]

if not passwd:
  gLogger.error( "Error: No argument provided\n%s:" % Script.scriptName )
  Script.showHelp()
  exit( 1 )

from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
csAPI = CSAPI()

for sct in ['Systems/Bookkeeping',
            'Systems/Bookkeeping/Production',
            'Systems/Bookkeeping/Production/Databases',
            'Systems/Bookkeeping/Production/Databases/BookkeepingDB' ]:
  res = csAPI.createSection( sct )
  if not res['OK']:
    gLogger.error( res['Message'] )
    exit( 1 )

csAPI.setOption( 'Systems/Bookkeeping/Production/Databases/BookkeepingDB/LHCbDIRACBookkeepingUser', 'LHCB_DIRACBOOKKEEPING_INT_R' )
csAPI.setOption( 'Systems/Bookkeeping/Production/Databases/BookkeepingDB/LHCbDIRACBookkeepingServer', 'LHCB_DIRACBOOKKEEPING_INT_W' )
csAPI.setOption( 'Systems/Bookkeeping/Production/Databases/BookkeepingDB/LHCbDIRACBookkeepingPassword', passwd )
csAPI.setOption( 'Systems/Bookkeeping/Production/Databases/BookkeepingDB/LHCbDIRACBookkeepingTNS', 'int12r' )

csAPI.commit()
