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


# Now setting the followings:
#     System
#     {
#       Bookkeeping
#       {
#         Production
#         {
#           Databases
#           {
#             BookkeepingDB
#             {
#               LHCbDIRACBookkeepingUser=LHCB_DIRACBOOKKEEPING_INT_R
#               LHCbDIRACBookkeepingServer=LHCB_DIRACBOOKKEEPING_INT_W
#               LHCbDIRACBookkeepingPassword=passwd
#               LHCbDIRACBookkeepingTNS=int12r
#             }
#           }
#         }
#       }
#     }
#
#     Operations
#     {
#       Defaults
#       {
#         Services
#         {
#           Catalogs
#           {
#             BookkeepingDB
#             {
#               AccessType=Write
#               Status=Active
#               Conditions
#               {
#                 WRITE=Proxy=group.not_in(lhcb_user)
#               }
#             }
#           }
#         }
#       }


for sct in ['Systems/Bookkeeping',
            'Systems/Bookkeeping/Production',
            'Systems/Bookkeeping/Production/Databases',
            'Systems/Bookkeeping/Production/Databases/BookkeepingDB',
            'Operations',
            'Operations/Defaults',
            'Operations/Defaults/Services',
            'Operations/Defaults/Services/Catalogs',
            'Operations/Defaults/Services/Catalogs/BookkeepingDB',
            'Operations/Defaults/Services/Catalogs/BookkeepingDB/Conditions']:
  res = csAPI.createSection( sct )
  if not res['OK']:
    gLogger.error( res['Message'] )
    exit( 1 )

csAPI.setOption( 'Systems/Bookkeeping/Production/Databases/BookkeepingDB/LHCbDIRACBookkeepingUser',
                 'LHCB_DIRACBOOKKEEPING_INT_R' )
csAPI.setOption( 'Systems/Bookkeeping/Production/Databases/BookkeepingDB/LHCbDIRACBookkeepingServer',
                 'LHCB_DIRACBOOKKEEPING_INT_W' )
csAPI.setOption( 'Systems/Bookkeeping/Production/Databases/BookkeepingDB/LHCbDIRACBookkeepingPassword', passwd )
csAPI.setOption( 'Systems/Bookkeeping/Production/Databases/BookkeepingDB/LHCbDIRACBookkeepingTNS', 'int12r' )

csAPI.setOption( 'Operations/Defaults/Services/Catalogs/BookkeepingDB/AccessType', 'Write' )
csAPI.setOption( 'Operations/Defaults/Services/Catalogs/BookkeepingDB/Status', 'Active' )
csAPI.setOption( 'Operations/Defaults/Services/Catalogs/BookkeepingDB/Conditions/WRITE',
                 'Proxy=group.not_in(lhcb_user)' )


# Now setting a FileCatalogs section as the following:
#     FileCatalogs
#     {
#       BookkeepingDB
#       {
#         AccessType = Read-Write
#         Status = Active
#         Master = True
#       }
#     }
for sct in ['Resources/FileCatalogs',
            'Resources/FileCatalogs/BookkeepingDB']:
  res = csAPI.createSection( sct )
  if not res['OK']:
    print res['Message']
    exit( 1 )

csAPI.setOption( 'Resources/FileCatalogs/BookkeepingDB/AccessType', 'Write' )
csAPI.setOption( 'Resources/FileCatalogs/BookkeepingDB/Status', 'Active' )
csAPI.setOption( 'Resources/FileCatalogs/BookkeepingDB/CatalogURL', 'Bookkeeping/BookkeepingManager' )


csAPI.commit()
