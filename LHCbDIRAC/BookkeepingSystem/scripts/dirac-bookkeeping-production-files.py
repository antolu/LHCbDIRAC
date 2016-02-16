#!/usr/bin/env python
########################################################################
# File :    dirac-bookkeeping-production-files.py
# Author :  Zoltan Mathe
########################################################################
"""
  Retrieve files of a given type for a production
"""
__RCSID__ = "$Id: dirac-bookkeeping-production-files.py 69359 2013-08-08 13:57:13Z phicharp $"
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... ProdID Type' % Script.scriptName,
                                     'Arguments:',
                                     '  ProdID:   Production ID (integer)',
                                     '  Type:     File Type (For example: ALL, DST, SIM, DIGI, RDST, MDF)' ] ) )
Script.parseCommandLine()
args = Script.getPositionalArgs()

if not len( args ) == 2:
  Script.showHelp()

try:
  prodID = int( args[0] )
except:
  Script.showHelp()
filetype = args[1]

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

client = BookkeepingClient()
res = client.getProductionFiles( prodID, filetype )
if not res['OK']:
  print 'ERROR: Failed to retrieve production files: %s' % res['Message']
else:
  if not res['Value']:
    print 'No files found for production %s with type %s' % ( prodID, filetype )
  else:
    print  '%s %s %s %s %s' % ( 'FileName'.ljust( 100 ),
                               'Size'.ljust( 10 ),
                               'GUID'.ljust( 40 ),
                               'Replica'.ljust( 8 ),
                               'Visible'.ljust( 8 ) )
    for lfn in sorted( res['Value'] ):
      size = res['Value'][lfn]['FileSize']
      guid = res['Value'][lfn]['GUID']
      hasReplica = res['Value'][lfn]['GotReplica']
      visible = res['Value'][lfn]['Visible']
      print '%s %s %s %s %s' % ( lfn.ljust( 100 ),
                                str( size ).ljust( 10 ),
                                guid.ljust( 40 ),
                                str( hasReplica ).ljust( 8 ),
                                str( visible ).ljust( 8 ) )

