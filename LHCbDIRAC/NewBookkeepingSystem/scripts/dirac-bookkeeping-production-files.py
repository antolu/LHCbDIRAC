#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-bookkeeping-production-files.py
# Author :  Zoltan Mathe
########################################################################
"""
  Retrieve files of a given type for a production
"""
__RCSID__ = "$Id$"
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... ProdID Type' % Script.scriptName,
                                     'Arguments:',
                                     '  ProdID:   Production ID (integer)',
                                     '  Type:     File Type (ie, ALL, DST, SIM, DIGI, RDST, MDF)' ] ) )
Script.parseCommandLine()
args = Script.getPositionalArgs()

if not len( args ) == 2:
  Script.showHelp()

try:
  prodID = int( args[0] )
except:
  Script.showHelp()
type = args[1]

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.List import sortList

client = RPCClient( 'Bookkeeping/NewBookkeepingManager' )
res = client.getProductionFiles( prodID, type )
if not res['OK']:
  print 'ERROR: Failed to retrieve production files: %s' % res['Message']
else:
  if not res['Value']:
    print 'No files found for production %s with type %s' % ( prodID, type )
  else:
    print  '%s %s %s %s' % ( 'FileName'.ljust( 100 ), 'Size'.ljust( 10 ), 'GUID'.ljust( 40 ), 'Replica'.ljust( 8 ) )
    for lfn in sortList( res['Value'].keys() ):
      size = res['Value'][lfn]['FileSize']
      guid = res['Value'][lfn]['GUID']
      hasReplica = res['Value'][lfn]['GotReplica']
      print '%s %s %s %s' % ( lfn.ljust( 100 ), str( size ).ljust( 10 ), guid.ljust( 40 ), hasReplica.ljust( 8 ) )
