#!/usr/bin/env python
########################################################################
# File :    dirac-bookkeeping-production-jobs
# Author :  Zoltan Mathe
########################################################################
"""
  Retrieve from Bookkeeping the number of Jobs at each Site for a given Production
"""
__RCSID__ = "$Id: dirac-bookkeeping-production-jobs.py 69359 2013-08-08 13:57:13Z phicharp $"

from DIRAC.Core.Base import Script
import DIRAC

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... ProdID' % Script.scriptName,
                                     'Arguments:',
                                     '  ProdID:   Production ID' ] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

if len( args ) < 1:
  Script.showHelp()

exitCode = 0

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()
try:
  prod = long( args[0] )
except:
  Script.showHelp()
  DIRAC.exit( 1 )

res = bk.getNbOfJobsBySites( prod )

if res['OK']:
  if not res['Value']:
    print "No jobs for production", prod
    DIRAC.exit( 0 )
  sites = dict( [( site, num ) for num, site in res['Value']] )
  shift = 0
  for site in sites:
    shift = max( shift, len( site ) + 2 )
  print 'Site Name'.ljust( shift ), 'Number of jobs'
  for site in sorted( sites ):
    print site.ljust( shift ), str( sites[site] )
else:
  print "ERROR getting number of jobs for %s:" % str( prod ), res['Message']
  exitCode = 2

