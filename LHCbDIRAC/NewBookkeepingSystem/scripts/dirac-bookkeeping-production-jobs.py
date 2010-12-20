#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-bookkeeping-production-jobs
# Author :  Zoltan Mathe
########################################################################
"""
  Retrieve from Bookkeeping the number of Jobs at each Site for a given Production
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script

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

from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()
prod = long( args[0] )

res = bk.getNbOfJobsBySites( prod )

if res['OK']:
    sites = res['Value']
    print 'Site Name   : Number of jobs'
    for site in sites:
      print site[1] + ':' + str( site[0] )
else:
    print "ERROR %s: %s" % str( prod ), res['Message']
    exitCode = 2

