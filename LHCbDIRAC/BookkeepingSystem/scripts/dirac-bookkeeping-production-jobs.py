#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-bookkeeping-production-jobs
# Author : Zoltan Mathe
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$ $"
import DIRAC
from DIRAC.Core.Base import Script

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <Production> ' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 1:
  usage()
  
exitCode = 0

from DIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()
prod=long(args[0])

res = bk.getNbOfJobsBySites(prod)

if res['OK']:
    sites = res['Value']
    print 'Site Name   : Number of jobs'
    for site in sites:
      print site[1]+':' +str(site[0])
else:
    print "ERROR %s: %s" % str(prod),res['Message'] 
    exitCode = 2

