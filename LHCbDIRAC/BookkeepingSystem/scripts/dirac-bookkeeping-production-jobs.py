#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/branches/LHCbDIRAC/BookkeepingSystem/bk_2010101501/scripts/dirac-bookkeeping-production-jobs.py $
# File :   dirac-bookkeeping-production-jobs
# Author : Zoltan Mathe
########################################################################
__RCSID__   = "$Id: dirac-bookkeeping-production-jobs.py 18177 2009-11-11 14:02:57Z zmathe $"
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

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
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

