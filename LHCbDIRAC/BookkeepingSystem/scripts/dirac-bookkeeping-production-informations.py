#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/BookkeepingSystem/scripts/dirac-bookkeeping-production-informations.py,v 1.3 2009/01/13 17:02:42 zmathe Exp $
# File :   dirac-bookkeeping-production-informations
# Author : Zoltan Mathe
########################################################################
__RCSID__   = "$Id: dirac-bookkeeping-production-informations.py,v 1.3 2009/01/13 17:02:42 zmathe Exp $"
__VERSION__ = "$ $"
from DIRACEnvironment import DIRAC
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

res = bk.getProductionInformations(prod)

if res['OK']:
    val = res['Value']
    print "Production Info: "
    infs = val["Production Info"]
    if infs != None:
      for info in infs:
        print info
    print "Number of jobs  ",val["Number of jobs"][0][0]
    files = val["Number of files"]
    if len(files) != 0:
      print "Total number of files:",files[0][2]
    else:
      print "Total number of files: 0"
    for file in files:
      print "         " + str(file[1])+":"+str(file[0])
    nbevent = val["Number of events"]
    if len(nbevent) != 0:
      print "Number of events",nbevent
    else:
      print "Number of events",0
else:
    print "ERROR %s: %s" % str(prod),res['Message'] 
    exitCode = 2

DIRAC.exit(exitCode)