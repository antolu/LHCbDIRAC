#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-bookkeeping-production-informations
# Author : Zoltan Mathe
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$ $"
import DIRAC
from DIRAC.Core.Base import Script

import types

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <Production> ' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 1:
  usage()
  
exitCode = 0

from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()
prod=long(args[0])

res = bk.getProductionInformations_new(prod)

if res['OK']:
    val = res['Value']
    print "Production Info: "
    
    infs = val["Production informations"]
    if infs != None:
      for inf in infs:
        if inf[2] != None:
          print '    Configuration Name:',inf[0]
          print '    Configuration Version:',inf[1]
          print '    Event type:', inf[2]
     
    steps = val['Steps']

    if type(steps) == types.StringType:
      print steps
    else:
      
      for i in steps:
        
        print "-----------------------" 
        print " StepName: %s "%(i[0])
        print "    ApplicationName    : %s"%(i[1])
        print "    ApplicationVersion : %s"%(i[2])
        print "    OptionFiles        : %s"%(i[3])
        print "    DDB                : %s"%(i[4])
        print "    CONDDB             : %s"%(i[5])
        print "    ExtraPackages      :%s"%(i[6])
        print "-----------------------"
    print "Number of Steps  ",val["Number of jobs"][0][0]
    files = val["Number of files"]
    if len(files) != 0:
      print "Total number of files:",files[0][2]
    else:
      print "Total number of files: 0"
    for file in files:
      print "         " + str(file[1])+":"+str(file[0])
    nbevent = val["Number of events"]
    if len(nbevent) != 0:
      print "Number of events"
      print "File Type".ljust(20)+"Number of events".ljust(20)+"Event Type".ljust(20)+"EventInputStat"
      for i in nbevent:
        print str(i[0]).ljust(20)+str(i[1]).ljust(20)+str(i[2]).ljust(20)+str(i[3])
    else:
      print "Number of events",0
    print 'Path: ',  val['Path']
else:
    print "ERROR %s: %s" % (str(prod),res['Message'] )
    exitCode = 2
DIRAC.exit(exitCode)
