#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/BookkeepingSystem/scripts/dirac-bookkeeping-production-informations.py,v 1.5 2009/05/08 15:23:25 zmathe Exp $
# File :   dirac-bookkeeping-production-informations
# Author : Zoltan Mathe
########################################################################
__RCSID__   = "$Id: dirac-bookkeeping-production-informations.py,v 1.5 2009/05/08 15:23:25 zmathe Exp $"
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

    if steps != None:
      for step in steps:
        print 'Step0:' + str(step[0])+'-'+str(step[1])
        print '      Option files: '+str(step[2])
        print '      DDDb: '+str(step[3])
        print '      ConDDb: '+str(step[4])
        
        print 'Step1:' + str(step[5])+'-'+str(step[6])
        print '      Option files: '+str(step[7])
        print '      DDDb: '+str(step[8])
        print '      ConDDb: '+str(step[9])
        
        print 'Step2:' + str(step[10])+'-'+str(step[11])
        print '      Option files: '+str(step[12])
        print '      DDDb: '+str(step[13])
        print '      ConDDb: '+str(step[14])
        
        print 'Step3:' + str(step[15])+'-'+str(step[16])
        print '      Option files: '+str(step[17])
        print '      DDDb: '+str(step[18])
        print '      ConDDb: '+str(step[19])
        
        print 'Step4:' + str(step[20])+'-'+str(step[21])
        print '      Option files: '+str(step[22])
        print '      DDDb: '+str(step[23])
        print '      ConDDb: '+str(step[24])
        
        print 'Step5:' + str(step[25])+'-'+str(step[26])
        print '      Option files: '+str(step[27])
        print '      DDDb: '+str(step[28])
        print '      ConDDb: '+str(step[29])
        
        print 'Step6:' + str(step[30])+'-'+str(step[31])
        print '      Option files: '+str(step[32])
        print '      DDDb: '+str(step[33])
        print '      ConDDb: '+str(step[34])
        

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
      print "Number of events",nbevent
    else:
      print "Number of events",0
else:
    print "ERROR %s: %s" % (str(prod),res['Message'] )
    exitCode = 2
DIRAC.exit(exitCode)
