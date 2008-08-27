#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/BookkeepingSystem/scripts/Attic/dirac-bookkeeping-simulationconditions.py,v 1.1 2008/08/27 13:23:56 zmathe Exp $
# File :   dirac-bookkeeping-simulationconditions
# Author : Zoltan Mathe
########################################################################
__RCSID__   = "$Id: dirac-bookkeeping-simulationconditions.py,v 1.1 2008/08/27 13:23:56 zmathe Exp $"
__VERSION__ = "$ $"

import sys,string,re
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script

Script.parseCommandLine( ignoreErrors = True )

from DIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()

args = Script.getPositionalArgs()
list = False
insert = False

def usage():
  print 'Usage: %s <list> or <insert>' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 1:
  usage()

exitCode = 0

if 'list' in args:
  res=bk.getSimConditions()
  if res['OK']:
    dbresult = res['Value']
    for record in dbresult:
      print 'SimId:'+str(record[0])+'| SimDescription:'+str(record[1])+'| BeamCond:'+str(record[2])+'| BeamEnergy:'+str(record[3])+'| Generator:'+str(record[4])+'| MagneticField:'+str(record[5])+'| DetectorCond'+str(record[6])+'| Luminosity:'+str(record[7])    
elif 'insert' in args:
  desc = raw_input("SimDescription:" )
  beamcond = raw_input("BeamCond:")
  beamEnergy = raw_input("BeamEnergy:")
  generator = raw_input("Generator:")
  magneticField = raw_input("MagneticField:")
  detectorCond = raw_input("DetectorCond:")
  luminosity = raw_input("Luminosity:")  
  print 'Do you want to add this new simulation conditions? (yes or no)'
  value = raw_input('Choice:')
  choice=value.lower()
  if choice in ['yes','y']:
    res = bk.insertSimConditions(desc,beamcond, beamEnergy, generator, magneticField, detectorCond, luminosity)
    if res['OK']:
      print 'The simulation condition added successfully!'
    else:
      print "Error discovered!",res['Message']
  elif choice in ['no','n']:
    print 'Aborded!'
  else:
    print 'Unespected choice:',value
else:
  usage();

DIRAC.exit(exitCode)