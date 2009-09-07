#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/BookkeepingSystem/scripts/dirac-bookkeeping-productions-summary.py,v 1.1 2009/09/07 17:43:41 zmathe Exp $
# File :   dirac-bookkeeping-productions-summary
# Author : Zoltan Mathe
########################################################################
__RCSID__   = "$ $"
__VERSION__ = "$ $"

from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script

import types

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

exitCode = 0

from DIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()

print 'It you do not know the attributes, the default value is ALL or press ENTER'

cName = raw_input("Configuration Name(ALL is the default value):")
if cName == '':
  cName = 'ALL'
cVersion = raw_input("Configuration Version(ALL is the default value):")
if cVersion == '':
  cVersion = 'ALL'

production = raw_input("Production(ALL is the default value):")
if production == '':
  production = 'ALL'
      
simdesc = raw_input("SimulationDescription(ALL is the default value):")
if simdesc == '':
  simdesc = 'ALL'
    
pgroup= raw_input("Processing pass group(ALL is the default value):")
if pgroup == '':
  pgroup= 'ALL'
      
ftype = raw_input("File type(ALL is the default value):")
if ftype == '':
  ftype = 'ALL'
    
evttype= raw_input("Event type(ALL is the default value):") 
if evttype == '':
  evttype = 'ALL'

dict = {'ConfigurationName':cName,'ConfigurationVersion':cVersion,'Production':production,'SimulationDescription':simdesc,'ProcessingPassGroup':pgroup,'FileType':ftype,'EventType':evttype}
res = bk.getProductionSummary(dict)
print res
if not res["OK"]:
  print res["Message"]
else:
  value = res['Value']
  records = value['Records']
  nbRec = value['TotalRecords']
  params = value['ParameterNames']
  width=10
  print params[0].ljust(30)+str(params[1]).ljust(30)+str(params[2]).ljust(30)+str(params[3]).ljust(30)+str(params[4]).ljust(30)+str(params[5]).ljust(30)+str(params[6]).ljust(20)+str(params[7]).ljust(20)+str(params[8]).ljust(20)
  for record in records:
    print str(record[0]).ljust(15)+str(record[1]).ljust(15)+str(record[2]).ljust(20)+str(record[3]).ljust(width)+str(record[4]).ljust(width)+str(record[5]).ljust(width)+str(record[6]).ljust(width)+str(record[7]).ljust(width)+str(record[8]).ljust(width)
    

DIRAC.exit(exitCode)
