#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/BookkeepingSystem/scripts/Attic/dirac-bookkeeping-eventtype-mgt.py,v 1.2 2008/08/01 15:12:26 zmathe Exp $
# File :   dirac-bookkeeping-eventtype-mgt
# Author : Zoltan Mathe
########################################################################
__RCSID__   = "$Id: dirac-bookkeeping-eventtype-mgt.py,v 1.2 2008/08/01 15:12:26 zmathe Exp $"
__VERSION__ = "$ $"

import sys,string,re
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch( "", "insert=", "insert event type" )
Script.registerSwitch( "", "update=", "Update event types" )
Script.registerSwitch( "", "help=", "Usages:" )

Script.parseCommandLine( ignoreErrors = True )

from DIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()

args = Script.getPositionalArgs()

fileName = ''
insert = False
update = False

def usage():
  print 'Usage: %s [Try -h,--help for more information]' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) > 0:
  usage()

exitCode = 0

for switch in Script.getUnprocessedSwitches():
  if switch[0] == "insert":
    fileName= switch[1]
    insert = True
  elif switch[0] == "update":
    update = True
    fileName= switch[1]
  elif switch[0] == "help":
    command()
      

def command():
  #print 'dirac-bookkeeping-eventMgmt [-u|-i  <file name>] | -h | --help\n'
  print 'This tool is used to update or insert new event type.'
  print 'The <file name> list the event on which operate. Each entry  has the following format and is per line.'
  print 'EVTTYPEID="<evant id>", DESCRIPTION="<description>", PRIMARY="<primary description>"'
  print 'Options:\n   -u: update event type\n   -i: insert event type'
  print '   -h|--help: print this help'


def process_event(eventline):
  wrongSyntax=0
  try:
    eventline.index('EVTTYPEID')
    eventline.index('DESCRIPTION')
    eventline.index('PRIMARY')
  except ValueError:
    print '\nthe file syntax is wrong!!!\n'+eventline+'\n\n'
    usage()
    sys.exit(0)
  parameters=eventline.split(',')
  result={}
  ma=re.match("^ *?((?P<id00>EVTTYPEID) *?= *?(?P<value00>[0-9]+)|(?P<id01>DESCRIPTION|PRIMARY) *?= *?\"(?P<value01>.*?)\") *?, *?((?P<id10>EVTTYPEID) *?= *?(?P<value10>[0-9]+)|(?P<id11>DESCRIPTION|PRIMARY) *?= *?\"(?P<value11>.*?)\") *?, *?((?P<id20>EVTTYPEID) *?= *?(?P<value20>[0-9]+)|(?P<id21>DESCRIPTION|PRIMARY) *?= *?\"(?P<value21>.*?)\") *?$",eventline)
  if not ma:
    print "syntax error at: \n"+eventline
    usage()
    sys.exit(0)
  else:
    for i in range(3):
      if ma.group('id'+str(i)+'0'):
        if ma.group('id'+str(i)+'0') in result:
          print '\nthe parameter '+ma.group('id'+str(i)+'0')+' cannot appear twice!!!\n'+eventline+'\n\n'
          sys.exit(0)
        else:
          result[ma.group('id'+str(i)+'0')]=ma.group('value'+str(i)+'0')
      else:
       if ma.group('id'+str(i)+'1') in result:
         print '\nthe parameter '+ma.group('id'+str(i)+'1')+' cannot appear twice!!!\n'+eventline+'\n\n'
         sys.exit(0)
       else:
         result[ma.group('id'+str(i)+'1')]=ma.group('value'+str(i)+'1')
  return result

try:
  events=open(fileName)
except Exception,ex:
  print 'Cannot open file '+fileName
  DIRAC.exit(2)

if insert:
  for line in events:
    res = process_event(line)
    result = bk.addEventType(res['EVTTYPEID'],res['DESCRIPTION'],res['PRIMARY'])
    if result["OK"]:
      print result['Value']
    else:
      print result['Message']
elif update:
  for line in events:
    res = process_event(line)
    result = bk.updateEventType(res['EVTTYPEID'],res['DESCRIPTION'],res['PRIMARY'])
    if result["OK"]:
      print result['Value']
    else:
      print result['Message']

DIRAC.exit(exitCode)