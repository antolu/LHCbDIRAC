#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-bookkeeping-eventtype-mgt-insert
# Author : Zoltan Mathe
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$ $"

import sys,string,re
import DIRAC

from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = True )

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()

def usage():
  print 'This tool is insert new event type.'
  print 'The <file name> list the event on which operate. Each entry  has the following format and is per line.'
  print 'EVTTYPEID="<evant id>", DESCRIPTION="<description>", PRIMARY="<primary description>"'
  print 'Usage: dirac-bookkeeping-eventtype-mgt-insert filename'
  DIRAC.exit(2)

fileName = ''
args = sys.argv     
if len(args) < 2:
  usage()

exitCode = 0

fileName = args[1]
  

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


for line in events:
  res = process_event(line)
  result = bk.addEventType(res['EVTTYPEID'],res['DESCRIPTION'],res['PRIMARY'])
  if result["OK"]:
    print result['Value']
  else:
    print result['Message']
DIRAC.exit(exitCode)