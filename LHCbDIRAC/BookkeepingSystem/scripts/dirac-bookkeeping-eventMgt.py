#!/usr/bin/env python
from DIRAC.Interfaces.API.Dirac                       import Dirac
from DIRAC.Core.Base import Script
import getopt
import sys
import re

__RCSID__ = "$Id: dirac-bookkeeping-eventMgt.py,v 1.2 2008/08/01 10:12:46 zmathe Exp $"

Script.parseCommandLine( ignoreErrors = True )
args=Script.getPositionalArgs()
#args=sys.argv[1:]

def usage():
  print 'dirac-bookkeeping-eventMgmt [-u|-i  <file name>] | -h | --help\n'
  print 'This tool is used to update or insert new event type.'
  print 'The <file name> list the event on which operate. Each entry  has the following format and is per line.'
  print 'EVTTYPEID="<evant id>", DESCRIPTION="<description>", PRIMARY="<primary description>"'
  print 'Options:\n   -u: update event type\n   -i: insert event type'
  print '   -h|--help: print this help'

file=''
option=''

if len(args) == 0 or len(args) > 2:
  print 'Too few/many argments.\nUSAGE:\n'
  usage()
  sys.exit(1)
option = args[0]
if option in ['-h','--help']:
  usage()
  sys.exit

if option not in ['-u','-i','-h','--help']:
   print 'Options not valid.\nUSAGE\n'
   usage()
   sys.exit(1)
   

if len(args)==2:
  file = args[1]
else:
  print 'The file name is missing.\nUSAGE:\n'
  usage()
  sys.exit(1)

try:
  events=open(file)
except Exception,ex:
    print 'cannot open file '+file
    print ex
    sys.exit(1)
for line in events:
   process_event(line)

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

  
