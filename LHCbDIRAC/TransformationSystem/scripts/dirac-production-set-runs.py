#!/usr/bin/env python
"""
  Set the Start or End Run for a given Transformation of add a set runs.
"""
__RCSID__ = '$Id: dirac-production-set-run.py$'
"""
Examples of Usage :
  with 1234 = ProdID
  with 99000 = RunNumber
  dirac-production-set-run 1234 -l         (list of runs.)
  dirac-production-set-run 1234 -a 99000   (list of runs.)
  dirac-production-set-run 1234 -e 99000   (list of runs.)
  
"""

import DIRAC
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... Prod -Option [RunNumber|RunList]' % Script.scriptName,
                                     'Arguments:',
                                     '  Prod:      DIRAC Production Id', 
                                     '  RunNumber: New Start or End run',
                                     '  RunList: List of Runs to be added',
                                     'Examples:\n',
                                     'dirac-production-set-run.py 92 --list                        (show the list of runs for production 92)\n',
                                     'dirac-production-set-run.py 92 --add 98200,98201             (add some discrete run to production 92)\n',
                                     'dirac-production-set-run.py 92 --add 98200,98201,99000:99100 (add some discrete run and a range of runs to production 92)\n',
                                     'dirac-production-set-run.py 92 --start 98200                 (change the start run for production 92)\n',
                                     'dirac-production-set-run.py 92 --end   98200                 (change the end run  for production 92)\n'
                                     ]) )

Script.registerSwitch('','end', "Specify endrun for the production" )
Script.registerSwitch('','start', "Specify startrun for the production" )
Script.registerSwitch('','add', "add List of runs to the production" )
Script.registerSwitch('','list', "List the runs for the production" )
Script.parseCommandLine ( ignoreErrors = True )

args = Script.getPositionalArgs()
switches = Script.getUnprocessedSwitches()

if (len( args )>2 or len( switches )!=1):
  Script.showHelp()

from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient


if not args[0]=='h':
  prodId = int( args[0] )
  client = TransformationClient()
  try:
    res = client.getBookkeepingQueryForTransformation(prodId)
    bkDict = res['Value']
  except:
    print "Error retrieving bkquery for Production %s"%prodId



for switch in switches[0]:
  opt = switch.lower()
  if opt=='end':
    if (len(args)!=2):
      Script.showHelp()
    else:  
      if (bkDict.has_key("EndRun") and bkDict.has_key("StartRun")):
        endRun = int(res['Value']['EndRun'])
        runId = int( args[1] )
        res = client.setBookkeepingQueryEndRunForTransformation(prodId,runId)
        if res['OK']:
          print "End run of production %s is now %s" % ( str( prodId ), str( runId ) )
        else:
          print res['Message']
      else:
        print "Production %s has RunNumbers key" %str( prodId )
  
  if opt=='start':
    if (len(args)!=2):
      Script.showHelp()
    else:  
      if (bkDict.has_key("EndRun") and bkDict.has_key("StartRun")):
        runId = int( args[1] )
        res = client.setBookkeepingQueryStartRunForTransformation(prodId,runId)
        if res['OK']:
          print "Start run of production %s is now %s" % ( str( prodId ), str( runId ) )
        else:
          print res['Message']
      else:
        print "Production %s has RunNumbers key" %str( prodId )
        
  if opt=='add':
    if (len(args)!=2):
      Script.showHelp()
    else:  
      runId = str( args[1] )
      if (bkDict.has_key("RunNumbers")):
        res = client.addBookkeepingQueryRunListTransformation(prodId,runId)
        if res['OK']:
          print "Run list modified for production %s" % str( prodId )
        else:
          print res['Message']
      else:
        print "Production %s has start run and end run" % str( prodId )

  if opt in ( 'l', 'list' ):
    if (len(args)!=1):
      Script.showHelp()
    else:  
      if bkDict.has_key('RunNumbers'):
        print  bkDict['RunNumbers']   
      else:
        print "Start run of production %s is %s" % ( str( prodId ), bkDict['StartRun'] )
        print "End run of production %s is %s" % ( str( prodId ), bkDict['EndRun'] )
