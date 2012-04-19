"""
  Set the Start or End Run for a given Transformation of add a set runs.
"""
__RCSID__ = '$Id: dirac-production-set-run.py$'
"""
Examples of Usage :
  with 1234 = ProdID
  with 99000 = RunNumber
  dirac-production-set-run 1234 -l         (list of runs. If the query is built with "RunNumbers" key.)
  dirac-production-set-run 1234 -a 99000   (list of runs. If the query is built with "RunNumbers" key.)
  dirac-production-set-run 1234 -e 99000   (list of runs. If the query is built with "RunNumbers" key.)
  
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
                                     'dirac-production-set-run.py 92 -l                         (show the list of runs for production 92)\n',
                                     'dirac-production-set-run.py 92 -p 98200,98201             (add some discrete run to production 92 - only if "RunNumbers " is already defined in the BKQuery)\n',
                                     'dirac-production-set-run.py 92 -p 98200,98201,99000:99100 (add some discrete run and a range of runs to production 92 - only if "RunNumbers" is already defined in the BKQuery)\n',
                                     'dirac-production-set-run.py 92 -a                         (change the start run for production 92 - only if "StartRun" is already defined in the BKQuery)\n',
                                     'dirac-production-set-run.py 92 -e                         (change the end run  for production 92 - only if  "EndRun" is already defined in the BKQuery)\n'
                                     ]) )

Script.registerSwitch( 'e', 'end', "Specify endrun for the production" )
Script.registerSwitch( 'a', 'start', "Specify startrun for the production" )
Script.registerSwitch( 'p', 'add', "add List of runs to the production" )
Script.registerSwitch( 'l', 'list', "List the runs for the production" )
Script.parseCommandLine ( ignoreErrors = True )

args = Script.getPositionalArgs()
switches = Script.getUnprocessedSwitches()

if len( args )>2 or len( switches )!=1:
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
  if opt in ( 'e', 'end' ):
    try:
      runId = int( args[1] )
    except ValueError:
        print "EndRun not integer"
        DIRAC.exit(2)
    if (bkDict.has_key("EndRun") and bkDict.has_key("StartRun")):
      endRun = int(res['Value']['EndRun'])
      res = client.convertBookkeepingQueryRunListTransformation(prodId)
      res = client.setBookkeepingQueryEndRunForTransformation(prodId,runId)
      if res['OK']:
        print "End run of production %s is now %s" % ( str( prodId ), str( runId ) )
      else:
        print res['Message']
    else:
      runList =bkDict['RunNumbers']
      if str(runId) not in runList:
        res = client.setBookkeepingQueryEndRunForTransformation(prodId,runId)          
        if res['OK']:
          print "End run of production %s is now %s" % ( str( prodId ), str( runId ) )
        else:
          print res['Message']
      else:
        print "Cannot descrease the EndRun for Production %s" % str( prodId ) 
  
  if opt in ( 'a', 'start' ):
    try:
      runId = int( args[1] )
    except ValueError:
        print "StartRun not integer"
        DIRAC.exit(2)
    if (bkDict.has_key("EndRun") and bkDict.has_key("StartRun")):
      res = client.convertBookkeepingQueryRunListTransformation(prodId)
      res = client.setBookkeepingQueryStartRunForTransformation(prodId,runId)
      if res['OK']:
        print "Start run of production %s is now %s" % ( str( prodId ), str( runId ) )
      else:
        print res['Message']
    else:
      runList =bkDict['RunNumbers']
      if str(runId) not in runList:
        res = client.setBookkeepingQueryStartRunForTransformation(prodId,runId)          
        if res['OK']:
          print "Start run of production %s is now %s" % ( str( prodId ), str( runId ) )
        else:
          print res['Message']
      else:
        print "Cannot increase the StartRun for Production %s" % str( prodId ) 

  if opt in ( 'p', 'add' ):    
    runId = str( args[1] )
    if (bkDict.has_key("EndRun") and bkDict.has_key("StartRun")):
      res = client.convertBookkeepingQueryRunListTransformation(prodId)
      res = client.addBookkeepingQueryRunListTransformation(prodId,runId)
      if res['OK']:
        print "Run list modified for production %s" % str( prodId )
      else:
        print res['Message']
    else:
      runList =bkDict['RunNumbers']
      res = client.addBookkeepingQueryRunListTransformation(prodId,runId)          
      if res['OK']:
        print "Run list modified for production %s" % str( prodId )
      else:
        print res['Message']

  if opt in ( 'l', 'list' ):
    if bkDict.has_key('RunNumbers'):
      print  bkDict['RunNumbers']   
    else:
      print "Start run of production %s is %s" % ( str( prodId ), bkDict['StartRun'] )
      print "End run of production %s is %s" % ( str( prodId ), bkDict['EndRun'] )
