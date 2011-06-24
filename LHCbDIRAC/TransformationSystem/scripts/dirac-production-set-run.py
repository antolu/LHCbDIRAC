#! /usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/ProductionManagementSystem/scripts/dirac-production-check-outputs.py $
# File :    dirac-production-set-endrun.py
# Author :  R. Graciani
########################################################################
"""
  Set the Start or End Run for a given Transformation
"""
__RCSID__ = '$Id: dirac-production-set-run.py 32419 2011-01-12 17:27:33Z rgracian $'

from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... Prod' % Script.scriptName,
                                     'Arguments:',
                                     '  Prod:      DIRAC Production Id' ] ) )

Script.registerSwitch( 'e:', 'end=', "Specify endrun for the production" )
Script.registerSwitch( 'a:', 'start=', "Specify startrun for the production" )

Script.parseCommandLine ( ignoreErrors = True )

args = Script.getPositionalArgs()
if len( args ) != 1:
  Script.showHelp()

from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

prodId = int( args[0] )
client = TransformationClient()

switches = Script.getUnprocessedSwitches()

for switch in switches:
  opt   = switch[0].lower()
  runId = int( switch[1] )
  
  if opt in ( 'e', 'end' ):    
    res = client.setBookkeepingQueryEndRunForTransformation( prodId, runId )
    if res['OK']:
      print "End run of production %s is now %s" % ( str( prodId ), str( runId ) )
    else:
      print res['Message']
  
  if opt in ( 'a', 'start' ):    
    res = client.setBookkeepingQueryStartRunForTransformation( prodId, runId )
    if res['OK']:
      print "Start run of production %s is now %s" % ( str( prodId ), str( runId ) )
    else:
      print res['Message']

