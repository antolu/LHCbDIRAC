#! /usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/ProductionManagementSystem/scripts/dirac-production-check-outputs.py $
# File :    dirac-production-set-endrun.py
# Author :  R. Graciani
########################################################################
"""
  Set the End Run for a given Transformation
"""
__RCSID__ = '$Id: dirac-production-check-outputs.py 32419 2011-01-12 17:27:33Z rgracian $'

from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... Prod Run' % Script.scriptName,
                                     'Arguments:',
                                     '  Prod:      DIRAC Production Id',
                                     '  Run:       New EndRun value' ] ) )
Script.parseCommandLine ( ignoreErrors = True )

args = Script.getPositionalArgs()

if len( args ) != 2:
  Script.showHelp()

from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

prodId = int( args[0] )
runId = int( args[1] )

client = TransformationClient()
res = client.setBookkeepingQueryEndRunForTransformation( prodId, runId )

if res['OK']:
  print "End run of production %s is now %s" % ( str( prodId ), str( runId ) )
else:
  print res['Message']
