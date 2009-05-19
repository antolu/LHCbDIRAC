#! /usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ProductionManagementSystem/scripts/dirac-production-MC09-create.py,v 1.2 2009/05/19 06:51:27 acsmith Exp $
# File :   dirac-production-MC09-create.py
# Author : Andrew C. Smith
########################################################################
__RCSID__   = "$Id: dirac-production-MC09-create.py,v 1.2 2009/05/19 06:51:27 acsmith Exp $"
__VERSION__ = "$Revision: 1.2 $"
import DIRAC
from DIRAC.Core.Base import Script
import os, sys

Script.registerSwitch( "ga", "Gauss=", "Gauss version to use" )
Script.registerSwitch( "bo", "Boole=", "Boole version to use" )
Script.registerSwitch( "br", "Brunel=", "Brunel version to use" )
Script.registerSwitch( "lh", "LHCb=", "LHCb version to use" )
Script.registerSwitch( "ap", "AppConfig=", "AppConfig version to use" )
Script.registerSwitch( "mc", "MCTruth=", "Save event MC truth information" )
Script.registerSwitch( "ev", "JobEvents=", "Events to produce per job" )
Script.registerSwitch( "me", "MergeFiles=", "Number of DST files to merge" )
Script.registerSwitch( "de", "InputProd=", "Perform the merging of the input production only")
Script.registerSwitch( "de", "Debug=", "Only create workflow XML")
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s [<options>] <EventType>' %(Script.scriptName)
  print ' Generate an MC09 production for the supplied event type <EventType>'
  DIRAC.exit(2)

if len(args) != 1:
  usage()

eventTypeID = str(args[0])

# Default values of options
mcTruth = False
numberOfEvents = '1000'
gaussVersion = 'v37r0'
booleVersion = 'v18r0'
brunelVersion = 'v35r0p1'
lhcbVersion = 'v26r3'
appConfigVersion = 'v2r4'
fileGroup = 20
debug = False
inputProd = 0
dstOutputSE = 'Tier1_MC-DST'
if eventTypeID != '30000000':
  dstOutputSE = 'CERN_MC_M-DST'  

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower()=="gauss":
    gaussVersion=switch[1]
  elif switch[0].lower()=="boole":
    booleVersion=switch[1]
  elif switch[0].lower()=="brunel":
    brunelVersion=switch[1]
  elif switch[0].lower()=="lhcb":
    lhcbVersion=switch[1]
  elif switch[0].lower()=="appconfig":
    appConfigVersion=switch[1]
  elif switch[0].lower()=="mctruth":
    mcTruth=switch[1]
  elif switch[0].lower()=="jobevents":
    numberOfEvents=switch[1]
  elif switch[0].lower()=="mergefiles":
    fileGroup=int(switch[1])
  elif switch[0].lower()=='inputprod':
    inputProd = int(switch[1])
  elif switch[0].lower()=='debug':
    debug = True

from DIRAC.LHCbSystem.Client.Production import Production

prodGroup = 'MC09-NoTruth'
gaussOpts = '$APPCONFIGOPTS/Gauss/MC09-b5TeV-md100.py;$APPCONFIGOPTS/Conditions/MC09-20090402-vc-md100.py;$DECFILESROOT/options/@{eventType}.opts;$LBPYTHIAROOT/options/Pythia.opts'
booleOpts = '$APPCONFIGOPTS/Boole/MC09-NoTruth.py;$APPCONFIGOPTS/Conditions/MC09-20090402-vc-md100.py'
brunelOpts = '$APPCONFIGOPTS/Brunel/MC09-NoTruth.py;$APPCONFIGOPTS/Conditions/MC09-20090402-vc-md100.py'
if mcTruth:
  prodGroup = 'MC09-WithTruth'
  booleOpts = '$APPCONFIGOPTS/Boole/MC09-WithTruth.py;$APPCONFIGOPTS/Conditions/MC09-20090402-vc-md100.py'
  brunelOpts = '$APPCONFIGOPTS/Brunel/MC09-WithTruth.py;$APPCONFIGOPTS/Conditions/MC09-20090402-vc-md100.py'

if not inputProd:
  production = Production()
  production.setProdType('MCSimulation')

  production.setWorkflowName('%s-EventType%s-Gauss%s_Boole%s_Brunel%s_AppConfig%s-%sEvents' % (prodGroup,eventTypeID,gaussVersion,booleVersion,brunelVersion,appConfigVersion,numberOfEvents))
  production.setWorkflowDescription('MC09 workflow with Gauss %s, Boole %s and Brunel %s (AppConfig %s) %s generating %s events of type %s.' % (gaussVersion,booleVersion,brunelVersion,appConfigVersion,prodGroup,numberOfEvents,eventTypeID))
  production.setBKParameters('MC','MC09',prodGroup,'Beam5TeV-VeloClosed-MagDown')
  production.setDBTags("sim-20090402-vc-md100","head-20090330")

  production.addGaussStep(gaussVersion,'Pythia',numberOfEvents,gaussOpts,eventType=eventTypeID,extraPackages='AppConfig.%s' % appConfigVersion)
  production.addBooleStep(booleVersion,'digi',booleOpts,extraPackages='AppConfig.%s' % appConfigVersion)
  production.addBrunelStep(brunelVersion,'dst',brunelOpts,extraPackages='AppConfig.%s' % appConfigVersion,inputDataType='digi',outputSE=dstOutputSE)

  production.addFinalizationStep()
  production.setFileMask('dst')
  production.setProdGroup(prodGroup)
  production.setProdPriority('0')

  if debug:
    production.createWorkflow()
    sys.exit(0)
  res = production.create()
  if not res['OK']:
    gLogger.error('Failed to create production.',res['Message'])
    DIRAC.exit(2)
  if not res['Value']:
    gLogger.error('No production ID returned')
    DIRAC.exit(2)
  inputProd = int(res['Value'])

if not fileGroup:
  sys.exit(0)

merge = Production()
merge.setProdType('Merge')
merge.setWorkflowName('%s-EventType%s-Merging-LHCb%s-prod%s-files%s' % (prodGroup,eventTypeID,lhcbVersion,inputProd,fileGroup))
merge.setWorkflowDescription('MC09 workflow for merging for DSTs %s using LHCb %s with %s input files from production %s (event type %s ).' % (prodGroup,lhcbVersion,fileGroup,inputProd,eventTypeID))
merge.setBKParameters('MC','MC09',prodGroup,'Beam5TeV-VeloClosed-MagDown')
merge.setDBTags("sim-20090402-vc-md100","head-20090330")

mergeDataType='DST'
mergedOutputSE='Tier1_MC_M-DST'

inputData=['/lhcb/MC/2009/DST/00004672/0000/00004672_00000242_3.dst']
merge.addMergeStep(lhcbVersion,optionsFile='$STDOPTS/PoolCopy.opts',eventType=eventTypeID,inputData=inputData,inputDataType=mergeDataType,outputSE=mergedOutputSE,inputProduction=inputProd)

merge.addFinalizationStep(removeInputData=False)
merge.setFileMask('dst') 
merge.setProdPriority('10')
merge.setProdGroup(prodGroup)
inputBKQuery = { 'ProductionID'   : inputProd,
                 'FileType'       : mergeDataType,
                 'EventType'      : int(eventTypeID),
                 'DataQualityFlag': 'UNCHECKED'}
merge.setInputBKSelection(inputBKQuery)
merge.setJobFileGroupSize(fileGroup)
merge.create(bkScript=False)
DIRAC.exit(0)
