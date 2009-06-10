#! /usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ProductionManagementSystem/scripts/dirac-production-MC09-create.py,v 1.13 2009/06/10 17:10:46 paterson Exp $
# File :   dirac-production-MC09-create.py
# Author : Andrew C. Smith
########################################################################
__RCSID__   = "$Id: dirac-production-MC09-create.py,v 1.13 2009/06/10 17:10:46 paterson Exp $"
__VERSION__ = "$Revision: 1.13 $"
import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Base import Script
import os, re, shutil,string

# Default values of options
mcTruth = False
numberOfEvents = '1000'
gaussVersion = 'v37r2'
gaussOpts = '$APPCONFIGOPTS/Gauss/MC09-b5TeV-md100.py'
gaussGen = 'Pythia'
booleVersion = 'v18r1'
booleOpts = ''
brunelVersion = 'v34r7'
brunelOpts = ''
lhcbVersion = 'v26r3'
lhcbOpts = '$STDOPTS/PoolCopy.opts'
conditions = 'MC09-20090602-vc-md100.py'
appConfigVersion = 'v2r7'
dstOutputSE = 'Tier1_MC-DST'
fileGroup = 40
debug = False
inputProd = 0
bkSimulationCondition = 'Beam5TeV-VeloClosed-MagDown-Nu1'
bkProcessingPass = 'MC09-Sim03Reco02'
generateScripts = 0
mergeType = 'DST'
#merge priority is fixed but MC priority can be useful
mcPriority = 1
appendName=''

Script.registerSwitch( "ga", "Gauss=","             Gauss version to use          [%s]" % gaussVersion  )
Script.registerSwitch( "gO", "GaussOpts=","         Gauss options to use          [%s]" % gaussOpts )
Script.registerSwitch( "gG", "GaussGen=","          Gauss generator to use        [%s]" % gaussGen )
Script.registerSwitch( "bo", "Boole=","             Boole version to use          [%s]" % booleVersion )
Script.registerSwitch( "bO", "BooleOpts=","         Boole options to use          [%s]" % booleOpts )
Script.registerSwitch( "br", "Brunel=","            Brunel version to use         [%s]" % brunelVersion  )
Script.registerSwitch( "bO", "BrunelOpts=","        Brunel options to use         [%s]" % brunelOpts )
Script.registerSwitch( "lh", "LHCb=","              LHCb version to use           [%s]" % lhcbVersion )
Script.registerSwitch( "lO", "LHCbOpts=","          LHCb options to use           [%s]" % lhcbOpts )
Script.registerSwitch( "co", "Conditions=","        Conditions file to use        [%s]" % conditions )
Script.registerSwitch( "ap", "AppConfig=","         AppConfig version to use      [%s]" % appConfigVersion )
Script.registerSwitch( "se", "OutputSE=","          OututSE to save temp dsts     [%s]" % dstOutputSE)
Script.registerSwitch( "mc", "MCTruth=","           Save event MC truth           [%s]" % mcTruth)
Script.registerSwitch( "ev", "JobEvents=","         Events to produce per job     [%s]" % numberOfEvents )
Script.registerSwitch( "me", "MergeFiles=","        Number of files to merge      [%s]" % fileGroup )
Script.registerSwitch( "ip", "InputProd=","         Merge given input prod        [%s]" % inputProd )
Script.registerSwitch( "sc", "SimCond=","           BK simulation condition       [%s]" % bkSimulationCondition)
Script.registerSwitch( "pp", "ProcPass=","          BK processing pass            [%s]" % bkProcessingPass)
Script.registerSwitch( "de", "Debug=","             Only create workflow XML      [%s]" % debug)
Script.registerSwitch( "m" , "MergeType=","         Type of files to merge        [%s]" % mergeType)
Script.registerSwitch( "p" , "MCPriority=","        Priority of the MC production [%s]" % mcPriority)
Script.registerSwitch( "n" , "AppendName=","        String to append to prod name [%s]" % appendName)
Script.registerSwitch( "g" , "Generate","   Flag to only create production script [%s]" % generateScripts)
Script.parseCommandLine( ignoreErrors = False )
args = Script.getPositionalArgs()

prodScript = []

from DIRAC.LHCbSystem.Client.Production import Production

prodScript.append('# Production API script generated using:\n#%s' %(__RCSID__))
prodScript.append('from DIRAC.LHCbSystem.Client.Production import Production')
prodScript.append('production = Production()')

def usage():
  print 'Usage: %s [<options>] <EventType>' %(Script.scriptName)
  print ' Generate an MC09 production for the supplied event type <EventType>'
  DIRAC.exit(2)

def saveProdScript(fileName,elogEntry,prodList):
  if os.path.exists(fileName):
    gLogger.info('%s already exists, creating backup file' %fileName)
    shutil.copy(fileName,'%s.backup' %(fileName))
  fopen = open(fileName,'w')
  fopen.write(string.join(prodList,'\n')+'\n')
  fopen.close()
  print '###################################################\n'
  print 'Production script written to %s\n' %(fileName)
  print 'Retain the below for an appropriate ELOG entry:\n'
  print elogEntry
  print '###################################################\n'

if len(args) != 1:
  usage()

eventTypeID = str(args[0])
elogStr = ""

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower()=="gauss":
    gaussVersion=switch[1]
  elif switch[0].lower()=="gaussopts":
    gaussOpts=switch[1]
  elif switch[0].lower()=="gaussgen":
    gaussGen=switch[1]
  elif switch[0].lower()=="boole":
    booleVersion=switch[1]
  elif switch[0].lower()=="booleopts":
    booleOpts=switch[1]
  elif switch[0].lower()=="brunel":
    brunelVersion=switch[1]
  elif switch[0].lower()=="brunelopts":
    brunelOpts=switch[1]
  elif switch[0].lower()=="lhcb":
    lhcbVersion=switch[1]
  elif switch[0].lower()=="lhcbopts":
    lhcbOpts=switch[1]
  elif switch[0].lower()=="conditions":
    conditions=switch[1]
  elif switch[0].lower()=="appconfig":
    appConfigVersion=switch[1]
  elif switch[0].lower()=="outputse":
    dstOutputSE=switch[1]
  elif switch[0].lower()=="mctruth":
    mcTruth=switch[1]
  elif switch[0].lower()=="jobevents":
    numberOfEvents=switch[1]
  elif switch[0].lower()=="mergefiles":
    fileGroup=int(switch[1])
  elif switch[0].lower()=='inputprod':
    inputProd = int(switch[1])
  elif switch[0].lower()=='simcond':
    bkSimulationCondition=switch[1]
  elif switch[0].lower()=='procpass':
    bkProcessingPass=switch[1]
  elif switch[0].lower()=='debug':
    debug = True
  elif switch[0].lower()=='generate':
    generateScripts=1
  elif switch[0].lower()=='mergetype':
    mergeType = switch[1].upper()
  elif switch[0].lower()=='mcpriority':
    mcPriority=int(switch[1])
  elif switch[0].lower()=='appendname':
    appendName=str(switch[1])

if generateScripts:
  gLogger.info('Generate flag is enabled, will create Production API script')

#########################################################
# There must be an easier way to retrieving the tags
#########################################################
appConfigConditions = '/afs/cern.ch/lhcb/software/releases/DBASE/AppConfig/%s/options/Conditions/%s' % (appConfigVersion,conditions)
conditionsFile = '$APPCONFIGOPTS/Conditions/%s' % conditions
if not os.path.exists(appConfigConditions):
  gLogger.warn('The supplied conditions file does not exist',appConfigConditions)
  conditionsFile = '$APPCONFIGOPTS/Conditions/%s.py' % conditions
  appConfigConditions = '%s.py' % appConfigConditions
  if not os.path.exists(appConfigConditions):
    gLogger.error('The supplied conditions file does not exist',appConfigConditions)
    DIRAC.exit(2)
oFile = open(appConfigConditions)
oFileStr = oFile.read()
exp = re.compile(r'LHCbApp\(\).DDDBtag\s+=\s+"(\S+)"')
match = re.search(exp,oFileStr)
if not match:
  gLogger.error('Failed to determine the DDDB tag')
  DIRAC.exit(2)
dddbTag = match.group(1)
gLogger.info('Determined the DDDB tag to be %s' % dddbTag)
exp = re.compile(r'LHCbApp\(\).CondDBtag\s+=\s+"(\S+)"')
match = re.search(exp,oFileStr)
if not match:
  gLogger.error('Failed to determine the ConbDB tag')
  DIRAC.exit(2)
condbTag = match.group(1)
gLogger.info('Determined the CondDB tag to be %s' % condbTag)
if not mergeType in ['DST','MDF']:
  gLogger.error('MergeType must be MDF or DST, not %s' %mergeType)
  DIRAC.exit(2)

#########################################################

#########################################################
# Prepare the options files for all the applications
#########################################################
gaussOpts = gaussOpts.replace(' ',';')
gaussOpts = '%s;%s;$DECFILESROOT/options/@{eventType}.opts;$LB%sROOT/options/%s.opts' % (gaussOpts,conditionsFile,gaussGen.upper(),gaussGen)
gLogger.info("Gauss options: %s" % gaussOpts)
# Prepare the truth options
if mcTruth:
  bkProcessingPass = '%s-withTruth' % bkProcessingPass
  booleTruth = '$APPCONFIGOPTS/Boole/MC09-WithTruth.py'
  brunelTruth = '$APPCONFIGOPTS/Brunel/MC09-WithTruth.py'
  saveBrunelHistos=True
else:
  bkProcessingPass = '%s-withoutTruth' % bkProcessingPass
  booleTruth = '$APPCONFIGOPTS/Boole/MC09-NoTruth.py'
  brunelTruth = '$APPCONFIGOPTS/Brunel/MC09-NoTruth.py'
  saveBrunelHistos=False
# Boole
if booleOpts:
  booleOpts = booleOpts.replace(' ',';')
  booleOpts = '%s;%s;%s' % (booleTruth,booleOpts,conditionsFile)
else:
  booleOpts = '%s;%s' % (booleTruth,conditionsFile)
gLogger.info("Boole options: %s" % booleOpts)
# Brunel
if brunelOpts:
  brunelOpts = brunelOpts.replace(' ',';')
  brunelOpts = '%s;%s;%s' % (brunelTruth,brunelOpts,conditionsFile)
else:
  brunelOpts = '%s;%s' % (brunelTruth,conditionsFile)
gLogger.info("Brunel options: %s" % brunelOpts)
# LHCb
lhcbOpts = lhcbOpts.replace(' ',';')
gLogger.info("LHCb options: %s" % lhcbOpts)
#########################################################

#########################################################
# Create the MC production
#########################################################
if not inputProd:
  production = Production()
  production.setProdType('MCSimulation')

  production.setWorkflowName('%s-%s-EventType%s-Gauss%s_Boole%s_Brunel%s_AppConfig%s-%sEvents%s' % (bkSimulationCondition,bkProcessingPass,eventTypeID,gaussVersion,booleVersion,brunelVersion,appConfigVersion,numberOfEvents,appendName))
  production.setWorkflowDescription('MC09 workflow with Gauss %s, Boole %s and Brunel %s (AppConfig %s) %s generating %s events of type %s.' % (gaussVersion,booleVersion,brunelVersion,appConfigVersion,bkProcessingPass,numberOfEvents,eventTypeID))
  production.setBKParameters('MC','MC09',bkProcessingPass,bkSimulationCondition)
  production.setDBTags(condbTag,dddbTag)

  production.addGaussStep(gaussVersion,gaussGen,numberOfEvents,gaussOpts,eventType=eventTypeID,extraPackages='AppConfig.%s' % appConfigVersion)
  production.addBooleStep(booleVersion,'digi',booleOpts,extraPackages='AppConfig.%s' % appConfigVersion)
  production.addBrunelStep(brunelVersion,'dst',brunelOpts,extraPackages='AppConfig.%s' % appConfigVersion,inputDataType='digi',outputSE=dstOutputSE,histograms=saveBrunelHistos)

  production.addFinalizationStep()
  production.setFileMask('dst')
  production.setProdGroup(bkProcessingPass)
  production.setProdPriority(mcPriority)

  prodScript.append("production.setProdType('MCSimulation')")
  prodScript.append("production.setWorkflowName('%s-EventType%s-Gauss%s_Boole%s_Brunel%s_AppConfig%s-%sEvents%s')" % (bkProcessingPass,eventTypeID,gaussVersion,booleVersion,brunelVersion,appConfigVersion,numberOfEvents,appendName))
  prodScript.append("production.setWorkflowDescription('MC09 workflow with Gauss %s, Boole %s and Brunel %s (AppConfig %s) %s generating %s events of type %s.')" % (gaussVersion,booleVersion,brunelVersion,appConfigVersion,bkProcessingPass,numberOfEvents,eventTypeID))
  prodScript.append("production.setBKParameters('MC','MC09','%s','%s')" %(bkProcessingPass,bkSimulationCondition))
  prodScript.append("production.setDBTags('%s','%s')" %(condbTag,dddbTag))
  prodScript.append("production.addGaussStep('%s','%s','%s','%s',eventType='%s',extraPackages='AppConfig.%s')" %(gaussVersion,gaussGen,numberOfEvents,gaussOpts,eventTypeID,appConfigVersion))
  prodScript.append("production.addBooleStep('%s','digi','%s',extraPackages='AppConfig.%s')" %(booleVersion,booleOpts,appConfigVersion))
  prodScript.append("production.addBrunelStep('%s','dst','%s',extraPackages='AppConfig.%s',inputDataType='digi',outputSE='%s',histograms=%s)" %(brunelVersion,brunelOpts,appConfigVersion,dstOutputSE,saveBrunelHistos))
  prodScript.append("production.addFinalizationStep()")
  prodScript.append("production.setFileMask('dst')")
  prodScript.append("production.setProdGroup('%s')" %(bkProcessingPass))
  prodScript.append("production.setProdPriority(%s)" %mcPriority)
  prodScript.append("#production.createWorkflow()")
  prodScript.append("production.create()")
  inputProd=12345
  elogStr = """I have created a MC09 production (%s) with the following parameters:
\nConditions tag: "%s"
DDDB tag:       "%s"
AppConfig Version %s
\nGauss Version %s
Gauss Opts %s
Event type '%s'
Event gen '%s'
No Events  %s
\nBoole Version %s
Boole Opts = %s
\nBrunel Version %s
Brunel Opts = %s
\nSaving MC truth = %s""" % (inputProd,condbTag,dddbTag,appConfigVersion,gaussVersion,gaussOpts,eventTypeID,gaussGen,numberOfEvents,booleVersion,booleOpts,brunelVersion,brunelOpts,mcTruth)

  if generateScripts:
    #fileName = '%s-EventType%s-Gauss%s_Boole%s_Brunel%s_AppConfig%s-%sEvents.py' % (bkProcessingPass,eventTypeID,gaussVersion,booleVersion,brunelVersion,appConfigVersion,numberOfEvents)
    fileName = '%s-EventType%s-%sEvents.py' % (bkProcessingPass,eventTypeID,numberOfEvents)
    saveProdScript(fileName,elogStr,prodScript)
    DIRAC.exit(0)

  if debug:
    production.createWorkflow()
    DIRAC.exit(0)
  res = production.create()
  if not res['OK']:
    gLogger.error('Failed to create production.',res['Message'])
    DIRAC.exit(2)
  if not res['Value']:
    gLogger.error('No production ID returned')
    DIRAC.exit(2)
  inputProd = int(res['Value'])
  elogStr = string.replace(elogStr,'12345',str(inputProd))
#########################################################

#########################################################
# Create the merging production
#########################################################
if fileGroup:
  mergeDataType=mergeType
  appString='LHCb%s' %(lhcbVersion)
  if mergeDataType=='MDF':
    appString='MergeMDF'
  merge = Production()
  merge.setProdType('Merge')
  merge.setWorkflowName('%s-EventType%s-Merging-%s-prod%s-files%s%s' % (bkProcessingPass,eventTypeID,appString,inputProd,fileGroup,appendName))
  merge.setWorkflowDescription('MC09 workflow for merging %ss %s using %s with %s input files from production %s (event type %s ).' % (mergeDataType,bkProcessingPass,appString,fileGroup,inputProd,eventTypeID))
  merge.setBKParameters('MC','MC09',bkProcessingPass,bkSimulationCondition)
  merge.setDBTags(condbTag,dddbTag)

  mergedOutputSE='Tier1_MC_M-DST'
  inputData=['/lhcb/MC/2009/DST/00004672/0000/00004672_00000242_3.dst']
  merge.addMergeStep(lhcbVersion,optionsFile=lhcbOpts,eventType=eventTypeID,inputData=inputData,inputDataType=mergeDataType,outputSE=mergedOutputSE,inputProduction=inputProd)

  merge.addFinalizationStep(sendBookkeeping=False,removeInputData=True)
  merge.setFileMask(mergeDataType.lower())
  merge.setProdPriority('9')
  merge.setProdGroup(bkProcessingPass)
  inputBKQuery = { 'ProductionID'   : inputProd,
                 'FileType'       : mergeDataType,
                 'EventType'      : int(eventTypeID),
                 'DataQualityFlag': 'UNCHECKED'}
  merge.setInputBKSelection(inputBKQuery)
  merge.setJobFileGroupSize(fileGroup)

  prodScript.append("production.setProdType('Merge')")
  prodScript.append("production.setWorkflowName('%s-EventType%s-Merging-%s-prod%s-files%s%s')" %(bkProcessingPass,eventTypeID,appString,inputProd,fileGroup,appendName))
  prodScript.append("production.setWorkflowDescription('MC09 workflow for merging for %ss %s using %s with %s input files from production %s (event type %s ).')" % (mergeDataType,bkProcessingPass,appString,fileGroup,inputProd,eventTypeID))
  prodScript.append("production.setBKParameters('MC','MC09','%s','%s')" %(bkProcessingPass,bkSimulationCondition))
  prodScript.append("production.setDBTags('%s','%s')" %(condbTag,dddbTag))
  prodScript.append("production.addMergeStep('%s',optionsFile='%s',eventType='%s',inputData=%s,inputDataType='%s',outputSE='%s',inputProduction='%s')" %(lhcbVersion,lhcbOpts,eventTypeID,inputData,mergeDataType,mergedOutputSE,inputProd))
  prodScript.append("production.addFinalizationStep(sendBookkeeping=False,removeInputData=True)")
  prodScript.append("production.setFileMask('%s')" %(mergeDataType.lower()))
  prodScript.append("production.setProdGroup('%s')" %(bkProcessingPass))
  prodScript.append("production.setInputBKSelection(%s)" %inputBKQuery)
  prodScript.append("production.setJobFileGroupSize(%s)" %fileGroup)
  prodScript.append("production.setProdPriority('9')")
  prodScript.append("#production.createWorkflow()")
  if mergeDataType=='MDF':
    prodScript.append("production.create(bkScript=True)")
  else:
    prodScript.append("production.create(bkScript=False)")

  meringProd=12345
  if mergeDataType=='MDF':
    lhcbOpts='NA'

  elogStr = """%s\nTo merge the DSTs produced production (%s) has been created with the following parameters:
\nMerging with %s
LHCb Opts %s
Input files %s
\nThe events for this production will appear in the bookkeeping under
MC/MC09/%s/%s/%s/DST""" % (elogStr,meringProd,appString,lhcbOpts,fileGroup,bkSimulationCondition,bkProcessingPass,eventTypeID)

  if generateScripts:
    fileName = '%s-%s-Merging-%s-p%s-%sf.py' %(bkProcessingPass,eventTypeID,appString,inputProd,fileGroup)
    saveProdScript(fileName,elogStr,prodScript)
    DIRAC.exit(0)

  bk=False
  if mergeDataType=='MDF':
    bk=True
  res = merge.create(bkScript=bk)
  if not res['OK']:
    gLogger.error('Failed to create merging production.',res['Message'])
    DIRAC.exit(2)
  if not res['Value']:
    gLogger.error('No production ID returned')
    DIRAC.exit(2)
  meringProd = int(res['Value'])

#########################################################

print '###################################################\n'
print string.replace(elogStr,'12345',str(meringProd))
print '###################################################\n'
DIRAC.exit(0)
