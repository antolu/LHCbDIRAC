#! /usr/bin/env python
#############################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ProductionManagementSystem/scripts/dirac-production-fest-create.py,v 1.7 2009/07/03 13:38:27 acsmith Exp $
# File :   dirac-production-fest-create.py
# Author : Stuart Paterson
#############################################################################
__RCSID__   = "$Id: dirac-production-fest-create.py,v 1.7 2009/07/03 13:38:27 acsmith Exp $"
__VERSION__ = "$Revision: 1.7 $"
import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Base import Script
import os, re, shutil,string

#############################################################################
# Permanent defaults
#############################################################################
wfName = ''
permanentType = 'DataReconstruction'
expressEventType = '91000000'
fullEventType = '90000000'
reprocessingEventType = fullEventType
brunelInputDataType = 'mdf'
brunelOutputDataType = 'rdst'
davinciOutputDataType = 'root'
bkFileType = 'RAW'
bkInputProcPass = 'Real Data'
saveHistos = True
fileMask = 'rdst;root'
wfDescription = ''
prodPriority='8'
prodTypeList = ['full','express','align','reprocessing']
useOracle=True
debug=False
generateScript=False
dqFlag = 'OK'

#############################################################################
#Variables that can change
#############################################################################
bkConfigName = 'Fest'
bkConfigVersion = 'Fest'
bkGroupDescription = 'Reco01'
bkProcessingPass = 'FEST-%s' %bkGroupDescription
bkDataTakingConditions = 'DataTaking6153'
#############################################################################
conditions = 'MC09-20090602-vc-md100.py'
condDBTag = 'head-20090508' #'sim-20090402-vc-md100'
ddDBTag = 'head-20090508' #'MC09-20090602'
#############################################################################
brunelVersion = 'v34r7'
brunelOpts = '$APPCONFIGOPTS/Brunel/FEST-200903.py' #;$APPCONFIGOPTS/UseOracle.py'
brunelEventType = '90000000'
brunelData = 'LFN:/lhcb/data/2009/RAW/EXPRESS/FEST/FEST/50606/050606_0000000002.raw'
brunelSE = 'CERN-RDST'
brunelEvents = '-1'
#############################################################################
davinciVersion = 'v23r1'
davinciOpts = '$APPCONFIGOPTS/DaVinci/DVMonitorDst.py'
davinciEvents = '-1'
#############################################################################
appConfigVersion = 'v2r7p2'
prodType = 'full'
deriveProdFrom = 0
#############################################################################



#############################################################################
#Register switches & parse command line
#############################################################################
#Script.registerSwitch( "p", "ProdType=", "Production Type [%s]"   % prodType  )
Script.registerSwitch( "b:", "BKConfigName=","    Config Name [%s]"   % bkConfigName  )
Script.registerSwitch( "k:", "BKConfigVersion="," Config Version [%s]"   % bkConfigVersion  )
Script.registerSwitch( "m:", "GroupDesc=","       Group Description [%s]" % bkGroupDescription  )
Script.registerSwitch( "t:", "DataTakingConds="," Data Taking Conditions [%s]"   % bkDataTakingConditions  )
#Script.registerSwitch( "n:", "Conditions=","      DB Conditions File To Use [%s]"   % conditions  )
Script.registerSwitch( "n:", "CondDBTag=","       Conditions DB Tag [%s]"   % condDBTag  )
Script.registerSwitch( "a:", "DDDBTag=","         Det Desc DB Tag [%s]"   % ddDBTag  )
#Script.registerSwitch( "v:", "EventType=","Event Type [%s]"   % brunelEventType )
Script.registerSwitch( "v:", "BrunelVersion=","   Brunel Version [%s]"   % brunelVersion  )
Script.registerSwitch( "u:", "BrunelOpts=","      Brunel Options [%s]"   % brunelOpts )
Script.registerSwitch( "y:", "BrunelEvents=","    Events To Process (Brunel) [%s]"   % brunelEvents )
Script.registerSwitch( "l:", "BrunelSE=","        Storage Element [%s]"   % brunelSE )
Script.registerSwitch( "q:", "DaVinciVersion=","  DaVinci Version [%s]"   % davinciVersion  )
Script.registerSwitch( "u:", "DaVinciOpts=","     DaVinci Options [%s]"   % davinciOpts )
Script.registerSwitch( "j:", "DaVinciEvents=","   Events To Process (DaVinci) [%s]"   % davinciEvents )
Script.registerSwitch( "k:", "AppConfig=","       App Config Version [%s]"   % appConfigVersion  )
Script.registerSwitch( "p:", "DeriveProd=","      Input Prod To Derive From [%s]"   % deriveProdFrom  )
#And pure flags:
Script.registerSwitch( "r", "UseOracle","         To disable Oracle CondDB access (default is enabled)" )
Script.registerSwitch( "d", "Debug","             Only create workflow XML (default is disabled)")
Script.registerSwitch( "g" ,"Generate","          Only create production script (default is disabled)")
Script.parseCommandLine( ignoreErrors = False )
args = Script.getPositionalArgs()


elogStr = ""

#############################################################################
#Import Production API
#############################################################################

from DIRAC.LHCbSystem.Client.Production import Production

prodScript = ["#! /usr/bin/env python"]
prodScript.append('# Production API script generated using:\n#%s' %(__RCSID__))
prodScript.append('from DIRAC.LHCbSystem.Client.Production import Production')
prodScript.append('production = Production()')

def usage():
  print 'Usage: %s [<options>] <ProductionType>' %(Script.scriptName)
  print 'Generate a FEST production specifying one of the following production types:\n%s' %(string.join(prodTypeList,', '))
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
  print '###################################################\n'
  print elogEntry
  print '###################################################\n'

if len(args) != 1:
  usage()

prodType = str(args[0]).lower()
if not prodType in prodTypeList:
  gLogger.error('Production type must be one of:\n%s' %(string.join(prodTypeList,', ')))
  DIRAC.exit(2)

#############################################################################
#Process switches
#############################################################################

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() in ('b','bkconfigname'):
    bkConfigName=switch[1]
  elif switch[0].lower() in ('k','bkconfigversion'):
    bkConfigVersion=switch[1]
  elif switch[0].lower() in ('m','groupdesc'):
    bkGroupDescription=switch[1]
  elif switch[0].lower() in ('t','datatakingconds'):
    bkDataTakingConditions=switch[1]
  elif switch[0].lower() in ('n','conddbtag'):
    condDBTag=switch[1]
  elif switch[0].lower() in ('a','dddbtag'):
    ddDBTag=switch[1]
#  elif switch[0].lower() in ('v','eventtype'):
#    brunelEventType=switch[1]
  elif switch[0].lower() in ('v','brunelversion'):
    brunelVersion=switch[1]
  elif switch[0].lower() in ('u','brunelopts'):
    brunelOpts=switch[1]
  elif switch[0].lower() in ('y','brunelevents'):
    brunelEvents=switch[1]
  elif switch[0].lower() in ('l','brunelse'):
    brunelEvents=switch[1]
  elif switch[0].lower() in ('q','davinciversion'):
    davinciVersion=switch[1]
  elif switch[0].lower() in ('u','davincivopts'):
    davinciOpts=switch[1]
  elif switch[0].lower() in ('j','davincievents'):
    davinciOpts=switch[1]
  elif switch[0].lower() in ('k','appconfig'):
    appConfigVersion=switch[1]
  elif switch[0].lower() in ('p','deriveprod'):
    deriveProdFrom=switch[1]
  elif switch[0].lower() in ('r','useoracle'):
    gLogger.info('UseOracle flag set, disabling Oracle access (default is True)')
    useOracle=False
  elif switch[0].lower() in ('d','debug'):
    gLogger.info('Debug flag enabled, setting log level to debug')
    debug = True
    gLogger.setLevel('debug')
  elif switch[0].lower() in ('g','generate'):
    generateScript=True

if generateScript:
  gLogger.info('Generate flag is enabled, will create Production API script')

#############################################################################
#Production preamble
#############################################################################

wfName = '%s' %(prodType.upper())
if prodType=='express':
  brunelEventType=expressEventType
  dqFlag = 'UNCHECKED'
elif prodType=='full':
  prodPriority='7'
  brunelEventType=fullEventType
  dqFlag = 'OK'
else:
  prodPriority='6'
  brunelEventType=reprocessingEventType
  dqFlag = 'MAYBE' #to allow separation of reconstruction / reprocessing activities

inputBKQuery = { 'SimulationConditions'     : 'All',
            'DataTakingConditions'     : bkDataTakingConditions,
            'ProcessingPass'           : bkInputProcPass,
            'FileType'                 : bkFileType,
            'EventType'                : brunelEventType,
            'ConfigName'               : bkConfigName,
            'ConfigVersion'            : bkConfigVersion,
            'ProductionID'             : 0,
            'DataQualityFlag'          : dqFlag}

gLogger.info('Set %s production event type to %s' %(prodType.upper(),brunelEventType))
gLogger.info('==> BK Query Information:')
adj  = 20
for n,v in inputBKQuery.items():
  gLogger.info(str(n).ljust(adj)+' = '+str(v))

wfName = '%s_Brunel%s_DaVinci%s_AppConfig%s_%s' %(wfName,brunelVersion,davinciVersion,appConfigVersion,bkGroupDescription)
wfDescription = '%s %s %s data reconstruction production using Brunel %s and DaVinci %s selecting %s events.' %(bkConfigName,bkConfigVersion,bkGroupDescription,brunelVersion,davinciVersion,brunelEvents)

#############################################################################
#Treat the options
#############################################################################
#appConfigConditions = '/afs/cern.ch/lhcb/software/releases/DBASE/AppConfig/%s/options/Conditions/%s' % (appConfigVersion,conditions)
#conditionsFile = '$APPCONFIGOPTS/Conditions/%s' % conditions
#if not os.path.exists(appConfigConditions):
#  gLogger.warn('The supplied conditions file does not exist',appConfigConditions)
#  conditionsFile = '$APPCONFIGOPTS/Conditions/%s.py' % conditions
#  appConfigConditions = '%s.py' % appConfigConditions
#  if not os.path.exists(appConfigConditions):
#    gLogger.error('The supplied conditions file does not exist',appConfigConditions)
#    DIRAC.exit(2)
#oFile = open(appConfigConditions)
#oFileStr = oFile.read()
#exp = re.compile(r'LHCbApp\(\).DDDBtag\s+=\s+"(\S+)"')
#match = re.search(exp,oFileStr)
#if not match:
#  gLogger.error('Failed to determine the DDDB tag')
#  DIRAC.exit(2)
#ddDBTag = match.group(1)
gLogger.info('The DDDB tag is set to %s' % ddDBTag)
wfName += '_DDDB%s' %ddDBTag
#exp = re.compile(r'LHCbApp\(\).CondDBtag\s+=\s+"(\S+)"')
#match = re.search(exp,oFileStr)
#if not match:
#  gLogger.error('Failed to determine the CondDB tag')
#  DIRAC.exit(2)
#condDBTag = match.group(1)
gLogger.info('The CondDB tag is set to %s' % condDBTag)
wfName += '_CondDB%s' %condDBTag
brunelOpts = brunelOpts.replace(' ',';')
#brunelOpts = '%s;%s' % (brunelOpts,conditionsFile)
if useOracle:
  #only allow to use Oracle with LFC disabled via CORAL
  brunelOpts = '%s;$APPCONFIGOPTS/UseOracle.py;$APPCONFIGOPTS/DisableLFC.py' %brunelOpts
  wfName += '_UseOracle_LFCDisabled'

gLogger.info('Brunel options are: %s' %brunelOpts)

elogStr = ' has been created with the following parameters:'
elogStr += '\nBK Config Name Version: %s %s' %(bkConfigName,bkConfigVersion)
elogStr += '\nBK Processing Pass: %s' %bkProcessingPass
elogStr += '\nData Quality Flag: "%s"' %dqFlag
elogStr += '\nConditions Tag: "%s"' %condDBTag
elogStr += '\nDDDB Tag: "%s"' %ddDBTag
elogStr += '\nAppConfig Version: %s' %appConfigVersion
elogStr += '\nEvent Type: %s' %brunelEventType
elogStr += '\nNumber Of Events: %s' %brunelEvents
elogStr += '\nBrunel Version: %s' %brunelVersion
elogStr += '\nBrunel Options: %s' %brunelOpts
elogStr += '\nDaVinci Version: %s' %davinciVersion
elogStr += '\nDaVinci Options: %s' %davinciOpts
elogStr += '\nBK Input Data Query:'
for n,v in inputBKQuery.items():
  elogStr+=('\n   %s = %s' %(n,v))

#############################################################################
#Create production
#############################################################################

appConfigStr = 'AppConfig.%s' %appConfigVersion

production = Production()
production.setProdType(permanentType)
production.setWorkflowName(wfName)
production.setWorkflowDescription(wfDescription)
production.setBKParameters(bkConfigName,bkConfigVersion,bkProcessingPass,bkDataTakingConditions)
production.setDBTags(condDBTag,ddDBTag)
production.setInputBKSelection(inputBKQuery)

production.addBrunelStep(brunelVersion,brunelOutputDataType,brunelOpts,extraPackages=appConfigStr,
                         eventType=brunelEventType,inputData=brunelData,inputDataType=brunelInputDataType,
                         outputSE=brunelSE,histograms=saveHistos,numberOfEvents=brunelEvents)

production.addDaVinciStep(davinciVersion,davinciOutputDataType,davinciOpts,extraPackages=appConfigStr,histograms=saveHistos)

production.addBrunelStep
production.addFinalizationStep()
production.setFileMask(fileMask)
production.setProdGroup(bkProcessingPass)
production.setProdPriority(prodPriority)

prodScript.append('production=Production()')
prodScript.append('production.setProdType("%s")' %permanentType)
prodScript.append('production.setWorkflowName("%s")' %wfName)
prodScript.append('production.setWorkflowDescription("%s")' %wfDescription)
prodScript.append('production.setBKParameters("%s","%s","%s","%s")' %(bkConfigName,bkConfigVersion,bkGroupDescription,bkDataTakingConditions))
prodScript.append('production.setDBTags("%s","%s")' %(condDBTag,ddDBTag))
prodScript.append('production.setInputBKSelection(%s)' %inputBKQuery)
prodScript.append('production.addBrunelStep("%s","%s","%s",extraPackages="%s",eventType="%s",inputData="%s",inputDataType="%s",outputSE="%s",histograms=%s,numberOfEvents="%s")' % (brunelVersion,brunelOutputDataType,brunelOpts,appConfigStr,brunelEventType,brunelData,brunelInputDataType,brunelSE,saveHistos,brunelEvents))
prodScript.append('production.addDaVinciStep("%s","%s","%s",extraPackages="%s",histograms=%s)' %(davinciVersion,davinciOutputDataType,davinciOpts,appConfigStr,saveHistos))
prodScript.append('production.addFinalizationStep()')
prodScript.append('production.setFileMask("%s")' %fileMask)
prodScript.append('production.setProdGroup("%s")' %(bkProcessingPass))
prodScript.append('production.setProdPriority(%s)' %(prodPriority))
prodScript.append('production.createWorkflow()')
prodScript.append('#production.create(bkQuery=%s,groupSize=1,derivedProduction=%s,bkScript=False)' %(inputBKQuery,deriveProdFrom))

if generateScript:
  gLogger.info('Creating production API script...')
  saveProdScript('%s.py' %(wfName),'A FEST %s production %s' %(prodType,elogStr),prodScript)
  DIRAC.exit(0)

if debug:
  gLogger.info('Creating production workflow...')
  production.createWorkflow()
  DIRAC.exit(0)

gLogger.info('Creating production...')
result = production.create(bkQuery=inputBKQuery,groupSize=1,derivedProduction=deriveProdFrom,bkScript=False)
if not result['OK']:
  gLogger.error('Failed to create merging production.',res['Message'])
  DIRAC.exit(2)
if not result['Value']:
  gLogger.error('No production ID returned')
  DIRAC.exit(2)

prodID = int(result['Value'])
elogStr = 'FEST %s ProductionID %s %s' %(prodType,prodID,elogStr)
print '###################################################\n'
print elogStr
print '###################################################\n'
DIRAC.exit(0)
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
