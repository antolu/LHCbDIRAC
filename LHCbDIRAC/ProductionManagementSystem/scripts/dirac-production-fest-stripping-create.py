#! /usr/bin/env python
#############################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ProductionManagementSystem/scripts/dirac-production-fest-stripping-create.py,v 1.1 2009/07/01 16:38:47 acsmith Exp $
#############################################################################
__RCSID__   = "$Id: dirac-production-fest-stripping-create.py,v 1.1 2009/07/01 16:38:47 acsmith Exp $"
__VERSION__ = "$Revision: 1.1 $"

import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Base import Script
import os, re, shutil,string

#############################################################################
# Permanent defaults
#############################################################################
wfName = ''
permanentType = 'DataStripping'
expressEventType = '91000000'
fullEventType = '90000000'
strippingEventType = fullEventType
davinci1InputDataType = 'rdst'
davinci1OutputDataType = 'fetc'
bkInputProcPass = 'Reco01'
saveHistos = True
fileMask = 'dst;root'
prodPriority = '7'
useOracle = True
debug = False
generateScript = False
dqFlag = 'OK'

#############################################################################
#Variables that can change
#############################################################################
bkConfigName = 'Fest'
bkConfigVersion = 'Fest'
bkGroupDescription = 'Strip01'
bkProcessingPass = 'FEST-%s' %bkGroupDescription
bkDataTakingConditions = 'DataTaking6153'
#############################################################################
conditions = 'MC09-20090602-vc-md100.py'
condDBTag = 'head-20090508' #'sim-20090402-vc-md100'
ddDBTag = 'head-20090508' #'MC09-20090602'
#############################################################################
davinciVersion = 'v23r2'
davinci1Opts = '$APPCONFIGOPTS/DaVinci/DVStrippingEtc-FEST.py'
inputEventType = '90000000'
daVinciData = 'LFN:/lhcb/data/2009/RAW/EXPRESS/FEST/FEST/50913/050913_0000000002.raw'
daVinciSE = 'Tier1_MC-DST'
daVinciEvents = '-1'
#############################################################################
appConfigVersion = 'v2r8'
deriveProdFrom = 0
#############################################################################

#############################################################################
#Register switches & parse command line
#############################################################################
Script.registerSwitch( "b:", "BKConfigName=","    Config Name [%s]"   % bkConfigName  )
Script.registerSwitch( "k:", "BKConfigVersion="," Config Version [%s]"   % bkConfigVersion  )
Script.registerSwitch( "m:", "GroupDesc=","       Group Description [%s]" % bkGroupDescription  )
Script.registerSwitch( "t:", "DataTakingConds="," Data Taking Conditions [%s]"   % bkDataTakingConditions  )
Script.registerSwitch( "n:", "CondDBTag=","       Conditions DB Tag [%s]"   % condDBTag  )
Script.registerSwitch( "a:", "DDDBTag=","         Det Desc DB Tag [%s]"   % ddDBTag  )
Script.registerSwitch( "v:", "EventType=","Event Type [%s]"   % inputEventType )
Script.registerSwitch( "q:", "DaVinciVersion=","  DaVinci Version [%s]"   % davinciVersion  )
Script.registerSwitch( "u:", "DaVinci1Opts=","    Step 1 DaVinci Options [%s]"   % davinci1Opts )
Script.registerSwitch( "j:", "DaVinciEvents=","   Events To Process (DaVinci) [%s]"   % daVinciEvents )
Script.registerSwitch( "k:", "AppConfig=","       App Config Version [%s]"   % appConfigVersion  )
Script.registerSwitch( "p:", "DeriveProd=","      Input Prod To Derive From [%s]"   % deriveProdFrom  )
#And pure flags:
Script.registerSwitch( "r", "UseOracle","         To disable Oracle CondDB access (default is enabled)" )
Script.registerSwitch( "d", "Debug","             Only create workflow XML (default is disabled)")
Script.registerSwitch( "g" ,"Generate","          Only create production script (default is disabled)")
Script.parseCommandLine(ignoreErrors=False)
args = Script.getPositionalArgs()

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
  elif switch[0].lower() in ('v','eventtype'):
    inputEventType=switch[1]
  elif switch[0].lower() in ('q','davinciversion'):
    davinciVersion=switch[1]
  elif switch[0].lower() in ('u','davinci1opts'):
    davinci1Opts=switch[1]
  elif switch[0].lower() in ('j','davincievents'):
    daVinciEvents=switch[1]
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

#############################################################################
# Determine the BK input data dictionary
#############################################################################

inputBKQuery = { 'SimulationConditions'     : 'All',
                 'DataTakingConditions'     : bkDataTakingConditions,
                 'ProcessingPass'           : bkInputProcPass,
                 'FileType'                 : bkFileType,
                 'EventType'                : inputEventType,
                 'ConfigName'               : bkConfigName,
                 'ConfigVersion'            : bkConfigVersion,
                 'ProductionID'             : 0,
                 'DataQualityFlag'          : dqFlag}

gLogger.info('==> BK Query Information:')
for n,v in inputBKQuery.items():
  gLogger.info(str(n).ljust(20)+' = '+str(v))

#############################################################################
# Determine the DDDB and CondDB tags
#############################################################################
"""
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
ddDBTag = match.group(1)
exp = re.compile(r'LHCbApp\(\).CondDBtag\s+=\s+"(\S+)"')
match = re.search(exp,oFileStr)
if not match:
  gLogger.error('Failed to determine the CondDB tag')
  DIRAC.exit(2)
condDBTag = match.group(1)
"""
gLogger.info('The DDDB tag is set to %s' % ddDBTag)
gLogger.info('The CondDB tag is set to %s' % condDBTag)

#############################################################################
# Generate the production
#############################################################################

wfName = 'Stripping'
wfName = '%s_DaVinci%s_AppConfig%s_%s_DDDB%s_CondDB%s' %(wfName,davinciVersion,appConfigVersion,bkGroupDescription,ddDBTag,condDBTag)
wfDescription = '%s %s %s data stripping production using DaVinci %s selecting %s events.' %(bkConfigName,bkConfigVersion,bkGroupDescription,davinciVersion,daVinciEvents)

prodScript= ['# Production API script generated using:\n#%s' %(__RCSID__)]

# Import the production client
from DIRAC.LHCbSystem.Client.Production import Production
prodScript.append('from DIRAC.LHCbSystem.Client.Production import Production')

# Create the production object
production = Production()
prodScript.append('production = Production()')

# Set the production type
production.setProdType(permanentType)
prodScript.append('production.setProdType("%s")' %permanentType)

# Set the workflow name
production.setWorkflowName(wfName)
prodScript.append('production.setWorkflowName("%s")' %wfName)

# Set the workflow description
production.setWorkflowDescription(wfDescription)
prodScript.append('production.setWorkflowDescription("%s")' %wfDescription)

# Set the input DB tags
production.setDBTags(condDBTag,ddDBTag)
prodScript.append('production.setDBTags("%s","%s")' %(condDBTag,ddDBTag))

# Set the input BK paramerts
production.setInputBKSelection(inputBKQuery)
prodScript.append('production.setInputBKSelection(%s)' %inputBKQuery)

# Set the output BK parameters
production.setBKParameters(bkConfigName,bkConfigVersion,bkProcessingPass,bkDataTakingConditions)
prodScript.append('production.setBKParameters("%s","%s","%s","%s")' %(bkConfigName,bkConfigVersion,bkGroupDescription,bkDataTakingConditions))

# Configure the output file make
production.setFileMask(fileMask)
prodScript.append('production.setFileMask("%s")' %fileMask)

# Configure the finalization step
production.addFinalizationStep()
prodScript.append('production.addFinalizationStep()')

# Set the production group for the produciton monitoring
production.setProdGroup(bkProcessingPass)
prodScript.append('production.setProdGroup("%s")' %(bkProcessingPass))

# Set the production priority
production.setProdPriority(prodPriority)
prodScript.append('production.setProdPriority(%s)' %(prodPriority))

appConfigStr = 'AppConfig.%s' % appConfigVersion

# Add the first davinci step
davinci1Opts = davinci1Opts.replace(' ',';')
#hack
davinci1OutputDataType = 'dst'
production.addDaVinciStep(davinciVersion,davinci1OutputDataType,davinci1Opts,
                          extraPackages=appConfigStr,
                          eventType=inputEventType,
                          inputData=daVinciData,
                          inputDataType=davinci1InputDataType,
                          histograms=saveHistos,
                          numberOfEvents=daVinciEvents)
prodScript.append('production.addDaVinciStep("%s","%s","%s",extraPackages="%s",eventType=%s,histograms=%s,numberOfEvents=%s)' %(davinciVersion,davinci1OutputDataType,davinci1Opts,appConfigStr,inputEventType,saveHistos,daVinciEvents))

#############################################################################
# Create the workflow for local testing
#############################################################################
if debug:
  gLogger.info('Creating production workflow...')
  production.createWorkflow()
  DIRAC.exit(0)

#############################################################################
# Create the production script
#############################################################################
if generateScript:
  gLogger.info('Creating production API script...')
  prodScript.append('#production.createWorkflow()')
  prodScript.append('production.create(bkQuery=%s,derivedProduction=%s,bkScript=False)' %(inputBKQuery,deriveProdFrom))
  outputFileName = '%s.py' %(wfName)
  if os.path.exists(outputFileName):
    gLogger.info('%s already exists, creating backup file' % outputFileName)
    shutil.copy(outputFileName,'%s.backup' %(outputFileName))
  fopen = open(outputFileName,'w')
  fopen.write(string.join(prodScript,'\n')+'\n')
  fopen.close()
  print '###################################################\n'
  print 'Production script written to %s\n' %(outputFileName)
  print '###################################################\n'
  DIRAC.exit(0)

print 'got here, shouldnt'
DIRAC.exit(2)

#############################################################################
# Create  the production
#############################################################################
gLogger.info('Creating production...')
res = production.create(bkQuery=inputBKQuery,derivedProduction=deriveProdFrom,bkScript=False)
if not res['OK']:
  gLogger.error('Failed to create stripping production.',res['Message'])
  DIRAC.exit(2)
if not res['Value']:
  gLogger.error('No production ID returned')
  DIRAC.exit(2)

prodID = int(res['Value'])
elogStr = 'FEST Stripping ProductionID %s %s' %(prodID,elogStr)
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
print '###################################################\n'
print elogStr
print '###################################################\n'
DIRAC.exit(0)


"""
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

if useOracle:
  #only allow to use Oracle with LFC disabled via CORAL
  brunelOpts = '%s;$APPCONFIGOPTS/UseOracle.py;$APPCONFIGOPTS/DisableLFC.py' %brunelOpts
  wfName += '_UseOracle_LFCDisabled'

production.addBrunelStep(brunelVersion,brunelOutputDataType,brunelOpts,extraPackages=appConfigStr,
                         eventType=brunelEventType,
                         inputData=brunelData,
                         inputDataType=brunelInputDataType,
                         outputSE=brunelSE,
                         histograms=saveHistos,
                         numberOfEvents=brunelEvents)
prodScript.append('production.addBrunelStep("%s","%s","%s",extraPackages="%s",eventType="%s",inputData="%s",inputDataType="%s",outputSE="%s",histograms=%s)' %(brunelVersion,brunelOutputDataType,brunelOpts,appConfigStr,brunelEventType,brunelData,brunelInputDataType,brunelSE,saveHistos))

"""



