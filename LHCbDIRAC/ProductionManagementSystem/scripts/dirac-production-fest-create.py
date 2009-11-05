#! /usr/bin/env python
#############################################################################
# $HeadURL$
#############################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.14 $"
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
brunelOutputDataType = 'dst'
#brunelOutputDataType = 'rdst'     # we SHOULD produce rdsts but we DaVinci can't run the monitoring on them
davinciOutputDataType = 'root'
#fileMask = 'rdst;root'            # and since we DONT produce rdsts we should save the DSTs
fileMask= 'dst;root'
bkFileType = 'RAW'
bkInputProcPass = 'Real Data'
saveHistos = True
wfDescription = ''
prodPriority='2'
prodTypeList = ['full','express','align','reprocessing']
useOracle=True
debug=False
generateScript=False
dqFlag = 'OK'
brunelExtraOpts = ''
plugin = 'CCRC_RAW'

#############################################################################
#Variables that can change
#############################################################################
bkConfigName = 'Fest'
bkConfigVersion = 'Fest'
bkGroupDescription = 'Reco01'
bkDataTakingConditions = 'DataTaking6153'
#############################################################################
conditions = 'MC09-20090602-vc-md100.py'
condDBTag = 'head-20090508' #'sim-20090402-vc-md100'
ddDBTag = 'head-20090508' #'MC09-20090602'
#############################################################################
brunelVersion = 'v35r3'
brunelOpts = '$APPCONFIGOPTS/Brunel/FEST-200903.py' #;$APPCONFIGOPTS/UseOracle.py'
brunelEventType = '90000000'
brunelData = 'LFN:/lhcb/data/2009/RAW/EXPRESS/FEST/FEST/50606/050606_0000000002.raw'
brunelSE = 'CERN-RDST'
brunelEvents = '-1'
#############################################################################
davinciVersion = 'v23r3'
davinciOpts = '$APPCONFIGOPTS/DaVinci/DVMonitorDst.py'
davinciEvents = '-1'
#############################################################################
appConfigVersion = 'v2r8'
prodType = 'full'
deriveProdFrom = 0
runNumber = 0
alignmentLFN = ''
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
Script.registerSwitch( "v:", "BrunelVersion=","   Brunel Version [%s]"   % brunelVersion  )
Script.registerSwitch( "u:", "BrunelOpts=","      Brunel Options [%s]"   % brunelOpts )
Script.registerSwitch( "y:", "BrunelEvents=","    Events To Process (Brunel) [%s]"   % brunelEvents )
Script.registerSwitch( "l:", "BrunelSE=","        Storage Element [%s]"   % brunelSE )
Script.registerSwitch( "q:", "DaVinciVersion=","  DaVinci Version [%s]"   % davinciVersion  )
Script.registerSwitch( "u:", "DaVinciOpts=","     DaVinci Options [%s]"   % davinciOpts )
Script.registerSwitch( "j:", "DaVinciEvents=","   Events To Process (DaVinci) [%s]"   % davinciEvents )
Script.registerSwitch( "k:", "AppConfig=","       App Config Version [%s]"   % appConfigVersion  )
Script.registerSwitch( "p:", "DeriveProd=","      Input Prod To Derive From [%s]"   % deriveProdFrom  )
Script.registerSwitch( "x:", "RunNumber=","       Run Number of Input Data [%s]"   % runNumber  )
Script.registerSwitch( "z:", "AlignmentLFN=","    AlignmentDB LFN [%s]"   % alignmentLFN)
#And pure flags:
Script.registerSwitch( "r", "UseOracle","         To disable Oracle CondDB access (default is enabled)" )
Script.registerSwitch( "d", "Debug","             Only create workflow XML (default is disabled)")
Script.registerSwitch( "g" ,"Generate","          Only create production script (default is disabled)")
Script.parseCommandLine( ignoreErrors = False )
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
  elif switch[0].lower() in ('x','runnumber'):
    runNumber=switch[1]
  elif switch[0].lower() in ('z','alignmentlfn'):
    alignmentLFN=switch[1]
  elif switch[0].lower() in ('r','useoracle'):
    gLogger.info('UseOracle flag set, disabling Oracle access (default is True)')
    useOracle=False
  elif switch[0].lower() in ('d','debug'):
    gLogger.info('Debug flag enabled, setting log level to debug')
    debug = True
    gLogger.setLevel('debug')
  elif switch[0].lower() in ('g','generate'):
    generateScript=True

bkProcessingPass = 'FEST-%s' % bkGroupDescription

def usage():
  print 'Usage: %s [<options>] <ProductionType>' % (Script.scriptName)
  print 'Generate a FEST production specifying one of the following production types:\n%s' %(string.join(prodTypeList,', '))
  print 'Use %s --help for all options' % Script.scriptName
  DIRAC.exit(2)

if len(args) != 1:
  usage()

prodType = str(args[0]).lower()
if not prodType in prodTypeList:
  gLogger.error('Production type must be one of:\n%s' %(string.join(prodTypeList,', ')))
  DIRAC.exit(2)

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
#############################################################################
#Production preamble
#############################################################################
if prodType=='express':
  brunelEventType=expressEventType
  dqFlag = 'UNCHECKED' 
  prodPriority='8'
elif prodType=='align':
  brunelEventType=fullEventType
  dqFlag = 'All'
  prodPriority='7'
  if not runNumber:
    gLogger.error('For alignment productions a run number should be supplied (using --RunNumber=)')
    DIRAC.exit(2) 
  runNumber = -int(runNumber) # The run number is stored as a negative number in the BK
  if not alignmentLFN:
    gLogger.error('For alignment productions an alignment DB input LFN should be supplied (using --AlignmentLFN=)')
    DIRAC.exit(2)
  brunelExtraOpts = """from Configurables import CondDB, CondDBAccessSvc;alignCond = CondDBAccessSvc('AlignCond');alignCond.ConnectionString = 'sqlite_file:%s/LHCBCOND';CondDB().addLayer(alignCond)""" % os.path.basename(alignmentLFN)
  bkGroupDescription.replace('Reco','Align')
elif prodType=='full':
  brunelEventType=fullEventType
  dqFlag = 'OK'
  prodPriority='6'
elif prodType == 'reprocessing':
  brunelEventType=reprocessingEventType
  dqFlag = 'All'
  prodPriority='5'

#############################################################################
# Determine the BK input data dictionary
#############################################################################

inputBKQuery = { 'SimulationConditions'     : 'All',
                   'DataTakingConditions'     : bkDataTakingConditions,
                   'ProcessingPass'           : bkInputProcPass,
                   'FileType'                 : bkFileType,
                   'EventType'                : brunelEventType,
                   'ConfigName'               : bkConfigName,
                   'ConfigVersion'            : bkConfigVersion,
                   'ProductionID'             : runNumber,
                   'DataQualityFlag'          : dqFlag}

#############################################################################
# Give some information about what will be done
#############################################################################
gLogger.info('==> BK Query Information:')
for n,v in inputBKQuery.items():
  gLogger.info(str(n).ljust(20)+' = '+str(v))
gLogger.info('The DDDB tag is set to %s' % ddDBTag)
gLogger.info('The CondDB tag is set to %s' % condDBTag)


#############################################################################
# Generate the production
#############################################################################
wfName = '%s_Brunel%s_DaVinci%s_AppConfig%s_%s_DDDB%s_CondDB%s' %(prodType.upper(),brunelVersion,davinciVersion,appConfigVersion,bkGroupDescription,ddDBTag,condDBTag)
wfDescription = '%s %s %s data reconstruction production using Brunel %s and DaVinci %s selecting %s events.' %(bkConfigName,bkConfigVersion,bkGroupDescription,brunelVersion,davinciVersion,brunelEvents)
  
if useOracle:
  #only allow to use Oracle with LFC disabled via CORAL
  brunelOpts = '%s;$APPCONFIGOPTS/UseOracle.py;$APPCONFIGOPTS/DisableLFC.py' % brunelOpts
  wfName += '_UseOracle_LFCDisabled'

prodScript = ["#! /usr/bin/env python"]
prodScript.append('# Production API script generated using:\n#%s' %(__RCSID__))

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

# Set the production to derive from
production.setAncestorProduction(deriveProdFrom)
prodScript.append('production.setAncestorProduction("%s")' % deriveProdFrom)

# Set the input BK parameters
production.setInputBKSelection(inputBKQuery)
prodScript.append('production.setInputBKSelection(%s)' %inputBKQuery)

# Set the output BK parameters
production.setBKParameters(bkConfigName,bkConfigVersion,bkProcessingPass,bkDataTakingConditions)
prodScript.append('production.setBKParameters("%s","%s","%s","%s")' %(bkConfigName,bkConfigVersion,bkGroupDescription,bkDataTakingConditions))

# Configure the output file mask
production.setFileMask(fileMask)
prodScript.append('production.setFileMask("%s")' %fileMask)

# Set the production group for the produciton monitoring
production.setProdGroup(bkProcessingPass)
prodScript.append('production.setProdGroup("%s")' %(bkProcessingPass))

if prodType in ('reprocessing','full'): 
  production.setProdPlugin(plugin)
  prodScript.append('production.setProdPlugin("%s")' % plugin)

# If the alignment LFN is defined allow it to be downloaded
if alignmentLFN:
  production.setAlignmentDBLFN(alignmentLFN)
  prodScript.append('production.setAlignmentDBLFN("%s")' % (alignmentLFN))

appConfigStr = 'AppConfig.%s' % appConfigVersion
# Add the Brunel step
brunelOpts = brunelOpts.replace(' ',';')
production.addBrunelStep(brunelVersion,brunelOutputDataType,brunelOpts,
                         extraPackages=appConfigStr,
                         eventType=brunelEventType,
                         inputData=brunelData,
                         inputDataType=brunelInputDataType,
                         outputSE=brunelSE,
                         histograms=saveHistos,
                         extraOpts=brunelExtraOpts,
                         numberOfEvents=brunelEvents)
prodScript.append('production.addBrunelStep("%s","%s","%s",\n\
                         extraPackages="%s",\n\
                         eventType="%s",\n\
                         inputData="%s",\n\
                         inputDataType="%s",\n\
                         outputSE="%s",\n\
                         histograms=%s,\n\
                         extraOpts="%s",\n\
                         numberOfEvents="%s")' % (brunelVersion,brunelOutputDataType,brunelOpts,appConfigStr,brunelEventType,brunelData,brunelInputDataType,brunelSE,saveHistos,brunelExtraOpts,brunelEvents))

# Add the davinci step
davinciOpts = davinciOpts.replace(' ',';')
production.addDaVinciStep(davinciVersion,davinciOutputDataType,davinciOpts,
                         extraPackages=appConfigStr,
                         histograms=saveHistos)
prodScript.append('production.addDaVinciStep("%s","%s","%s",\n\
                         extraPackages="%s",\n\
                         histograms=%s)' % (davinciVersion,davinciOutputDataType,davinciOpts,appConfigStr,saveHistos))

# Configure the finalization step
production.addFinalizationStep()
prodScript.append('production.addFinalizationStep()')

#############################################################################
# In case it is required create the eLog entry
#############################################################################
elogStr =  'FEST %s ProductionID XXXXX has been created with the following parameters:' % (prodType)
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
  prodScript.append('production.createWorkflow()')
  prodScript.append('#production.create(bkScript=False)')
  outputFileName = '%s.py' %(wfName)
  if os.path.exists(outputFileName):
    gLogger.info('%s already exists, creating backup file' % outputFileName)
    shutil.copy(outputFileName,'%s.backup' %(outputFileName))
  fopen = open(outputFileName,'w')
  fopen.write(string.join(prodScript,'\n')+'\n')
  fopen.close()
  print '###################################################\n'
  print 'Production script written to %s\n' %(outputFileName)
  print 'Retain the below for an appropriate ELOG entry:\n'
  print '###################################################\n'
  print elogStr
  print '###################################################\n'
  DIRAC.exit(0)

#############################################################################
#Create production
#############################################################################
gLogger.info('Creating production...')
result = production.create(bkScript=False)
if not result['OK']:
  gLogger.error('Failed to create %s production.' % prodType,result['Message'])
  DIRAC.exit(2)
if not result['Value']:
  gLogger.error('No production ID returned')
  DIRAC.exit(2)

prodID = int(result['Value'])
print '###################################################\n'
print elogStr.replace('XXXXX',str(prodID))
print '###################################################\n'
DIRAC.exit(0)
