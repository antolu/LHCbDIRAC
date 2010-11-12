#################################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/Workflow/Templates/Certification_Testing_run.py $
#################################################################################

"""  The Certification Testing Template creates productions that aim 
     to fully commission a DIRAC release.  The requirements of this 
     script are:
     
     - simple to execute
     - must run in a self-contained way without direct editing
     - must test LHCb MC and data processing production workflows
     - processing pass information is dynamic and must be persisted and 
       maintained in the CS
     - use CM defaults for outputs
     
     in order to solve the above use-case the following recipe will be 
     used:
     
    * Gauss->Boole->Brunel MC simulation production
      o also producing MDF files
    * LHCb merging production to check standard MC full chain
    * Standard MC transformation to distribute outputs
    * Reconstruction production starting from the MDF produced above
      o allows to test the staging if we put the MDF files on tape storage
    * Twiki documentation to help others run the above and interpret the results.
    
    Updates on the horizon include:
    * adding Moore to the sim production.
    * Reconstruction merging to check final output files in the BK
     
"""

__RCSID__   = "$Id: Certification_Testing_run.py 23248 2010-03-18 07:57:40Z paterson $"

#################################################################################
# Some import statements and standard DIRAC script preamble
#################################################################################
import sys,os,string,time,re
from DIRAC.Core.Base import Script
Script.parseCommandLine()
args = Script.getPositionalArgs()

import DIRAC

from DIRAC import gConfig,gLogger
gLogger = gLogger.getSubLogger('Certification_Testing')

#################################################################################
# Any functions to make things simpler
#################################################################################

csPath = '/Operations/CertificationTesting'

def getProjectParameters(projectName):
  """ Get and check project parameters.
  """
  projectSection = '%s/Projects/%s' %(csPath,projectName)
  result = gConfig.getOptionsDict(projectSection)
  if not result['OK']:
    gLogger.error('Could not get project section %s from the CS with result:\n%s' %(projectSection,result))
    DIRAC.exit(2)
  
  # A project minimally must have a version and options field specified  
  if not result['Value'].has_key('Version'):
    gLogger.error('Section %s does not contain the %s Version' %(projectSection,projectName))
    DIRAC.exit(2)
  
  if not result['Value'].has_key('Options'):
    gLogger.error('Section %s does not contain the %s Options' %(projectSection,projectName))
    DIRAC.exit(2)
  
  if projectName=='Brunel':
    if not result['Value'].has_key('RecoOptions'):
      gLogger.error('No %s/RecoOptions were found for Brunel' %(projectSection))
      DIRAC.exit(2)  

#for opts in ['TaggingOptions','MergingOptions']:
#  if not davinci.has_key(opts):
#    gLogger.error('No %s were found for DaVinci' %(opts))
#    DIRAC.exit(2)

  gLogger.verbose('%s version %s, %s' %(projectName,result['Value']['Version'],result['Value']['Options']))
  return result['Value']  

def getBKQueryDict():
  """ Return a BK query dictionary object with all the useful keys for
      this script.
  """
  query = {  'SimulationConditions'     : 'All',
             'DataTakingConditions'     : 'All',
             'ProcessingPass'           : 'All',
             'FileType'                 : 'All',
             'EventType'                : 'All',
             'ConfigName'               : 'All',
             'ConfigVersion'            : 'All',
             'ProductionID'             :     0  }
  return query.copy()

def getParam(pType,pName,pDefault):
  """ Just to clean up the CS operations and make it easier to 
      change sections or the way these are retrieved at a later 
      date if necessary. 
  """
  paramPath = '%s/%s/%s' %(csPath,pType,pName)
  return gConfig.getValue(paramPath,pDefault)
  
#################################################################################
# Some parameters must be defined before doing anything
#################################################################################

from LHCbDIRAC.Interfaces.API.Production import Production
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction

diracProd = DiracProduction()

###########################################
# Parameters requiring to be maintained in
# the CS i.e. no default values here
###########################################

#have one set of tags for MC and another for reconstruction
conditionsDBTag = getParam('General','MCCondDBTag','')
detDescDBTag = getParam('General','MCDDDBTag','')
recoConditionsDBTag = getParam('General','DataCondDBTag','')
recoDetDescDBTag = getParam('General','DataDDDBTag','')

#resulting dictionary supplies Version, Options, ExtraPackages
gauss = getProjectParameters('Gauss')
boole = getProjectParameters('Boole')
lhcb = getProjectParameters('LHCb')
#Brunel is used once in simulation and again in the reco, needs special treatment
brunel = getProjectParameters('Brunel')
#DaVinci has several uses only when merging is included
davinci = getProjectParameters('DaVinci')

###########################################
# Configurable parameters, some of these 
# need to be changed occasionally and can 
# be passed on the command line, *.cfg 
# file or CS if necessary
###########################################

#General parameters for all prods 
paramType = 'General'
appendName =  getParam(paramType,'AppendName','') #default applied below
evtType = getParam(paramType,'EventType','30000000')
subMode = getParam(paramType,'SubmissionMode','automatic')
debug = getParam(paramType,'Debug','False') #flag to just print JDL and create workflows etc. 

#BK parameters
configName = getParam('BK','ConfigName','certification')
configVersion = getParam('BK','ConfigVersion','test')

#MC parameters
paramType = 'MC'
events = getParam(paramType,'NumberOfEvents','10')
eventNumberTotal=getParam(paramType,'EventNumberTotal','1000000')
cpu = getParam(paramType,'CPU','100000')
priority = getParam(paramType,'Priority','4')
extend = getParam(paramType,'Extend','50')
brunelAppType = getParam(paramType,'BrunelAppType','DST')
booleAppType = getParam(paramType,'BooleAppType','DIGI')
sysConfig = getParam(paramType,'SystemConfig','ANY')
outputFileMask = getParam(paramType,'OutputFileMask',[])
simCond = getParam(paramType,'SimulationCondition','Beam3500GeV-VeloClosed-MagDown-Nu1')
gaussGenerator = getParam(paramType,'GaussGenerator','Pythia')
mdfOutputSE = getParam(paramType,'MDFOutputSE','Tier1-RAW')

#MC merging parameters
paramType = 'MCMerging' 
mergingPlugin = getParam(paramType,'Plugin','Standard')
mergingGroupSize = getParam(paramType,'GroupSize','2')
mergingPriority = getParam(paramType,'Priority','6')
mergingRemoveIDFlag = getParam(paramType,'RemoveInputDataFlag','True')
mergingTransFlag = getParam(paramType,'TransformationFlag','True')

#Reconstruction parameters
paramType = 'Reconstruction'
recoPriority = getParam(paramType,'Priority','8')
recoCPU = getParam(paramType,'CPU','100000')
recoPlugin = getParam(paramType,'Plugin','ByShare') # AtomicRun, Standard...
recoGroupSize = getParam(paramType,'GroupSize','1')

#Reconstruction merging parameters - maybe in due course

###########################################
# Fixed parameters and manipulations 
###########################################

if not appendName:
  appendName = time.asctime().replace(':','_').replace(' ','_')  

maskList = [m.lower() for m in outputFileMask]
if not brunelAppType.lower() in maskList:
  maskList.append(brunelAppType.lower())
if not 'mdf' in maskList:
  maskList.append('mdf')
outputFileMask = string.join(maskList,';')

mergingRemoveIDFlag = eval(mergingRemoveIDFlag)
mergingTransFlag = eval(mergingTransFlag)
debug = eval(debug)

prodGroup = '%s_%s' %(configName.capitalize(),appendName)

#The below is to get an ordinary Boole digi step to produce MDF 
mdfOutput = {"outputDataName":"@{STEP_ID}.mdf","outputDataType":"mdf","outputDataSE":mdfOutputSE}

booleExtraOpts = 'Boole().Outputs  = [\"DIGI\",\"MDF\"];'
booleExtraOpts += "OutputStream(\"RawWriter\").Output = \"DATAFILE=\'PFN:@{STEP_ID}.mdf\' SVC=\'LHCb::RawDataCnvSvc\' OPT=\'RECREATE\'\";"

#Since the MC production is being published to see the MDF, let's add something 
#to the processing pass of the subsequent MC merging production
mergingProdGroup = '%s-MCMerged' %(prodGroup)

#Reconstruction prod group
recoProdGroup = 'Reconstruction-%s' %(appendName)

#Some reco parameters that are fixed
recoType="FULL"
recoAppType = "SDST"
recoIDPolicy='download'
recoTransFlag = False

#################################################################################
# Now create the MC simulation production
#################################################################################

production = Production()
production.setSystemConfig(sysConfig)
production.setProdType('MCSimulation')
production.setWorkflowName('MC_Test_%sEvents_%s' %(events,appendName))
production.setBKParameters(configName,configVersion,prodGroup,simCond)
production.setDBTags(conditionsDBTag,detDescDBTag)
#This isn't associated to a request so is outside of the status machine
#but fill the events total with a large number anyway to track progress.
production.setSimulationEvents(events,eventNumberTotal)

prodDescription = 'A four step workflow Gauss->Boole->Brunel + Merging'

production.addGaussStep(gauss['Version'],gaussGenerator,events,gauss['Options'],
                        eventType=evtType,extraPackages=gauss['ExtraPackages'],
                        condDBTag=conditionsDBTag,ddDBTag=detDescDBTag)
production.addBooleStep(boole['Version'],booleAppType.lower(),boole['Options'],extraOpts=booleExtraOpts,
                        extraPackages=boole['ExtraPackages'],extraOutputFile=mdfOutput,
                        condDBTag=conditionsDBTag,ddDBTag=detDescDBTag)
production.addBrunelStep(brunel['Version'],brunelAppType.lower(),brunel['Options'],
                         extraPackages=brunel['ExtraPackages'],inputDataType=booleAppType.lower(),
                         condDBTag=conditionsDBTag,ddDBTag=detDescDBTag)
  
prodDescription = '%s for BK %s %s event type %s with %s events per job and final\
                   application file type %s.' %(prodDescription,configName,configVersion,evtType,events,brunelAppType)
gLogger.info(prodDescription)
production.setWorkflowDescription(prodDescription)  
production.addFinalizationStep()
production.setCPUTime(cpu)
production.setProdGroup(prodGroup)
production.setProdPriority(priority)
production.setOutputMode('Local') # 'Any' was used but this means all outputs go to CERN...
production.setFileMask(outputFileMask)

#To test the sim prod you can uncomment the below, at least this worked when the template was written :). 
#print production.runLocal()
#print 'Exiting ...'
#DIRAC.exit(0)

###########################################
# Publish and extend the MC production
###########################################
mcProdID=''

if not debug:
  #Note we should publish the production to get the MDF files in the BK!
  result = production.create(transformation=False,bkScript=False)
  if not result['OK']:
    gLogger.error('Error during production creation:\n%s. Exiting...' %(result['Message']))
    DIRAC.exit(2)
    
  mcProdID = result['Value']
  msg = 'MC Production %s successfully created' %mcProdID
  if extend:
    diracProd.extendProduction(mcProdID,extend,printOutput=True)
    msg += ', extended by %s jobs' %extend
  
  diracProd.production(mcProdID,subMode,printOutput=True)
  msg += ' and started in %s submission mode.' %(subMode)
  gLogger.info(msg)
else:
  gLogger.info('========> Debug flag is set, printing JDL and creating a workflow')
  print production._dumpParameters()
  production.createWorkflow()

#################################################################################
# This is the start of the merging production definition 
#################################################################################

# First define the necessary fields in the BK query
mergingBKQuery = getBKQueryDict()
mergingBKQuery['FileType']=brunelAppType.upper()
mergingBKQuery['EventType']=evtType
#in case we are just using debug mode
if mcProdID:
  mergingBKQuery['ProductionID']=int(mcProdID)

#Start the production definition
mergeProd = Production()
mergeProd.setSystemConfig(sysConfig)
mergeProd.setProdType('Merge')
mergeProd.setWorkflowName('MC_Merging_%s_Test_%s' %(brunelAppType,appendName))
mergeProd.setWorkflowDescription('MC workflow for merging outputs from a production %s.' %(mcProdID))
mergeProd.setBKParameters(configName,configVersion,mergingProdGroup,simCond)
mergeProd.setDBTags(conditionsDBTag,detDescDBTag)

mergeProd.addMergeStep(lhcb['Version'],optionsFile=lhcb['Options'],eventType=evtType,
                       inputDataType=brunelAppType.lower(),inputProduction=mcProdID,inputData=[],
                       condDBTag=conditionsDBTag,ddDBTag=detDescDBTag)

mergeProd.addFinalizationStep(removeInputData=mergingRemoveIDFlag)
mergeProd.setInputBKSelection(mergingBKQuery)
mergeProd.setInputDataPolicy('download')
mergeProd.setJobFileGroupSize(mergingGroupSize)
mergeProd.setProdGroup(mergingProdGroup)
mergeProd.setProdPriority(mergingPriority)
mergeProd.setFileMask(brunelAppType.lower())
mergeProd.setProdPlugin(mergingPlugin)

###########################################
# Publish and extend the merging production
###########################################

mcMergeID = None

if not debug:
  result = mergeProd.create(bkScript=False,transformation=mergingTransFlag)
  if not result['OK']:
    gLogger.error('Error during merging production creation:\n%s\n' %(result['Message']))
    DIRAC.exit(2)
  
  mcMergeID = result['Value']
  diracProd.production(mcMergeID,subMode,printOutput=True)
  gLogger.info('MC Merging Production %s successfully created and started in %s submission mode.' %(mcMergeID,subMode))
else:
  gLogger.info('========> Debug flag is set, printing JDL and creating a workflow')
  print mergeProd._dumpParameters()
  mergeProd.createWorkflow()

#################################################################################
# This is the start of the reconstruction production definition
#################################################################################

# First define the necessary fields in the BK query (could use MC prodID to
# navigate to the MDF files but will try to be as authentic as possible with
# the query
recoBKQuery = getBKQueryDict()
recoBKQuery['SimulationConditions']=simCond
recoBKQuery['ProcessingPass']=prodGroup
recoBKQuery['FileType']='MDF'
recoBKQuery['EventType']=evtType
recoBKQuery['ConfigName']=configName
recoBKQuery['ConfigVersion']=configVersion

recoProd = Production()
recoProd.setSystemConfig(sysConfig)
recoProd.setCPUTime(recoCPU)
recoProd.setProdType('DataReconstruction')
recoProd.setWorkflowName('Reconstruction_FULL_Test_%s' %(appendName))
recoProd.setWorkflowDescription("%s Real data reconstruction production." %(prodGroup))
recoProd.setBKParameters(configName,configVersion,recoProdGroup,simCond)
recoProd.setDBTags(recoConditionsDBTag,recoDetDescDBTag)

recoProd.addBrunelStep(brunel['Version'],recoAppType.lower(),brunel['RecoOptions'],
                       extraPackages=brunel['ExtraPackages'],eventType=evtType,inputData=[],
                       inputDataType='mdf',dataType='Data',histograms=True)

recoProd.addDaVinciStep(davinci['Version'],'dst',davinci['Options'],
                        extraPackages=davinci['ExtraPackages'],inputDataType=recoAppType.lower(),
                        dataType='Data',histograms=True)

recoProd.addFinalizationStep()
recoProd.setInputBKSelection(recoBKQuery)
recoProd.setProdGroup(recoProdGroup)
recoProd.setFileMask('dst;root;sdst')
recoProd.setProdPriority(recoPriority)
recoProd.setProdPlugin(recoPlugin)
recoProd.setInputDataPolicy(recoIDPolicy)
recoProd.setJobFileGroupSize(recoGroupSize)

###########################################
# Publish and extend the reco production
###########################################

if not debug:
  result = recoProd.create(bkQuery=recoBKQuery,bkScript=False,transformation=recoTransFlag)
  if not result['OK']:
    gLogger.error('Error during reconstruction production creation:\n%s\n' %(result['Message']))
    DIRAC.exit(2)
  
  recoProdID = result['Value']
  diracProd.production(recoProdID,subMode,printOutput=True)
  gLogger.info('Reconstruction Production %s successfully created and started in %s submission mode.' %(recoProdID,subMode))
else:
  gLogger.info('========> Debug flag is set, printing JDL and creating a workflow')
  print recoProd._dumpParameters()
  recoProd.createWorkflow()

if not debug:  
  gLogger.info('Execution successful, created MCSimulation %s, MCMerging %s with transformation and Reconstruction %s' %(mcProdID,mcMergeID,recoProdID)) 
else:
  gLogger.info('Workflows were created and parameters printed at verbose level since /Operations/CertificationTesting/General/Debug=True flag was specified')
DIRAC.exit(0)

#################################################################################
# This is the start of the reconstruction merging production definition - TODO
#################################################################################

#recoMergeBKQuery = getBKQueryDict()

###########################################
# Publish and extend the reco production
###########################################

#if not debug:
#  result = recoMergeProd.create(bkQuery=recoMergeBKQuery,bkScript=True,transformation=recoTransFlag)
#  if not result['OK']:
#    gLogger.error('Error during reconstruction production creation:\n%s\n' %(result['Message']))
#    DIRAC.exit(2)
#  
#  recoMergeProdID = result['Value']
#  diracProd.production(recoMergeProdID,subMode,printOutput=True)
#  gLogger.info('Reco merging production %s successfully created and started in %s submission mode.' %(recoMergeProdID,subMode))
#else:
#  gLogger.info('========> Debug flag is set, printing JDL and creating a workflow')
#  print recoMergeProd._dumpParameters()
#  recoMergeProd.createWorkflow()
#  
#################################################################################
# End of the template.
#################################################################################