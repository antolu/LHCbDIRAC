########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Client/Production.py,v 1.1 2009/03/31 22:37:44 paterson Exp $
# File :   Production.py
# Author : Stuart Paterson
########################################################################

""" Production API

    Notes:
    - Tested for simulation type workflows only
    - Reconstruction is yet to be fully implemented
    - Automatic construction and publishing of the BK pass info is yet to
      be implemented (but now trivial)
"""

__RCSID__ = "$Id: Production.py,v 1.1 2009/03/31 22:37:44 paterson Exp $"

import string, re, os, time, shutil, types, copy

try:
  from LHCbSystem.Client.LHCbJob import *
except Exception,x:
  from DIRAC.LHCbSystem.Client.LHCbJob import *

COMPONENT_NAME='LHCbSystem/Client/Production'

class Production(LHCbJob):

  #############################################################################
  def __init__(self,script=None):
    """Instantiates the Workflow object and some default parameters.
    """
    Job.__init__(self,script)
    self.csSection = '/Production/Defaults'
    self.gaudiStepCount = 0
    self.currentStepPrefix = ''
    self.inputDataType = 'DATA' #Default, other options are MDF, ETC
    self.tier1s=gConfig.getValue('%s/Tier1s' %(self.csSection),['LCG.CERN.ch','LCG.CNAF.it','LCG.NIKHEF.nl','LCG.PIC.es','LCG.RAL.uk','LCG.GRIDKA.de','LCG.IN2P3.fr'])
    self.histogramName =gConfig.getValue('%s/HistogramName' %(self.csSection),'@{applicationName}_@{STEP_ID}_Hist.root')
    self.histogramSE =gConfig.getValue('%s/HistogramSE' %(self.csSection),'CERN-HIST')
    self.systemConfig = gConfig.getValue('%s/SystemConfig' %(self.csSection),'slc4_ia32_gcc34')
    self.inputDataDefault = gConfig.getValue('%s/InputDataDefault' %(self.csSection),'/lhcb/data/2009/RAW/EXPRESS/FEST/FEST/44878/044878_0000000002.raw')
    self.ioDict = {}
    self.gaussList = []
    self.prodTypes = ['DataReconstruction','DataStripping','MCSimulation','MCStripping','Merge']
    self.name='unspecifiedWorkflow'
    if not script:
      self.__setDefaults()

  #############################################################################
  def __setDefaults(self):
    """Sets some default parameters.
    """
    self.setType('MCSimulation')
    self.setSystemConfig(self.systemConfig)
    self.setCPUTime('300000')
    self.setLogLevel('verbose')
    self.setJobGroup('@{PRODUCTION_ID}')
    self.setFileMask('dummy')

    #General workflow parameters
    self._setParameter('PRODUCTION_ID','string','00012345','ProductionID')
    self._setParameter('JOB_ID','string','00012345','ProductionJobID')
    self._setParameter('poolXMLCatName','string','pool_xml_catalog.xml','POOLXMLCatalogName')
    self._setParameter('Priority','JDL','1','Priority')
    self._setParameter('emailAddress','string','lhcb-datacrash@cern.ch','CrashEmailAddress')
    self._setParameter('DataType','string','MC','Priority') #MC or DATA
    self._setParameter('outputMode','string','Local','SEResolutionPolicy')

 #TODO: resolve this problem
 #   inputData = self.inputDataDefault
 #   self._setParameter('sourceData',inputData,'string','LinkToInputData')

    #Options related parameters
    self._setParameter('CondDBTag','string','sim-20090112','CondDBTag')
    self._setParameter('DDDBTag','string','head-20090112','DetDescTag')
    self._setParameter('EventMaxDefault','string','-1','DefaultNumberOfEvents')

    #BK related parameters
    self._setParameter('configName','string','MC','ConfigName')
    self._setParameter('configVersion','string','2009','ConfigVersion')
    self._setParameter('conditions','string','','SimOrDataTakingCondsString')
    #To review
    #workflow.addParameter(Parameter("OUTPUT_MAX","20","string","","",True,False,"nb max of output to keep"))

    #To handle
    #workflow.addParameter(Parameter("sourceData",indata,"string","","",True, False, "Application Name"))

  #############################################################################
  def _setParameter(self,name,parameterType,parameterValue,description):
    """Set parameters checking in CS in case some defaults need to be changed.
    """
    if gConfig.getValue('%s/%s' %(self.csSection,name),''):
      self.log.debug('Setting %s from CS defaults = %s' %(name,gConfig.getValue('%s/%s' %(self.csSection,name))))
      self._addParameter(self.workflow,name,parameterType,gConfig.getValue('%s/%s' %(self.csSection,name),'default'),description)
    else:
      self.log.debug('Setting parameter %s = %s' %(name,parameterValue))
      self._addParameter(self.workflow,name,parameterType,parameterValue,description)

  #############################################################################
  def _getOptions(self,appName,appType,extraOpts=None,spillOver=False,pileUp=True):
    """ Simple function to create the default options for a given project name.

        Assumes CondDB tags and event max are required.
    """
    options = []
    #General options
    dddbOpt = "LHCbApp().DDDBtag = \"@{DDDBTag}\""
    conddbOpt = "LHCbApp().CondDBtag = \"@{CondDBTag}\""
    evtOpt = "LHCbApp().EvtMax = @{numberOfEvents}"
    if appName.lower()=='gauss':
      options.append("MessageSvc().Format = '%u % F%18W%S%7W%R%T %0W%M';MessageSvc().timeFormat = '%Y-%m-%d %H:%M:%S UTC'")
      options.append("OutputStream(\"GaussTape\").Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\"")
      #Below 3 lines added to replace evt/evt printout
      options.append('from Configurables import SimInit')
      options.append('GaussSim = SimInit("GaussSim")')
      options.append('GaussSim.OutputLevel = 2')
      options.append("HistogramPersistencySvc().OutputFile = \"%s\"" %(self.histogramName))
    elif appName.lower()=='boole':
      if appType.lower()=='mdf':
        options.append("OutputStream(\"RawWriter\").Output = \"DATAFILE=\'PFN:@{outputData}\' SVC=\'LHCb::RawDataCnvSvc\' OPT=\'RECREATE\'\"")
        options.append("OutputStream(\"RawWriter\").OutputLevel = INFO")
      else:
        options.append("OutputStream(\"DigiWriter\").Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\"")
        options.append("MessageSvc().Format = '%u % F%18W%S%7W%R%T %0W%M';MessageSvc().timeFormat = '%Y-%m-%d %H:%M:%S UTC'")
      if spillOver:
        options.append('Boole().UseSpillover = True')
        if pileUp:
          options.append("EventSelector(\"SpilloverSelector\").Input = [\"DATAFILE=\'@{spilloverData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\",\"DATAFILE=\'@{pileupData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\"]")
        else:
          options.append("EventSelector(\"SpilloverSelector\").Input = [\"DATAFILE=\'@{spilloverData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\"]")
      options.append("HistogramPersistencySvc().OutputFile = \"%s\"" %(self.histogramName))
    elif appName.lower()=='brunel':
      options.append("#include \"$BRUNELOPTS/SuppressWarnings.opts\"")
      options.append("MessageSvc().Format = '%u % F%18W%S%7W%R%T %0W%M';MessageSvc().timeFormat = '%Y-%m-%d %H:%M:%S UTC'")
      options.append("OutputStream(\"DstWriter\").Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\"")
      options.append("HistogramPersistencySvc().OutputFile = \"%s\"" %(self.histogramName))
    elif appName.lower()=='davinci':
      options.append("MessageSvc().Format = '%u % F%18W%S%7W%R%T %0W%M';MessageSvc().timeFormat = '%Y-%m-%d %H:%M:%S UTC'")
      options.append('from DaVinci.Configuration import *')
      options.append("DaVinci().HistogramFile = \"%s\"" %(self.histogramName))
      options.append('DaVinci().EvtMax=@{numberOfEvents}')
    else:
      self.log.warn('No specific options found for project %s' %appName)

    if extraOpts:
      options.append(extraOpts)

    options.append(dddbOpt)
    options.append(conddbOpt)
    options.append(evtOpt)
    return options

  #############################################################################
  def addGaussStep(self,appVersion,eventType,generatorName,numberOfEvents,optionsFile,extraPackages,outputSE=None,histograms=True,overrideOpts=''):
    """ Wraps around addGaudiStep and getOptions.
    """
    firstEventNumber=1
    if not overrideOpts:
      optionsLine = self._getOptions('Gauss','sim',extraOpts=None,spillOver=False,pileUp=False)
      self.log.info('Default options are:\n%s' %(string.join(optionsLine,'\n')))
      optionsLine = string.join(optionsLine,';')
    else:
      optionsLine = overrideOpts

    if not outputSE:
      outputSE='CERN-RDST'
    self._setParameter('dataType','string','MC','DataType') #MC or DATA to be reviewed
    gaussStep = self._addGaudiStep('Gauss',appVersion,'sim',numberOfEvents,eventType,optionsFile,optionsLine,extraPackages,outputSE,'','None',histograms,firstEventNumber,'','')
    self.gaussList.append(gaussStep.getName())
    gaussStep.setValue('numberOfEventsInput', 0)
    gaussStep.setValue('numberOfEventsOutput', 0)
    gaussStep.setValue('generatorName',generatorName)

  #############################################################################
  def addBooleStep(self,appVersion,appType,eventType,optionsFile,extraPackages,outputSE=None,histograms=True,spillover=False,pileup=True,inputData='previousStep',overrideOpts=''):
    """ Wraps around addGaudiStep and getOptions.
        appType is mdf / digi
        currently assumes input data type is sim
    """
    firstEventNumber=0
    numberOfEvents='-1'
    inputDataType='sim'
    inputData='previousStep'

    if not overrideOpts:
      optionsLine = self._getOptions('Boole',appType,extraOpts=None,spillOver=spillover,pileUp=pileup)
      self.log.info('Default options are:\n%s' %(string.join(optionsLine,'\n')))
      optionsLine = string.join(optionsLine,';')
    else:
      optionsLine = overrideOpts

    if not outputSE:
      outputSE='CERN-RDST'

    spilloverValue=''
    pileupValue=''
    if spillover:
      spilloverValue=self.gaussList[1]
      if not self.gaussList:
        raise TypeError,'No Gauss step outputs found'
      if pileup:
        if not len(self.gaussList)>2:
          raise TypeError,'Not enough Gauss steps for pile up'
        pileupValue=self.gaussList[2]

    self._setParameter('dataType','string','MC','DataType') #MC or DATA to be reviewed
    self._addGaudiStep('Boole',appVersion,appType,numberOfEvents,eventType,optionsFile,optionsLine,extraPackages,outputSE,inputData,inputDataType,histograms,firstEventNumber,spilloverValue,pileupValue)

  #############################################################################
  def addBrunelStep(self,appVersion,appType,eventType,optionsFile,extraPackages,inputData='previousStep',inputDataType='mdf',outputSE=None,histograms=True,overrideOpts='',numberOfEvents='-1'):
    """ Wraps around addGaudiStep and getOptions.
        appType is rdst / dst
        inputDataType is mdf / digi
        enough to set one of the above
        TODO: stripping case - to review
    """
    if appType.lower()=='rdst':
      dataType='DATA'
      if not outputSE:
        outputSE='Tier1-RDST'
    else:
      if not appType.lower()=='dst':
        raise TypeError,'Application type not recognised'
      if inputDataType.lower()=='digi':
        dataType='MC'
        if not outputSE:
          outputSE='Tier1_MC_M-DST' #for simulated master dst
      elif inputDataType.lower()=='fetc': #TODO: stripping case - to review
        dataType='DATA'
        if not outputSE:
          outputSE='Tier1 _M-DST' #for real data master dst

    if not overrideOpts:
      optionsLine = self._getOptions('Brunel',appType,extraOpts=None,spillOver=False,pileUp=False)
      self.log.info('Default options are:\n%s' %(string.join(optionsLine,'\n')))
      optionsLine = string.join(optionsLine,';')
    else:
      optionsLine = overrideOpts

    firstEventNumber=0
    self._setParameter('dataType','string',dataType,'DataType') #MC or DATA to be reviewed
    self._addGaudiStep('Brunel',appVersion,appType,numberOfEvents,eventType,optionsFile,optionsLine,extraPackages,outputSE,inputData,inputDataType,histograms,firstEventNumber,'','')

  #############################################################################
  def addDaVinciStep(self,appVersion,appType,eventType,optionsFile,extraPackages,inputData='previousStep',inputDataType='rdst',outputSE=None,histograms=True,overrideOpts='',numberOfEvents='-1'):
    """ Wraps around addGaudiStep and getOptions.
        appType is  dst / dst /undefined at the moment ;)
        inputDataType is rdst / root

        TODO: stripping case - to review
    """
    firstEventNumber=0
    self._setParameter('dataType','string','MC','DataType') #MC or DATA to be reviewed
    self._addGaudiStep('Brunel',appVersion,appType,numberOfEvents,eventType,optionsFile,optionsLine,extraPackages,outputSE,'','None',histograms,firstEventNumber,'','')

  #############################################################################
  def _addGaudiStep(self,appName,appVersion,appType,numberOfEvents,eventType,optionsFile,optionsLine,extraPackages,outputSE,inputData='previousStep',inputDataType='None',histograms=True,firstEventNumber=0,spillover='',pileup=''):
    """Helper function.
    """
    if not type(appName) == type(' ') or not type(appVersion) == type(' '):
      raise TypeError,'Expected strings for application name and version'

    self.gaudiStepCount +=1
    gaudiStep =  self.__getGaudiApplicationStep('%s_%s' %(appName,self.gaudiStepCount))

    if spillover:
      gaudiStep.setLink('spilloverData',spillover,'outputData')
    else:
      gaudiStep.setValue('spilloverData',None)
    if pileup:
      gaudiStep.setLink('pileupData',pileup,'outputData')
    else:
      gaudiStep.setValue('pileupData',None)

    gaudiStep.setValue('applicationName',appName)
    gaudiStep.setValue('applicationVersion',appVersion)
    gaudiStep.setValue('applicationType',appType)
    gaudiStep.setValue('optionsFile',optionsFile)
    gaudiStep.setValue('optionsLine',optionsLine)
    gaudiStep.setValue('optionsLinePrev','None')
    gaudiStep.setValue('numberOfEvents', numberOfEvents)
    gaudiStep.setValue('eventType',eventType)
    gaudiStep.setValue('extraPackages',extraPackages)
    self.__addSoftwarePackages(extraPackages)

    if firstEventNumber:
      gaudiStep.setValue('firstEventNumber',firstEventNumber)

    if not inputData:
      self.log.verbose('Assume %s step has no input data requirement' %appName)
    elif inputData=='previousStep':
      self.log.verbose('Taking input data as output from previous Gaudi step')
      if not self.ioDict.has_key(self.gaudiStepCount-1):
        raise TypeError,'Expected previously defined Gaudi step for input data'
      gaudiStep.setLink('inputData',self.ioDict[self.gaudiStepCount-1],'outputData')
    else:
      self.log.verbose('Assume input data requirement should be added to job')
      gaudiStep.setValue('inputData',string.join(inputData,';'))
      self.setInputData(inputData)
    if inputDataType != 'None':
      gaudiStep.setValue('inputDataType',string.upper(inputDataType))

    gaudiStep.setValue('applicationLog', '@{applicationName}_@{STEP_ID}.log')
    gaudiStep.setValue('outputData','@{STEP_ID}.@{applicationType}')
    outputList=[]
    outputList.append({"outputDataName":"@{STEP_ID}.@{applicationType}","outputDataType":"@{applicationType}","outputDataSE":outputSE})
    if histograms:
      outputList.append({"outputDataName":self.histogramName,"outputDataType":"HIST","outputDataSE":self.histogramSE})

    gaudiStep.setValue('listoutput',(outputList))

    #Ensure the global input data type is null
    description = 'Default input data type field'
    self._addParameter(self.workflow,'InputDataType','JDL','',description)

    # now we have to tell DIRAC to install the necessary software
    self.__addSoftwarePackages('%s.%s' %(appName,appVersion))

    #to keep track of the inputs / outputs for a given workflow track the step number and name
    self.ioDict[self.gaudiStepCount]=gaudiStep.getName()
    return gaudiStep

  #############################################################################
  def __addSoftwarePackages(self,nameVersion):
    """ Internal method to accumulate software packages.
    """
    swPackages = 'SoftwarePackages'
    description='LHCbSoftwarePackages'
    if not self.workflow.findParameter(swPackages):
      self._addParameter(self.workflow,swPackages,'JDL',nameVersion,description)
    else:
      apps = self.workflow.findParameter(swPackages).getValue()
      apps = apps.split(';')
      apps.append(nameVersion)
      tmp = []
      for app in apps:
        if app:
          tmp.append(app)
      apps = string.join(tmp,';')
      self._addParameter(self.workflow,swPackages,'JDL',apps,description)

  #############################################################################
  def __getGaudiApplicationStep(self,name):
    """Internal function.

      This method controls the definition for a GaudiApplication step.
    """
    gaudiApp = ModuleDefinition('GaudiApplication')
    gaudiApp.setDescription('A generic Gaudi Application module that can execute any provided project name and version')
    gaudiApp.setBody('from WorkflowLib.Module.GaudiApplication import GaudiApplication\n')

    analyseLog = ModuleDefinition('LogChecker')
    analyseLog.setDescription('Check LogFile module')
    analyseLog.setBody('from WorkflowLib.Module.LogChecker import *\n')

    genBKReport = ModuleDefinition('BookkeepingReport')
    genBKReport.setDescription('Bookkeeping Report module')
    genBKReport.setBody('from WorkflowLib.Module.BookkeepingReport import * \n')
    #self._addParameter(genBKReport,'STEP_ID','string','','StepID')
    #must update job addParam function to deal with self
    genBKReport.addParameter(Parameter("STEP_ID","","string","self","STEP_ID",True,False,"StepID"))

    gaudiAppDefn = StepDefinition('Gaudi_App_Step')
    gaudiAppDefn.addModule(gaudiApp)
    gaudiAppDefn.createModuleInstance('GaudiApplication', 'gaudiApp')
    gaudiAppDefn.addModule(analyseLog)
    gaudiAppDefn.createModuleInstance('LogChecker', 'analyseLog')
    gaudiAppDefn.addModule(genBKReport)
    gaudiAppDefn.createModuleInstance('BookkeepingReport', 'genBKReport')
    gaudiAppDefn.addParameterLinked(analyseLog.parameters)
    gaudiAppDefn.addParameterLinked(gaudiApp.parameters)

    self._addParameter(gaudiAppDefn,'inputData','string','','StepInputData')
    self._addParameter(gaudiAppDefn,'inputDataType','string','','InputDataType')
    self._addParameter(gaudiAppDefn,'eventType','string','','EventType')
    self._addParameter(gaudiAppDefn,'outputData','string','','InputData')
    self._addParameter(gaudiAppDefn,'generatorName','string','','GeneratorName')
    self._addParameter(gaudiAppDefn,'applicationName','string','','ApplicationName')
    self._addParameter(gaudiAppDefn,'applicationVersion','string','','ApplicationVersion')
    self._addParameter(gaudiAppDefn,'applicationType','string','',"ApplicationType")
    self._addParameter(gaudiAppDefn,'applicationLog','string','','ApplicationLogFile')
    self._addParameter(gaudiAppDefn,'optionsFile','string','','OptionsFile')
    self._addParameter(gaudiAppDefn,'optionsLine','string','','OptionsLines')
    self._addParameter(gaudiAppDefn,'optionsLinePrev','string','','PreviousOptionsLines')
    self._addParameter(gaudiAppDefn,'numberOfEvents','string','','NumberOfEvents')
    self._addParameter(gaudiAppDefn,'numberOfEventsInput','string','','NumberOfEventsInput')
    self._addParameter(gaudiAppDefn,'numberOfEventsOutput','string','','NumberOfEventsOutput')
    self._addParameter(gaudiAppDefn,'listoutput','list',[],'StepOutputList')
    self._addParameter(gaudiAppDefn,'extraPackages','string','','ExtraPackages')
    self._addParameter(gaudiAppDefn,'firstEventNumber','string','int','FirstEventNumber')
    self._addParameter(gaudiAppDefn,'spilloverData','string','','spilloverData')
    self._addParameter(gaudiAppDefn,'pileupData','string','','pileupData')
    self.workflow.addStep(gaudiAppDefn)
    return self.workflow.createStepInstance('Gaudi_App_Step',name)

  #############################################################################
  def addFinalizationStep(self,sendBookkeeping=True,uploadData=True,uploadLogs=True,sendFailover=True):
    """ Adds the finalization step with enable flags for each module.
    """
    for param in [sendBookkeeping,uploadData,uploadLogs,sendFailover]:
      if not type(param)==type(True):
        raise TypeError,'All arguments to addFinalizationStep must be boolean'

    sendBK = ModuleDefinition('SendBookkeeping')
    sendBK.setDescription('Sends the BK reports')
    self._addParameter(sendBK,'Enable','bool','True','EnableFlag')
    sendBK.setBody('from WorkflowLib.Module.SendBookkeeping import SendBookkeeping \n')

    dataUpload = ModuleDefinition('UploadOutputData')
    dataUpload.setDescription('Uploads the output data')
    self._addParameter(dataUpload,'Enable','bool','True','EnableFlag')
    dataUpload.setBody('from WorkflowLib.Module.UploadOutputData import UploadOutputData \n')

    logUpload = ModuleDefinition('UploadLogFile')
    logUpload.setDescription('Uploads the log files')
    self._addParameter(logUpload,'Enable','bool','True','EnableFlag')
    logUpload.setBody('from WorkflowLib.Module.UploadLogFile import UploadLogFile \n')

    failoverRequest = ModuleDefinition('FailoverRequest')
    failoverRequest.setDescription('Sends any failover requests')
    self._addParameter(failoverRequest,'Enable','bool','True','EnableFlag')
    failoverRequest.setBody('from WorkflowLib.Module.FailoverRequest import * \n')

    finalization = StepDefinition('Job_Finalization')

    sendBK.setLink('Enable','self','BKEnable')
    finalization.addModule(sendBK)
    finalization.createModuleInstance('SendBookkeeping','sendBK')
    self._addParameter(finalization,'BKEnable','bool','True','EnableFlag')

    dataUpload.setLink('Enable','self','UploadEnable')
    finalization.addModule(dataUpload)
    finalization.createModuleInstance('UploadOutputData','dataUpload')
    self._addParameter(finalization,'UploadEnable','bool','True','EnableFlag')

    logUpload.setLink('Enable','self','LogEnable')
    finalization.addModule(logUpload)
    finalization.createModuleInstance('UploadLogFile','logUpload')
    self._addParameter(finalization,'LogEnable','bool','True','EnableFlag')

    failoverRequest.setLink('Enable','self','FailoverEnable')
    finalization.addModule(failoverRequest)
    finalization.createModuleInstance('FailoverRequest','failoverRequest')
    self._addParameter(finalization,'FailoverEnable','bool','True','EnableFlag')

    self.workflow.addStep(finalization)
    finalizeStep = self.workflow.createStepInstance('Job_Finalization', 'finalization')
    finalizeStep.setValue('BKEnable',sendBookkeeping)
    finalizeStep.setValue('UploadEnable',uploadData)
    finalizeStep.setValue('LogEnable',uploadLogs)
    finalizeStep.setValue('FailoverEnable',sendFailover)

  #############################################################################
  def createWorkflow(self):
    """ Create XML for local test e.g. configname test and version <year> etc.
    """
    self.workflow.toXMLFile('%s.xml' %self.name)

  #############################################################################
  def createProduction(self):
    """ Will create the production and subsequently publish to the BK, this
        currently relies on the conditions information being present there.
    """
    return True

  #############################################################################
  def setWorkflowLib(self,tag):
    """Set workflow lib version for the production.
    """
    tag = gConfig.getValue('%s/WorkflowLibVersion' %(self.csSection),'v9r9')
    lfn = 'LFN:/lhcb/applications/WorkflowLib-wkf-TAG.tar.gz'.replace('TAG',tag)
    self.log.debug('Setting workflow library LFN to %s' %lfn)
    self._setParameter('InputSandbox','JDL',lfn,'WorkflowLibVersion')

  #############################################################################
  def setFileMask(self,fileMask):
    """Output data related parameters.
    """
    self._setParameter('outputDataFileMask','string',fileMask,'outputDataFileMask')

  #############################################################################
  def setWorkflowName(self,name):
    """Set workflow name.
    """
    self.workflow.setName(name)
    self.name = name

  #############################################################################
  def setWorkflowDescription(self,desc):
    """Set workflow name.
    """
    self.workflow.setDescription(desc)

  #############################################################################
  def setProdType(self,prodType):
    """Set prod type.
    """
    if not prodType in self.prodTypes:
      raise TypeError,'Prod must be one of %s' %(string.join(self.prodTypes,', '))
    self.setType(prodType)

  #############################################################################
  def banTier1s(self):
    """ Sets Tier1s as banned.
    """
    self.setBannedSites(self.tier1s)

  #############################################################################
  def setTargetSite(self,site):
    """ Sets destination for all jobs.
    """
    self.setDestination(site)

  #############################################################################
  def setBKParameters(self,configName,configVersion,groupDescription,conditions):
    """ Sets BK parameters for production.
    """
    self._setParameter('configName','string',configName,'ConfigName')
    self._setParameter('configVersion','string',configVersion,'ConfigVersion')
    self._setParameter('groupDescription','string',groupDescription,'GroupDescription')
    self._setParameter('conditions','string',conditions,'SimOrDataTakingCondsString')
    self._setParameter('simDescription','string',conditions,'SimDescription')

  #############################################################################
  def getBKProcessingPass(self,xmlFile=''):
    """
    """
    return True

  #############################################################################
  def setDBTags(self,conditions='sim-20090112',detector='head-20090112'):
    """ Sets destination for all jobs.
    """
    self._setParameter('CondDBTag','string',conditions,'CondDBTag')
    self._setParameter('DDDBTag','string',detector,'DetDescTag')

  #############################################################################
  def setProdPriority(self,priority):
    """ Sets destination for all jobs.
    """
    self._setParameter('Priority','JDL',str(priority),'UserPriority')

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#