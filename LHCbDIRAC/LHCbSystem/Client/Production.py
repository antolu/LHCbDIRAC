########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Client/Production.py,v 1.28 2009/07/13 12:40:37 acsmith Exp $
# File :   Production.py
# Author : Stuart Paterson
########################################################################

""" Production API

    Notes:
    - Supports simulation + reconstruction type workflows only
    - Stripping is yet to be fully implemented (pending something to test)
    - Now have create() method that takes a workflow or Production object
      and publishes to the production management system, in addition this
      can automatically construct and publish the BK pass info

    To add:
    - Use getOutputLFNs() function to add production output directory parameter
"""

__RCSID__ = "$Id: Production.py,v 1.28 2009/07/13 12:40:37 acsmith Exp $"

import string, re, os, time, shutil, types, copy

from DIRAC.Interfaces.API.DiracProduction             import DiracProduction
from DIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from DIRAC.Core.Workflow.Workflow                     import *
from DIRAC.Core.DISET.RPCClient                       import RPCClient

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
    self.prodVersion=__RCSID__
    self.csSection = '/Production/Defaults'
    self.gaudiStepCount = 0
    self.currentStepPrefix = ''
    self.inputDataType = 'DATA' #Default, other options are MDF, ETC
    self.tier1s=gConfig.getValue('%s/Tier1s' %(self.csSection),['LCG.CERN.ch','LCG.CNAF.it','LCG.NIKHEF.nl','LCG.PIC.es','LCG.RAL.uk','LCG.GRIDKA.de','LCG.IN2P3.fr'])
    self.histogramName =gConfig.getValue('%s/HistogramName' %(self.csSection),'@{applicationName}_@{STEP_ID}_Hist.root')
    self.histogramSE =gConfig.getValue('%s/HistogramSE' %(self.csSection),'CERN-HIST')
    self.systemConfig = gConfig.getValue('%s/SystemConfig' %(self.csSection),'slc4_ia32_gcc34')
    self.inputDataDefault = gConfig.getValue('%s/InputDataDefault' %(self.csSection),'/lhcb/data/2009/RAW/EXPRESS/FEST/FEST/44878/044878_0000000002.raw')
    self.defaultProdID = '12345'
    self.defaultProdJobID = '12345'
    self.ioDict = {}
    self.gaussList = []
    self.prodTypes = ['DataReconstruction','DataStripping','MCSimulation','MCStripping','Merge']
    self.name='unspecifiedWorkflow'
    self.firstEventType = ''
    self.bkSteps = {}
    self.prodGroup = ''
    self.plugin = ''
    self.inputFileMask = ''
    self.inputBKSelection = {}
    self.jobFileGroupSize = 0
    self.ancestorProduction = ''
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

    #version control
    self._setParameter('productionVersion','string',self.prodVersion,'ProdAPIVersion')

    #General workflow parameters
    self._setParameter('PRODUCTION_ID','string',self.defaultProdID.zfill(8),'ProductionID')
    self._setParameter('JOB_ID','string',self.defaultProdJobID.zfill(8),'ProductionJobID')
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
    #HACK# Is appType supposed to be input or output data type or just some flag to get the desired behaviour?
    options = []
    #General options
    dddbOpt = "LHCbApp().DDDBtag = \"@{DDDBTag}\""
    conddbOpt = "LHCbApp().CondDBtag = \"@{CondDBTag}\""
    evtOpt = "LHCbApp().EvtMax = @{numberOfEvents}"
    options.append("MessageSvc().Format = '%u % F%18W%S%7W%R%T %0W%M';MessageSvc().timeFormat = '%Y-%m-%d %H:%M:%S UTC'")
    options.append("HistogramPersistencySvc().OutputFile = \"%s\"" % (self.histogramName))
    if appName.lower()=='gauss':
      options.append("OutputStream(\"GaussTape\").Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\"")
      #Below 3 lines added to replace evt/evt printout
      #options.append('from Configurables import SimInit')
      #options.append('GaussSim = SimInit("GaussSim")')
      #options.append('GaussSim.OutputLevel = 2')

    elif appName.lower()=='boole':
      if appType.lower()=='mdf':
        options.append("OutputStream(\"RawWriter\").Output = \"DATAFILE=\'PFN:@{outputData}\' SVC=\'LHCb::RawDataCnvSvc\' OPT=\'RECREATE\'\"")
        options.append("OutputStream(\"RawWriter\").OutputLevel = INFO")
      else:
        options.append("OutputStream(\"DigiWriter\").Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\"")
      if spillOver:
        options.append('Boole().UseSpillover = True')
        if pileUp:
          options.append("EventSelector(\"SpilloverSelector\").Input = [\"DATAFILE=\'@{spilloverData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\",\"DATAFILE=\'@{pileupData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\"]")
        else:
          options.append("EventSelector(\"SpilloverSelector\").Input = [\"DATAFILE=\'@{spilloverData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\"]")

    elif appName.lower()=='brunel':
      options.append("#include \"$BRUNELOPTS/SuppressWarnings.opts\"")
      options.append("OutputStream(\"DstWriter\").Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\"")
      options.append("from Configurables import Brunel")
      if appType.lower()=='xdst':
        options.append("Brunel().OutputType = 'XDST'")
      elif appType.lower()=='dst':
        options.append("Brunel().OutputType = 'DST'")
      elif appType.lower()=='rdst':
        options.append("Brunel().OutputType = 'RDST'")

    elif appName.lower()=='davinci':
      options.append('from DaVinci.Configuration import *') 
      options.append('DaVinci().EvtMax=@{numberOfEvents}')
      options.append("DaVinci().HistogramFile = \"%s\"" % (self.histogramName))
      # If we want to generate an FETC for the first step of the stripping workflow
      if appType.lower()=='fetc':
        options.append("DaVinci().ETCFile = \"@{outputData}\"")
      elif appType.lower() == 'dst':
        options.append("OutputStream(\"DstWriter\").Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\"")
      elif appType.lower() == 'dsts':
        options.append("OutputStream(\"DSTBExclusive\").Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\"")
        # NEED TO DEAL WITH MULTIPLE STREAMS
        #options.append("OutputStream(\"DSTTopological\").Output = \"DATAFILE=\'PFN:02@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\"")
      elif appType.lower() == 'davincihist':
        options.append('from Configurables import InputCopyStream')
        options.append('InputCopyStream().Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'REC\'\"')
        options.append('DaVinci().MoniSequence.append(InputCopyStream())')

    elif appName.lower()=='merge':
      options.append('EventSelector.PrintFreq = 200')
      options.append('OutputStream(\"InputCopyStream\").Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\"')
      return options

    else:
      self.log.warn('No specific options found for project %s' %appName)

    if extraOpts:
      options.append(extraOpts)

    options.append(dddbOpt)
    options.append(conddbOpt)
    options.append(evtOpt)
    return options

  #############################################################################
  def __getEventType(self,eventType):
    """ Checks whether or not the global event type should be set.
    """
    if eventType.lower()=='firststep' and not self.firstEventType:
      raise TypeError,'Must specify event type for initial step'
    elif eventType.lower()=='firststep' and self.firstEventType:
      eventType=self.firstEventType
    elif not self.firstEventType:
      self.firstEventType = eventType

    self.log.verbose('Setting event type for current step to %s' %(eventType))
    return eventType

  #############################################################################
  def addGaussStep(self,appVersion,generatorName,numberOfEvents,optionsFile,eventType='firstStep',extraPackages='',outputSE=None,histograms=False,overrideOpts=''):
    """ Wraps around addGaudiStep and getOptions.
    """
    eventType = self.__getEventType(eventType)
    firstEventNumber=1
    if not overrideOpts:
      optionsLine = self._getOptions('Gauss','sim',extraOpts=None,spillOver=False,pileUp=False)
      self.log.info('Default options for Gauss are:\n%s' %(string.join(optionsLine,'\n')))
      optionsLine = string.join(optionsLine,';')
    else:
      optionsLine = overrideOpts

    if not outputSE:
      outputSE='Tier1-RDST'
      self.log.info('Setting default outputSE to %s' %(outputSE))

    self._setParameter('dataType','string','MC','DataType') #MC or DATA to be reviewed
    gaussStep = self._addGaudiStep('Gauss',appVersion,'sim',numberOfEvents,optionsFile,optionsLine,eventType,extraPackages,outputSE,'','None',histograms,firstEventNumber,'','',{})
    self.gaussList.append(gaussStep.getName())
    gaussStep.setValue('numberOfEventsInput', 0)
    gaussStep.setValue('numberOfEventsOutput', 0)
    gaussStep.setValue('generatorName',generatorName)

  #############################################################################
  def addBooleStep(self,appVersion,appType,optionsFile,eventType='firstStep',extraPackages='',outputSE=None,histograms=False,spillover=False,pileup=True,inputData='previousStep',overrideOpts='',extraOutputFile={}):
    """ Wraps around addGaudiStep and getOptions.
        appType is mdf / digi
        currently assumes input data type is sim
    """
    eventType = self.__getEventType(eventType)
    firstEventNumber=0
    numberOfEvents='-1'
    inputDataType='sim'
    inputData='previousStep'

    if not overrideOpts:
      optionsLine = self._getOptions('Boole',appType,extraOpts=None,spillOver=spillover,pileUp=pileup)
      self.log.info('Default options for Boole are:\n%s' %(string.join(optionsLine,'\n')))
      optionsLine = string.join(optionsLine,';')
    else:
      optionsLine = overrideOpts

    if not outputSE:
      outputSE='Tier1-RDST'
      self.log.info('Setting default outputSE to %s' %(outputSE))

    spilloverValue=''
    pileupValue=''
    if spillover:
      inputData = 'firstStep'
      spilloverValue=self.gaussList[1]
      if not self.gaussList:
        raise TypeError,'No Gauss step outputs found'
      if pileup:
        if not len(self.gaussList)>2:
          raise TypeError,'Not enough Gauss steps for pile up'
        pileupValue=self.gaussList[2]

    if extraOutputFile:
      self.log.info('Adding output file to Boole step %s' %extraOutputFile)

    self._setParameter('dataType','string','MC','DataType') #MC or DATA to be reviewed
    self._addGaudiStep('Boole',appVersion,appType,numberOfEvents,optionsFile,optionsLine,eventType,extraPackages,outputSE,inputData,inputDataType,histograms,firstEventNumber,spilloverValue,pileupValue,extraOutputFile)

  #############################################################################
  def addBrunelStep(self,appVersion,appType,optionsFile,eventType='firstStep',extraPackages='',inputData='previousStep',inputDataType='mdf',outputSE=None,histograms=False,overrideOpts='',extraOpts='',numberOfEvents='-1'):
    """ Wraps around addGaudiStep and getOptions.
        appType is rdst / dst / xdst
        inputDataType is mdf / digi
        enough to set one of the above
        TODO: stripping case - to review
    """
    eventType = self.__getEventType(eventType)
    if appType.lower()=='rdst':
      dataType='DATA'
      if not outputSE:
        outputSE='Tier1-RDST'
    #HACK# We are explicitly producing DSTs until DaVinci is fixed to run DVMonitor on rDSTs.
    elif appType.lower()=='dst':
      dataType = 'DATA'
      if not outputSE:
        #outputSE='Tier1_M-DST' #for real data master dst 
        outputSE='Tier1-RDST' #for real data DSTs from brunel
        self.log.info('Setting default outputSE to %s' %(outputSE))
    else:
      if not appType.lower() in ['dst','xdst']:
        raise TypeError,'Application type not recognised'
      if inputDataType.lower()=='digi':
        dataType='MC'
        if not outputSE:
          outputSE='Tier1_MC_M-DST' #for simulated master dst
          self.log.info('Setting default outputSE to %s' %(outputSE))
      elif inputDataType.lower()=='fetc': #TODO: stripping case - to review
        dataType='DATA'
        if not outputSE:
          outputSE='Tier1_M-DST' #for real data master dst
          self.log.info('Setting default outputSE to %s' %(outputSE))

    if not overrideOpts:
      optionsLine = self._getOptions('Brunel',appType,extraOpts=None,spillOver=False,pileUp=False)
      self.log.info('Default options for Brunel are:\n%s' %(string.join(optionsLine,'\n')))
      optionsLine = string.join(optionsLine,';')
    else:
      optionsLine = overrideOpts
    if extraOpts:
      optionsLine = "%s;%s" % (optionsLine,extraOpts.replace('\n',';'))
    firstEventNumber=0
    self._setParameter('dataType','string',dataType,'DataType') #MC or DATA to be reviewed
    self._addGaudiStep('Brunel',appVersion,appType,numberOfEvents,optionsFile,optionsLine,eventType,extraPackages,outputSE,inputData,inputDataType,histograms,firstEventNumber,'','',{})

  #############################################################################
  def addDaVinciStep(self,appVersion,appType,optionsFile,eventType='firstStep',extraPackages='',inputData='previousStep',inputDataType='rdst',outputSE=None,histograms=False,overrideOpts='',numberOfEvents='-1'):
    """ Wraps around addGaudiStep and getOptions.
        appType is  dst / dst /undefined at the moment ;)
        inputDataType is rdst / root

    """
    eventType = self.__getEventType(eventType)
    firstEventNumber=0
    appTypes = ['dst','fetc','dsts','root']
    if not appType in appTypes:
      raise TypeError,'Application type not currently supported (%s)' % appTypes
    if not inputDataType in ('rdst','dst'):
      raise TypeError,'Only RDST input data type currently supported'

    dataType='DATA'
    if not outputSE:
      outputSE='Tier1_M-DST'
      self.log.info('Setting default outputSE to %s' %(outputSE))

    if not overrideOpts:
      optionsLine = self._getOptions('DaVinci',appType,extraOpts=None,spillOver=False,pileUp=False)
      self.log.info('Default options for DaVinci are:\n%s' %(string.join(optionsLine,'\n')))
      optionsLine = string.join(optionsLine,';')
    else:
      optionsLine = overrideOpts

    self._setParameter('dataType','string',dataType,'DataType') #MC or DATA to be reviewed
    self._addGaudiStep('DaVinci',appVersion,appType,numberOfEvents,optionsFile,optionsLine,eventType,extraPackages,outputSE,inputData,inputDataType,histograms,firstEventNumber,'','',{})

  #############################################################################
  def addMergeStep(self,appVersion='v26r3',optionsFile='$STDOPTS/PoolCopy.opts',inputProduction='',eventType='firstStep',extraPackages='',inputData='previousStep',inputDataType='dst',outputSE=None,overrideOpts='',numberOfEvents='-1'):
    """Wraps around addGaudiStep.  The merging uses a standard Gaudi step with
       any available LHCb project as the application.

       Note: for MDF merging case must not add a finalization step.
    """
    if inputProduction:
      result = self._setInputProductionBKStepInfo(inputProduction)
      if not result['OK']:
        self.log.error(result)
        raise TypeError,'inputProduction must exist and have BK parameters'

    firstEventNumber=0
    histograms=False
    appName = 'LHCb'
    #appVersion =''
    #optionsFile = ''
    appType = inputDataType
    if not outputSE:
      outputSE='Tier1_MC_M-DST'
      self.log.info('Setting default outputSE to %s' %(outputSE))

    if not overrideOpts:
      optionsLine = self._getOptions('Merge',appType,extraOpts=None,spillOver=False,pileUp=False)
      self.log.info('Default options for Merging are:\n%s' %(string.join(optionsLine,'\n')))
      optionsLine = string.join(optionsLine,';')
    else:
      optionsLine = overrideOpts

    self._setParameter('dataType','string','MC','DataType') #MC or DATA to be reviewed, doesn't look like this is used anywhere...
    if inputDataType.lower()=='mdf':
      self._addMergeMDFStep('LHCb',appVersion,appType,numberOfEvents,optionsFile,optionsLine,eventType,extraPackages,outputSE,inputData,inputDataType,histograms,firstEventNumber,'','',{})
    else:
      self._addGaudiStep('LHCb',appVersion,appType,numberOfEvents,optionsFile,optionsLine,eventType,extraPackages,outputSE,inputData,inputDataType,histograms,firstEventNumber,'','',{})

  #############################################################################
  def _addMergeMDFStep(self,appName,appVersion,appType,numberOfEvents,optionsFile,optionsLine,eventType,extraPackages,outputSE,inputData='previousStep',inputDataType='None',histograms=False,firstEventNumber=0,spillover='',pileup='',extraOutput={}):
    """Helper function.
    """
    if not type(appName) == type(' ') or not type(appVersion) == type(' '):
      raise TypeError,'Expected strings for application name and version'

    self.gaudiStepCount +=1
    MergeMDFModule = ModuleDefinition('MergeMDF')
    MergeMDFModule.setDescription('Merge MDF Files Module')
    MergeMDFModule.setBody('from WorkflowLib.Module.MergeMDF import MergeMDF\n')

    MergeMDFStep = StepDefinition('Merge_MDF_Step')
    MergeMDFStep.addModule(MergeMDFModule)
    MergeMDFStep.createModuleInstance('MergeMDF', 'MergeMDFModule')
    MergeMDFStep.addParameterLinked(MergeMDFModule.parameters)
    self._addParameter(MergeMDFStep,'applicationLog','string','','ApplicationLogFile')
    self._addParameter(MergeMDFStep,'listoutput','list',[],'StepOutputList')

    self.workflow.addStep(MergeMDFStep)

    stepInstance = self.workflow.createStepInstance('Merge_MDF_Step', 'MergeMDF_%s' %eventType)
    outputList =[{"outputDataName":"@{STEP_ID}.mdf","outputDataType":"MDF","outputDataSE":outputSE}]
    stepInstance.setValue('listoutput',(outputList))
    stepInstance.setValue('applicationLog', 'Merge_@{STEP_ID}.log')
    self.setInputData(inputData)
    return stepInstance

  #############################################################################
  def _addGaudiStep(self,appName,appVersion,appType,numberOfEvents,optionsFile,optionsLine,eventType,extraPackages,outputSE,inputData='previousStep',inputDataType='None',histograms=False,firstEventNumber=0,spillover='',pileup='',extraOutput={}):
    """Helper function.
    """
    if not type(appName) == type(' ') or not type(appVersion) == type(' '):
      raise TypeError,'Expected strings for application name and version'

    self.gaudiStepCount +=1
    gaudiStep =  self.__getGaudiApplicationStep('%s_%s' %(appName,self.gaudiStepCount))

    #lower the appType
    if appType:
      appType = string.lower(appType)

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
    if type(optionsFile)==type([]):
      optionsFile = string.join(optionsFile,';')

    gaudiStep.setValue('optionsFile',optionsFile)
    gaudiStep.setValue('optionsLine',optionsLine)
    gaudiStep.setValue('optionsLinePrev','None')
    gaudiStep.setValue('numberOfEvents', numberOfEvents)
    gaudiStep.setValue('eventType',eventType)
    if extraPackages:
      if type(extraPackages)==type([]):
        extraPackages = string.join(extraPackages,';')
      gaudiStep.setValue('extraPackages',extraPackages)
      self.__addSoftwarePackages(extraPackages)

    if firstEventNumber:
      gaudiStep.setValue('firstEventNumber',firstEventNumber)

    if not inputData:
      self.log.verbose('Assume %s step has no input data requirement or is linked to the overall input data' %appName)
      gaudiStep.setLink('inputData','self','InputData')
    elif inputData=='previousStep':
      self.log.verbose('Taking input data as output from previous Gaudi step')
      if not self.ioDict.has_key(self.gaudiStepCount-1):
        raise TypeError,'Expected previously defined Gaudi step for input data'
      gaudiStep.setLink('inputData',self.ioDict[self.gaudiStepCount-1],'outputData')
    elif inputData=='firstStep':
      self.log.verbose('Taking input data as output from first Gaudi step')
      stepKeys = self.ioDict.keys()
      stepKeys.sort()
      gaudiStep.setLink('inputData',self.ioDict[stepKeys[0]],'outputData')
    else:
      self.log.verbose('Assume input data requirement should be added to job')
      self.setInputData(inputData)
      gaudiStep.setLink('inputData','self','InputData')
      #such that it can be overwritten during submission
      #but also the template value can be used for testing

    if inputDataType != 'None':
      gaudiStep.setValue('inputDataType',string.upper(inputDataType))

    gaudiStep.setValue('applicationLog', '@{applicationName}_@{STEP_ID}.log')
    gaudiStep.setValue('outputData','@{STEP_ID}.@{applicationType}')
    outputList=[]
    outputList.append({"outputDataName":"@{STEP_ID}.@{applicationType}","outputDataType":"@{applicationType}","outputDataSE":outputSE})
    if histograms:
      outputList.append({"outputDataName":self.histogramName,"outputDataType":"HIST","outputDataSE":self.histogramSE})
    if extraOutput:
      outputList.append(extraOutput)

    gaudiStep.setValue('listoutput',(outputList))

    #Ensure the global input data type is null
    description = 'Default input data type field'
    self._addParameter(self.workflow,'InputDataType','JDL','',description)

    # now we have to tell DIRAC to install the necessary software
    self.__addSoftwarePackages('%s.%s' %(appName,appVersion))
    dddbOpt = "@{DDDBTag}"
    conddbOpt = "@{CondDBTag}"
    #to construct the BK processing pass structure, starts from step '0'
    stepID = 'Step%s' %(self.gaudiStepCount-1)
    bkOptionsFile = optionsFile
    if re.search('@{eventType}',optionsFile):
      bkOptionsFile = string.replace(optionsFile,'@{eventType}',str(eventType))
    self.bkSteps[stepID]={'ApplicationName':appName,'ApplicationVersion':appVersion,'OptionFiles':bkOptionsFile,'DDDb':dddbOpt,'CondDb':conddbOpt,'ExtraPackages':extraPackages}
    self.__addBKPassStep()
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
  def __addBKPassStep(self):
    """ Internal method to accumulate software packages.
    """
    bkPass = 'BKProcessingPass'
    description='BKProcessingPassInfo'
    self._addParameter(self.workflow,bkPass,'dict',self.bkSteps,description)

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
    self._addParameter(gaudiAppDefn,'outputData','string','','OutputData')
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
  def addFinalizationStep(self,sendBookkeeping=True,uploadData=True,uploadLogs=True,sendFailover=True,removeInputData=False):
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

    if removeInputData:
      removeInputs = ModuleDefinition('RemoveInputData')
      removeInputs.setDescription('Removes input data after merged output data uploaded to an SE')
      self._addParameter(removeInputs,'Enable','bool','True','EnableFlag')
      removeInputs.setBody('from WorkflowLib.Module.RemoveInputData import RemoveInputData \n')

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
    self._addParameter(finalization,'BKEnable','bool',str(sendBookkeeping),'EnableFlag')

    dataUpload.setLink('Enable','self','UploadEnable')
    finalization.addModule(dataUpload)
    finalization.createModuleInstance('UploadOutputData','dataUpload')
    self._addParameter(finalization,'UploadEnable','bool',str(uploadData),'EnableFlag')

    logUpload.setLink('Enable','self','LogEnable')
    finalization.addModule(logUpload)
    finalization.createModuleInstance('UploadLogFile','logUpload')
    self._addParameter(finalization,'LogEnable','bool',str(uploadLogs),'EnableFlag')

    if removeInputData:
      removeInputs.setLink('Enable','self','DataRemovalEnable')
      finalization.addModule(removeInputs)
      finalization.createModuleInstance('RemoveInputData','removeInputs')
      self._addParameter(finalization,'DataRemovalEnable','bool',str(removeInputData),'EnableFlag') #always true in this case

    failoverRequest.setLink('Enable','self','FailoverEnable')
    finalization.addModule(failoverRequest)
    finalization.createModuleInstance('FailoverRequest','failoverRequest')
    self._addParameter(finalization,'FailoverEnable','bool',str(sendFailover),'EnableFlag')

    self.workflow.addStep(finalization)
    finalizeStep = self.workflow.createStepInstance('Job_Finalization', 'finalization')
    finalizeStep.setValue('BKEnable',sendBookkeeping)
    finalizeStep.setValue('UploadEnable',uploadData)
    finalizeStep.setValue('LogEnable',uploadLogs)
    finalizeStep.setValue('FailoverEnable',sendFailover)

  #############################################################################
  def createWorkflow(self):
    """ Create XML for local testing.
    """
    name = '%s.xml' %self.name
    if os.path.exists(name):
      shutil.move(name,'%s.backup' %name)
    self.workflow.toXMLFile(name)

  #############################################################################
  def getDetailedInfo(self,productionID):
    """ Return detailed information for a given production.
    """
    return self.getParameter(int(productionID),'DetailedInfo')

  #############################################################################
  def _setProductionParameters(self,prodID,groupDescription='',bkPassInfo={},bkInputQuery={},derivedProduction='',prodXMLFile='',printOutput=False):
    """ Under development.
        Return detailed information for a given production.
    """
    if not prodXMLFile: #e.g. setting parameters for old productions
      prodXMLFile = 'Production%s.xml' %prodID
      if os.path.exists(prodXMLFile):
        self.log.info('Using %s for production body' %prodXMLFile)
      else:
        prodClient = RPCClient('ProductionManagement/ProductionManager',timeout=120)
        result = prodClient.getProductionBody(int(prodID))
        if not result['OK']:
          return S_ERROR("Error during command execution: %s" % result['Message'])
        if not result['Value']:
          return S_ERROR("No body of production %s was found" % prodID)

        body = result['Value']
        fd = open( prodXMLFile, 'wb' )
        fd.write(body)
        fd.close()

    if not bkPassInfo:
      bkPassInfo = self.workflow.findParameter('BKProcessingPass').getValue()
    if not groupDescription:
      groupDescription = self.workflow.findParameter('groupDescription').getValue()

    prodWorkflow = Workflow(prodXMLFile)
    parameters = {}
    parameters['CondDBTag']=self.workflow.findParameter('CondDBTag').getValue()
    parameters['DDDBTag']=self.workflow.findParameter('DDDBTag').getValue()
    parameters['ConfigName']=self.workflow.findParameter('configName').getValue()
    parameters['ConfigVersion']=self.workflow.findParameter('configVersion').getValue()
    #now have to go through the BK steps to construct info string

    info = 'Production %s is created with the following parameters:\n' %prodID
#(,appConfigVersion,gaussVersion,gaussOpts,eventTypeID,gaussGen,numberOfEvents,booleVersion,booleOpts,brunelVersion,brunelOpts,mcTruth)

    prodWorkflow

    if printOutput:
      print '='*20+'>BKPassInfo'
      print bkPassInfo
      print '='*20+'>BKInputQuery'
      print bkPassInfo
      print '='*20+'>BKGroupDescription'
      print bkPassInfo
      print '='*20+'>ProductionInfo'
      print bkPassInfo
      return S_OK()

    result = self.setParameter(int(prodID),'BKProcessingPass',bkPassInfo)
    if not result['OK']:
      self.log.error(result['Message'])
    result = self.setParameter(int(prodID),'BKInputQuery',bkInputQuery)
    if not result['OK']:
      self.log.error(result['Message'])
    result = self.setParameter(int(prodID),'BKGroupDescription',groupDescription)
    if not result['OK']:
      self.log.error(result['Message'])
    result = self.setParameter(int(prodID),'DetailedInfo',info)
    if not result['OK']:
      self.log.error(result['Message'])

    return S_OK()

  #############################################################################
  def create(self,publish=True,fileMask='',bkQuery={},groupSize=1,derivedProduction=0,bkScript=True,wfString=''):
    """ Will create the production and subsequently publish to the BK, this
        currently relies on the conditions information being present in the
        worklow.  Production parameters are also added at this point.

        publish = True - will add production to the production management system
                  False - does not publish the production, allows to check the BK script

        bkScript = True - will write a script that can be checked first before
                          adding to BK
                   False - will print BK parameters but publish the production

        The workflow XML is created regardless of the flags.
    """
    prodID = self.defaultProdID

    if wfString:
      from DIRAC.Core.Workflow.Workflow import fromXMLString
      self.workflow = fromXMLString(wfString)
    try:
      self.createWorkflow()
    except Exception,x:
      self.log.error(x)
      return S_ERROR('Could not create workflow')

    workflowName = self.workflow.getName()
    fileName = '%s.xml' %workflowName
    self.log.info('Workflow XML file name is: %s' %fileName)

    bkConditions = self.workflow.findParameter('conditions').getValue()

    bkDict = {}
    bkDict['Steps']=self.workflow.findParameter('BKProcessingPass').getValue()
    bkDict['GroupDescription']=self.workflow.findParameter('groupDescription').getValue()

    bkClient = BookkeepingClient()
    #Add the BK conditions metadata / name
    simConds = bkClient.getSimConditions()
    if not simConds['OK']:
      self.log.error('Could not retrieve conditions data from BK:\n%s' %simConds)
      return simConds
    simulationDescriptions = []
    for record in simConds['Value']:
      simulationDescriptions.append(str(record[1]))

    if not bkConditions in simulationDescriptions:
      self.log.info('Assuming BK conditions %s are DataTakingConditions' %bkConditions)
      bkDict['DataTakingConditions']=bkConditions
    else:
      self.log.info('Found simulation conditions for %s' %bkConditions)
      sim = {}
      for record in simConds['Value']:
        if bkConditions==str(record[1]):
          simDesc = bkConditions
          beamcond = str(record[2])
          energy = str(record[3])
          gen = str(record[4])
          mag = str(record[5])
          detcond = str(record[6])
          lumi = str(record[7])
          sim = {'BeamEnergy':energy,'Generator': gen,'Luminosity': lumi,'MagneticField':mag,'BeamCond': beamcond,'DetectorCond':detcond,'SimDescription':simDesc}

      if not sim:
        raise TypeError,'Could not determine simulation conditions from BK'

      bkDict['SimulationConditions']=sim

    #Now update the DB tags to be correct for the bookkeeping
    stepKeys = bkDict['Steps'].keys()
    for step in stepKeys:
      bkDict['Steps'][step]['DDDb']=self.workflow.findParameter('DDDBTag').getValue()
      bkDict['Steps'][step]['CondDb']=self.workflow.findParameter('CondDBTag').getValue()

    if publish:
      dirac = DiracProduction()
      if self.inputFileMask:
        fileMask = self.inputFileMask
      if self.jobFileGroupSize:
        groupSize = self.jobFileGroupSize
      if self.inputBKSelection:
        bkQuery=self.inputBKSelection
      if self.ancestorProduction:
        derivedProduction = self.ancestorProduction
      result = dirac.createProduction(fileName,fileMask,groupSize,bkQuery,self.plugin,self.prodGroup,self.type,derivedProduction)
      if not result['OK']:
        self.log.error('Problem creating production:\n%s' %result)
        return result
      prodID = result['Value']
      self.log.info('Production %s successfully created' %prodID)
    else:
      self.log.info('Publish flag is disabled, using default production ID')

    bkDict['Production']=int(prodID)

    if bkScript:
      self.log.info('Writing BK publish script...')
      self._writeBKScript(bkDict,prodID)
    else:
      self.log.info('Production BK parameters are:')
      for n,v in bkDict.items():
        print n,v

    if publish and not bkScript:
      self.log.info('Attempting to publish production to the BK')
      result = bkClient.addProduction(bkDict)
      if not result['OK']:
        self.log.error(result)
      self.log.info('BK publishing result: %s' %result)

    #self._setProductionParameters(prodID,fileName,bkDict['GroupDescription'],bkDict,bkQuery,derivedProduction)
    return S_OK(prodID)

  #############################################################################
  def _writeBKScript(self,bkDict,prodID):
    """Writes the production publishing script for the BK.
    """
    bkName = 'insertBKPass%s.py' %(prodID)
    if os.path.exists(bkName):
      shutil.move(bkName,'%s.backup' %bkName)
    fopen = open(bkName,'w')
    bkLines = ['# Bookkeeping publishing script created on %s by' %(time.asctime())]
    bkLines.append('# by %s' %self.prodVersion)
    bkLines.append('from DIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient')
    bkLines.append('bkClient = BookkeepingClient()')
    bkLines.append('bkDict = %s' %bkDict)
    bkLines.append('print bkClient.addProduction(bkDict)')
    fopen.write(string.join(bkLines,'\n')+'\n')
    fopen.close()

  #############################################################################
  def getOutputLFNs(self,prodID=None,prodJobID=None,inputDataLFN=None):
    """ Will construct the output LFNs for the production for visual inspection.
    """
    self.workflow.toXMLFile('%s.xml' %self.name)
    dirac = DiracProduction()
    if not prodID:
      prodID = self.defaultProdID
    if not prodJobID:
      prodJobID = self.defaultProdJobID
    result = dirac._getOutputLFNs('%s.xml' %self.name,productionID=prodID,jobID=prodJobID,inputData=inputDataLFN)
    if not result['OK']:
      return result
    lfns = result['Value']
    self.log.info(lfns)
    return result

  #############################################################################
  def _setInputProductionBKStepInfo(self,prodID):
    """ This private method will attempt to retrieve the input production XML file
        in order to construct the BK XML
    """
    prodClient = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    result = prodClient.getProductionBody(int(prodID))
    if not result['OK']:
      return S_ERROR("Error during command execution: %s" % result['Message'])
    if not result['Value']:
      return S_ERROR("No body of production %s was found" % prodID)

    prodXMLFile = 'InputProduction%s.xml' %prodID
    body = result['Value']
    fd = open( prodXMLFile, 'wb' )
    fd.write(body)
    fd.close()

    prodWorkflow = Workflow(prodXMLFile)
    passDict = prodWorkflow.findParameter('BKProcessingPass').getValue()
    if not passDict:
      return S_ERROR('Could not find BKProcessingPass for production %s' %prodID)

    #for e.g. MDF case must remove some info
#    if stepBKCutOff:
#      tmpPassDict = passDict
#      stepKeys = passDict.keys()
#      stepKeys.sort()
#      for step in stepKeys:
#        if int(step.replace('Step',''))>stepBKCutOff:
#          self.log.info('Removing %s from BK processing pass' %step)
#          del tmpPassDict[step]
#      passDict=tmpPassDict

    #add merge step...
    for stepID in passDict.keys():
      self.log.info('Adding BK processing step %s from production %s:\n%s' %(stepID,prodID,passDict[stepID]))
      self.bkSteps[stepID]=passDict[stepID]
    self.__addBKPassStep()
    self.gaudiStepCount+=len(passDict.keys())
    #note that the merging production step number will follow on from the previous production
    return S_OK()

  #############################################################################
  def setParameter(self,prodID,pname,pvalue):
    """Set a production parameter.
    """
    prodClient = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    result = prodClient.addTransformationParameter(int(prodID),pname,pvalue)
    return result

  #############################################################################
  def getParameter(self,prodID,pname=''):
    """Get a production parameter.
    """
    prodClient = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    result = prodClient.getTransformation(int(prodID))
    if pname:
      if result['Value'].has_key(pname):
        return S_OK(result['Value'][pname])
      else:
        return S_ERROR('Production %s does not have parameter %s' %(prodID,pname))

    return result

  #############################################################################
  def setAlignmentDBLFN(self,lfn):
    """ Set the input LFN to be used for the alignment conditions database'
    """
    if not re.search("LFN:",lfn):
      lfn = "LFN:%s" % lfn
    self.log.info('Setting alignment DB LFN to %s' %lfn)
    self._setParameter('InputSandbox','JDL',lfn,'AlignmentDB')

  #############################################################################
  def setWorkflowLib(self,tag):
    """Set workflow lib version for the production.
    """
    if not tag:
      tag = gConfig.getValue('%s/WorkflowLibVersion' %(self.csSection),'v9r9')
      self.log.verbose('Setting workflow library tag to %s' %tag)

    lfn = 'LFN:/lhcb/applications/WorkflowLib-wkf-TAG.tar.gz'.replace('TAG',tag)
    self.log.info('Setting workflow library LFN to %s' %lfn)
    self._setParameter('InputSandbox','JDL',lfn,'WorkflowLibVersion')

  #############################################################################
  def setFileMask(self,fileMask):
    """Output data related parameters.
    """
    if type(fileMask)==type([]):
      fileMask = string.join(fileMask,';')
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

  #############################################################################
  def setProdGroup(self,group):
    """ Sets a user defined tag for the production as appears on the monitoring page
    """
    self.prodGroup = group

  #############################################################################
  def setProdPlugin(self,plugin):
    """ Sets the plugin to be used to creating the production jobs
    """
    available_plugins = ['CCRC_RAW']
    if plugin in available_plugins:
      self.plugin = plugin
    else:
      self.plugin = 'Standard'

  #############################################################################
  def setInputFileMask(self,fileMask):
    """ Sets the input data selection when using file mask.
    """
    self.inputFileMask = fileMask

  #############################################################################
  def setInputBKSelection(self,bkQuery):
    """ Sets the input data selection when using the bookkeeping.
    """
    self.inputBKSelection = bkQuery

  #############################################################################
  def setJobFileGroupSize(self,files):
    """ Sets the number of files to be input to each job created.
    """
    self.jobFileGroupSize = files

  #############################################################################
  def setAncestorProduction(self,prod):
    """ Sets the original production from which this is to be derived
    """
    self.ancestorProduction = prod

