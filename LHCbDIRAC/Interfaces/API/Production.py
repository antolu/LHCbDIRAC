########################################################################
# $HeadURL$
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
    - Uses getOutputLFNs() function to add production output directory parameter
"""

__RCSID__ = "$Id$"

import string, re, os, time, shutil, types, copy

from DIRAC.Core.Workflow.Workflow                     import *
from DIRAC.Core.DISET.RPCClient                       import RPCClient
from DIRAC.Core.Utilities.List                        import removeEmptyElements
from DIRAC.Interfaces.API.Dirac                       import Dirac

from LHCbDIRAC.ProductionManagementSystem.Client.Transformation import Transformation
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient       import BookkeepingClient
from LHCbDIRAC.Interfaces.API.DiracProduction                   import DiracProduction
from LHCbDIRAC.Interfaces.API.LHCbJob                           import *
from LHCbDIRAC.Core.Utilities.ProductionOptions                 import getOptions

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
    self.tier1s=gConfig.getValue('%s/Tier1s' %(self.csSection),['LCG.CERN.ch','LCG.CNAF.it','LCG.NIKHEF.nl','LCG.PIC.es','LCG.RAL.uk','LCG.GRIDKA.de','LCG.IN2P3.fr','LCG.SARA.nl'])
    self.histogramName =gConfig.getValue('%s/HistogramName' %(self.csSection),'@{applicationName}_@{STEP_ID}_Hist.root')
    self.histogramSE =gConfig.getValue('%s/HistogramSE' %(self.csSection),'CERN-HIST')
    self.systemConfig = gConfig.getValue('%s/SystemConfig' %(self.csSection),'ANY')
    #self.systemConfig = gConfig.getValue('%s/SystemConfig' %(self.csSection),'x86_64-slc5-gcc43-opt')
    #self.systemConfig = gConfig.getValue('%s/SystemConfig' %(self.csSection),'slc4_ia32_gcc34')
    self.inputDataDefault = gConfig.getValue('%s/InputDataDefault' %(self.csSection),'/lhcb/data/2009/RAW/EXPRESS/FEST/FEST/44878/044878_0000000002.raw')
    self.defaultProdID = '12345'
    self.defaultProdJobID = '12345'
    self.ioDict = {}
    self.gaussList = []
    self.prodTypes = ['DataReconstruction','DataStripping','MCSimulation','MCStripping','Merge','Test']
    self.pluginsTriggeringStreamTypes = ['ByFileTypeSize','ByRunFileTypeSize','ByRun','AtomicRun']
    self.name='unspecifiedWorkflow'
    self.firstEventType = ''
    self.bkSteps = {}
    self.prodGroup = ''
    self.plugin = ''
    self.inputFileMask = ''
    self.inputBKSelection = {}
    self.jobFileGroupSize = 0
    self.ancestorProduction = ''
    self.importLine = """
from LHCbDIRAC.Workflow.Modules.<MODULE> import <MODULE>
"""
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

    #Options related parameters
    self._setParameter('CondDBTag','string','sim-20090112','CondDBTag')
    self._setParameter('DDDBTag','string','head-20090112','DetDescTag')
    self._setParameter('EventMaxDefault','string','-1','DefaultNumberOfEvents')
    #BK related parameters
    self._setParameter('configName','string','MC','ConfigName')
    self._setParameter('configVersion','string','2009','ConfigVersion')
    self._setParameter('conditions','string','','SimOrDataTakingCondsString')

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
  def __checkArguments(self,extraPackages,optionsFile):
    """ Checks for typos in the structure of standard arguments to workflows.
        In case of any non-standard settings will raise an exception preventing
        creation of the production. Must be called after setting the first event type
        of the production.
    """
    separator = ';'
    if not extraPackages:
      extraPackages=[]

    if not optionsFile:
      optionsFile=[]

    if extraPackages:
      if not re.search(';',extraPackages):
        extraPackages=[extraPackages]
      else:
        extraPackages=string.split(extraPackages,';')
    if optionsFile:
      if not re.search(';',optionsFile):
        optionsFile = [optionsFile]

    for p in extraPackages:
      self.log.verbose('Checking extra package: %s' %(p))
      if not re.search('.',p):
        raise TypeError,'Must have extra packages in the following format "Name.Version" not %s' %(p)

    for o in optionsFile:
      if re.search('DECFILESROOT',o):
        self.log.verbose('Production has event type %s specified, checking event type options: %s' %(self.firstEventType,o))
        if re.search('@',o) or re.search('%s' %self.firstEventType,o):
          self.log.verbose('Options: %s specify event type correctly' %(o))
        else:
          raise TypeError,'Event type options must be referred to as the event type number or workflow parameter'

    self.log.verbose('Extra packages and event type options are correctly specified')
    return S_OK()

  #############################################################################
  def addGaussStep(self,appVersion,generatorName,numberOfEvents,optionsFile,eventType='firstStep',extraPackages='',outputSE=None,histograms=False,overrideOpts='',extraOpts='',condDBTag='global',ddDBTag='global'):
    """ Wraps around addGaudiStep and getOptions.
    """
    eventType = self.__getEventType(eventType)
    self.__checkArguments(extraPackages, optionsFile)
    firstEventNumber=1
    if not overrideOpts:
      optionsLine = getOptions('Gauss','sim',extraOpts=None,histogram=self.histogramName,condDB=condDBTag,ddDB=ddDBTag)
      self.log.verbose('Default options for Gauss are:\n%s' %(string.join(optionsLine,'\n')))
      optionsLine = string.join(optionsLine,';')
    else:
      optionsLine = overrideOpts

    if extraOpts:
      extraOpts = removeEmptyElements(string.split(extraOpts,'\n'))
      extraOpts = string.join(extraOpts,';')
      optionsLine = "%s\n%s" % (optionsLine,extraOpts)

    if not outputSE:
      outputSE='Tier1-RDST'
      self.log.verbose('Setting default outputSE to %s' %(outputSE))

    self._setParameter('dataType','string','MC','DataType') #MC or DATA to be reviewed
    gaussStep = self._addGaudiStep('Gauss',appVersion,'sim',numberOfEvents,optionsFile,optionsLine,eventType,extraPackages,outputSE,'','None',histograms,firstEventNumber,{},condDBTag,ddDBTag,'')
    self.gaussList.append(gaussStep.getName())
    gaussStep.setValue('numberOfEventsInput', 0)
    gaussStep.setValue('numberOfEventsOutput', 0)
    gaussStep.setValue('generatorName',generatorName)

  #############################################################################
  def addBooleStep(self,appVersion,appType,optionsFile,eventType='firstStep',extraPackages='',outputSE=None,histograms=False,inputData='previousStep',overrideOpts='',extraOpts='',extraOutputFile={},condDBTag='global',ddDBTag='global'):
    """ Wraps around addGaudiStep and getOptions.
        appType is mdf / digi / xdigi
        currently assumes input data type is sim
    """
    eventType = self.__getEventType(eventType)
    self.__checkArguments(extraPackages, optionsFile)
    firstEventNumber=0
    numberOfEvents='-1'
    inputDataType='sim'
    inputData='previousStep'

    if not overrideOpts:
      optionsLine = getOptions('Boole',appType,extraOpts=None,histogram=self.histogramName,condDB=condDBTag,ddDB=ddDBTag)
      self.log.verbose('Default options for Boole are:\n%s' %(string.join(optionsLine,'\n')))
      optionsLine = string.join(optionsLine,';')
    else:
      optionsLine = overrideOpts

    if extraOpts:
      extraOpts = removeEmptyElements(string.split(extraOpts,'\n'))
      extraOpts = string.join(extraOpts,';')
      optionsLine = "%s\n%s" % (optionsLine,extraOpts)

    if not outputSE:
      outputSE='Tier1-RDST'
      self.log.verbose('Setting default outputSE to %s' %(outputSE))

    if extraOutputFile:
      self.log.verbose('Adding output file to Boole step %s' %extraOutputFile)

    self._setParameter('dataType','string','MC','DataType') #MC or DATA to be reviewed
    self._addGaudiStep('Boole',appVersion,appType,numberOfEvents,optionsFile,optionsLine,eventType,extraPackages,outputSE,inputData,inputDataType,histograms,firstEventNumber,extraOutputFile,condDBTag,ddDBTag,'')

  #############################################################################
  def addBrunelStep(self,appVersion,appType,optionsFile,eventType='firstStep',extraPackages='',inputData='previousStep',inputDataType='mdf',outputSE=None,histograms=False,overrideOpts='',extraOpts='',numberOfEvents='-1',dataType='DATA',condDBTag='global',ddDBTag='global'):
    """ Wraps around addGaudiStep and getOptions.
        appType is rdst / dst / xdst / sdst
        inputDataType is mdf / digi
        enough to set one of the above
    """
    eventType = self.__getEventType(eventType)
    self.__checkArguments(extraPackages, optionsFile)

    if appType.lower() in ['rdst','sdst']:
      dataType='DATA'
      if not outputSE:
        outputSE='Tier1-RDST'
        #outputSE='Tier1_M-DST'
        self.log.verbose('Setting default outputSE to %s' %(outputSE))
    else:
      if not appType.lower() in ['dst','xdst']:
        raise TypeError,'Application type not recognised'
      if inputDataType.lower()=='digi':
        dataType='MC'
        if not outputSE:
          outputSE='Tier1_MC_M-DST'
          self.log.verbose('Setting default outputSE to %s' %(outputSE))
      elif inputDataType.lower()=='fetc':
        #Must rely on data type for fetc only
        if not outputSE:
          if dataType.lower()=='mc':
            outputSE='Tier1_MC_M-DST'
            self.log.verbose('Setting default outputSE to %s' %(outputSE))
          else:
            outputSE='Tier1_M-DST' #for real data master dst
            self.log.verbose('Setting default outputSE to %s' %(outputSE))

    if not overrideOpts:
      optionsLine = getOptions('Brunel',appType,extraOpts=None,histogram=self.histogramName,condDB=condDBTag,ddDB=ddDBTag)
      self.log.verbose('Default options for Brunel are:\n%s' %(string.join(optionsLine,'\n')))
      optionsLine = string.join(optionsLine,';')
    else:
      optionsLine = overrideOpts

    if extraOpts:
      extraOpts = removeEmptyElements(string.split(extraOpts,'\n'))
      extraOpts = string.join(extraOpts,';')
      optionsLine = "%s\n%s" % (optionsLine,extraOpts)

    firstEventNumber=0
    self._setParameter('dataType','string',dataType,'DataType') #MC or DATA to be reviewed
    self._addGaudiStep('Brunel',appVersion,appType,numberOfEvents,optionsFile,optionsLine,eventType,extraPackages,outputSE,inputData,inputDataType,histograms,firstEventNumber,{},condDBTag,ddDBTag,'')

  #############################################################################
  def addDaVinciStep(self,appVersion,appType,optionsFile,eventType='firstStep',extraPackages='',inputData='previousStep',inputDataType='rdst',outputSE=None,histograms=False,overrideOpts='',extraOpts='',numberOfEvents='-1',dataType='DATA',condDBTag='global',ddDBTag='global',inputProduction=''):
    """ Wraps around addGaudiStep and getOptions.
        appType is  dst / dst / setc / fetc / merge / undefined at the moment ;)
        inputDataType is rdst / fetc / sdst
    """
    eventType = self.__getEventType(eventType)
    self.__checkArguments(extraPackages, optionsFile)
    firstEventNumber=0
    appTypes = ['dst','fetc','setc','rdst','davincihist','merge']
    inputDataTypes = ['rdst','dst','sdst']
    if not appType.lower() in appTypes:
      raise TypeError,'Application type not currently supported (%s)' % appTypes
    if not inputDataType.lower() in inputDataTypes and not appType.lower() == 'merge' and not appType.lower() == 'setc':
      raise TypeError,'Only %s input data types currently supported' %(string.join(inputDataTypes,', '))

    if inputDataType.lower() in ['rdst','sdst']:
      dataType='DATA'
      if not outputSE:
        outputSE='Tier1_M-DST'
        self.log.verbose('Setting default outputSE to %s' %(outputSE))
    elif inputDataType.lower()=='dst':
      if not dataType:
        raise TypeError,'Must clarify MC / DATA for DST->DST processing'
      if not outputSE:
        if dataType.upper()=='MC':
          outputSE='Tier1_MC_M-DST'
          self.log.verbose('Setting default outputSE to %s' %(outputSE))
        else:
          outputSE='Tier1_M-DST'
          self.log.verbose('Setting default outputSE to %s' %(outputSE))

    if appType.lower()=='merge':
      if not outputSE:
        outputSE='Tier1_M-DST'
        self.log.verbose('Setting default outputSE to %s' %(outputSE))
      if inputProduction:
        result = self._setInputProductionBKStepInfo(inputProduction,{})
        if not result['OK']:
          self.log.error(result)
          raise TypeError,'inputProduction must exist and have BK parameters'
      else:
        self.log.error('DaVinci merging application type selected, must have an input production ID')
        raise TypeError,'inputProduction must be specified'

    if appType.lower()=='setc':
      if not outputSE:
        outputSE='Tier1_M-DST'
        self.log.verbose('Setting default outputSE to %s' %(outputSE))

    if not overrideOpts:
      optionsLine = getOptions('DaVinci',appType,extraOpts=None,inputType=inputDataType,histogram=self.histogramName,condDB=condDBTag,ddDB=ddDBTag)
      self.log.verbose('Default options for DaVinci are:\n%s' %(string.join(optionsLine,'\n')))
      optionsLine = string.join(optionsLine,';')
    else:
      optionsLine = overrideOpts

    if extraOpts:
      extraOpts = removeEmptyElements(string.split(extraOpts,'\n'))
      extraOpts = string.join(extraOpts,';')
      optionsLine = "%s\n%s" % (optionsLine,extraOpts)
#      print optionsLine

    if appType.lower()=='davincihist':
      appType='dst'

    if  appType.lower()=='merge':
      appType=inputDataType.lower()

    self._setParameter('dataType','string',dataType,'DataType') #MC or DATA to be reviewed
    self._addGaudiStep('DaVinci',appVersion,appType,numberOfEvents,optionsFile,optionsLine,eventType,extraPackages,outputSE,inputData,inputDataType,histograms,firstEventNumber,{},condDBTag,ddDBTag,'')

  #############################################################################
  def addMooreStep(self,appVersion,appType,optionsFile,eventType='firstStep',extraPackages='',inputData='previousStep',inputDataType='raw',outputSE=None,histograms=False,overrideOpts='',extraOpts='',numberOfEvents='-1',dataType='MC',condDBTag='global',ddDBTag='global',outputAppendName=''):
    """ Wraps around addGaudiStep and getOptions.
        appType is digi (simulation) / dst and inputDataType is digi / raw only at the moment.
    """
    eventType = self.__getEventType(eventType)
    self.__checkArguments(extraPackages, optionsFile)

    firstEventNumber=0
    appTypes = ['dst','digi']
    inputDataTypes = ['raw','digi']
    if not appType in appTypes:
      raise TypeError,'Application type not currently supported (%s)' % appTypes
    if not inputDataType in inputDataTypes:
      raise TypeError,'Only RAW and DIGI input data type currently supported'

    if not dataType.lower()=='mc':
      raise TypeError,'Only MC data type is supported'

    if not outputSE:
      outputSE='Tier1_MC_M-DST'
      if inputDataType.lower()=='digi':
        outputSE='Tier1-RDST' #convention for intermediate outputs that are not saved by default

    self.log.verbose('Setting default outputSE to %s' %(outputSE))

    if not overrideOpts:
      optionsLine = getOptions('Moore',appType,extraOpts=None,inputType=inputDataType,histogram=self.histogramName,condDB=condDBTag,ddDB=ddDBTag)
      self.log.verbose('Default options for Moore are:\n%s' %(string.join(optionsLine,'\n')))
      optionsLine = string.join(optionsLine,';')
    else:
      optionsLine = overrideOpts

    if extraOpts:
      extraOpts = removeEmptyElements(string.split(extraOpts,'\n'))
      extraOpts = string.join(extraOpts,';')
      optionsLine = "%s\n%s" % (optionsLine,extraOpts)

    self._setParameter('dataType','string',dataType,'DataType') #MC or DATA to be reviewed
    self._addGaudiStep('Moore',appVersion,appType,numberOfEvents,optionsFile,optionsLine,eventType,extraPackages,outputSE,inputData,inputDataType,histograms,firstEventNumber,{},condDBTag,ddDBTag,outputAppendName)

  #############################################################################
  def addMergeStep(self,appVersion='v26r3',optionsFile='$STDOPTS/PoolCopy.opts',inputProduction='',eventType='firstStep',extraPackages='',inputData='previousStep',inputDataType='dst',outputSE=None,overrideOpts='',extraOpts='',numberOfEvents='-1',passDict={},condDBTag='global',ddDBTag='global',dataType='MC'):
    """Wraps around addGaudiStep.  The merging uses a standard Gaudi step with
       any available LHCb project as the application.
    """
    eventType = self.__getEventType(eventType)
    self.__checkArguments(extraPackages, optionsFile)
    if inputProduction:
      result = self._setInputProductionBKStepInfo(inputProduction,passDict)
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
      self.log.verbose('Setting default outputSE to %s' %(outputSE))

    if not overrideOpts:
      optionsLine = getOptions('Merge',appType,extraOpts=None,condDB=condDBTag,ddDB=ddDBTag)
      self.log.verbose('Default options for Merging are:\n%s' %(string.join(optionsLine,'\n')))
      optionsLine = string.join(optionsLine,';')
    else:
      optionsLine = overrideOpts

    if extraOpts:
      extraOpts = removeEmptyElements(string.split(extraOpts,'\n'))
      extraOpts = string.join(extraOpts,';')
      optionsLine = "%s\n%s" % (optionsLine,extraOpts)

    self._setParameter('dataType','string',dataType,'DataType')
    if inputDataType.lower()=='mdf':
      self._addMergeMDFStep('LHCb',appVersion,appType,numberOfEvents,optionsFile,optionsLine,eventType,extraPackages,outputSE,inputData,inputDataType,histograms,firstEventNumber,{})
    else:
      self._addGaudiStep('LHCb',appVersion,appType,numberOfEvents,optionsFile,optionsLine,eventType,extraPackages,outputSE,inputData,inputDataType,histograms,firstEventNumber,{},condDBTag,ddDBTag,'')

  #############################################################################
  def _addMergeMDFStep(self,appName,appVersion,appType,numberOfEvents,optionsFile,optionsLine,eventType,extraPackages,outputSE,inputData='previousStep',inputDataType='None',histograms=False,firstEventNumber=0,extraOutput={}):
    """Helper function.
    """
    if not type(appName) == type(' ') or not type(appVersion) == type(' '):
      raise TypeError,'Expected strings for application name and version'

    self.gaudiStepCount +=1
    MergeMDFModule = ModuleDefinition('MergeMDF')
    MergeMDFModule.setDescription('Merge MDF Files Module')

    body = string.replace(self.importLine,'<MODULE>','MergeMDF')
    MergeMDFModule.setBody(body)

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
  def _addGaudiStep(self,appName,appVersion,appType,numberOfEvents,optionsFile,optionsLine,eventType,extraPackages,outputSE,inputData='previousStep',inputDataType='None',histograms=False,firstEventNumber=0,extraOutput={},condDBTag='global',ddDBTag='global',outputAppendName=''):
    """Helper function.
    """
    if not type(appName) == type(' ') or not type(appVersion) == type(' '):
      raise TypeError,'Expected strings for application name and version'

    self.gaudiStepCount +=1
    gaudiStep =  self.__getGaudiApplicationStep('%s_%s' %(appName,self.gaudiStepCount))

    #lower the appType if not creating a template
    if appType and not re.search('{{',appType):
      appType = string.lower(appType)

    gaudiStep.setValue('applicationName',appName)
    gaudiStep.setValue('applicationVersion',appVersion)
    if outputAppendName:
      appType = string.lower('%s.%s' %(outputAppendName,appType))

    gaudiStep.setValue('applicationType',appType)
    if type(optionsFile)==type([]):
      optionsFile = string.join(optionsFile,';')
    optionsFile = optionsFile.replace(' ','')

    gaudiStep.setValue('optionsFile',optionsFile)
    gaudiStep.setValue('optionsLine',optionsLine)
    gaudiStep.setValue('optionsLinePrev','None')
    gaudiStep.setValue('numberOfEvents', numberOfEvents)
    gaudiStep.setValue('eventType',eventType)
    if extraPackages:
      if type(extraPackages)==type([]):
        extraPackages = string.join(extraPackages,';')

      extraPackages = extraPackages.replace(' ','')
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
      if re.search('{{',inputDataType):
        gaudiStep.setValue('inputDataType',inputDataType)
      else:
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
    if not condDBTag.lower() == 'global':
      self.log.verbose('Specific CondDBTag setting found for %s step, setting to: %s' %(appName,condDBTag))
      conddbOpt = condDBTag.replace(' ','')
    if not ddDBTag.lower() == 'global':
      self.log.verbose('Specific DDDBTag setting found for %s step, setting to: %s' %(appName,ddDBTag))
      dddbOpt = ddDBTag.replace(' ','')

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
      apps = removeEmptyElements(apps)
      apps = string.join(apps,';')
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
    body = string.replace(self.importLine,'<MODULE>','GaudiApplication')
    gaudiApp.setBody(body)

    analyseLog = ModuleDefinition('AnalyseLogFile')
    analyseLog.setDescription('Check LogFile module')
    body = string.replace(self.importLine,'<MODULE>','AnalyseLogFile')
    analyseLog.setBody(body)

    errorLogging = ModuleDefinition('ErrorLogging')
    errorLogging.setDescription('Error loggging module')
    body = string.replace(self.importLine,'<MODULE>','ErrorLogging')
    errorLogging.setBody(body)

    genBKReport = ModuleDefinition('BookkeepingReport')
    genBKReport.setDescription('Bookkeeping Report module')
    body = string.replace(self.importLine,'<MODULE>','BookkeepingReport')
    genBKReport.setBody(body)
    genBKReport.addParameter(Parameter("STEP_ID","","string","self","STEP_ID",True,False,"StepID"))

    gaudiAppDefn = StepDefinition('Gaudi_App_Step')
    gaudiAppDefn.addModule(gaudiApp)
    gaudiAppDefn.createModuleInstance('GaudiApplication', 'gaudiApp')
    gaudiAppDefn.addModule(analyseLog)
    gaudiAppDefn.createModuleInstance('AnalyseLogFile', 'analyseLog')
    gaudiAppDefn.addModule(errorLogging)
    gaudiAppDefn.createModuleInstance('ErrorLogging','errorLog')
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
    body = string.replace(self.importLine,'<MODULE>','SendBookkeeping')
    sendBK.setBody(body)

    dataUpload = ModuleDefinition('UploadOutputData')
    dataUpload.setDescription('Uploads the output data')
    self._addParameter(dataUpload,'Enable','bool','True','EnableFlag')
    body = string.replace(self.importLine,'<MODULE>','UploadOutputData')
    dataUpload.setBody(body)

    if removeInputData:
      removeInputs = ModuleDefinition('RemoveInputData')
      removeInputs.setDescription('Removes input data after merged output data uploaded to an SE')
      self._addParameter(removeInputs,'Enable','bool','True','EnableFlag')
      body = string.replace(self.importLine,'<MODULE>','RemoveInputData')
      removeInputs.setBody(body)

    logUpload = ModuleDefinition('UploadLogFile')
    logUpload.setDescription('Uploads the log files')
    self._addParameter(logUpload,'Enable','bool','True','EnableFlag')
    body = string.replace(self.importLine,'<MODULE>','UploadLogFile')
    logUpload.setBody(body)

    failoverRequest = ModuleDefinition('FailoverRequest')
    failoverRequest.setDescription('Sends any failover requests')
    self._addParameter(failoverRequest,'Enable','bool','True','EnableFlag')
    body = string.replace(self.importLine,'<MODULE>','FailoverRequest')
    failoverRequest.setBody(body)

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
    name = '%s.xml' % self.name
    if os.path.exists(name):
      shutil.move(name,'%s.backup' %name)
    self.workflow.toXMLFile(name)

  #############################################################################
  def runLocal(self):
    """ Create XML workflow for local testing then reformulate as a job and run locally.
    """
    name = '%s.xml' % self.name
    if os.path.exists(name):
      shutil.move(name,'%s.backup' %name)
    self.workflow.toXMLFile(name)
    j = LHCbJob(name)
    d = Dirac()
    return d.submit(j,mode='local')

  #############################################################################
  def getDetailedInfo(self,productionID):
    """ Return detailed information for a given production.
    """
    return self.getParameters(int(productionID),'DetailedInfo')

  #############################################################################
  def _setProductionParameters(self,prodID,groupDescription='',bkPassInfo={},bkInputQuery={},derivedProd=0,prodXMLFile='',reqID=0,printOutput=False,disable=False):
    """ This method will publish production parameters.
    """
    if not prodXMLFile: #e.g. setting parameters for old productions
      prodXMLFile = 'Production%s.xml' %prodID
      if os.path.exists(prodXMLFile):
        self.log.verbose('Using %s for production body' %prodXMLFile)
      else:
        prodClient = RPCClient('ProductionManagement/ProductionManager',timeout=120)
        result = prodClient.getTransformationParameters(int(prodID),['Body'])
        if not result['OK']:
          return S_ERROR("Error during command execution: %s" % result['Message'])
        if not result['Value']:
          return S_ERROR("No body of production %s was found" % prodID)

        body = result['Value']
        fd = open( prodXMLFile, 'wb' )
        fd.write(body)
        fd.close()

    prodWorkflow = Workflow(prodXMLFile)
    if not bkPassInfo:
      try:
        bkPassInfo = prodWorkflow.findParameter('BKProcessingPass').getValue()
      except Exception,x:
        return S_ERROR('Could not determine BKProcessingPass from workflow')
    if not groupDescription:
      groupDescription = prodWorkflow.findParameter('groupDescription').getValue()

    parameters = {}

    parameters['Priority']=prodWorkflow.findParameter('Priority').getValue()
    parameters['CondDBTag']=prodWorkflow.findParameter('CondDBTag').getValue()
    parameters['DDDBTag']=prodWorkflow.findParameter('DDDBTag').getValue()
    parameters['configName']=prodWorkflow.findParameter('configName').getValue()
    parameters['configVersion']=prodWorkflow.findParameter('configVersion').getValue()
    parameters['outputDataFileMask']=prodWorkflow.findParameter('outputDataFileMask').getValue()
    parameters['JobType']=prodWorkflow.findParameter('JobType').getValue()
    parameters['DataType']=prodWorkflow.findParameter('DataType').getValue()

    if prodWorkflow.findParameter('InputData'): #now only comes from BK query
      prodWorkflow.findParameter('InputData').setValue('')
      self.log.verbose('Resetting input data for production to null in workflow template, now comes from a BK query...')
      prodWorkflow.toXMLFile(prodXMLFile)

    if prodWorkflow.findParameter('ParentRequestID'):
      parameters['TransformationFamily']=prodWorkflow.findParameter('ParentRequestID').getValue()

    for i in prodWorkflow.step_instances:
      if i.findParameter('eventType'):
        parameters['eventType']=i.findParameter('eventType').getValue()

    if not parameters.has_key('eventType'):
      return S_ERROR('Could not determine eventType from workflow')

    if prodWorkflow.findParameter('group_size'):
      parameters['group_size']=prodWorkflow.findParameter('group_size').getValue()

    #parameters['numberOfEvents']=prodWorkflow.findParameter('numberOfEvents').getValue()
    parameters['BKCondition']=prodWorkflow.findParameter('conditions').getValue()

    if not bkInputQuery and parameters['JobType'].lower() != 'mcsimulation':
      prodClient = RPCClient('ProductionManagement/ProductionManager',timeout=120)
      res = prodClient.getBookkeepingQueryForTransformation(int(prodID))
      if not res['OK']:
        self.log.error(res)
        return S_ERROR('Could not obtain production info')
      bkInputQuery = res['Value']

    parameters['BKInputQuery']=bkInputQuery
    parameters['BKProcessingPass']=bkPassInfo
    parameters['groupDescription']=groupDescription
    parameters['RequestID']=reqID
    parameters['DerivedProduction']=derivedProd

    inputDataFile = ''
    if parameters['BKInputQuery'] and not parameters['DataType'].lower()=='mc':
      bkDict = parameters['BKInputQuery']
      #To prevent not finding a file remove DQ flag distinction here
      if bkDict.has_key('DataQualityFlag'):
        if not bkDict['DataQualityFlag'].lower() == 'all':
          self.log.info('Removing DQ flag "%s" just to get a dataset' %(bkDict['DataQualityFlag']))
          del bkDict['DataQualityFlag']

      for name,value in bkDict.items():
        if name == "ProductionID" or name == "EventType" or name == "BkQueryID" :
          if value == 0:
            del bkDict[name]
          else:
            bkDict[name] = str(value)
        elif not type(value) == type(' '):
          continue
        else:
          if value.lower() == "all":
            del bkDict[name]

      bkserver = RPCClient('Bookkeeping/BookkeepingManager')
      self.log.verbose('Production has input data query, will attempt to retrieve an input data file for LFN construction')
      result = bkserver.getFilesWithGivenDataSets(bkDict)
      if not result['OK']:
        self.log.error('Could not obtain data from input BK query')
        return S_ERROR('Problem contacting BK for input data sets')

      if result['Value']:
       inputDataFile = result['Value'][0]
       self.log.verbose('Found an input data set from input BK query: %s' %inputDataFile)
      else:
       self.log.verbose('No input datasets found from BK query: %s' %(bkDict))
       return S_ERROR('No input datasets found from BK query to set parameters.')

    dummyProdJobID = '99999999'
    result = self.getOutputLFNs(prodID,dummyProdJobID,inputDataFile,prodXMLFile)
    if not result['OK']:
      self.log.error('Could not create production LFNs',result)

    outputLFNs = result['Value']
    parameters['OutputLFNs']=outputLFNs

    outputDirectories = []
    del outputLFNs['BookkeepingLFNs'] #since ProductionOutputData uses the file mask
    for i in outputLFNs.values():
      for j in i:
        outputDir = '%s%s' %(j.split(str(prodID))[0],prodID)
        if not outputDir in outputDirectories:
          outputDirectories.append(outputDir)

    parameters['OutputDirectories']=outputDirectories
    #Create detailed information string similar to ELOG entry
    info = []
    info.append('%s Production %s for event type %s has following parameters:\n' %(parameters['JobType'],prodID,parameters['eventType']))
    info.append('Production priority: %s' %(parameters['Priority']))
    info.append('BK Config Name Version: %s %s' %(parameters['configName'],parameters['configVersion']))
    info.append('BK Processing Pass Name: %s' %(parameters['groupDescription']))
    info.append('CondDB Tag: %s' %(parameters['CondDBTag']))
    info.append('DDDB Tag: %s\n' %(parameters['DDDBTag']))
 #   info.append('Number of events: %s' %(parameters['numberOfEvents']))
    #Now for the steps of the workflow
    stepKeys = bkPassInfo.keys()
    stepKeys.sort()
    for step in stepKeys:
      info.append('====> %s %s %s' %(bkPassInfo[step]['ApplicationName'],bkPassInfo[step]['ApplicationVersion'],step))
      info.append('  %s Option Files:' %(bkPassInfo[step]['ApplicationName']))
      for opts in bkPassInfo[step]['OptionFiles'].split(';'):
        info.append('    %s' %opts)
      info.append('  ExtraPackages: %s' %(bkPassInfo[step]['ExtraPackages']))

    if parameters['BKInputQuery']:
      info.append('\nBK Input Data Query:')
      for n,v in parameters['BKInputQuery'].items():
        info.append('    %s = %s' %(n,v))

    #BK output directories (very useful)
    bkPaths = []
    bkOutputPath = '%s/%s/%s/%s/%s' %(parameters['configName'],parameters['configVersion'],parameters['BKCondition'],parameters['groupDescription'],parameters['eventType'])
    fileTypes = parameters['outputDataFileMask']
    fileTypes = [a.upper() for a in fileTypes.split(';')]

    #Annoying that histograms are extension root
    if 'ROOT' in fileTypes:
      fileTypes.remove('ROOT')
      fileTypes.append('HIST')

    for f in fileTypes:
      bkPaths.append('%s/%s' %(bkOutputPath,f))
    parameters['BKPaths']=bkPaths
    info.append('\nBK Browsing Paths:\n%s' %(string.join(bkPaths,'\n')))
    infoString = string.join(info,'\n')
    parameters['DetailedInfo']=infoString

    if printOutput:
      for n,v in parameters.items():
        print '='*len(n),n,'='*len(n)
        print v

    if not disable:
      for n,v in parameters.items():
        result = self.setProdParameter(prodID,n,v)
        if not result['OK']:
          self.log.error(result['Message'])

    return S_OK(parameters)

  #############################################################################
  def create(self,publish=True,fileMask='',bkQuery={},groupSize=1,derivedProduction=0,bkScript=True,wfString='',requestID=0,reqUsed=0,transformation=True,transReplicas=0):
    """ Will create the production and subsequently publish to the BK, this
        currently relies on the conditions information being present in the
        worklow.  Production parameters are also added at this point.

        publish = True - will add production to the production management system
                  False - does not publish the production, allows to check the BK script

        bkScript = True - will write a script that can be checked first before
                          adding to BK
                   False - will print BK parameters but publish the production

        transformation = True - will create a transformation to distribute the output data if bkScript is False
                         False - will not create the transformation or a transformation script in case bkScript=True

        The workflow XML is created regardless of the flags.
    """
    prodID = self.defaultProdID
    #Note after testing that the parent ID always seems to be propagated, setting the below removes the need to add something in the templates.
    self.setParentRequest(requestID)

    if wfString:
      self.workflow = fromXMLString(wfString)
      self.name = self.workflow.getName()

    workflowName = self.workflow.getName()
    fileName = '%s.xml' %workflowName
    self.log.verbose('Workflow XML file name is: %s' %fileName)

    try:
      self.createWorkflow()
    except Exception,x:
      self.log.error(x)
      return S_ERROR('Could not create workflow')

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

    realDataFlag = False
    if not bkConditions in simulationDescriptions:
      self.log.verbose('Assuming BK conditions %s are DataTakingConditions' %bkConditions)
      bkDict['DataTakingConditions']=bkConditions
      realDataFlag = True
    else:
      self.log.verbose('Found simulation conditions for %s' %bkConditions)
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

    prodID = 0
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

      result = dirac.createProduction(fileName,fileMask=fileMask,groupSize=groupSize,bkQuery=bkQuery,plugin=self.plugin,productionGroup=self.prodGroup,productionType=self.type,derivedProd=derivedProduction)
      if not result['OK']:
        self.log.error('Problem creating production:\n%s' %result)
        return result
      prodID = result['Value']
      self.log.info('Production %s successfully created' %prodID)
    else:
      self.log.verbose('Publish flag is disabled, using default production ID')

    bkDict['Production']=int(prodID)
    if bkQuery:
      if bkQuery.has_key('ProcessingPass'):
        if not bkQuery['ProcessingPass']=='All':
          inputProcPass = bkQuery['ProcessingPass']
          self.log.verbose('Adding input BK processing pass for production %s from input data query: %s' %(prodID,inputProcPass))
          bkDict['InputProductionTotalProcessingPass']=inputProcPass

    if bkScript:
      self.log.verbose('Writing BK publish script...')
      self._writeBKScript(bkDict,prodID)
    else:
      for n,v in bkDict.items():
        self.log.verbose('%s BK parameter is: %s' %(n,v))

    if publish and not bkScript:
      self.log.verbose('Attempting to publish production to the BK')
      result = bkClient.addProduction(bkDict)
      if not result['OK']:
        self.log.error(result)
      self.log.verbose('BK publishing result: %s' %result)

    if requestID and publish:
      reqClient = RPCClient('ProductionManagement/ProductionRequest',timeout=120)
      reqDict = {'ProductionID':long(prodID),'RequestID':requestID,'Used':reqUsed,'BkEvents':0}
      result = reqClient.addProductionToRequest(reqDict)
      if not result['OK']:
        self.log.error('Attempt to add production %s to request %s failed, dictionary below:\n%s' %(prodID,requestID,reqDict))
      else:
        self.log.info('Successfully added production %s to request %s with Used flag set to %s' %(prodID,requestID,reqUsed))

    if publish:
      try:
        self._setProductionParameters(prodID,prodXMLFile=fileName,groupDescription=bkDict['GroupDescription'],bkPassInfo=bkDict['Steps'],bkInputQuery=bkQuery,reqID=requestID,derivedProd=derivedProduction)
      except Exception,x:
        self.log.error('Failed to set production parameters with exception\n%s\nThis can be done later...' %(str(x)))

    if transformation and not bkScript:
      if not bkQuery.has_key('FileType'):
        return S_ERROR('BK query does not include FileType!')
      bkFileType = bkQuery['FileType']
      result = self._createTransformation(prodID,bkFileType,transReplicas,reqID=requestID,realData=realDataFlag,prodPlugin=self.plugin)
      if not result['OK']:
        self.log.error('Transformation creation failed with below result, can be done later...\n%s' %(result))
      else:
        self.log.info('Successfully created transformation %s for production %s' %(result['Value'],prodID))

      transID = result['Value']
      if transID and prodID:
        result = self.setProdParameter(prodID,'AssociatedTransformation',transID)
        if not result['OK']:
          self.log.error('Could not set AssociatedTransformation parameter to %s for %s with result %s' %(transID,prodID,result))
#        self.log.verbose('Attempting to set the transformation group: %s' %(self.prodGroup))
#        result = self.setProdParameter(transID,'TransformationGroup',self.prodGroup)
#        if not result['OK']:
#          self.log.error('Could not set TransformationGroup parameter to %s for %s with result %s' %(self.prodGroup,transID,result))
        if requestID:
          result = self.setProdParameter(transID,'TransformationFamily',requestID)
          if not result['OK']:
            self.log.error('Could not set TransformationFamily parameter to %s for %s with result %s' %(requestID,transID,result))        
        
    elif transformation:
      if not bkQuery.has_key('FileType'):
        return S_ERROR('BK query does not include FileType!')
      bkFileType = bkQuery['FileType']
      self.log.info('transformation is %s, bkScript generation is %s, writing transformation script' %(transformation,bkScript))
      transID = self._createTransformation(prodID,bkFileType,transReplicas,reqID=requestID,realData=realDataFlag,script=True,prodPlugin=self.plugin)
      if not transID['OK']:
        self.log.error('Problem writing transformation script, result was: %s' %transID)
      else:
        self.log.verbose('Successfully created transformation script for prod %s' %prodID)
    else:
      self.log.info('transformation is %s, bkScript generation is %s, will not write transformation script' %(transformation,bkScript))

    return S_OK(prodID)

  #############################################################################
  def _createTransformation(self,inputProd,fileType,replicas,reqID=0,realData=True,script=False,prodPlugin=''):
    """ Create a transformation to distribute the output data for a given production.
    """
    streams=False
#    if prodPlugin.lower() == 'byfiletypesize' or prodPlugin.lower() == 'byrunfiletypesize' or prodPlugin.lower()=='byrun' or prodPlugin.lower()=='atomicrun':
    if prodPlugin.lower() in [string.lower(i) for i in self.pluginsTriggeringStreamTypes]:
      streams=True
      self.log.info('Found streaming plugin, adding all possible BK file types for query')
      fileType = gConfig.getValue('/Operations/Bookkeeping/FileTypes',[])
      self.log.verbose('DataTypes retrieved from /Operations/Bookkeeping/FileTypes are:\n%s' %(string.join(fileType,', ')))
      tmpTypes = []
      #restrict to '.DST' file types:
      for fType in fileType:
        if re.search('\.DST$',fType):
          tmpTypes.append(fType)
      fileType = tmpTypes
      fileType.append('DST')
      self.log.info('Data types for replication will be: %s' %(string.join(fileType,', ')))

    inputProd = int(inputProd)
    replicas = int(replicas)
    plugin = 'LHCbMCDSTBroadcast'
    if realData:
     plugin = 'LHCbDSTBroadcast'
    tName = '%sReplication_Prod%s' %(fileType,inputProd)
    if streams:
      tName = 'StreamsReplication_Prod%s' %(inputProd)
    if reqID:
      tName = 'Request_%s_%s' %(reqID,tName)

    if script:
      transLines = ['# Transformation publishing script created on %s by' %(time.asctime())]
      transLines.append('# by %s' %self.prodVersion)
      transLines.append('from LHCbDIRAC.ProductionManagementSystem.Client.Transformation import Transformation')
      transLines.append('transformation=Transformation()')
      transLines.append('transformation.setTransformationName("%s")' %(tName))
      if type(fileType)==type([]):
        transLines.append("""transformation.setBkQuery({"ProductionID":%s,"FileType":%s})""" %(inputProd,fileType))
      else:
        transLines.append('transformation.setBkQuery({"ProductionID":%s,"FileType":"%s"})' %(inputProd,fileType))
      transLines.append('transformation.setDescription("Replication of transformation %s output data")' %(inputProd))
      transLines.append('transformation.setLongDescription("This transformation is to replicate the output data from transformation %s according to the computing model")' %(inputProd))
      transLines.append('transformation.setType("Replication")')
      transLines.append('transformation.setPlugin("%s")' %plugin)
      if replicas > 1:
        transLines.append('transformation.setDestinations(%s)' %replicas)
      transLines.append('transformation.addTransformation()')
      transLines.append('transformation.setStatus("Active")')
      transLines.append('transformation.setAgentType("Automatic")')
      transLines.append('print transformation.getTransformationID()')
      if os.path.exists('%s.py' %tName):
        shutil.move('%s.py' %tName,'%s.py.backup' %tName)
      fopen = open('%s.py' %tName,'w')
      fopen.write(string.join(transLines,'\n')+'\n')
      fopen.close()
      return S_OK()

    transformation = Transformation()
    transformation.setTransformationName(tName)
    transformation.setBkQuery({'ProductionID':inputProd,'FileType':fileType})
    transformation.setDescription('Replication of transformation %s output data' % inputProd)
    transformation.setLongDescription('This transformation is to replicate the output data from transformation %s according to the computing model' %(inputProd))
    transformation.setType('Replication')
    transformation.setPlugin(plugin)
    if replicas > 1:
     transformation.setDestinations(replicas)
    transformation.addTransformation()
    transformation.setStatus('Active')
    transformation.setAgentType('Automatic')

    return transformation.getTransformationID()

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
    bkLines.append('from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient')
    bkLines.append('bkClient = BookkeepingClient()')
    bkLines.append('bkDict = %s' %bkDict)
    bkLines.append('print bkClient.addProduction(bkDict)')
    fopen.write(string.join(bkLines,'\n')+'\n')
    fopen.close()

  #############################################################################
  def getOutputLFNs(self,prodID=None,prodJobID=None,inputDataLFN=None,prodXMLFile=''):
    """ Will construct the output LFNs for the production for visual inspection.
    """
    if not prodXMLFile:
      self.log.verbose('Using workflow object to generate XML file')
      prodXMLFile=self.workflow.toXMLFile('%s.xml' %self.name)
    dirac = DiracProduction()
    if not prodID:
      prodID = self.defaultProdID
    if not prodJobID:
      prodJobID = self.defaultProdJobID
    result = dirac._getOutputLFNs(prodXMLFile,productionID=prodID,jobID=prodJobID,inputData=inputDataLFN)
    if not result['OK']:
      return result
    lfns = result['Value']
    self.log.verbose(lfns)
    return result

  #############################################################################
  def _setInputProductionBKStepInfo(self,prodID,passDict={}):
    """ This private method will attempt to retrieve the input production XML file
        in order to construct the BK XML
    """
    if not passDict:
      prodClient = RPCClient('ProductionManagement/ProductionManager',timeout=120)
      result = prodClient.getTransformationParameters(int(prodID),['Body'])
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

    #add merge step...
    for stepID in passDict.keys():
      self.log.verbose('Adding BK processing step %s from production %s:\n%s' %(stepID,prodID,passDict[stepID]))
      self.bkSteps[stepID]=passDict[stepID]
    self.__addBKPassStep()
    self.gaudiStepCount+=len(passDict.keys())
    #note that the merging production step number will follow on from the previous production
    return S_OK()

  #############################################################################
  def setProdParameter(self,prodID,pname,pvalue):
    """Set a production parameter.
    """
    if type(pvalue)==type([]):
      pvalue=string.join(pvalue,'\n')

    prodClient = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    if type(pvalue)==type(2):
      pvalue = str(pvalue)
    result = prodClient.setTransformationParameter(int(prodID),str(pname),str(pvalue))
    if not result['OK']:
      self.log.error('Problem setting parameter %s for production %s and value:\n%s' %(prodID,pname,pvalue))
    return result

  #############################################################################
  def getParameters(self,prodID,pname='',printOutput=False):
    """Get a production parameter or all of them if no parameter name specified.
    """
    prodClient = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    result = prodClient.getTransformation(int(prodID),True)
    if not result['OK']:
      self.log.error(result)
      return S_ERROR('Could not retrieve parameters for production %s' %prodID)

    if not result['Value']:
      self.log.info(result)
      return S_ERROR('No additional parameters available for production %s' %prodID)

    if pname:
      if result['Value'].has_key(pname):
        return S_OK(result['Value'][pname])
      else:
        self.log.verbose(result)
        return S_ERROR('Production %s does not have parameter %s' %(prodID,pname))

    if printOutput:
      for n,v in result['Value'].items():
        if not n.lower()=='body':
          print '='*len(n),'\n',n,'\n','='*len(n)
          print v
        else:
          print '*Omitted Body from printout*'

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
  def setOutputMode(self,outputMode):
    """ Sets output mode for all jobs, this can be 'Local' or 'Any'.
    """
    if not outputMode.lower().capitalize() in ('Local','Any'):
      raise TypeError,'Output mode must be Local or Any'
    self._setParameter('outputMode','string',outputMode.lower().capitalize(),'SEResolutionPolicy')

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
    self._setParameter('CondDBTag','string',conditions.replace(' ',''),'CondDBTag')
    self._setParameter('DDDBTag','string',detector.replace(' ',''),'DetDescTag')

  #############################################################################
  def setParentRequest(self,parentID):
    """ Sets the parent request ID for a production.
    """
    self._setParameter('ParentRequestID','string',str(parentID).replace(' ',''),'ParentRequestID')

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
    self.plugin = plugin

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

  #############################################################################
  def setWorkflowString(self, wfString):
    """ Uses the supplied string to create the workflow
    """
    self.workflow = fromXMLString(wfString)
    self.name = self.workflow.getName()

  #############################################################################
  def disableCPUCheck(self):
    """ Uses the supplied string to create the workflow
    """
    self._setParameter('DisableCPUCheck','JDL','True','DisableWatchdogCPUCheck')

