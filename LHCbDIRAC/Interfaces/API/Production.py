########################################################################
# $HeadURL$
# File :   Production.py
# Author : Stuart Paterson
########################################################################

""" Production API

    Notes:
    - Supports simulation + reconstruction + stripping workflows 
    - create() method that takes a workflow or Production object
      and publishes to the production management system, in addition this
      can automatically construct and publish the BK pass info and transformations
    - Uses getOutputLFNs() function to add production output directory parameter
"""

__RCSID__ = "$Id$"

import string, shutil

from DIRAC.Core.Workflow.Workflow                     import *
from DIRAC.Core.Utilities.List                        import removeEmptyElements, uniqueElements
from DIRAC                                            import gConfig

from LHCbDIRAC.Core.Utilities.ProductionOptions                   import getOptions
from LHCbDIRAC.Core.Utilities.ProductionData                      import preSubmissionLFNs
from LHCbDIRAC.Workflow.Utilities.Utils import getStepDefinition

COMPONENT_NAME = 'LHCbSystem/Client/Production'

class Production():

  #############################################################################

  def __init__( self, script = None, lhcbJobIn = None, BKKClientIn = None,
                transClientIn = None, transformationIn = None ):
    """Instantiates the Workflow object and some default parameters.
    """

    if lhcbJobIn is not None:
      self.LHCbJob = lhcbJobIn
    else:
      from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
      self.LHCbJob = LHCbJob( script )

    if BKKClientIn is not None:
      self.BKKClient = BKKClientIn
    else:
      from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
      self.BKKClient = BookkeepingClient()

    if transClientIn is not None:
      self.transClient = transClientIn
    else:
      from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
      self.transClient = TransformationClient()

    if transformationIn is not None:
      self.transformation = transformationIn
    else:
      from LHCbDIRAC.TransformationSystem.Client.Transformation import Transformation
      self.transformation = Transformation()

    self.prodVersion            = __RCSID__
    self.csSection              = '/Operations/Productions/%s' % gConfig.getValue( "DIRAC/Setup" )
    self.LHCbJob.gaudiStepCount = 0
    defaultHistName = '@{applicationName}_@{STEP_ID}_Hist.root'
#    defaultHistName    = '@{STEP_ID}_@{applicationName}_Hist.root'
    self.histogramName = gConfig.getValue( '%s/HistogramName' % ( self.csSection ), defaultHistName )
    self.histogramSE   = gConfig.getValue( '%s/HistogramSE' % ( self.csSection ), 'CERN-HIST' )
    self.systemConfig  = gConfig.getValue( '%s/SystemConfig' % ( self.csSection ), 'ANY' )
    #self.systemConfig = gConfig.getValue('%s/SystemConfig' %(self.csSection),'x86_64-slc5-gcc43-opt')
    #self.systemConfig = gConfig.getValue('%s/SystemConfig' %(self.csSection),'slc4_ia32_gcc34')
    self.defaultProdID      = '12345'
    self.defaultProdJobID   = '12345'
    self.ioDict             = {}
    self.name               = 'unspecifiedWorkflow'
    self.firstEventType     = ''
    self.bkSteps            = {}
    self.prodGroup          = ''
    self.plugin             = ''
    self.inputFileMask      = ''
    self.inputBKSelection   = {}
    self.jobFileGroupSize   = 0
    self.ancestorProduction = ''
    self.importLine         = """LHCbDIRAC.Workflow.Modules"""
    if not script:
      self.__setDefaults()

  #############################################################################
  
  def __setDefaults( self ):
    """Sets some default parameters.
    """

    #TODO: see for others to be put here
    self.setJobParameters({
                           'Type'          : 'MCSimulation', 
                           'SystemConfig' : self.systemConfig, 
                           'CPUTime'      : '1000000',
                           'LogLevel'     : 'verbose',
                           'JobGroup'     : '@{PRODUCTION_ID}' })

    self.setFileMask( '' )

    #version control
    self._setParameter( 'productionVersion', 'string', self.prodVersion, 'ProdAPIVersion' )

    #General workflow parameters
    self._setParameter( 'PRODUCTION_ID', 'string', self.defaultProdID.zfill( 8 ), 'ProductionID' )
    self._setParameter( 'JOB_ID', 'string', self.defaultProdJobID.zfill( 8 ), 'ProductionJobID' )
    self._setParameter( 'poolXMLCatName', 'string', 'pool_xml_catalog.xml', 'POOLXMLCatalogName' )
    self._setParameter( 'Priority', 'JDL', '1', 'Priority' )
    self._setParameter( 'emailAddress', 'string', 'lhcb-datacrash@cern.ch', 'CrashEmailAddress' )
    self._setParameter( 'outputMode', 'string', 'Local', 'SEResolutionPolicy' )

    #Options related parameters
    self._setParameter( 'CondDBTag', 'string', 'sim-20090112', 'CondDBTag' )
    self._setParameter( 'DDDBTag', 'string', 'head-20090112', 'DetDescTag' )
    self._setParameter( 'EventMaxDefault', 'string', '-1', 'DefaultNumberOfEvents' )
    #BK related parameters
    self._setParameter( 'configName', 'string', 'MC', 'ConfigName' )
    self._setParameter( 'configVersion', 'string', '2009', 'ConfigVersion' )
    self._setParameter( 'conditions', 'string', '', 'SimOrDataTakingCondsString' )

  #############################################################################

  def setJobParameters( self, parametersDict ):
    """ Set an (LHCb)Job parameter
    
        The parametersDict is in the form {'parameterName': 'value'} 
        Each parameter calls LHCbJob.setparameterName(value) 
    """

    for parameter in parametersDict.keys():
      getattr( self.LHCbJob, 'set' + parameter )( parametersDict[parameter] )

  #############################################################################

  def _setParameter( self, name, parameterType, parameterValue, description ):
    """Set parameters checking in CS in case some defaults need to be changed.
    """
    proposedParam = gConfig.getValue( '%s/%s' % ( self.csSection, name ), '' )
    if proposedParam:
      self.LHCbJob.log.debug( 'Setting %s from CS defaults = %s' % ( name, proposedParam ) )
      self.LHCbJob._addParameter( self.LHCbJob.workflow, name, parameterType, proposedParam, description )
    else:
      self.LHCbJob.log.debug( 'Setting parameter %s = %s' % ( name, parameterValue ) )
      self.LHCbJob._addParameter( self.LHCbJob.workflow, name, parameterType, parameterValue, description )

  #############################################################################

  def __getEventType( self, eventType ):
    """ Checks whether or not the global event type should be set.
    """
    if eventType.lower() == 'firststep' and not self.firstEventType:
      raise TypeError, 'Must specify event type for initial step'
    elif eventType.lower() == 'firststep' and self.firstEventType:
      eventType = self.firstEventType
    elif not self.firstEventType:
      self.firstEventType = eventType

    self.LHCbJob.log.verbose( 'Setting event type for current step to %s' % ( eventType ) )
    return eventType

  #############################################################################

  def __checkArguments( self, extraPackages, optionsFile ):
    """ Checks for typos in the structure of standard arguments to workflows.
        In case of any non-standard settings will raise an exception preventing
        creation of the production. Must be called after setting the first event type
        of the production.
    """
    if not extraPackages:
      extraPackages = []

    if not optionsFile:
      optionsFile = []

    if extraPackages:
      if not re.search( ';', extraPackages ):
        extraPackages = [extraPackages]
      else:
        extraPackages = string.split( extraPackages, ';' )
    if optionsFile:
      if not re.search( ';', optionsFile ):
        optionsFile = [optionsFile]

    for p in extraPackages:
      self.LHCbJob.log.verbose( 'Checking extra package: %s' % ( p ) )
      if not re.search( '.', p ):
        raise TypeError, 'Must have extra packages in the following format "Name.Version" not %s' % ( p )

    for o in optionsFile:
      if re.search( 'DECFILESROOT', o ):
        self.LHCbJob.log.verbose( '%s specified, checking event type options: %s' % ( self.firstEventType, o ) )
        if re.search( '@', o ) or re.search( '%s' % self.firstEventType, o ):
          self.LHCbJob.log.verbose( 'Options: %s specify event type correctly' % ( o ) )
        else:
          raise TypeError, 'Event type options must be the event type number or workflow parameter'

    self.LHCbJob.log.verbose( 'Extra packages and event type options are correctly specified' )
    return S_OK()

  #############################################################################

  def addGaussStep( self, appVersion, generatorName, numberOfEvents, optionsFile, eventType = 'firstStep',
                   extraPackages = '', outputSE = None, histograms = False, overrideOpts = '', extraOpts = '',
                   appType = 'sim', condDBTag = 'global', ddDBTag = 'global',
                   stepID = '', stepName = '', stepVisible = '' ):
    """ Wraps around addGaudiStep and getOptions.
        appType can be sim / gen 
    """

    eventType = self.__getEventType( eventType )
    self.__checkArguments( extraPackages, optionsFile )
    firstEventNumber = 1
    if not overrideOpts:
      optionsLine = getOptions( 'Gauss', appType, extraOpts = None, histogram = self.histogramName,
                               condDB = condDBTag, ddDB = ddDBTag )
      self.LHCbJob.log.verbose( 'Default options for Gauss are:\n%s' % ( string.join( optionsLine, '\n' ) ) )
      optionsLine = string.join( optionsLine, ';' )
    else:
      optionsLine = overrideOpts

    if extraOpts:
      extraOpts = removeEmptyElements( string.split( extraOpts, '\n' ) )
      extraOpts = string.join( extraOpts, ';' )
      optionsLine = "%s\n%s" % ( optionsLine, extraOpts )

    if not outputSE:
      outputSE = 'Tier1-RDST'
      self.LHCbJob.log.verbose( 'Setting default outputSE to %s' % ( outputSE ) )

    gaussStep = self._addGaudiStep( 'Gauss', appVersion, appType, numberOfEvents, optionsFile,
                                   optionsLine, eventType, extraPackages, outputSE, '', 'None',
                                   histograms, firstEventNumber, {}, condDBTag, ddDBTag, '',
                                   stepID, stepName, stepVisible )
    gaussStep.setValue( 'numberOfEventsInput', 0 )
    gaussStep.setValue( 'numberOfEventsOutput', 0 )
    gaussStep.setValue( 'generatorName', generatorName )

  #############################################################################
  def addBooleStep( self, appVersion, appType, optionsFile, eventType = 'firstStep', extraPackages = '',
                   outputSE = None, histograms = False, inputData = 'previousStep', overrideOpts = '',
                   extraOpts = '', extraOutputFile = [], condDBTag = 'global', ddDBTag = 'global',
                   stepID = '', stepName = '', stepVisible = '' ):
    """ Wraps around addGaudiStep and getOptions.
        appType is mdf / digi / xdigi
        currently assumes input data type is sim
    """
    eventType = self.__getEventType( eventType )
    self.__checkArguments( extraPackages, optionsFile )
    firstEventNumber = 0
    numberOfEvents = '-1'
    inputDataType = 'sim'
    inputData = 'previousStep'

    if not overrideOpts:
      optionsLine = getOptions( 'Boole', appType, extraOpts = None, histogram = self.histogramName,
                               condDB = condDBTag, ddDB = ddDBTag )
      self.LHCbJob.log.verbose( 'Default options for Boole are:\n%s' % ( string.join( optionsLine, '\n' ) ) )
      optionsLine = string.join( optionsLine, ';' )
    else:
      optionsLine = overrideOpts

    if extraOpts:
      extraOpts = removeEmptyElements( string.split( extraOpts, '\n' ) )
      extraOpts = string.join( extraOpts, ';' )
      optionsLine = "%s\n%s" % ( optionsLine, extraOpts )

    if not outputSE:
      outputSE = 'Tier1-RDST'
      self.LHCbJob.log.verbose( 'Setting default outputSE to %s' % ( outputSE ) )

    if extraOutputFile:
      self.LHCbJob.log.verbose( 'Adding output file to Boole step %s' % extraOutputFile )

    self._addGaudiStep( 'Boole', appVersion, appType, numberOfEvents, optionsFile, optionsLine,
                       eventType, extraPackages, outputSE, inputData, inputDataType, histograms,
                       firstEventNumber, extraOutput = extraOutputFile,
                       condDBTag = condDBTag, ddDBTag = ddDBTag,
                       outputAppendName = '',
                       stepID = stepID, stepName = stepName, stepVisible = stepVisible )

  #############################################################################
  def addBrunelStep( self, appVersion, appType, optionsFile, eventType = 'firstStep', extraPackages = '',
                    inputData = 'previousStep', inputDataType = 'mdf', outputSE = None, histograms = False,
                    overrideOpts = '', extraOpts = '', numberOfEvents = '-1', dataType = 'DATA',
                    condDBTag = 'global', ddDBTag = 'global',
                    stepID = '', stepName = '', stepVisible = '' ):
    """ Wraps around addGaudiStep and getOptions.
        appType is rdst / dst / xdst / sdst
        inputDataType is mdf / digi
        enough to set one of the above
    """
    eventType = self.__getEventType( eventType )
    self.__checkArguments( extraPackages, optionsFile )
    if appType.lower() in ['rdst', 'sdst']:
      if not outputSE:
        outputSE = 'Tier1-RDST'
        self.LHCbJob.log.verbose( 'Setting default outputSE to %s' % ( outputSE ) )
    else:
      if not appType.lower() in ['dst', 'xdst']:
        raise TypeError, 'Application type not recognised'
      if inputDataType.lower() == 'digi':
        if not outputSE:
          outputSE = 'Tier1_MC-DST'
          self.LHCbJob.log.verbose( 'Setting default outputSE to %s' % ( outputSE ) )
      elif inputDataType.lower() == 'fetc':
        #Must rely on data type for fetc only
        if not outputSE:
          if dataType.lower() == 'mc':
            outputSE = 'Tier1_MC-DST'
            self.LHCbJob.log.verbose( 'Setting default outputSE to %s' % ( outputSE ) )
          else:
            outputSE = 'Tier1-DST' #for real data master dst
            self.LHCbJob.log.verbose( 'Setting default outputSE to %s' % ( outputSE ) )

    if not overrideOpts:
      optionsLine = getOptions( 'Brunel', appType, extraOpts = None, histogram = self.histogramName,
                               condDB = condDBTag, ddDB = ddDBTag )
      self.LHCbJob.log.verbose( 'Default options for Brunel are:\n%s' % ( string.join( optionsLine, '\n' ) ) )
      optionsLine = string.join( optionsLine, ';' )
    else:
      optionsLine = overrideOpts

    if extraOpts:
      extraOpts = removeEmptyElements( string.split( extraOpts, '\n' ) )
      extraOpts = string.join( extraOpts, ';' )
      optionsLine = "%s\n%s" % ( optionsLine, extraOpts )

    firstEventNumber = 0

    self._addGaudiStep( 'Brunel', appVersion, appType, numberOfEvents, optionsFile, optionsLine,
                       eventType, extraPackages, outputSE, inputData, inputDataType, histograms,
                       firstEventNumber, extraOutput = [], condDBTag = condDBTag, ddDBTag = ddDBTag,
                       outputAppendName = '',
                       stepID = stepID, stepName = stepName, stepVisible = stepVisible )

  #############################################################################
  def addDaVinciStep( self, appVersion, appType, optionsFile, eventType = 'firstStep', extraPackages = '',
                     inputData = 'previousStep', inputDataType = 'rdst', outputSE = None, histograms = False,
                     overrideOpts = '', extraOpts = '', numberOfEvents = '-1', dataType = 'DATA',
                     condDBTag = 'global', ddDBTag = 'global', inputProduction = '',
                     extraOutput = [],
                     stepID = '', stepName = '', stepVisible = '' ):
    """ Wraps around addGaudiStep and getOptions.
        appType is  dst / dst / setc / fetc / merge / undefined at the moment ;)
        inputDataType is dst / rdst / fetc / sdst 
        [inputProduction is not used and is only there for backwards compatibility]
    """
    eventType = self.__getEventType( eventType )
    self.__checkArguments( extraPackages, optionsFile )
    firstEventNumber = 0

    if inputDataType.lower() == 'dst':
      if not dataType:
        raise TypeError, 'Must clarify MC / DATA for DST->DST processing'
      if not outputSE:
        if dataType.upper() == 'MC':
          outputSE = 'Tier1_MC-DST'
          self.log.verbose( 'Setting default outputSE to %s' % ( outputSE ) )
        else:
          outputSE = 'Tier1-DST'
          self.log.verbose( 'Setting default outputSE to %s' % ( outputSE ) )
    else:
      dataType = 'DATA'
      if not outputSE:
        outputSE = 'Tier1-DST'
        self.log.verbose( 'Setting default outputSE to %s' % ( outputSE ) )

    if appType.lower() in ['merge']: #,'mdst']:
      if not outputSE:
        outputSE = 'Tier1-DST'
        self.LHCbJob.log.verbose( 'Setting default outputSE to %s' % ( outputSE ) )

    if appType.lower() in ['setc', 'mdst']:
      if not outputSE:
        outputSE = 'Tier1-DST'
        self.LHCbJob.log.verbose( 'Setting default outputSE to %s' % ( outputSE ) )

    if not overrideOpts:
      optionsLine = getOptions( 'DaVinci', appType, extraOpts = None, inputType = inputDataType,
                                histogram = self.histogramName, condDB = condDBTag, ddDB = ddDBTag )
      self.LHCbJob.log.verbose( 'Default options for DaVinci are:\n%s' % ( string.join( optionsLine, '\n' ) ) )
      optionsLine = string.join( optionsLine, ';' )
    else:
      optionsLine = overrideOpts

    if extraOpts:
      extraOpts = removeEmptyElements( string.split( extraOpts, '\n' ) )
      extraOpts = string.join( extraOpts, ';' )
      optionsLine = "%s\n%s" % ( optionsLine, extraOpts )
#      print optionsLine

    if appType.lower() == 'davincihist':
      appType = 'dst'

    if  appType.lower() == 'merge':
      appType = inputDataType.lower()

    self._addGaudiStep( 'DaVinci', appVersion, appType, numberOfEvents, optionsFile, optionsLine, eventType,
                       extraPackages, outputSE, inputData, inputDataType, histograms,
                       firstEventNumber, extraOutput, condDBTag, ddDBTag, '',
                       stepID, stepName, stepVisible )

  #############################################################################
  def addMooreStep( self, appVersion, appType, optionsFile, eventType = 'firstStep', extraPackages = '',
                   inputData = 'previousStep', inputDataType = 'raw', outputSE = None, histograms = False,
                   overrideOpts = '', extraOpts = '', numberOfEvents = '-1',
                   condDBTag = 'global', ddDBTag = 'global', outputAppendName = '',
                   stepID = '', stepName = '', stepVisible = '' ):
    """ Wraps around addGaudiStep and getOptions.
        appType is digi (simulation) / dst and inputDataType is digi / raw only at the moment.
    """
    eventType = self.__getEventType( eventType )
    self.__checkArguments( extraPackages, optionsFile )

    firstEventNumber = 0
    appTypes = ['dst', 'digi']
    inputDataTypes = ['raw', 'digi']
    if not appType in appTypes:
      raise TypeError, 'Application type not currently supported (%s)' % appTypes
    if not inputDataType in inputDataTypes:
      raise TypeError, 'Only RAW and DIGI input data type currently supported'

    if not outputSE:
      outputSE = 'Tier1_MC-DST'
      if inputDataType.lower() == 'digi':
        outputSE = 'Tier1-RDST' #convention for intermediate outputs that are not saved by default

    self.LHCbJob.log.verbose( 'Setting default outputSE to %s' % ( outputSE ) )

    if not overrideOpts:
      optionsLine = getOptions( 'Moore', appType, extraOpts = None, inputType = inputDataType,
                               histogram = self.histogramName, condDB = condDBTag, ddDB = ddDBTag )
      self.LHCbJob.log.verbose( 'Default options for Moore are:\n%s' % ( string.join( optionsLine, '\n' ) ) )
      optionsLine = string.join( optionsLine, ';' )
    else:
      optionsLine = overrideOpts

    if extraOpts:
      extraOpts = removeEmptyElements( string.split( extraOpts, '\n' ) )
      extraOpts = string.join( extraOpts, ';' )
      optionsLine = "%s\n%s" % ( optionsLine, extraOpts )

    self._addGaudiStep( 'Moore', appVersion, appType, numberOfEvents, optionsFile, optionsLine,
                       eventType, extraPackages, outputSE, inputData, inputDataType, histograms,
                       firstEventNumber, extraOutput = [], condDBTag = condDBTag, ddDBTag = ddDBTag,
                       outputAppendName = outputAppendName,
                       stepID = stepID, stepName = stepName, stepVisible = stepVisible )


  #############################################################################

  def addMergeStep( self, appVersion = 'v26r3', optionsFile = '$STDOPTS/PoolCopy.opts', inputProduction = '',
                   eventType = 'firstStep', extraPackages = '', inputData = 'previousStep',
                   inputDataType = 'dst', outputSE = None, overrideOpts = '', extraOpts = '', numberOfEvents = '-1',
                   condDBTag = 'global', ddDBTag = 'global', dataType = 'MC',
                   extraOutput = [],
                   stepID = '', stepName = '', stepVisible = '' ):
    """Wraps around addGaudiStep.  The merging uses a standard Gaudi step with
       any available LHCb project as the application.
    """
    eventType = self.__getEventType( eventType )
    self.__checkArguments( extraPackages, optionsFile )
    firstEventNumber = 0
    histograms = False
    #appVersion =''
    #optionsFile = ''
    appType = inputDataType
    if not outputSE:
      outputSE = 'Tier1_MC-DST'
      self.LHCbJob.log.verbose( 'Setting default outputSE to %s' % ( outputSE ) )

    if not overrideOpts:
      optionsLine = getOptions( 'Merge', appType, extraOpts = None, condDB = condDBTag, ddDB = ddDBTag )
      self.LHCbJob.log.verbose( 'Default options for Merging are:\n%s' % ( string.join( optionsLine, '\n' ) ) )
      optionsLine = string.join( optionsLine, ';' )
    else:
      optionsLine = overrideOpts

    if extraOpts:
      extraOpts = removeEmptyElements( string.split( extraOpts, '\n' ) )
      extraOpts = string.join( extraOpts, ';' )
      optionsLine = "%s\n%s" % ( optionsLine, extraOpts )

    self._addGaudiStep( 'LHCb', appVersion, appType, numberOfEvents, optionsFile, optionsLine,
                       eventType, extraPackages, outputSE, inputData, inputDataType, histograms,
                       firstEventNumber, extraOutput, condDBTag, ddDBTag, '',
                       stepID, stepName, stepVisible )
    #if using LHCb to merge we won't want to abandon the output

  #############################################################################

  #NEW: try to put here common stuff of the addXXXXStep
  def addApplicationStep( self, stepDict, outputSE, eventType, extraPackages, optionsFile, overrideOpts ):
    """ stepDict contains everything that is in the step, for this production, e.g.:
        {'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': 13718, 'ApplicationVersion': 'v28r3p1', 
        'ExtraPackages': 'AppConfig.v3r104', 'StepName': 'Stripping14-Merging', 
        'ProcessingPass': 'Merging', 'Visible': 'N', 'DDDB': 'head-20110302', 
        'OptionFiles': '$APPCONFIGOPTS/Merging/DV-Stripping14-Merging.py', 'CONDDB': 'head-20110407',
        'fileTypesIn': ['SDST'], 
        'fileTypesOut': ['BHADRON.DST', 'CALIBRATION.DST', 'CHARM.MDST', 'CHARMCOMPLETEEVENT.DST']}
        
        Note: this step treated here does not necessarily corresponds to a step of the BKK:
        the case where they should be different is the merging case.
    """

    #common to all
    eventType = self.__getEventType( eventType ) #only to be reported in the BKK (and eventually by the StepAccounting
    self.__checkArguments( extraPackages, optionsFile )
    #firstEventNumber is useless here (set by GaudiApplication module for Gauss only

    outputFilesDict = self._constructOutputFilesDict( stepDict['fileTypesOut'], outputSE )

    #TODO: FS: horrible way of dealing with options. I should really eliminate this file and use something smarter. 
    if not overrideOpts:

      if len( stepDict['fileTypesIn'] ) > 1:
        raise ValueError, "too many input file types"
      inputDataType = stepDict['fileTypesIn'][0]

      fileTypesOut = stepDict['fileTypesOut']
      for ft in fileTypesOut:
        if 'hist' in ft.lower():
          fileTypesOut.remove( ft )

      if len( fileTypesOut ) == 1:
        outputFileType = fileTypesOut[0]
      else:
        outputFileType = 'stripping'

      optionsLine = getOptions( stepDict['ApplicationName'], outputFileType, inputType = inputDataType,
                                histogram = self.histogramName, condDB = stepDict['CONDDB'], ddDB = stepDict['DDDB'] )
      self.LHCbJob.log.verbose( 'Default options for DaVinci are:\n%s' % ( string.join( optionsLine, '\n' ) ) )
      optionsLine = string.join( optionsLine, ';' )
    else:
      optionsLine = overrideOpts







  def _constructOutputFilesDict( self, filesList, outputSE ):
    """ build list of dictionary of output files, including HIST case, and fix outputSE for file
    """

    outputList = []

    for fileType in filesList:
      fileDict = {}
      if 'hist' in fileType.lower():
        fileDict['outputDataName'] = '@{STEP_ID}.' + fileType.lower()
        fileDict['outputDataSE'] = outputSE
      else:
        fileDict['outputDataName'] = self.histogramName
        fileDict['outputDataSE'] = self.histogramSE
      fileDict['outputDataType'] = fileType.lower()

      outputList.append( fileDict )

    return outputList

  #############################################################################

  def _addGaudiStep( self, appName, appVersion, appType, numberOfEvents, optionsFile, optionsLine, eventType,
                     extraPackages, outputSE, inputData = 'previousStep', inputDataType = 'None',
                     histograms = False, firstEventNumber = 0, extraOutput = [],
                     condDBTag = 'global', ddDBTag = 'global', outputAppendName = '',
                     stepID = 0, stepName = '', stepVisible = '' ):
    """Helper function.
    """
    if not type( appName ) == type( ' ' ) or not type( appVersion ) == type( ' ' ):
      raise TypeError, 'Expected strings for application name and version'

    self.LHCbJob.gaudiStepCount += 1
#    gaudiStep = self.__getGaudiApplicationStep( '%s_%s' % ( appName, self.LHCbJob.gaudiStepCount ) )

    if 'Gaudi_App_Step' not in self.LHCbJob.workflow.step_definitions.keys():

      modulesNameList = gConfig.getValue( '%s/GaudiStep_Modules' % self.csSection, ['GaudiApplication',
                                                                                    'AnalyseXMLLogFile',
                                                                                    'ErrorLogging',
                                                                                    'BookkeepingReport'] )
      #pName, pType, pValue, pDesc
      parametersList = [
                        ['inputData', 'string', '', 'StepInputData'],
                        ['inputDataType', 'string', '', 'InputDataType'],
                        ['eventType', 'string', '', 'EventType'],
                        ['outputData', 'string', '', 'OutputData'],
                        ['generatorName', 'string', '', 'GeneratorName'],
                        ['applicationName', 'string', '', 'ApplicationName'],
                        ['applicationVersion', 'string', '', 'ApplicationVersion'],
                        ['applicationType', 'string', '', 'ApplicationType'],
                        ['applicationLog', 'string', '', 'ApplicationLogFile'],
                        ['optionsFile', 'string', '', 'OptionsFile'],
                        ['optionsLine', 'string', '', 'OptionsLines'],
                        ['optionsLinePrev', 'string', '', 'PreviousOptionsLines'],
                        ['numberOfEvents', 'string', '', 'NumberOfEvents'],
                        ['numberOfEventsInput', 'string', '', 'NumberOfEventsInput'],
                        ['numberOfEventsOutput', 'string', '', 'NumberOfEventsOutput'],
                        ['listoutput', 'list', [], 'StepOutputList'],
                        ['extraPackages', 'string', '', 'ExtraPackages'],
                        ['firstEventNumber', 'string', 'int', 'FirstEventNumber'],
                        ]

      gaudiStepDef = getStepDefinition( 'Gaudi_App_Step', modulesNameList = modulesNameList,
                                        parametersList = parametersList )
      self.LHCbJob.workflow.addStep( gaudiStepDef )

    #create the step instance add it to the wf, and return it
    gaudiStepInstance = self.LHCbJob.workflow.createStepInstance( 'Gaudi_App_Step',
                                                                  '%s_%s' % ( appName, self.LHCbJob.gaudiStepCount ) )

    #lower the appType if not creating a template
    if type( appType ) == str and appType and not re.search( '{{', appType ):
      appType = string.lower( appType )
    if outputAppendName:
      appType = string.lower( '%s.%s' % ( outputAppendName, appType ) )
    if type( optionsFile ) == type( [] ):
      optionsFile = string.join( optionsFile, ';' )
    optionsFile = optionsFile.replace( ' ', '' )

    valuesToSet = [
                   ['applicationName', appName ],
                   ['applicationVersion', appVersion ],
                   ['applicationType', appType ],
                   ['optionsFile', optionsFile ],
                   ['optionsLine', optionsLine],
                   ['optionsLinePrev', 'None'],
                   ['numberOfEvents', numberOfEvents],
                   ['eventType', eventType], 
#                  Failed attempt to standarise output names
#                   ['applicationLog', '@{STEP_ID}_@{applicationName}_Logs.log'],
#                   ['outputData', '@{STEP_ID}_@{applicationName}_Data.@{applicationType}'],
                   ['applicationLog', '@{applicationName}_@{STEP_ID}.log'],
                   ['outputData', '@{STEP_ID}.@{applicationType}'],
                   ]

    if extraPackages:
      if type( extraPackages ) == type( [] ):
        extraPackages = string.join( extraPackages, ';' )

      extraPackages = extraPackages.replace( ' ', '' )
      valuesToSet.append( [ 'extraPackages', extraPackages ] )
      self.__addSoftwarePackages( extraPackages )

    if firstEventNumber:
      valuesToSet.append( [ 'firstEventNumber', firstEventNumber ] )

    if inputDataType != 'None':
      if re.search( '{{', inputDataType ):
        valuesToSet.append( [ 'inputDataType', inputDataType ] )
      else:
        valuesToSet.append( [ 'inputDataType', string.upper( inputDataType ) ] )

    for pName, value in valuesToSet:
      gaudiStepInstance.setValue( pName, value )



    if not inputData:
      self.LHCbJob.log.verbose( '%s step has no data requirement or is linked to the overall input data' % appName )
      gaudiStepInstance.setLink( 'inputData', 'self', 'InputData' )
    elif inputData == 'previousStep':
      self.LHCbJob.log.verbose( 'Taking input data as output from previous Gaudi step' )
      if not self.ioDict.has_key( self.LHCbJob.gaudiStepCount - 1 ):
        raise TypeError, 'Expected previously defined Gaudi step for input data'
      gaudiStepInstance.setLink( 'inputData', self.ioDict[self.LHCbJob.gaudiStepCount - 1], 'outputData' )
    elif inputData == 'firstStep':
      self.LHCbJob.log.verbose( 'Taking input data as output from first Gaudi step' )
      stepKeys = self.ioDict.keys()
      stepKeys.sort()
      gaudiStepInstance.setLink( 'inputData', self.ioDict[stepKeys[0]], 'outputData' )
    else:
      self.LHCbJob.log.verbose( 'Assume input data requirement should be added to job' )
      self.LHCbJob.setInputData( inputData )
      gaudiStepInstance.setLink( 'inputData', 'self', 'InputData' )
      #such that it can be overwritten during submission
      #but also the template value can be used for testing

    outputList = []

    #trying to control the stripping case, with many output file types
    if not extraOutput:
      extraOutput = [appType]

    for fileType in extraOutput:
      gaudiStepOutputItem = {}

      gaudiStepOutputItem['outputDataName'] = '@{STEP_ID}.' + fileType.lower()
      gaudiStepOutputItem['outputDataType'] = fileType.lower()
      gaudiStepOutputItem['outputDataSE'] = outputSE

      outputList.append( gaudiStepOutputItem )

    if histograms:
      histoFile = {}
      histoFile['outputDataName'] = self.histogramName
      histoFile['outputDataType'] = 'HIST'
      histoFile['outputDataSE'] = self.histogramSE
      outputList.append( histoFile )
#    if extraOutput:
#      outputList.append( extraOutput )

    gaudiStepInstance.setValue( 'listoutput', ( outputList ) )

    #Ensure the global input data type is null
    description = 'Default input data type field'
    self.LHCbJob._addParameter( self.LHCbJob.workflow, 'InputDataType', 'JDL', '', description )

    # now we have to tell DIRAC to install the necessary software
    self.__addSoftwarePackages( '%s.%s' % ( appName, appVersion ) )
    dddbOpt = "@{DDDBTag}"
    conddbOpt = "@{CondDBTag}"
    if not condDBTag.lower() == 'global':
      self.LHCbJob.log.verbose( 'Specific CondDBTag setting found for %s step, setting to: %s' % ( appName, condDBTag ) )
      conddbOpt = condDBTag.replace( ' ', '' )
    if not ddDBTag.lower() == 'global':
      self.LHCbJob.log.verbose( 'Specific DDDBTag setting found for %s step, setting to: %s' % ( appName, ddDBTag ) )
      dddbOpt = ddDBTag.replace( ' ', '' )

    #to construct the BK processing pass structure, starts from step '0'
    stepIDInternal = 'Step%s' % ( self.LHCbJob.gaudiStepCount - 1 )
    bkOptionsFile = optionsFile
    if re.search( '@{eventType}', optionsFile ):
      bkOptionsFile = string.replace( optionsFile, '@{eventType}', str( eventType ) )

    stepBKInfo = {'ApplicationName':appName,
                  'ApplicationVersion':appVersion,
                  'OptionFiles':bkOptionsFile,
                  'DDDb':dddbOpt,
                  'CondDb':conddbOpt,
                  'ExtraPackages':extraPackages,
                  'BKStepID':stepID,
                  'StepName':stepName,
                  'StepVisible':stepVisible}

    self.bkSteps[stepIDInternal] = stepBKInfo
    self.__addBKPassStep()
    #to keep track of the inputs / outputs for a given workflow track the step number and name
    self.ioDict[self.LHCbJob.gaudiStepCount] = gaudiStepInstance.getName()

    return gaudiStepInstance

  #############################################################################
  def __addSoftwarePackages( self, nameVersion ):
    """ Internal method to accumulate software packages.
    """
    swPackages = 'SoftwarePackages'
    description = 'LHCbSoftwarePackages'
    if not self.LHCbJob.workflow.findParameter( swPackages ):
      self.LHCbJob._addParameter( self.LHCbJob.workflow, swPackages, 'JDL', nameVersion, description )
    else:
      apps = self.LHCbJob.workflow.findParameter( swPackages ).getValue()
      apps = apps.split( ';' )
      apps.append( nameVersion )
      apps = uniqueElements( removeEmptyElements( apps ) )
      apps = string.join( apps, ';' )
      self.LHCbJob._addParameter( self.LHCbJob.workflow, swPackages, 'JDL', apps, description )

  #############################################################################
  def __addBKPassStep( self ):
    """ Internal method to add BKK parameters
    """
    bkPass = 'BKProcessingPass'
    description = 'BKProcessingPassInfo'
    self.LHCbJob._addParameter( self.LHCbJob.workflow, bkPass, 'dict', self.bkSteps, description )

  #############################################################################

  def addFinalizationStep( self, modulesList = [] ):

    if 'Job_Finalization' not in self.LHCbJob.workflow.step_definitions.keys():

      if not modulesList:
        modulesNameList = gConfig.getValue( '%s/FinalizationStep_Modules' % self.csSection, ['UploadOutputData',
                                                                                             'UploadLogFile',
                                                                                             'FailoverRequest'] )
      else:
        modulesNameList = modulesList

      jobFinalizationStepDef = getStepDefinition( 'Job_Finalization', modulesNameList = modulesNameList )
      self.LHCbJob.workflow.addStep( jobFinalizationStepDef )

    #create the step instance add it to the wf
    self.LHCbJob.workflow.createStepInstance( 'Job_Finalization', 'finalization' )

  #############################################################################
  def createWorkflow( self, name = '' ):
    """ Create XML for local testing.
    """
    if not name:
      name = self.name
    if not re.search( 'xml$', name ):
      name = '%s.xml' % name
    if os.path.exists( name ):
      shutil.move( name, '%s.backup' % name )
    name = name.replace( '/', '' ).replace( '\\', '' )
    self.LHCbJob.workflow.toXMLFile( name )
    return S_OK( name )

  #############################################################################

  def runLocal( self, DiracLHCb = None ):
    """ 
        Create XML workflow for local testing then reformulate as a job and run locally.
    """

    name = self.createWorkflow()['Value']
    # this "name" is the xml file 
    from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
    j = LHCbJob( name )
    # it makes a job (a Worklow, with Parameters, out of the xml file

    if DiracLHCb is not None:
      diracLHCb = DiracLHCb
    else:
      from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
      diracLHCb = DiracLHCb()

    return j.runLocal( diracLHCb, self.BKKClient )

  #############################################################################

  def getDetailedInfo( self, productionID ):
    """ Return detailed information for a given production.
    """
    return self.getParameters( int( productionID ), 'DetailedInfo' )

  #############################################################################
  def _getProductionParameters( self, prodID, groupDescription = '', bkPassInfo = {}, bkInputQuery = {},
                               derivedProd = 0, prodXMLFile = '', reqID = 0 ):
    """ This method will publish production parameters.
    """
    if not prodXMLFile: #e.g. setting parameters for old productions
      prodXMLFile = 'Production%s.xml' % prodID
      if os.path.exists( prodXMLFile ):
        self.LHCbJob.log.verbose( 'Using %s for production body' % prodXMLFile )
      else:
        result = self.transClient.getTransformationParameters( int( prodID ), ['Body'] )
        if not result['OK']:
          return S_ERROR( "Error during command execution: %s" % result['Message'] )
        if not result['Value']:
          return S_ERROR( "No body of production %s was found" % prodID )

        body = result['Value']
        fd = open( prodXMLFile, 'wb' )
        fd.write( body )
        fd.close()

    prodWorkflow = Workflow( prodXMLFile )
    if not bkPassInfo:
      try:
        bkPassInfo = prodWorkflow.findParameter( 'BKProcessingPass' ).getValue()
      except Exception:
        return S_ERROR( 'Could not determine BKProcessingPass from workflow' )
    if not groupDescription:
      groupDescription = prodWorkflow.findParameter( 'groupDescription' ).getValue()

    parameters = {}

    parameters['Priority'] = prodWorkflow.findParameter( 'Priority' ).getValue()
    parameters['CondDBTag'] = prodWorkflow.findParameter( 'CondDBTag' ).getValue()
    parameters['DDDBTag'] = prodWorkflow.findParameter( 'DDDBTag' ).getValue()
    parameters['configName'] = prodWorkflow.findParameter( 'configName' ).getValue()
    parameters['configVersion'] = prodWorkflow.findParameter( 'configVersion' ).getValue()
    parameters['outputDataFileMask'] = prodWorkflow.findParameter( 'outputDataFileMask' ).getValue()
    parameters['JobType'] = prodWorkflow.findParameter( 'JobType' ).getValue()
    parameters['GroupSize'] = self.jobFileGroupSize

    if parameters['JobType'].lower() == 'mcsimulation':
      # A.T. EventsPerTask is considered immutable by Andrew
      #if prodWorkflow.findParameter('EventsPerTask'):
      #  parameters['EventsPerTask']=prodWorkflow.findParameter('EventsPerTask').getValue()
      if prodWorkflow.findParameter( 'MaxNumberOfTasks' ):
        parameters['MaxNumberOfTasks'] = prodWorkflow.findParameter( 'MaxNumberOfTasks' ).getValue()

    if prodWorkflow.findParameter( 'InputData' ): #now only comes from BK query
      prodWorkflow.findParameter( 'InputData' ).setValue( '' )
      self.LHCbJob.log.verbose( 'Resetting input data for production to null, this comes from a BK query...' )
      prodXMLFile = self.createWorkflow( prodXMLFile )['Value']
      #prodWorkflow.toXMLFile(prodXMLFile)

    if prodWorkflow.findParameter( 'TransformationFamily' ):
      parameters['TransformationFamily'] = prodWorkflow.findParameter( 'TransformationFamily' ).getValue()

    for i in prodWorkflow.step_instances:
      if i.findParameter( 'eventType' ):
        parameters['eventType'] = i.findParameter( 'eventType' ).getValue()

    if not parameters.has_key( 'eventType' ):
      return S_ERROR( 'Could not determine eventType from workflow' )

    parameters['BKCondition'] = prodWorkflow.findParameter( 'conditions' ).getValue()

    if not bkInputQuery and parameters['JobType'].lower() != 'mcsimulation':
      res = self.transClient.getBookkeepingQueryForTransformation( int( prodID ) )
      if not res['OK']:
        self.LHCbJob.log.error( res )
        return S_ERROR( 'Could not obtain production info' )
      bkInputQuery = res['Value']

    parameters['BKInputQuery'] = bkInputQuery
    parameters['BKProcessingPass'] = bkPassInfo
    parameters['groupDescription'] = groupDescription
    parameters['RequestID'] = reqID
    parameters['DerivedProduction'] = derivedProd

    dummyProdJobID = '99999999'
    result = self.getOutputLFNs( prodID, dummyProdJobID, prodXMLFile )
    if not result['OK']:
      self.LHCbJob.log.error( 'Could not create production LFNs', result )

    outputLFNs = result['Value']
    parameters['OutputLFNs'] = outputLFNs

    outputDirectories = []
    del outputLFNs['BookkeepingLFNs'] #since ProductionOutputData uses the file mask
    for i in outputLFNs.values():
      for j in i:
        outputDir = '%s%s' % ( j.split( str( prodID ) )[0], prodID )
        if not outputDir in outputDirectories:
          outputDirectories.append( outputDir )

    parameters['OutputDirectories'] = outputDirectories
    #Create detailed information string similar to ELOG entry
    #TODO: put tags per step and include other interesting parameters
    info = []
    info.append( '%s Production %s for event type %s has following parameters:\n' % ( parameters['JobType'], prodID, parameters['eventType'] ) )
    info.append( 'Production priority: %s' % ( parameters['Priority'] ) )
    info.append( 'BK Config Name Version: %s %s' % ( parameters['configName'], parameters['configVersion'] ) )
    info.append( 'BK Processing Pass Name: %s' % ( parameters['groupDescription'] ) )
    info.append( 'CondDB Tag: %s' % ( parameters['CondDBTag'] ) )
    info.append( 'DDDB Tag: %s\n' % ( parameters['DDDBTag'] ) )
    #info.append('Number of events: %s' %(parameters['numberOfEvents']))
    #Now for the steps of the workflow
    stepKeys = bkPassInfo.keys()
    stepKeys.sort()
    for step in stepKeys:
      info.append( '====> %s %s %s' % ( bkPassInfo[step]['ApplicationName'], bkPassInfo[step]['ApplicationVersion'], step ) )
      info.append( '  %s Option Files:' % ( bkPassInfo[step]['ApplicationName'] ) )
      for opts in bkPassInfo[step]['OptionFiles'].split( ';' ):
        info.append( '    %s' % opts )
      info.append( '  ExtraPackages: %s' % ( bkPassInfo[step]['ExtraPackages'] ) )

    if parameters['BKInputQuery']:
      info.append( '\nBK Input Data Query:' )
      for n, v in parameters['BKInputQuery'].items():
        info.append( '    %s = %s' % ( n, v ) )

    #BK output directories (very useful)
    bkPaths = []
    bkOutputPath = '%s/%s/%s/%s/%s' % ( parameters['configName'], parameters['configVersion'], parameters['BKCondition'], parameters['groupDescription'], parameters['eventType'] )
    fileTypes = parameters['outputDataFileMask']
    fileTypes = [a.upper() for a in fileTypes.split( ';' )]

    #Annoying that histograms are extension root
    if 'ROOT' in fileTypes:
      fileTypes.remove( 'ROOT' )
      fileTypes.append( 'HIST' )

    for f in fileTypes:
      bkPaths.append( '%s/%s' % ( bkOutputPath, f ) )
    parameters['BKPaths'] = bkPaths
    info.append( '\nBK Browsing Paths:\n%s' % ( string.join( bkPaths, '\n' ) ) )
    infoString = string.join( info, '\n' )
    parameters['DetailedInfo'] = infoString

    return S_OK( parameters )

  #############################################################################
  def create( self, publish = True, fileMask = '', bkQuery = {}, groupSize = 1, derivedProduction = 0,
              bkScript = True, wfString = '', requestID = 0, reqUsed = 0,
              transformation = True, transReplicas = 0, bkProcPassPrepend = '', parentRequestID = 0, transformationPlugin = '' ):
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
    #Needs to be revisited in order to disentangle the many operations.
    if not parentRequestID:
      parentRequestID = requestID

    self.setParentRequest( parentRequestID )

    if wfString:
      self.LHCbJob.workflow = fromXMLString( wfString )
      self.name = self.LHCbJob.workflow.getName()

    try:
      fileName = self.createWorkflow()['Value']
    except Exception, x:
      self.LHCbJob.log.error( x )
      return S_ERROR( 'Could not create workflow' )

    self.LHCbJob.log.verbose( 'Workflow XML file name is: %s' % fileName )

    workflowBody = ''
    if os.path.exists( fileName ):
      fopen = open( fileName, 'r' )
      workflowBody = fopen.read()
      fopen.close()
    else:
      return S_ERROR( 'Could not get workflow body' )

    bkConditions = self.LHCbJob.workflow.findParameter( 'conditions' ).getValue()

    bkDict = {}
    bkSteps = self.LHCbJob.workflow.findParameter( 'BKProcessingPass' ).getValue()
    bkDict['Steps'] = bkSteps
    bkDict['GroupDescription'] = self.LHCbJob.workflow.findParameter( 'groupDescription' ).getValue()

    # After the reorganisation by steps release this stuff can be greatly simplified
    # only the stepID, stepName and stepVisible need to be tracked.
    # In the first instance I am just demonstrating the new functionality without making
    # sweeping changes
    bkDictStep = {}

    #Add the BK conditions metadata / name
    simConds = self.BKKClient.getSimConditions()
    if not simConds['OK']:
      self.LHCbJob.log.error( 'Could not retrieve conditions data from BK:\n%s' % simConds )
      return simConds
    simulationDescriptions = []
    for record in simConds['Value']:
      simulationDescriptions.append( str( record[1] ) )

    realDataFlag = False
    if not bkConditions in simulationDescriptions:
      self.LHCbJob.log.verbose( 'Assuming BK conditions %s are DataTakingConditions' % bkConditions )
      bkDict['DataTakingConditions'] = bkConditions
      bkDictStep['DataTakingConditions'] = bkConditions
      realDataFlag = True
    else:
      self.LHCbJob.log.verbose( 'Found simulation conditions for %s' % bkConditions )
      bkDict['SimulationConditions'] = bkConditions
      bkDictStep['SimulationConditions'] = bkConditions

    #Adding some MC transformation parameters if present
    maxNumberOfTasks = 0
    maxEventsPerTask = 0
    if self.LHCbJob.workflow.findParameter( 'MaxNumberOfTasks' ):
      maxNumberOfTasks = self.LHCbJob.workflow.findParameter( 'MaxNumberOfTasks' ).getValue()
    if self.LHCbJob.workflow.findParameter( 'EventsPerTask' ):
      maxEventsPerTask = self.LHCbJob.workflow.findParameter( 'EventsPerTask' ).getValue()

    descShort = self.LHCbJob.workflow.getDescrShort()
    descLong = self.LHCbJob.workflow.getDescription()

    prodID = 0
    if publish:
      if self.inputFileMask:
        fileMask = self.inputFileMask
      if self.jobFileGroupSize:
        groupSize = self.jobFileGroupSize
      if self.inputBKSelection:
        bkQuery = self.inputBKSelection
      if self.ancestorProduction:
        derivedProduction = self.ancestorProduction

      #This mechanism desperately needs to be reviewed
      result = self.transClient.addTransformation( fileName, descShort, descLong, self.LHCbJob.type, self.plugin, 'Manual',
                                                   fileMask, transformationGroup = self.prodGroup, groupSize = int( groupSize ),
                                                   inheritedFrom = int( derivedProduction ), body = workflowBody,
                                                   maxTasks = int( maxNumberOfTasks ),
                                                   eventsPerTask = int( maxEventsPerTask ),
                                                   bkQuery = bkQuery
                                                  )

      if not result['OK']:
        self.LHCbJob.log.error( 'Problem creating production:\n%s' % result )
        return result
      prodID = result['Value']
      self.LHCbJob.log.info( 'Production %s successfully created' % prodID )
    else:
      self.LHCbJob.log.verbose( 'Publish flag is disabled, using default production ID' )

    bkDict['Production'] = int( prodID )
    bkDictStep['Production'] = int( prodID )

    queryProdID = 0
    if bkQuery.has_key( 'ProductionID' ):
      queryProdID = int( bkQuery['ProductionID'] )
    queryProcPass = ''
    if bkQuery.has_key( 'ProcessingPass' ):
      if not bkQuery['ProcessingPass'] == 'All':
        queryProcPass = bkQuery['ProcessingPass']

    if bkQuery:
      if queryProdID:
        inputPass = self.BKKClient.getProductionProcessingPass( queryProdID )
        if not inputPass['OK']:
          self.LHCbJob.log.error( inputPass )
          self.LHCbJob.log.error( 'Production %s was created but BK processsing pass for %s was not found' % ( prodID, queryProdID ) )
          return inputPass
        inputPass = inputPass['Value']
        self.LHCbJob.log.info( 'Setting %s as BK input production for %s with processing pass %s' % ( queryProdID, prodID, inputPass ) )
        bkDict['InputProductionTotalProcessingPass'] = inputPass
        bkDictStep['InputProductionTotalProcessingPass'] = inputPass
      elif queryProcPass:
        self.LHCbJob.log.info( 'Adding input BK processing pass for production %s from input data query: %s' % ( prodID, queryProcPass ) )
        bkDict['InputProductionTotalProcessingPass'] = queryProcPass
        bkDictStep['InputProductionTotalProcessingPass'] = queryProcPass

    if bkProcPassPrepend:
      self.LHCbJob.log.info( 'The following path will be prepended to the BK processing pass for this production: %s' % ( bkProcPassPrepend ) )
      bkDict['InputProductionTotalProcessingPass'] = bkProcPassPrepend
      bkDictStep['InputProductionTotalProcessingPass'] = bkProcPassPrepend

    stepList = []
    stepKeys = bkSteps.keys()
    #The BK needs an ordered list of steps
    stepKeys.sort()
    for step in stepKeys:
      stepID = bkSteps[step]['BKStepID']
      if stepID:
        stepName = bkSteps[step]['StepName']
        stepVisible = bkSteps[step]['StepVisible']
        stepList.append( {'StepId':int( stepID ), 'StepName':stepName, 'Visible':stepVisible} )

    #This is the last component necessary for the BK publishing (post reorganisation)
    bkDictStep['Steps'] = stepList

    if bkScript:
      self.LHCbJob.log.verbose( 'Writing BK publish script...' )
      self._publishProductionToBK( bkDictStep, prodID, script = True )
    else:
      for n, v in bkDictStep.items():
        self.LHCbJob.log.verbose( '%s BK parameter is: %s' % ( n, v ) )

    if publish and not bkScript:
      self._publishProductionToBK( bkDictStep, prodID, script = False )

    if requestID and publish:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      reqClient = RPCClient( 'ProductionManagement/ProductionRequest', timeout = 120 )
      reqDict = {'ProductionID':long( prodID ), 'RequestID':requestID, 'Used':reqUsed, 'BkEvents':0}
      result = reqClient.addProductionToRequest( reqDict )
      if not result['OK']:
        self.LHCbJob.log.error( 'Attempt to add production %s to request %s failed, dictionary below:\n%s' % ( prodID, requestID, reqDict ) )
      else:
        self.LHCbJob.log.info( 'Successfully added production %s to request %s with Used flag set to %s' % ( prodID, requestID, reqUsed ) )

    if publish:
      try:
        paramsDict = self._getProductionParameters( prodID,
                                                    prodXMLFile = fileName,
                                                    groupDescription = bkDict['GroupDescription'],
                                                    bkPassInfo = bkDict['Steps'],
                                                    bkInputQuery = bkQuery,
                                                    reqID = requestID,
                                                    derivedProd = derivedProduction )
        for n, v in paramsDict.items():
          result = self.setProdParameter( prodID, n, v )
          if not result['OK']:
            self.LHCbJob.log.error( result['Message'] )

      except Exception, x:
        self.LHCbJob.log.error( 'Failed to set production parameters with exception\n%s\nThis can be done later...' % ( str( x ) ) )

    if transformation and not bkScript:
      if not bkQuery.has_key( 'FileType' ):
        return S_ERROR( 'BK query does not include FileType!' )
      bkFileType = bkQuery['FileType']
      result = self._createTransformation( prodID, bkFileType, transReplicas, reqID = requestID, realData = realDataFlag,
                                           prodPlugin = self.plugin, groupDescription = bkDict['GroupDescription'],
                                           parentRequestID = parentRequestID, transformationPlugin = transformationPlugin )
      if not result['OK']:
        self.LHCbJob.log.error( 'Transformation creation failed with below result, can be done later...\n%s' % ( result ) )
      else:
        self.LHCbJob.log.info( 'Successfully created transformation %s for production %s' % ( result['Value'], prodID ) )

      transID = result['Value']
      if transID and prodID:
        result = self.setProdParameter( prodID, 'AssociatedTransformation', transID )
        if not result['OK']:
          self.LHCbJob.log.error( 'Could not set AssociatedTransformation parameter to %s for %s with result %s' % ( transID, prodID, result ) )

    elif transformation:
      if not bkQuery.has_key( 'FileType' ):
        return S_ERROR( 'BK query does not include FileType!' )
      bkFileType = bkQuery['FileType']
      self.LHCbJob.log.info( 'transformation is %s, bkScript generation is %s, writing transformation script' % ( transformation, bkScript ) )
      transID = self._createTransformation( prodID, bkFileType, transReplicas, reqID = requestID, realData = realDataFlag,
                                           script = True, prodPlugin = self.plugin, groupDescription = bkDict['GroupDescription'],
                                           parentRequestID = parentRequestID, transformationPlugin = transformationPlugin )
      if not transID['OK']:
        self.LHCbJob.log.error( 'Problem writing transformation script, result was: %s' % transID )
      else:
        self.LHCbJob.log.verbose( 'Successfully created transformation script for prod %s' % prodID )
    else:
      self.LHCbJob.log.info( 'transformation is %s, bkScript generation is %s, will not write transformation script' % ( transformation, bkScript ) )

    return S_OK( prodID )

  #############################################################################
  def _createTransformation( self, inputProd, fileType, replicas, reqID = 0, realData = True,
                             script = False, prodPlugin = '', groupDescription = '',
                             parentRequestID = 0, transformationPlugin = '' ):
    """ Create a transformation to distribute the output data for a given production.
    """

    inputProd = int( inputProd )
    replicas = int( replicas )

    if transformationPlugin:
      plugin = transformationPlugin
    else:
      if realData:
        plugin = 'LHCbDSTBroadcast'
      else:
        plugin = 'LHCbMCDSTBroadcast'

    tName = '%sReplication_Prod%s' % ( fileType, inputProd )
    if reqID:
      tName = 'Request_%s_%s' % ( reqID, tName )

    if script:
      transLines = ['# Transformation publishing script created on %s by' % ( time.asctime() )]
      transLines.append( '# by %s' % self.prodVersion )
      transLines.append( 'from LHCbDIRAC.TransformationSystem.Client.Transformation import Transformation' )
      transLines.append( 'transformation=Transformation()' )
      transLines.append( 'transformation.setTransformationName("%s")' % ( tName ) )
      if type( fileType ) == type( [] ):
        transLines.append( """transformation.setBkQuery({"ProductionID":%s,"FileType":%s})""" % ( inputProd, fileType ) )
      else:
        transLines.append( 'transformation.setBkQuery({"ProductionID":%s,"FileType":"%s"})' % ( inputProd, fileType ) )
      transLines.append( 'transformation.setDescription("Replication of transformation %s output data")' % ( inputProd ) )
      transLines.append( 'transformation.setLongDescription("This transformation is to replicate the output data from transformation %s according to the computing model")' % ( inputProd ) )
      transLines.append( 'transformation.setType("Replication")' )
      transLines.append( 'transformation.setPlugin("%s")' % plugin )
      if replicas > 1:
        transLines.append( 'transformation.setDestinations(%s)' % replicas )
      transLines.append( 'transformation.addTransformation()' )
      transLines.append( 'transformation.setStatus("Active")' )
      transLines.append( 'transformation.setAgentType("Automatic")' )
      transLines.append( 'transformation.setTransformationGroup("%s")' % ( groupDescription ) )
      transLines.append( 'print transformation.getTransformationID()' )
      if os.path.exists( '%s.py' % tName ):
        shutil.move( '%s.py' % tName, '%s.py.backup' % tName )
      fopen = open( '%s.py' % tName, 'w' )
      fopen.write( string.join( transLines, '\n' ) + '\n' )
      fopen.close()
      return S_OK()


    self.transformation.setTransformationName( tName )
    self.transformation.setBkQuery( {'ProductionID':inputProd, 'FileType':fileType} )
    self.transformation.setDescription( 'Replication of transformation %s output data' % inputProd )
    self.transformation.setLongDescription( 'This transformation is to replicate the output data from transformation %s according to the computing model' % ( inputProd ) )
    self.transformation.setType( 'Replication' )
    self.transformation.setPlugin( plugin )
    if replicas > 1:
      self.transformation.setDestinations( replicas )
    self.transformation.setTransformationGroup( groupDescription )
    self.transformation.addTransformation()
    self.transformation.setStatus( 'Active' )
    self.transformation.setAgentType( 'Automatic' )
    transResult = self.transformation.getTransformationID()
    if not transResult['OK']:
      return transResult

    transID = transResult['Value']
    if parentRequestID:
      result = self.setProdParameter( transID, 'TransformationFamily', parentRequestID )
      if not result['OK']:
        self.LHCbJob.log.error( 'Could not set TransformationFamily parameter to %s for %s with result %s' % ( parentRequestID, transID, result ) )

    # Since other prods also have this parameter defined.
    result = self.setProdParameter( transID, 'groupDescription', groupDescription )
    if not result['OK']:
      self.LHCbJob.log.error( 'Could no set groupDescription parameter with result %s' % ( result['Message'] ) )

    # Set the detailed info parameter such that the "Show Details" portal option works for transformations.
    infoString = []
    infoString.append( 'Replication transformation %s was created for %s\nWith plugin %s' % ( transID, groupDescription, plugin ) )
    infoString.append( '\nBK Input Data Query:\n    ProductionID : %s\n    FileType     : %s' % ( inputProd, fileType ) )
    infoString = string.join( infoString, '\n' )
    result = self.setProdParameter( transID, 'DetailedInfo', infoString )
    if not result['OK']:
      self.LHCbJob.log.error( 'Could not set Transformation DetailedInfo parameter for %s with result %s' % ( transID, result ) )

    return transResult

  #############################################################################
  def _publishProductionToBK( self, bkDict, prodID, script = False ):
    """Publishes the production to the BK or writes a script to do so.
    """
    if script:
      bkName = 'insertBKPass%s.py' % ( prodID )
      if os.path.exists( bkName ):
        shutil.move( bkName, '%s.backup' % bkName )
      fopen = open( bkName, 'w' )
      bkLines = ['# Bookkeeping publishing script created on %s by' % ( time.asctime() )]
      bkLines.append( '# by %s' % self.prodVersion )
      bkLines.append( 'from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient import BookkeepingClient' )
      bkLines.append( 'bkClient = BookkeepingClient()' )
      bkLines.append( 'bkDict = %s' % bkDict )
      bkLines.append( 'print bkClient.addProduction(bkDict)' )
      fopen.write( string.join( bkLines, '\n' ) + '\n' )
      fopen.close()
      return S_OK( bkName )
    self.LHCbJob.log.verbose( 'Attempting to publish production %s to the BK' % ( prodID ) )
    result = self.BKKClient.addProduction( bkDict )
    if not result['OK']:
      self.LHCbJob.log.error( result )
    return result

  #############################################################################
  def getOutputLFNs( self, prodID = None, prodJobID = None, prodXMLFile = '' ):
    """ Will construct the output LFNs for the production for visual inspection.
    """
    #TODO: fix this construction: really necessary?

    if not prodXMLFile:
      self.LHCbJob.log.verbose( 'Using workflow object to generate XML file' )
      prodXMLFile = self.createWorkflow()
    if not prodID:
      prodID = self.defaultProdID
    if not prodJobID:
      prodJobID = self.defaultProdJobID

    from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
    job = LHCbJob( prodXMLFile )
    result = preSubmissionLFNs( job._getParameters(), job.createCode(),
                               productionID = prodID, jobID = prodJobID )
    if not result['OK']:
      return result
    lfns = result['Value']
    self.LHCbJob.log.verbose( lfns )
    return result

  #############################################################################

  def setProdParameter( self, prodID, pname, pvalue ):
    """Set a production parameter.
    """

    if type( pvalue ) == type( [] ):
      pvalue = string.join( pvalue, '\n' )

    if type( pvalue ) == type( 2 ):
      pvalue = str( pvalue )
    result = self.transClient.setTransformationParameter( int( prodID ), str( pname ), str( pvalue ) )
    if not result['OK']:
      self.LHCbJob.log.error( 'Problem setting parameter %s for production %s and value: %s' % ( pname, prodID, pvalue ) )
    return result

  #############################################################################

  def getParameters( self, prodID, pname = '', printOutput = False ):
    """Get a production parameter or all of them if no parameter name specified.
    """

    result = self.transClient.getTransformation( int( prodID ), True )
    if not result['OK']:
      self.LHCbJob.log.error( result )
      return S_ERROR( 'Could not retrieve parameters for production %s' % prodID )

    if not result['Value']:
      self.LHCbJob.log.info( result )
      return S_ERROR( 'No additional parameters available for production %s' % prodID )

    if pname:
      if result['Value'].has_key( pname ):
        return S_OK( result['Value'][pname] )
      else:
        self.LHCbJob.log.verbose( result )
        return S_ERROR( 'Production %s does not have parameter %s' % ( prodID, pname ) )

    if printOutput:
      for n, v in result['Value'].items():
        if not n.lower() == 'body':
          print '=' * len( n ), '\n', n, '\n', '='*len( n )
          print v
        else:
          print '*Omitted Body from printout*'

    return result

  #############################################################################
  def setAlignmentDBLFN( self, lfn ):
    """ Set the input LFN to be used for the alignment conditions database'
    """
    if not re.search( "LFN:", lfn ):
      lfn = "LFN:%s" % lfn
    self.LHCbJob.log.info( 'Setting alignment DB LFN to %s' % lfn )
    self._setParameter( 'InputSandbox', 'JDL', lfn, 'AlignmentDB' )

  #############################################################################
  def setWorkflowLib( self, tag ):
    """Set workflow lib version for the production.
    """
    if not tag:
      tag = gConfig.getValue( '%s/WorkflowLibVersion' % ( self.csSection ), 'v9r9' )
      self.LHCbJob.log.verbose( 'Setting workflow library tag to %s' % tag )

    lfn = 'LFN:/lhcb/applications/WorkflowLib-wkf-TAG.tar.gz'.replace( 'TAG', tag )
    self.LHCbJob.log.info( 'Setting workflow library LFN to %s' % lfn )
    self._setParameter( 'InputSandbox', 'JDL', lfn, 'WorkflowLibVersion' )

  #############################################################################
  def setFileMask( self, fileMask, stepMask = '' ):
    """Output data related parameters.
    """
    if type( fileMask ) == type( [] ):
      fileMask = string.join( fileMask, ';' )
    self._setParameter( 'outputDataFileMask', 'string', fileMask, 'outputDataFileMask' )

    if stepMask:
      if type( stepMask ) == type( [] ):
        stepMask = string.join( stepMask, ';' )
    self.LHCbJob._addParameter( self.LHCbJob.workflow, 'outputDataStep', 'string', stepMask, 'outputDataStep Mask' )

  #############################################################################
  def setWorkflowName( self, name ):
    """Set workflow name.
    """
    self.LHCbJob.workflow.setName( name )
    self.name = name

  #############################################################################
  def setWorkflowDescription( self, desc ):
    """Set workflow dscription.
    """
    self.LHCbJob.workflow.setDescription( desc )

  #############################################################################
  def setProdType( self, prodType ):
    """Set prod type.
    """
    self.LHCbJob.setType( prodType )

  #############################################################################
  def banTier1s( self ):
    """ Sets Tier1s as banned.
    """
    tier1s = []
    from DIRAC.ResourceStatusSystem.Utilities.CS import getSites, getSiteTier
    for site in getSites()['Value']:
      if getSiteTier( site )['Value'] in ( ['0'], ['1'] ):
        tier1s.append( site )

    self.LHCbJob.setBannedSites( tier1s )

  #############################################################################
  def setTargetSite( self, site ):
    """ Sets destination for all jobs.
    """
    self.LHCbJob.setDestination( site )

  #############################################################################
  def setOutputMode( self, outputMode ):
    """ Sets output mode for all jobs, this can be 'Local' or 'Any'.
    """
    if not outputMode.lower().capitalize() in ( 'Local', 'Any' ):
      raise TypeError, 'Output mode must be Local or Any'
    self._setParameter( 'outputMode', 'string', outputMode.lower().capitalize(), 'SEResolutionPolicy' )

  #############################################################################
  def setBKParameters( self, configName, configVersion, groupDescription, conditions ):
    """ Sets BK parameters for production.
    """
    self._setParameter( 'configName', 'string', configName, 'ConfigName' )
    self._setParameter( 'configVersion', 'string', configVersion, 'ConfigVersion' )
    self._setParameter( 'groupDescription', 'string', groupDescription, 'GroupDescription' )
    self._setParameter( 'conditions', 'string', conditions, 'SimOrDataTakingCondsString' )
    self._setParameter( 'simDescription', 'string', conditions, 'SimDescription' )

  #############################################################################
  def setDBTags( self, conditions = 'sim-20090112', detector = 'head-20090112' ):
    """ Sets destination for all jobs.
    """
    self._setParameter( 'CondDBTag', 'string', conditions.replace( ' ', '' ), 'CondDBTag' )
    self._setParameter( 'DDDBTag', 'string', detector.replace( ' ', '' ), 'DetDescTag' )

  #############################################################################
  def setParentRequest( self, parentID ):
    """ Sets the parent request ID for a production.
    """
    self._setParameter( 'TransformationFamily', 'string', str( parentID ).replace( ' ', '' ), 'ParentRequestID' )

  #############################################################################
  def setProdPriority( self, priority ):
    """ Sets destination for all jobs.
    """
    self._setParameter( 'Priority', 'JDL', str( priority ), 'UserPriority' )

  #############################################################################
  def setProdGroup( self, group ):
    """ Sets a user defined tag for the production as appears on the monitoring page
    """
    self.prodGroup = group
    self._setParameter( 'ProcessingType', 'JDL', str( group ), 'ProductionGroupOrType' )

  #############################################################################
  def setProdPlugin( self, plugin ):
    """ Sets the plugin to be used to creating the production jobs
    """
    self.plugin = plugin

  #############################################################################
  def setInputFileMask( self, fileMask ):
    """ Sets the input data selection when using file mask.
    """
    self.inputFileMask = fileMask

  #############################################################################
  def setInputBKSelection( self, bkQuery ):
    """ Sets the input data selection when using the bookkeeping.
    """
    self.inputBKSelection = bkQuery

  #############################################################################
  def setJobFileGroupSize( self, gb ):
    """ Sets the number of gb to be input to each job created (e.g. for merging productions)
        This parameter is used also as number of input files (e.g. for stripping/reco), so pay attention
    """
    self.jobFileGroupSize = gb

  #############################################################################
  def setAncestorProduction( self, prod ):
    """ Sets the original production from which this is to be derived
    """
    self.ancestorProduction = prod

  #############################################################################
  def setWorkflowString( self, wfString ):
    """ Uses the supplied string to create the workflow
    """
    self.LHCbJob.workflow = fromXMLString( wfString )
    self.name = self.LHCbJob.workflow.getName()

  #############################################################################
  def disableCPUCheck( self ):
    """ Uses the supplied string to create the workflow
    """
    self._setParameter( 'DisableCPUCheck', 'JDL', 'True', 'DisableWatchdogCPUCheck' )

  #############################################################################  
  def setSimulationEvents( self, eventsTotal, eventsPerTask ):
    """ In order to set the EventsPerTask and MaxNumberOfTasks parameters for 
        MC simulation productions.
    """
    if int( eventsTotal ) and int( eventsPerTask ):
      self._setParameter( 'EventsPerTask', 'string', str( eventsPerTask ), 'EventsPerTask' )
      #Add a 10% buffer
      maxJobs = int( 1.1 * int( eventsTotal ) / int( eventsPerTask ) )
      self._setParameter( 'MaxNumberOfTasks', 'string', str( maxJobs ), 'MaxNumberOfTasks' )
    else:
      self.LHCbJob.log.warn( 'Either EventsTotal "%s" or EventsPerTask "%s" were set to null' % ( eventsTotal, eventsPerTask ) )
