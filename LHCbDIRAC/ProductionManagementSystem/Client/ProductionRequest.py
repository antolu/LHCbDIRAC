""" Module for creating, describing and managing production requests objects
"""

import itertools, copy
from DIRAC import gLogger, S_OK

from DIRAC.Core.DISET.RPCClient                               import RPCClient
from DIRAC.ConfigurationSystem.Client.Helpers.Operations      import Operations

from LHCbDIRAC.Interfaces.API.DiracProduction                 import DiracProduction
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient     import BookkeepingClient
from LHCbDIRAC.ProductionManagementSystem.Client.Production   import Production

from LHCbDIRAC.Workflow.Modules.ModulesUtilities              import getEventsToProduce, getCPUNormalizationFactorAvg

class ProductionRequest( object ):
  """ Production request class - objects are usually created starting from a production request
  """
  #############################################################################

  def __init__( self, bkkClientIn = None, diracProdIn = None ):
    """ c'tor

        Some variables are defined here. A production request is made of:
        stepsList, productionsTypes, and various parameters of those productions
    """

    if bkkClientIn is None:
      self.bkkClient = BookkeepingClient()
    else:
      self.bkkClient = bkkClientIn

    if diracProdIn is None:
      self.diracProduction = DiracProduction()
    else:
      self.diracProduction = diracProdIn

    self.logger = gLogger.getSubLogger( 'ProductionRequest' )

    # parameters of the request
    self.requestID = 0
    self.parentRequestID = 0
    self.appendName = '1'
    self.outConfigName = ''
    self.prodsToLaunch = []  # productions to launch
    self.stepsListDict = []  # list of dict of steps
    self.stepsInProds = []  # a list of lists
    # parameters of the input data
    self.processingPass = ''
    self.dataTakingConditions = ''
    self.eventType = ''
    self.bkFileType = []
    self.dqFlag = ''
    self.startRun = 1
    self.endRun = 2
    self.runsList = ''
    self.configName = 'test'
    self.configVersion = 'certification'
    # parameters of the first production
    self.publishFlag = True
    self.testFlag = False
    self.derivedProduction = 0
    self.previousProdID = 0  # optional prod from which to start

    # parameters that are the same for each productions
    self.prodGroup = ''
    self.visibility = ''
    self.fractionToProcess = 0
    self.minFilesToProcess = 0
    self.modulesList = ['GaudiApplication', 'AnalyseLogFile', 'AnalyseXMLSummary',
                        'ErrorLogging', 'BookkeepingReport', 'StepAccounting' ]
    # used to compute the maximum number of events to produce
    # default values
    self.CPUTimeAvg = 200000
    self.CPUNormalizationFactorAvg = 1.0

    # parameters of each production (the length of each list has to be the same as the number of productions
    self.events = []
    self.CPUeList = []
    self.stepsList = []
    self.extraOptions = {}
    self.prodsTypeList = []
    self.bkQueries = []  # list of bk queries
    self.removeInputsFlags = []
    self.outputSEs = []
    self.priorities = []
    self.cpus = []
    self.inputs = []  # list of lists
    self.outputModes = []
    self.targets = []
    self.outputFileMasks = []
    self.outputFileSteps = []
    self.groupSizes = []
    self.plugins = []
    self.inputDataPolicies = []
    self.previousProds = [None]  # list of productions from which to take the inputs (the first is always None)
    self.multicore = []  # list of flags to override the multi core flags of the steps

  #############################################################################

  def resolveSteps( self ):
    """ Given a list of steps in strings, some of which might be missing,
        resolve it into a list of dictionary of steps
    """
    for stepID in self.stepsList:
      stepDict = self.bkkClient.getAvailableSteps( {'StepId':stepID} )
      if not stepDict['OK']:
        raise ValueError( stepDict['Message'] )
      else:
        stepDict = stepDict['Value']

      stepsListDictItem = {}
      for parameter, value in itertools.izip( stepDict['ParameterNames'],
                                              stepDict['Records'][0] ):
        if parameter.lower() in ['conddb', 'dddb', 'dqtag'] and value:
          if value.lower() == 'frompreviousstep':
            value = self.stepsListDict[-1][parameter]
        stepsListDictItem[parameter] = value

      s_in = self.bkkClient.getStepInputFiles( stepID )
      if not s_in['OK']:
        raise ValueError( s_in['Message'] )
      else:
        fileTypesList = [fileType[0].strip() for fileType in s_in['Value']['Records']]
        stepsListDictItem['fileTypesIn'] = fileTypesList

      s_out = self.bkkClient.getStepOutputFiles( stepID )
      if not s_out['OK']:
        raise ValueError( s_out['Message'] )
      else:
        fileTypesList = [fileType[0].strip() for fileType in s_out['Value']['Records']]
        stepsListDictItem['fileTypesOut'] = fileTypesList

      if stepsListDictItem['StepId'] in self.extraOptions:
        stepsListDictItem['ExtraOptions'] = self.extraOptions['StepId']

      stepsListDictItem['prodStepID'] = str( stepID ) + str( stepsListDictItem['fileTypesIn'] )

      if not stepsListDictItem.has_key( 'isMulticore' ):
        stepsListDictItem['isMulticore'] = 'N'

      if not stepsListDictItem.has_key( 'SystemConfig' ):
        stepsListDictItem['SystemConfig'] = ''

      if not stepsListDictItem.has_key( 'mcTCK' ):
        stepsListDictItem['mcTCK'] = ''

      self.stepsListDict.append( stepsListDictItem )

  #############################################################################

  def buildAndLaunchRequest( self ):
    """ uses _applyOptionalCorrections, _getProdsDescriptionDict,
        _buildProduction, and DiracProduction.launchProduction
    """

    if not self.stepsListDict:
      self.resolveSteps()

    self._applyOptionalCorrections()

    prodsDict = self._getProdsDescriptionDict()

    stepsListDict = list( self.stepsListDict )

    fromProd = self.previousProdID
    prodsLaunched = []

    self.logger.debug( prodsDict )
    # now we build and launch each productions
    for prodIndex, prodDict in prodsDict.items():

      if self.prodsToLaunch:
        if prodIndex not in self.prodsToLaunch:
          continue

      # build the list of steps in a production
      stepsInProd = []
      for stepID in prodDict['stepsInProd-ProdName']:
        for step in stepsListDict:
          if step['prodStepID'] == stepID:
            stepsInProd.append( stepsListDict.pop( stepsListDict.index( step ) ) )

      if prodDict['previousProd'] is not None:
        fromProd = prodsLaunched[prodDict['previousProd'] - 1 ]
        self.previousProdID = fromProd

      prod = self._buildProduction( prodType = prodDict['productionType'],
                                    stepsInProd = stepsInProd,
                                    extraOptions = self.extraOptions,
                                    outputSE = prodDict['outputSE'],
                                    priority = prodDict['priority'],
                                    cpu = prodDict['cpu'],
                                    inputDataList = prodDict['input'],
                                    outputMode = prodDict['outputMode'],
                                    inputDataPolicy = prodDict['inputDataPolicy'],
                                    outputFileMask = prodDict['outputFileMask'],
                                    outputFileStep = prodDict['outputFileStep'],
                                    target = prodDict['target'],
                                    removeInputData = prodDict['removeInputsFlag'],
                                    groupSize = prodDict['groupSize'],
                                    bkQuery = prodDict['bkQuery'],
                                    plugin = prodDict['plugin'],
                                    previousProdID = fromProd,
                                    derivedProdID = prodDict['derivedProduction'],
                                    transformationFamily = prodDict['transformationFamily'],
                                    events = prodDict['events'],
                                    CPUe = prodDict['CPUe'],
                                    multicore = prodDict['multicore'] )

      # check if we have to extend on multiple jobs
      if prodDict['productionType'] in ['Simulation', 'MCSimulation']:
        max_e = getEventsToProduce( prodDict['CPUe'], 1000000, 1.0 )
        if max_e == 0:
          extend = 0
        else:
          # getting the events to produce
          rpcProductionRequest = RPCClient( 'ProductionManagement/ProductionRequest' )
          res = rpcProductionRequest.getProductionRequestSummary( 'Accepted', 'Simulation' )
          if not res['OK']:
            return res
          eventsToProduceForRequest = res['Value'][self.requestID]['reqTotal']
          extend = int( eventsToProduceForRequest / max_e )
      else:
        extend = 0

      res = self.diracProduction.launchProduction( prod = prod,
                                                   publishFlag = self.publishFlag,
                                                   testFlag = self.testFlag,
                                                   requestID = self.requestID,
                                                   extend = extend,
                                                   tracking = prodDict['tracking'] )
      if not res['OK']:
        raise RuntimeError( res['Message'] )

      prodID = res['Value']
      prodsLaunched.append( prodID )

      if self.publishFlag:
        self.logger.notice( 'For request %d, submitted Production %d, of type %s, ID = %s' % ( self.requestID,
                                                                                             prodIndex,
                                                                                             prodDict['productionType'],
                                                                                             str( prodID ) ) )
    return S_OK( prodsLaunched )

  #############################################################################

  def _applyOptionalCorrections( self ):
    """ if needed, calls _splitIntoProductionSteps. It also applies other changes
    """
    if len( self.bkQueries ) != len( self.prodsTypeList ):
      self.bkQueries += ['fromPreviousProd'] * ( len( self.prodsTypeList ) - len( self.bkQueries ) )

    if len( self.previousProds ) != len( self.prodsTypeList ):
      self.previousProds += range( 1, len( self.prodsTypeList ) )

    if len( self.events ) != len( self.prodsTypeList ):
      self.events += ['-1'] * ( len( self.prodsTypeList ) - len( self.events ) )

    if len( self.CPUeList ) != len( self.prodsTypeList ):
      self.CPUeList += [1] * ( len( self.prodsTypeList ) - len( self.CPUeList ) )

    if not self.removeInputsFlags:
      removeInputsFlags = []
      for prodType in self.prodsTypeList:
        if prodType.lower() == 'merge':
          removeInputsFlags.append( True )
        else:
          removeInputsFlags.append( False )
      self.removeInputsFlags = removeInputsFlags

    if not self.outputFileMasks:
      self.outputFileMasks = [''] * len( self.prodsTypeList )

    if not self.outputFileSteps:
      self.outputFileSteps = [''] * len( self.prodsTypeList )

    if not self.inputs:
      self.inputs = [[]] * len( self.prodsTypeList )

    if not self.outputModes:
      self.outputModes = ['Any'] * len( self.prodsTypeList )

    if not self.targets:
      self.targets = [''] * len( self.prodsTypeList )

    if not self.inputDataPolicies:
      self.inputDataPolicies = ['download'] * len( self.prodsTypeList )

    if not self.multicore:
      self.multicore = ['True'] * len( self.prodsTypeList )

    # Checking if we need to split the merging step into many productions
    if 'merge' in [pt.lower() for pt in self.prodsTypeList]:
      i = 0
      indexes = []
      for pt in self.prodsTypeList:
        if pt.lower() == 'merge':
          indexes.append( i )
        i += 1

      for index in indexes:
        # In this case and only in this case I have to split the merging in many productions
        plugin = self.plugins[index]
        outputSE = self.outputSEs[index]
        priority = self.priorities[index]
        cpu = self.cpus[index]
        bkQuery = self.bkQueries[index]
        groupSize = self.groupSizes[index]
        preProd = self.previousProds[index]
        removeInputsFlag = self.removeInputsFlags[index]
        outputFileMask = self.outputFileMasks[index]
        outputFileStep = self.outputFileSteps[index]
        inputs = self.inputs[index]
        idp = self.inputDataPolicies[index]
        stepID = self.stepsList[index]
        events = self.events[index]
        CPUe = self.CPUeList[index]
        targets = self.targets[index]
        multicore = self.multicore[index]
        outputMode = self.outputModes[index]
        if plugin.lower() != 'byrunfiletypesizewithflush':
          stepToSplit = self.stepsListDict[index]
          numberOfProdsToInsert = len( stepToSplit['fileTypesOut'] )
          self.prodsTypeList.remove( 'Merge' )
          self.plugins.pop( index )
          self.outputSEs.pop( index )
          self.priorities.pop( index )
          self.cpus.pop( index )
          self.bkQueries.pop( index )
          self.previousProds.pop( index )
          self.groupSizes.pop( index )
          self.removeInputsFlags.pop( index )
          self.outputFileMasks.pop( index )
          self.outputFileSteps.pop( index )
          self.inputs.pop( index )
          self.inputDataPolicies.pop( index )
          self.stepsList.pop( index )
          self.events.pop( index )
          self.CPUeList.pop( index )
          self.targets.pop( index )
          self.multicore.pop( index )
          self.outputModes.pop( index )
          newSteps = _splitIntoProductionSteps( stepToSplit )
          newSteps.reverse()
          self.stepsListDict.remove( stepToSplit )
          last = self.stepsInProds.pop( index )[0]
          for x in range( numberOfProdsToInsert ):
            self.prodsTypeList.insert( index, 'Merge' )
            self.plugins.insert( index, plugin )
            self.outputSEs.insert( index, outputSE )
            self.priorities.insert( index, priority )
            self.cpus.insert( index, cpu )
            self.bkQueries.insert( index, bkQuery )
            self.groupSizes.insert( index, groupSize )
            self.removeInputsFlags.insert( index, removeInputsFlag )
            self.outputFileMasks.insert( index, outputFileMask )
            self.outputFileSteps.insert( index, outputFileStep )
            self.inputs.insert( index, inputs )
            self.inputDataPolicies.insert( index, idp )
            self.stepsList.insert( index, stepID )
            self.previousProds.insert( index, preProd )
            self.stepsListDict.insert( index, newSteps[x] )
            self.stepsInProds.insert( index + x, [last + x] )
            self.events.insert( index, events )
            self.CPUeList.insert( index, CPUe )
            self.targets.insert( index, targets )
            self.multicore.insert( index, multicore )
            self.outputModes.insert( index, outputMode )

    correctedStepsInProds = []
    toInsert = self.stepsInProds[0][0]
    lengths = [len( x ) for x in self.stepsInProds]
    for l in lengths:
      li = [toInsert + x for x in range( l )]
      toInsert += l
      correctedStepsInProds.append( li )

    self.stepsInProds = correctedStepsInProds

  #############################################################################

  def _getProdsDescriptionDict( self ):
    """ Returns a dictionary representing the description of the request (of all the productions in it)
    """

    prodsDict = {}

    prodNumber = 1

    for prodType, stepsInProd, bkQuery, removeInputsFlag, outputSE, priority, \
    cpu, inputD, outputMode, outFileMask, outFileStep, target, groupSize, plugin, idp, \
    previousProd, events, CPUe, multicore in itertools.izip( self.prodsTypeList,
                                                             self.stepsInProds,
                                                             self.bkQueries,
                                                             self.removeInputsFlags,
                                                             self.outputSEs,
                                                             self.priorities,
                                                             self.cpus,
                                                             self.inputs,
                                                             self.outputModes,
                                                             self.outputFileMasks,
                                                             self.outputFileSteps,
                                                             self.targets,
                                                             self.groupSizes,
                                                             self.plugins,
                                                             self.inputDataPolicies,
                                                             self.previousProds,
                                                             self.events,
                                                             self.CPUeList,
                                                             self.multicore
                                                             ):

      if not self.parentRequestID and self.requestID:
        transformationFamily = self.requestID
      else:
        transformationFamily = self.parentRequestID

      prodsDict[ prodNumber ] = {
                                 'productionType': prodType,
                                 'stepsInProd': [self.stepsList[index - 1] for index in stepsInProd],
                                 'bkQuery': bkQuery,
                                 'removeInputsFlag': removeInputsFlag,
                                 'tracking':0,
                                 'outputSE':outputSE,
                                 'priority': priority,
                                 'cpu': cpu,
                                 'input': inputD,
                                 'outputMode': outputMode,
                                 'outputFileMask':outFileMask,
                                 'outputFileStep':outFileStep,
                                 'target':target,
                                 'groupSize': groupSize,
                                 'plugin': plugin,
                                 'inputDataPolicy': idp,
                                 'derivedProduction': 0,
                                 'transformationFamily': transformationFamily,
                                 'previousProd': previousProd,
                                 'stepsInProd-ProdName': [str( self.stepsList[index - 1] ) + str( self.stepsListDict[index - 1]['fileTypesIn'] ) for index in stepsInProd],
                                 'events': events,
                                 'CPUe' : CPUe,
                                 'multicore': multicore
                                 }
      prodNumber += 1

    # tracking the last production(s)
    prodsDict[prodNumber - 1]['tracking'] = 1
    typeOfLastProd = prodsDict[prodNumber - 1]['productionType']
    index = 2
    try:
      while prodsDict[prodNumber - index]['productionType'] == typeOfLastProd:
        prodsDict[prodNumber - index]['tracking'] = 1
        index += 1
    except KeyError:
      pass

    # production derivation, if necessary
    if self.derivedProduction:
      prodsDict[1]['derivedProduction'] = self.derivedProduction

    return prodsDict

  #############################################################################

  def _buildProduction( self, prodType, stepsInProd,
                        extraOptions, outputSE,
                        priority, cpu,
                        inputDataList = [],
                        outputMode = 'Any',
                        inputDataPolicy = 'download',
                        outputFileMask = '',
                        outputFileStep = '',
                        target = '',
                        removeInputData = False,
                        groupSize = 1,
                        bkQuery = None,
                        plugin = '',
                        previousProdID = 0,
                        derivedProdID = 0,
                        transformationFamily = 0,
                        events = -1,
                        CPUe = 100,
                        multicore = 'True' ):
    """ Wrapper around Production API to build a production, given the needed parameters
        Returns a production object
    """
    prod = Production()

    # non optional parameters
    prod.LHCbJob.setType( prodType )
    try:
      fTypeIn = [ft.upper() for ft in stepsInProd[0]['fileTypesIn']]
    except IndexError:
      fTypeIn = []
    prod.LHCbJob.workflow.setName( 'Request_%d_%s_%s_EventType_%s_%s_%s' % ( self.requestID, prodType,
                                                                             self.prodGroup, self.eventType,
                                                                        ''.join( [x.split( '.' )[0] for x in fTypeIn] ),
                                                                             self.appendName ) )
    prod.setBKParameters( configName = self.outConfigName, configVersion = self.configVersion,
                          groupDescription = self.prodGroup, conditions = self.dataTakingConditions )
    prod.setParameter( 'eventType', 'string', self.eventType, 'Event Type of the production' )
    prod.setParameter( 'numberOfEvents', 'string', str( events ), 'Number of events requested' )
    prod.setParameter( 'CPUe', 'string', str( CPUe ), 'CPU time per event' )

    # maximum number of events to produce
    # try to get the CPU parameters from the configuration if possible
    op = Operations()
    CPUTimeAvg = op.getValue( 'Transformations/CPUTimeAvg' )
    if CPUTimeAvg is None:
      self.logger.info( 'Could not get CPUTimeAvg from config, defaulting to %d' % self.CPUTimeAvg )
    else:
      self.CPUTimeAvg = CPUTimeAvg

    try:
      self.CPUNormalizationFactorAvg = getCPUNormalizationFactorAvg()
    except RuntimeError:
      self.logger.info( 'Could not get CPUNormalizationFactorAvg, defaulting to %d' % self.CPUNormalizationFactorAvg )

    max_e = getEventsToProduce( CPUe, self.CPUTimeAvg, self.CPUNormalizationFactorAvg )
    prod.setParameter( 'maxNumberOfEvents', 'string', str( max_e ), 'Maximum number of events to produce (Gauss only)' )

    prod.setParameter( 'multicore', 'string', multicore, 'Flag for enabling gaudi parallel' )
    prod.prodGroup = self.prodGroup
    prod.priority = priority
    prod.LHCbJob.workflow.setDescrShort( 'prodDescription' )
    prod.LHCbJob.workflow.setDescription( 'prodDescription' )
    prod.setJobParameters( { 'CPUTime': cpu } )
    prod.plugin = plugin

    # optional parameters
    prod.jobFileGroupSize = groupSize
    if inputDataPolicy:
      prod.LHCbJob.setInputDataPolicy( inputDataPolicy )
    prod.setOutputMode( outputMode )
    if outputFileMask:
      outputFileMask = [m.lower() for m in outputFileMask.replace( ' ', '' ).split( ',' )]
    if outputFileStep:
      if type( outputFileStep ) == type( '' ):
        outputFileStep = [m.lower() for m in outputFileStep.replace( ' ', '' ).split( ',' )]
    prod.setFileMask( outputFileMask, outputFileStep )
    if target:
      if target == 'Tier2':
        prod.banTier1s()
      elif target != 'ALL':
        prod.LHCbJob.setDestination( target )
    if inputDataList:
      prod.LHCbJob.setInputData( inputDataList )
    if derivedProdID:
      prod.ancestorProduction = derivedProdID
    if transformationFamily:
      prod.transformationFamily = transformationFamily
    if self.fractionToProcess:
      prod.setParameter( 'FractionToProcess', 'string', str( self.fractionToProcess ), 'Fraction to process' )
    if self.minFilesToProcess:
      prod.setParameter( 'MinFilesToProcess', 'string', str( self.minFilesToProcess ), 'Min N of Files to process' )
    if self.processingPass:
      prod.setParameter( 'processingPass', 'string', self.processingPass, 'Processing pass of input for the request' )

    # Adding optional input BK query
    if bkQuery:
      if bkQuery.lower() == 'full':
        prod.inputBKSelection = self._getBKKQuery()
      elif bkQuery.lower() == 'frompreviousprod':
        prod.inputBKSelection = self._getBKKQuery( 'frompreviousprod', fTypeIn, previousProdID )

    self.logger.verbose( 'Launching with BK selection %s' % prod.inputBKSelection )

    # Adding the application steps
    firstStep = stepsInProd.pop( 0 )
    try:
      ep = extraOptions[firstStep['StepId']]
    except IndexError:
      ep = ''
    except KeyError:
      ep = ''
    stepName = prod.addApplicationStep( stepDict = firstStep,
                                        outputSE = outputSE,
                                        optionsLine = ep,
                                        inputData = '',
                                        modules = self.modulesList )
    prod.gaudiSteps.append( stepName )

    for step in stepsInProd:
      try:
        ep = extraOptions[step['StepId']]
      except IndexError:
        ep = ''
      except KeyError:
        ep = ''
      stepName = prod.addApplicationStep( stepDict = step,
                                          outputSE = outputSE,
                                          optionsLine = ep,
                                          inputData = 'previousStep',
                                          modules = self.modulesList )
      prod.gaudiSteps.append( stepName )

    # Adding the finalization step
    if removeInputData:
      prod.addFinalizationStep( ['UploadOutputData',
                                 'RemoveInputData',
                                 'UploadLogFile',
                                 'FailoverRequest'] )
    else:
      prod.addFinalizationStep( ['UploadOutputData',
                                 'UploadLogFile',
                                 'FailoverRequest'] )

    return prod

  #############################################################################

  def _getBKKQuery( self, mode = 'full', fileType = [], previousProdID = 0 ):
    """ simply creates the bkk query dictionary
    """

    if mode.lower() == 'full':
      bkQuery = {
                 'FileType'                 : ';;;'.join( self.bkFileType ),
                 'EventType'                : str( self.eventType ),
                 'ConfigName'               : self.configName,
                 'ConfigVersion'            : self.configVersion,
                 }

      if self.dataTakingConditions:
        bkQuery['DataTakingConditions'] = self.dataTakingConditions

      if self.processingPass:
        bkQuery['ProcessingPass'] = self.processingPass

      if self.dqFlag:
        bkQuery['DataQualityFlag'] = self.dqFlag.replace( ',', ';;;' ).replace( ' ', '' )

      if self.startRun and self.runsList or self.endRun and self.runsList:
        raise ValueError( "Please don't mix runs list with start/end run" )

      if self.endRun and self.startRun:
        if self.endRun < self.startRun:
          gLogger.error( "Your end run '%d' should be more than your start run '%d'!" % ( self.endRun, self.startRun ) )
          raise ValueError( "Error setting start or end run" )

      if self.startRun:
        bkQuery['StartRun'] = self.startRun
      if self.endRun:
        bkQuery['EndRun'] = self.endRun

      if self.runsList:
        bkQuery['RunNumbers'] = self.runsList.replace( ',', ';;;' ).replace( ' ', '' )

      if self.visibility:
        bkQuery['Visible'] = self.visibility


    elif mode.lower() == 'frompreviousprod':
      bkQuery = {
                 'FileType': ';;;'.join( fileType ).replace( ' ', '' ),
                 'ProductionID': int( previousProdID )
                 }

      if self.eventType:
        bkQuery['EventType'] = str( self.eventType )

      if self.dqFlag:
        bkQuery['DataQualityFlag'] = self.dqFlag.replace( ',', ';;;' ).replace( ' ', '' )

    return bkQuery

  ################################################################################
  # properties

  def set_stepsList( self, value ):
    listInt = []
    i = 0
    while True:
      try:
        listInt.append( int( value[i] ) )
        i = i + 1
      except ValueError:
        break
      except IndexError:
        break
    self._stepsList = listInt
  def get_stepsList( self ):
    return self._stepsList
  stepsList = property( get_stepsList, set_stepsList )

  def set_startRun( self, value ):
    if type( value ) == type( '' ):
      value = int( value )
    if value < 0:
      raise ValueError( "startRun can not be negative" )
    self._startRun = value
  def get_startRun( self ):
    return self._startRun
  startRun = property( get_startRun, set_startRun )

  def set_endRun( self, value ):
    if type( value ) == type( '' ):
      value = int( value )
    if value < 0:
      raise ValueError( "endRun can not be negative" )
    self._endRun = value
  def get_endRun( self ):
    return self._endRun
  endRun = property( get_endRun, set_endRun )

  def set_requestID( self, value ):
    if value == '':
      value = 0
    if type( value ) == type( '' ):
      value = int( value )
    if value < 0:
      raise ValueError( "requestID can not be negative" )
    self._requestID = value
  def get_requestID( self ):
    return self._requestID
  requestID = property( get_requestID, set_requestID )

  def set_parentRequestID( self, value ):
    if value == '':
      value = 0
    if type( value ) == type( '' ):
      value = int( value )
    if value < 0:
      raise ValueError( "parentRequestID can not be negative" )
    self._parentRequestID = value
  def get_parentRequestID( self ):
    return self._parentRequestID
  parentRequestID = property( get_parentRequestID, set_parentRequestID )

  def set_CPUeList( self, value ):
    if type( value ) != type( [] ):
      value = [value]
    value = [int( float( v ) ) for v in value]
    for x in value:
      if x < 0:
        raise ValueError( "CPUe can not be negative" )
    self._CPUeList = value
  def get_CPUeList( self ):
    return self._CPUeList
  CPUeList = property( get_CPUeList, set_CPUeList )

  def set_CPUTimeAvg( self, value ):
    if type( value ) == type( '' ):
      value = int( value )
    if value < 0:
      raise ValueError( "CPUTimeAvg can not be negative" )
    self._CPUTimeAvg = value
  def get_CPUTimeAvg( self ):
    return self._CPUTimeAvg
  CPUTimeAvg = property( get_CPUTimeAvg, set_CPUTimeAvg )

  def set_CPUNormalizationFactorAvg( self, value ):
    if type( value ) == type( '' ):
      value = float( value )
    if value < 0.0:
      raise ValueError( "CPUNormalizationFactorAvg can not be negative" )
    self._CPUNormalizationFactorAvg = value
  def get_CPUNormalizationFactorAvg( self ):
    return self._CPUNormalizationFactorAvg
  CPUNormalizationFactorAvg = property( get_CPUNormalizationFactorAvg, set_CPUNormalizationFactorAvg )

  def set_bkFileType( self, value ):
    if type( value ) == type( '' ):
      value = value.replace( ' ', '' ).split( ',' )
    self._bkFileType = value
  def get_bkFileType( self ):
    return self._bkFileType
  bkFileType = property( get_bkFileType, set_bkFileType )

#############################################################################

def _splitIntoProductionSteps( step ):
  """ Given a list of bookkeeping steps, produce production steps
  """
  prodSteps = []

  if len( step['fileTypesIn'] ) <= 1:
    prodSteps.append( step )
  else:
    if set( step['fileTypesOut'] ) > set( step['fileTypesIn'] ):
      raise ValueError( "Step outputs %s are not part of the inputs %s...?" % ( str( step['fileTypesOut'] ),
                                                                                str( step['fileTypesIn'] ) ) )
    for outputTypes in step['fileTypesOut']:
      prodStep = copy.deepcopy( step )
      prodStep['fileTypesIn'] = [outputTypes]
      prodStep['fileTypesOut'] = [outputTypes]
      prodStep['prodStepID'] = str( prodStep['StepId'] ) + str( prodStep['fileTypesIn'] )
      prodSteps.append( prodStep )

  return prodSteps

#############################################################################
