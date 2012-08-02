""" Module for creating, describing and managing production requests objects
"""

import itertools, copy
from DIRAC import gLogger
from LHCbDIRAC.Interfaces.API.Production import Production

class ProductionRequest( object ):
  """ Production request class - objects are usually created starting from a production request
  """
  #############################################################################

  def __init__( self, BKKClientIn = None, diracProdIn = None ):
    """ c'tor

        Some variables are defined here. A production request is made of:
        stepsList, productionsTypes, and various parameters of those productions
    """

    if BKKClientIn is None:
      from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
      self.BKKClient = BookkeepingClient()
    else:
      self.BKKClient = BKKClientIn

    if diracProdIn is None:
      from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction
      self.diracProduction = DiracProduction()
    else:
      self.diracProduction = diracProdIn

    self.logger = gLogger.getSubLogger( 'ProductionRequest' )

    self.requestID = 0
    self.appendName = '1'
    self.configName = 'test'
    self.configVersion = 'certification'
    self.eventType = ''
    self.events = -1
    self.sysConfig = ''
    self.generatorName = ''
    self.stepsList = []
    self.stepsListDict = []
    self.extraOptions = []
    self.prodsTypeList = []
    self.stepsInProds = [] #a list of lists
    self.bkQuery = {} #initial bk query
    self.removeInputsFlags = []
    self.outputSEs = []
    self.priorities = []
    self.CPUs = []
    self.inputs = [] # list of lists
    self.targets = []
    self.outputFileMasks = []
    self.groupSizes = []
    self.plugins = []
    self.prodGroup = ''
    self.previousProdID = 0
    self.publishFlag = True
    self.testFlag = False
    self.extend = 0
    self.dataTakingConditions = ''


  #############################################################################

  def resolveSteps( self ):
    """ Given a list of steps in strings, some of which might be missing,
        resolve it into a dictionary of steps
    """

    self.stepsList = __toIntList( self.stepsList )

    for stepID in self.stepsList:
      stepDict = self.BKKClient.getAvailableSteps( {'StepId':stepID} )
      if not stepDict['OK']:
        raise ValueError, stepDict['Message']
      else:
        stepDict = stepDict['Value']

      stepsListDictItem = {}
      for ( parameter, value ) in itertools.izip( stepDict['ParameterNames'],
                                                  stepDict['Records'][0] ):
        stepsListDictItem[parameter] = value

      s_in = self.BKKClient.getStepInputFiles( stepID )
      if not s_in['OK']:
        raise ValueError, s_in['Message']
      else:
        fileTypesList = [fileType[0].strip() for fileType in s_in['Value']['Records']]
        stepsListDictItem['fileTypesIn'] = fileTypesList

      s_out = self.BKKClient.getStepOutputFiles( stepID )
      if not s_out['OK']:
        raise ValueError, s_out['Message']
      else:
        fileTypesList = [fileType[0].strip() for fileType in s_out['Value']['Records']]
        stepsListDictItem['fileTypesOut'] = fileTypesList

      if stepsListDictItem['StepId'] in self.extraOptions:
        stepsListDictItem['ExtraOptions'] = self.extraOptions['StepId']

      self.stepsListDict.append( stepsListDictItem )

  #############################################################################

  def buildAndLaunchRequest( self ):
    """ uses _getProdsDescriptionDict, _buildProduction, and DiracProduction.launchProduction
    """
    if not self.stepsListDict:
      self.resolveSteps()

    stepsListDict = copy.deepcopy( self.stepsListDict )

    prodsDict = self._getProdsDescriptionDict()

    for prodIndex, prodDict in prodsDict.items():

      stepsInProd = []
      for stepID in prodDict['stepsInProd']:
        for step in stepsListDict:
          if step['StepId'] == stepID:
            stepsInProd.append( stepsListDict.pop( stepsListDict.index( step ) ) )


      prod = self._buildProduction( prodDict['productionType'], stepsInProd, self.extraOptions, prodDict['outputSE'],
                                    prodDict['priority'], prodDict['cpu'], prodDict['input'],
                                    outputFileMask = prodDict['outputFileMask'],
                                    target = prodDict['target'],
                                    removeInputData = prodDict['removeInputsFlag'],
                                    groupSize = prodDict['groupSize'],
                                    bkQuery = prodDict['bkQuery'],
                                    previousProdID = self.previousProdID )

      prodID = self.diracProduction.launchProduction( prod, self.publishFlag, self.testFlag,
                                                      self.requestID, self.extend, prodDict['tracking'] )

      self.logger.info( 'For request %s, submitted Production %d, of type %s, ID = %s' % ( str( self.requestID ),
                                                                                           prodIndex,
                                                                                           prodDict['productionType'],
                                                                                           str( prodID ) ) )

  #############################################################################

  def _getProdsDescriptionDict( self ):
    """ Returns a dictionary representing the description of the request
    """

    prodsDict = {}
    bkQuery = self.bkQuery

    if not self.removeInputsFlags:
      removeInputsFlags = []
      for prodType in self.prodsTypeList:
        if prodType.lower() == 'merge':
          removeInputsFlags.append( True )
        else:
          removeInputsFlags.append( False )
    else:
      removeInputsFlags = self.removeInputsFlags

    if not self.outputFileMasks:
      outputFileMasks = [''] * len( self.prodsTypeList )
    else:
      outputFileMasks = self.outputFileMasks

    if not self.inputs:
      inputD = [[]] * len( self.prodsTypeList )
    else:
      inputD = self.inputs

    if not self.targets:
      targets = [''] * len( self.prodsTypeList )
    else:
      targets = self.targets

    if not self.groupSizes:
      groupSizes = [1] * len( self.prodsTypeList )
    else:
      groupSizes = self.groupSizes

    prodNumber = 1

    for prodType, stepsInProd, removeInputsFlag, outputSE, priority, \
    cpu, inputD, outFileMask, target, groupSize, plugin in itertools.izip( self.prodsTypeList,
                                                                           self.stepsInProds,
                                                                           removeInputsFlags,
                                                                           self.outputSEs,
                                                                           self.priorities,
                                                                           self.CPUs,
                                                                           inputD,
                                                                           outputFileMasks,
                                                                           targets,
                                                                           groupSizes,
                                                                           self.plugins
                                                                           ):

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
                               'outputFileMask':outFileMask,
                               'target':target,
                               'groupSize': groupSize,
                               'plugin': plugin
                               }
      bkQuery = 'fromPreviousProd'
      prodNumber += 1

    #tracking the last production
    prodsDict[prodNumber - 1]['tracking'] = 1

    return prodsDict

  #############################################################################

  def _buildProduction( self, prodType, stepsInProd,
                        extraOptions, outputSE,
                        priority, cpu,
                        inputDataList = [],
                        outputMode = 'Any',
                        outputFileMask = '',
                        target = '',
                        removeInputData = False,
                        groupSize = 1,
                        bkQuery = None,
                        previousProdID = 0 ):
    """ Wrapper around Production API to build a production, given the needed parameters
    """
    prod = Production()

    #non optional parameters
    prod.LHCbJob.setType( prodType )
    prod.LHCbJob.workflow.setName( 'Request_%s_%s_%s_EventType_%s_%s' % ( self.requestID, prodType,
                                                                          self.prodGroup, self.eventType,
                                                                          self.appendName ) )
    prod.setBKParameters( self.configName, self.configVersion, self.prodGroup, self.dataTakingConditions )
    prod._setParameter( 'eventType', 'string', self.eventType, 'Event Type of the production' )
    prod._setParameter( 'numberOfEvents', 'string', str( self.events ), 'Number of events requested' )
    prod.prodGroup = self.prodGroup
    prod.priority = str( priority )
    prod.LHCbJob.workflow.setDescription( 'prodDescription' )
    prod.setJobParameters( { 'CPUTime': cpu } )
    prod._setParameter( 'generatorName', 'string', str( self.generatorName ), 'Generator Name' )

    #optional parameters
    prod.jobFileGroupSize = groupSize
    if self.sysConfig:
      prod.setJobParameters( { 'SystemConfig': self.sysConfig } )
    prod.setOutputMode( outputMode )
    if outputFileMask:
      maskList = [m.lower() for m in outputFileMask.replace( ' ', '' ).split( ',' )]
      outputFileMask = ';'.join( maskList )
      prod.setFileMask( outputFileMask )
    if target:
      if target == 'Tier2':
        prod.banTier1s()
      elif target != 'ALL':
        prod.LHCbJob.setDestination( target )
    if inputDataList:
      prod.LHCbJob.setInputData( inputDataList )

    #Adding optional input BK query
    if bkQuery == 'fromPreviousProd':
      BKQuery = {
                 'FileType': stepsInProd[0]['fileTypesIn'][0].upper(),
                 'EventType': self.eventType,
                 'ProductionID': int( previousProdID )
                 }
      prod.inputBKSelection = BKQuery

    #Adding the application steps
    firstStep = stepsInProd.pop( 0 )
    try:
      ep = extraOptions[firstStep['StepId']]
    except IndexError:
      ep = ''
    prod.addApplicationStep( stepDict = firstStep,
                             outputSE = outputSE,
                             optionsLine = ep,
                             inputData = '' )
    for step in stepsInProd:
      try:
        ep = extraOptions[step['StepId']]
      except IndexError:
        ep = ''
      prod.addApplicationStep( stepDict = step,
                               outputSE = outputSE,
                               optionsLine = ep,
                               inputData = 'previousStep' )

    #Adding the finalization step
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

def __toIntList( self, stringsList ):

  listInt = []
  i = 0
  while True:
    try:
      listInt.append( int( stringsList[i] ) )
      i = i + 1
    except ValueError:
      break
    except IndexError:
      break
  return listInt

#############################################################################

def _splitIntoProductionSteps( stepsList ):
  """ Given a list of bookkeeping steps, produce production steps
  """
  prodSteps = []

  for step in stepsList:
    if len( step['fileTypesIn'] ) <= 1:
      prodSteps.append( step )
    else:
      if set( step['fileTypesOut'] ) > set( step['fileTypesIn'] ):
        raise ValueError, "Step outputs %s are not part of the inputs %s...?" % ( str( step['fileTypesOut'] ),
                                                                                  str( step['fileTypesIn'] ) )
      for outputTypes in step['fileTypesOut']:
        prodStep = copy.deepcopy( step )
        prodStep['fileTypesIn'] = [outputTypes]
        prodStep['fileTypesOut'] = [outputTypes]
        prodSteps.append( prodStep )

  return prodSteps

#############################################################################
