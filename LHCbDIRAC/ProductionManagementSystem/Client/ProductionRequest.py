""" Module for creating, describing and managing production requests objects
"""

__RCSID__ = "$Id$"

import itertools, copy
from DIRAC import gLogger
from LHCbDIRAC.Interfaces.API.Production import Production

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
      from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
      self.bkkClient = BookkeepingClient()
    else:
      self.bkkClient = bkkClientIn

    if diracProdIn is None:
      from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction
      self.diracProduction = DiracProduction()
    else:
      self.diracProduction = diracProdIn

    self.logger = gLogger.getSubLogger( 'ProductionRequest' )

    self.requestID = 0
    self.parentRequestID = 0
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
    self.cpus = []
    self.inputs = [] # list of lists
    self.targets = []
    self.outputFileMasks = []
    self.groupSizes = []
    self.plugins = []
    self.inputDataPolicies = []
    self.prodGroup = ''
    self.derivedProduction = 0
    self.publishFlag = True
    self.testFlag = False
    self.extend = 0
    self.dataTakingConditions = ''
    self.processingPass = ''
    self.bkFileType = ''
    self.dqFlag = ''
    self.startRun = ''
    self.endRun = ''
    self.runsList = ''
    self.prodsToLaunch = []

  #############################################################################

  def resolveSteps( self ):
    """ Given a list of steps in strings, some of which might be missing,
        resolve it into a dictionary of steps
    """

    self.stepsList = _toIntList( self.stepsList )

    for stepID in self.stepsList:
      stepDict = self.bkkClient.getAvailableSteps( {'StepId':stepID} )
      if not stepDict['OK']:
        raise ValueError, stepDict['Message']
      else:
        stepDict = stepDict['Value']

      stepsListDictItem = {}
      for ( parameter, value ) in itertools.izip( stepDict['ParameterNames'],
                                                  stepDict['Records'][0] ):
        stepsListDictItem[parameter] = value

      s_in = self.bkkClient.getStepInputFiles( stepID )
      if not s_in['OK']:
        raise ValueError, s_in['Message']
      else:
        fileTypesList = [fileType[0].strip() for fileType in s_in['Value']['Records']]
        stepsListDictItem['fileTypesIn'] = fileTypesList

      s_out = self.bkkClient.getStepOutputFiles( stepID )
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

    self._applyOptionalCorrections()

    prodsDict = self._getProdsDescriptionDict()

    stepsListDict = copy.deepcopy( self.stepsListDict )

    prodID = 0

    #now we build and launch each productions
    for prodIndex, prodDict in prodsDict.items():

      if self.prodsToLaunch:
        if prodIndex not in self.prodsToLaunch:
          continue

      #build the list of steps in a production
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
                                    inputDataPolicy = prodDict['inputDataPolicy'],
                                    bkQuery = prodDict['bkQuery'],
                                    plugin = prodDict['plugin'],
                                    previousProdID = prodID,
                                    derivedProdID = prodDict['derivedProduction'],
                                    transformationFamily = prodDict['transformationFamily'] )

      res = self.diracProduction.launchProduction( prod = prod,
                                                   publishFlag = self.publishFlag,
                                                   testFlag = self.testFlag,
                                                   requestID = self.requestID,
                                                   extend = self.extend,
                                                   tracking = prodDict['tracking'] )
      if not res['OK']:
        raise RuntimeError, res['Message']

      self.extend = 0 #only extending the first one (MC)
      prodID = res['Value']

      if self.publishFlag:
        self.logger.info( 'For request %s, submitted Production %d, of type %s, ID = %s' % ( str( self.requestID ),
                                                                                             prodIndex,
                                                                                             prodDict['productionType'],
                                                                                             str( prodID ) ) )

  #############################################################################

  def _applyOptionalCorrections( self ):
    """ if needed, calls _splitIntoProductionSteps. It also applies other changes
    """

    #Checking if we need to split the merging step into many productions
    if 'merge' in [pt.lower() for pt in self.prodsTypeList]:
      i = 0
      indexes = []
      for pt in self.prodsTypeList:
        if pt.lower() == 'merge':
          indexes.append( i )
        i += 1

      for index in indexes:
        #In this case and only in this case I have to split the merging in many productions
        plugin = self.plugins[index]
        outputSE = self.outputSEs[index]
        priority = self.priorities[index]
        cpu = self.cpus[index]
        if 'byrunfiletypesizewithflush' != plugin.lower():
          stepToSplit = self.stepsListDict[index]
          numberOfProdsToInsert = len( stepToSplit['fileTypesOut'] )
          self.prodsTypeList.remove( 'Merge' )
          self.plugins.pop( index )
          self.outputSEs.pop( index )
          self.priorities.pop( index )
          self.cpus.pop( index )
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
            self.stepsListDict.insert( index, newSteps[x] )
            self.stepsInProds.insert( index + x, [last + x] )

    correctedStepsInProds = []
    toInsert = self.stepsInProds[0][0]
    lengths = [len( x ) for x in self.stepsInProds]
    for l in lengths:
      li = [toInsert + x for x in range( l )]
      toInsert += l
      correctedStepsInProds.append( li )

    self.stepsInProds = correctedStepsInProds

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
      self.outputFileMasks = [''] * len( self.prodsTypeList )

    if not self.inputs:
      self.inputs = [[]] * len( self.prodsTypeList )

    if not self.targets:
      self.targets = [''] * len( self.prodsTypeList )

    if not self.groupSizes:
      self.groupSizes = [1] * len( self.prodsTypeList )

    if not self.inputDataPolicies:
      self.inputDataPolicies = ['download'] * len( self.prodsTypeList )

  #############################################################################

  def _getProdsDescriptionDict( self ):
    """ Returns a dictionary representing the description of the request (of all the productions in it)
    """

    prodsDict = {}
    bkQuery = self.bkQuery

    prodNumber = 1

    for prodType, stepsInProd, removeInputsFlag, outputSE, priority, \
    cpu, inputD, outFileMask, target, groupSize, plugin, idp in itertools.izip( self.prodsTypeList,
                                                                                self.stepsInProds,
                                                                                self.removeInputsFlags,
                                                                                self.outputSEs,
                                                                                self.priorities,
                                                                                self.cpus,
                                                                                self.inputs,
                                                                                self.outputFileMasks,
                                                                                self.targets,
                                                                                self.groupSizes,
                                                                                self.plugins,
                                                                                self.inputDataPolicies
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
                                 'outputFileMask':outFileMask,
                                 'target':target,
                                 'groupSize': groupSize,
                                 'plugin': plugin,
                                 'inputDataPolicy': idp,
                                 'derivedProduction': 0,
                                 'transformationFamily': transformationFamily
                                 }
      bkQuery = 'fromPreviousProd'
      prodNumber += 1

    #tracking the last production(s)
    prodsDict[prodNumber - 1]['tracking'] = 1
    typeOfLastProd = prodsDict[prodNumber - 1]['productionType']
    index = 2
    while prodsDict[prodNumber - index]['productionType'] == typeOfLastProd:
      prodsDict[prodNumber - index]['tracking'] = 1
      index += 1

    #production derivation, if necessary
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
                        target = '',
                        removeInputData = False,
                        groupSize = 1,
                        bkQuery = None,
                        plugin = '',
                        previousProdID = 0,
                        derivedProdID = 0,
                        transformationFamily = 0 ):
    """ Wrapper around Production API to build a production, given the needed parameters
    """
    prod = Production()

    #non optional parameters
    prod.LHCbJob.setType( prodType )
    prod.LHCbJob.workflow.setName( 'Request_%s_%s_%s_EventType_%s_%s' % ( self.requestID, prodType,
                                                                          self.prodGroup, self.eventType,
                                                                          self.appendName ) )
    prod.setBKParameters( self.configName, self.configVersion, self.prodGroup, self.dataTakingConditions )
    prod.setParameter( 'eventType', 'string', self.eventType, 'Event Type of the production' )
    prod.setParameter( 'numberOfEvents', 'string', str( self.events ), 'Number of events requested' )
    prod.prodGroup = self.prodGroup
    prod.priority = str( priority )
    prod.LHCbJob.workflow.setDescription( 'prodDescription' )
    prod.setJobParameters( { 'CPUTime': cpu } )
    prod.setParameter( 'generatorName', 'string', str( self.generatorName ), 'Generator Name' )
    prod.plugin = plugin

    #optional parameters
    prod.jobFileGroupSize = groupSize
    if inputDataPolicy:
      prod.LHCbJob.setInputDataPolicy( inputDataPolicy )
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
    if derivedProdID:
      prod.ancestorProduction = derivedProdID
    if transformationFamily:
      prod.transformationFamily = transformationFamily

    #Adding optional input BK query
    if bkQuery == 'fromPreviousProd':
      prod.inputBKSelection = {
                               'FileType': stepsInProd[0]['fileTypesIn'][0].upper(),
                               'EventType': self.eventType,
                               'ProductionID': int( previousProdID )
                               }
    elif bkQuery is not None:
      prod.inputBKSelection = bkQuery

    #Adding the application steps
    firstStep = stepsInProd.pop( 0 )
    try:
      ep = extraOptions[firstStep['StepId']]
    except IndexError:
      ep = ''
    stepName = prod.addApplicationStep( stepDict = firstStep,
                                        outputSE = outputSE,
                                        optionsLine = ep,
                                        inputData = '' )
    prod.gaudiSteps.append( stepName )

    for step in stepsInProd:
      try:
        ep = extraOptions[step['StepId']]
      except IndexError:
        ep = ''
      stepName = prod.addApplicationStep( stepDict = step,
                                          outputSE = outputSE,
                                          optionsLine = ep,
                                          inputData = 'previousStep' )
      prod.gaudiSteps.append( stepName )

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

  def _buildFullBKKQuery( self ):
    """ simply creates the bkk query dictionary
    """

    self.bkQuery = {
                    'DataTakingConditions'     : self.dataTakingConditions,
                    'ProcessingPass'           : self.processingPass,
                    'FileType'                 : self.bkFileType,
                    'EventType'                : str( self.eventType ),
                    'ConfigName'               : self.configName,
                    'ConfigVersion'            : self.configVersion,
                    'DataQualityFlag'          : self.dqFlag.replace( ',', ';;;' ).replace( ' ', '' )
                    }

    if ( self.startRun and self.runsList ) or ( self.endRun and self.runsList ):
      raise ValueError, 'Please don\'t mix runs list with start/end run'

    if int( self.endRun ) and int( self.startRun ):
      if int( self.endRun ) < int( self.startRun ):
        gLogger.error( 'Your end run "%s" should be more than your start run "%s"!' % ( self.endRun, self.startRun ) )
        raise ValueError, 'Error setting start or end run'

    if int( self.startRun ):
      self.bkQuery['StartRun'] = int( self.startRun )
    if int( self.endRun ):
      self.bkQuery['EndRun'] = int( self.endRun )

    if self.runsList:
      self.bkQuery['RunNumbers'] = self.runsList.replace( ',', ';;;' ).replace( ' ', '' )

#############################################################################

def _toIntList( stringsList ):
  """ return list of int from list of strings
  """

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

def _splitIntoProductionSteps( step ):
  """ Given a list of bookkeeping steps, produce production steps
  """
  prodSteps = []

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
