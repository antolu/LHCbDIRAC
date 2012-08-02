""" Some utilities for the templates
"""

import itertools, copy

import DIRAC
from LHCbDIRAC.Interfaces.API.Production import Production

#############################################################################

def resolveSteps( stepsList, BKKClientIn = None ):
  """ Given a list of steps in strings, some of which might be missing,
      resolve it into a dictionary of steps
  """

  if BKKClientIn is None:
    from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
    BKKClient = BookkeepingClient()
  else:
    BKKClient = BKKClientIn

  stepsListInt = __toIntList( stepsList )

  stepsListDict = []

  for stepID in stepsListInt:
    stepDict = BKKClient.getAvailableSteps( {'StepId':stepID} )
    if not stepDict['OK']:
      raise ValueError, stepDict['Message']
    else:
      stepDict = stepDict['Value']

    stepsListDictItem = {}
    for ( parameter, value ) in itertools.izip( stepDict['ParameterNames'],
                                                stepDict['Records'][0] ):
      stepsListDictItem[parameter] = value

    s_in = BKKClient.getStepInputFiles( stepID )
    if not s_in['OK']:
      raise ValueError, s_in['Message']
    else:
      fileTypesList = [fileType[0].strip() for fileType in s_in['Value']['Records']]
      stepsListDictItem['fileTypesIn'] = fileTypesList

    s_out = BKKClient.getStepOutputFiles( stepID )
    if not s_out['OK']:
      raise ValueError, s_out['Message']
    else:
      fileTypesList = [fileType[0].strip() for fileType in s_out['Value']['Records']]
      stepsListDictItem['fileTypesOut'] = fileTypesList

    stepsListDict.append( stepsListDictItem )

  return stepsListDict

#############################################################################

def __toIntList( stringsList ):

  stepsListInt = []
  i = 0
  while True:
    try:
      stepsListInt.append( int( stringsList[i] ) )
      i = i + 1
    except ValueError:
      break
    except IndexError:
      break
  return stepsListInt

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

def buildProduction( prodType, stepsList, requestID, prodDesc,
                     configName, configVersion, dataTakingConditions, appendName,
                     extraOptions, outputSE,
                     eventType, events, priority, cpu,
                     inputDataList = [],
                     sysConfig = '',
                     generatorName = '',
                     outputMode = 'Any',
                     outputFileMask = '',
                     targetSite = '',
                     banTier1s = False,
                     removeInputData = False,
                     bkQuery = None,
                     previousProdID = 0 ):
  """ Wrapper around Production API to build a production, given the needed parameters
  """
  prod = Production()

  #non optional parameters
  prod.LHCbJob.setType( prodType )
  prod.LHCbJob.workflow.setName( 'Request_%s_%s_%s_EventType_%s_%s' % ( requestID, prodType,
                                                                        prodDesc, eventType, appendName ) )
  prod.setBKParameters( configName, configVersion, prodDesc, dataTakingConditions )
  prod._setParameter( 'eventType', 'string', eventType, 'Event Type of the production' )
  prod._setParameter( 'numberOfEvents', 'string', str( events ), 'Number of events requested' )
  prod.prodGroup = prodDesc
  prod.priority = str( priority )
  prod.LHCbJob.workflow.setDescription( 'prodDescription' )
  prod.setJobParameters( { 'CPUTime': cpu } )
  prod.setGeneratorName( generatorName )

  #optional parameters
  if sysConfig:
    prod.setJobParameters( { 'SystemConfig': sysConfig } )
  prod.setOutputMode( outputMode )
  if outputFileMask:
    prod.setFileMask( outputFileMask )
  if targetSite:
    prod.setTargetSite( targetSite )
  if banTier1s:
    prod.banTier1s()
  if inputDataList:
    prod.LHCbJob.setInputData( inputDataList )

  #Adding optional input BK query
  if bkQuery == 'fromPreviousProd':
    BKQuery = {
               'FileType': stepsList[0]['fileTypesIn'][0].upper(),
               'EventType': eventType,
               'ProductionID': int( previousProdID )
               }
    prod.setInputBKSelection( BKQuery )

  #Adding the application steps
  firstStep = stepsList.pop( 0 )
  try:
    ep = extraOptions[firstStep['StepId']]
  except IndexError:
    ep = ''
  prod.addApplicationStep( stepDict = firstStep,
                           outputSE = outputSE,
                           optionsLine = ep,
                           inputData = '' )
  for step in stepsList:
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
                               'FailoverRequest',
                               'UploadLogFile'] )
  else:
    prod.addFinalizationStep( ['UploadOutputData',
                               'FailoverRequest',
                               'UploadLogFile'] )

  return prod

#############################################################################

def launchProduction( prod, publishFlag, testFlag, requestID, parentReq,
                      extend = 0,
                      tracking = 0,
                      BKscriptFlag = False,
                      diracProd = None,
                      logger = None ):
  """ given a production object, launch it
  """
  if not logger:
    from DIRAC import gLogger
    logger = gLogger.getSubLogger( 'launching_Production' )

  if not diracProd:
    from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction
    diracProd = DiracProduction()

  if publishFlag == False and testFlag:
    logger.info( 'Test prod will be launched locally' )
    try:
      result = prod.runLocal()
      if result['OK']:
        logger.info( 'Template finished successfully' )
        DIRAC.exit( 0 )
      else:
        logger.error( 'Launching production: something wrong with execution!' )
        DIRAC.exit( 2 )
    except Exception, x:
      logger.error( 'prod test failed with exception:\n%s' % ( x ) )
      DIRAC.exit( 2 )

  result = prod.create( publish = publishFlag,
                        requestID = int( requestID ),
                        reqUsed = tracking,
                        transformation = False,
                        bkScript = BKscriptFlag,
                        parentRequestID = int( parentReq )
                        )

  if not result['OK']:
    logger.error( 'Error during prod creation:\n%s\ncheck that the wkf name is unique.' % ( result['Message'] ) )
    DIRAC.exit( 2 )

  if publishFlag:
    prodID = result['Value']
    msg = 'Production %s successfully created ' % ( prodID )

    if extend:
      diracProd.extendProduction( prodID, extend, printOutput = True )
      msg += ', extended by %s jobs' % extend

    if testFlag:
      diracProd.production( prodID, 'manual', printOutput = True )
      msg = msg + 'and started in manual mode.'
    else:
      diracProd.production( prodID, 'automatic', printOutput = True )
      msg = msg + 'and started in automatic mode.'
    logger.info( msg )

  else:
    prodID = 1
    logger.info( 'Production creation completed but not published (publishFlag was %s). Setting ID = %s (useless, just for the test)' % ( publishFlag, prodID ) )

  return prodID

#############################################################################

def getProdsDescriptionDict( prodsTypeList, stepsInProds, bkQuery, removeInputsFlags,
                             outputSEs, priorities, CPUs, inputs ):

  prodsDict = {}

  for prodType, stepsInProd, removeInputsFlag, outputSE, priority, cpu, inputD in itertools.izip( prodsTypeList,
                                                                                          stepsInProds,
                                                                                          removeInputsFlags,
                                                                                          outputSEs,
                                                                                          priorities,
                                                                                          CPUs,
                                                                                          inputs
                                                                ):

    prodsDict[ prodType ] = {
                             'stepsInProd': stepsInProd,
                             'bkQuery': bkQuery,
                             'removeInputsFlag': removeInputsFlag,
                             'tracking':0,
                             'outputSE':outputSE,
                             'priority': priority,
                             'cpu': cpu,
                             'input': inputD
                             }
    bkQuery = 'fromPreviousProd'

  #tracking the last production
  prodsDict[prodType]['tracking'] = 1

  return prodsDict
