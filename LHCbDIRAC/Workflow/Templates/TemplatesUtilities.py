__RCSID__ = "$Id"

""" Some utilities for the templates
"""

import itertools, copy

def builProductiondWorkflow( prodStepsListDict, productionObject ):
  """ 
  """

  for step in prodStepsListDict:
    #TODO: build this
    getattr( productionObject, 'add' + step['applicationName'] + 'Step' )( "list of parameters" )


def resolveSteps( stepsList, BKKClientIn = None ):
  """ Given a list of steps in strings, some of which might be missing,
      resolve it into a dictionary of steps
  """

  if BKKClientIn is None:
    from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
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
      fileTypesList = [fileType[0] for fileType in s_in['Value']['Records']]
      stepsListDictItem['fileTypesIn'] = fileTypesList

    s_out = BKKClient.getStepOutputFiles( stepID )
    if not s_out['OK']:
      raise ValueError, s_out['Message']
    else:
      fileTypesList = [fileType[0] for fileType in s_out['Value']['Records']]
      stepsListDictItem['fileTypesOut'] = fileTypesList

    stepsListDict.append( stepsListDictItem )

  return stepsListDict

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


def _splitIntoProductionSteps( stepsList ):
  """ Given a list of bookkeeping steps, produce production steps
  """
  prodSteps = []

  for step in stepsList:
    if len( step['fileTypesIn'] ) <= 1:
      prodSteps.append( step )
    else:
      if step['fileTypesIn'] != step['fileTypesOut']:
        raise ValueError, "Step inputs and output differ (this is supposed to be a merging step)"
      for input in step['fileTypesIn']:
        prodStep = copy.deepcopy( step )
        prodStep['fileTypesIn'] = [input]
        prodStep['fileTypesOut'] = [input]
        prodSteps.append( prodStep )

  return prodSteps
