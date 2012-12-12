import unittest
from mock import Mock

from LHCbDIRAC.ProductionManagementSystem.Client.ProductionRequest import ProductionRequest, _splitIntoProductionSteps

class bkClientFake:
  def getAvailableSteps( self, stepID ):
    if stepID == {'StepId':123}:
      return {'OK': True,
              'Value': {'TotalRecords': 1,
                        'ParameterNames': ['StepId', 'StepName', 'ApplicationName', 'ApplicationVersion',
                                           'OptionFiles', 'Visible', 'ExtraPackages', 'ProcessingPass', 'OptionsFormat',
                                           'DDDB', 'CONDDB', 'DQTag'],
                        'Records': [[123, 'Stripping14-Stripping', 'DaVinci', 'v2r2',
                                     'optsFiles', 'Yes', 'eps', 'procPass', '',
                                     '', '123456', '']]}}
    elif stepID == {'StepId':456}:
      return {'OK': True,
              'Value': {'TotalRecords': 1,
                        'ParameterNames': ['StepId', 'StepName', 'ApplicationName', 'ApplicationVersion',
                                           'OptionFiles', 'Visible', 'ExtraPackages', 'ProcessingPass', 'OptionsFormat',
                                           'DDDB', 'CONDDB', 'DQTag'],
                        'Records': [[456, 'Merge', 'LHCb', 'v1r2',
                                     'optsFiles', 'Yes', 'eps', 'procPass', '',
                                     '', 'fromPreviousStep', '']]}}

  def getStepInputFiles( self, stepID ):
    if stepID == 123:
      return {'OK': True, 'Value': {'TotalRecords': 7,
                                   'ParameterNames': ['FileType', 'Visible'],
                                   'Records': [['SDST', 'Y']]}}

    if stepID == 456:
      return {'OK': True, 'Value': {'TotalRecords': 7,
                                   'ParameterNames': ['FileType', 'Visible'],
                                   'Records': [['BHADRON.DST', 'Y'], ['CALIBRATION.DST', 'Y']]}}

  def getStepOutputFiles( self, stepID ):
    if stepID == 123:
      return {'OK': True, 'Value': {'TotalRecords': 7,
                                   'ParameterNames': ['FileType', 'Visible'],
                                   'Records': [['BHADRON.DST', 'Y'], ['CALIBRATION.DST', 'Y']]}}
    if stepID == 456:
      return {'OK': True, 'Value': {'TotalRecords': 7,
                                   'ParameterNames': ['FileType', 'Visible'],
                                   'Records': [['BHADRON.DST', 'Y'], ['CALIBRATION.DST', 'Y']]}}

class ClientTestCase( unittest.TestCase ):
  """ Base class for the Client test cases
  """
  def setUp( self ):

    self.bkClientMock = Mock()
    self.diracProdIn = Mock()
    self.diracProdIn.launchProduction.return_value = {'OK': True, 'Value': 321}

    self.bkClientFake = bkClientFake()

#############################################################################
# ProductionRequest.py
#############################################################################

class ProductionRequestSuccess( ClientTestCase ):

  def test_resolveStepsSuccess( self ):


    pr = ProductionRequest( self.bkClientFake, self.diracProdIn )
    pr.stepsList = ['123']
    pr.resolveSteps()
    self.assertEqual( pr.stepsListDict, [{'StepId': 123, 'StepName':'Stripping14-Stripping',
                                         'ApplicationName':'DaVinci', 'ApplicationVersion':'v2r2',
                                         'OptionFiles':'optsFiles', 'Visible':'Yes', 'ExtraPackages':'eps',
                                         'ProcessingPass':'procPass', 'OptionsFormat':'',
                                         'DDDB':'', 'CONDDB':'123456', 'DQTag':'',
                                         'fileTypesIn':['SDST'],
                                         'fileTypesOut':['BHADRON.DST', 'CALIBRATION.DST']}] )
    pr = ProductionRequest( self.bkClientFake, self.diracProdIn )
    pr.stepsList = ['123', '456']
    pr.resolveSteps()
    self.assertEqual( pr.stepsListDict, [{'StepId': 123, 'StepName':'Stripping14-Stripping',
                                         'ApplicationName':'DaVinci', 'ApplicationVersion':'v2r2',
                                         'OptionFiles':'optsFiles', 'Visible':'Yes', 'ExtraPackages':'eps',
                                         'ProcessingPass':'procPass', 'OptionsFormat':'',
                                         'DDDB':'', 'CONDDB':'123456', 'DQTag':'',
                                         'fileTypesIn':['SDST'],
                                         'fileTypesOut':['BHADRON.DST', 'CALIBRATION.DST']},
                                         {'StepId': 456, 'StepName':'Merge',
                                         'ApplicationName':'LHCb', 'ApplicationVersion':'v1r2',
                                         'OptionFiles':'optsFiles', 'Visible':'Yes', 'ExtraPackages':'eps',
                                         'ProcessingPass':'procPass', 'OptionsFormat':'',
                                         'DDDB':'', 'CONDDB':'fromPreviousStep', 'DQTag':'',
                                         'fileTypesIn':['BHADRON.DST', 'CALIBRATION.DST'],
                                         'fileTypesOut':['BHADRON.DST', 'CALIBRATION.DST']}
                           ] )
    pr = ProductionRequest( self.bkClientFake, self.diracProdIn )
    pr.stepsList = ['123', '456', '', '']
    pr.resolveSteps()
    self.assertEqual( pr.stepsListDict, [{'StepId': 123, 'StepName':'Stripping14-Stripping',
                                         'ApplicationName':'DaVinci', 'ApplicationVersion':'v2r2',
                                         'OptionFiles':'optsFiles', 'Visible':'Yes', 'ExtraPackages':'eps',
                                         'ProcessingPass':'procPass', 'OptionsFormat':'',
                                         'DDDB':'', 'CONDDB':'123456', 'DQTag':'',
                                         'fileTypesIn':['SDST'],
                                         'fileTypesOut':['BHADRON.DST', 'CALIBRATION.DST']},
                                         {'StepId': 456, 'StepName':'Merge',
                                         'ApplicationName':'LHCb', 'ApplicationVersion':'v1r2',
                                         'OptionFiles':'optsFiles', 'Visible':'Yes', 'ExtraPackages':'eps',
                                         'ProcessingPass':'procPass', 'OptionsFormat':'',
                                         'DDDB':'', 'CONDDB':'fromPreviousStep', 'DQTag':'',
                                         'fileTypesIn':['BHADRON.DST', 'CALIBRATION.DST'],
                                         'fileTypesOut':['BHADRON.DST', 'CALIBRATION.DST']}
                           ] )
    pr = ProductionRequest( self.bkClientFake, self.diracProdIn )
    pr.stepsList = ['123']
    pr.resolveSteps()
    self.assertEqual( pr.stepsListDict, [{'StepId': 123, 'StepName':'Stripping14-Stripping',
                                         'ApplicationName':'DaVinci', 'ApplicationVersion':'v2r2',
                                         'OptionFiles':'optsFiles', 'Visible':'Yes', 'ExtraPackages':'eps',
                                         'ProcessingPass':'procPass', 'OptionsFormat':'',
                                         'DDDB':'', 'CONDDB':'123456', 'DQTag':'',
                                         'fileTypesIn':['SDST'],
                                         'fileTypesOut':['BHADRON.DST', 'CALIBRATION.DST']}] )
    pr = ProductionRequest( self.bkClientFake, self.diracProdIn )
    pr.stepsList = ['123']
    pr.resolveSteps()
    self.assertEqual( pr.stepsListDict, [{'StepId': 123, 'StepName':'Stripping14-Stripping',
                                         'ApplicationName':'DaVinci', 'ApplicationVersion':'v2r2',
                                         'OptionFiles':'optsFiles', 'Visible':'Yes', 'ExtraPackages':'eps',
                                         'ProcessingPass':'procPass', 'OptionsFormat':'',
                                         'DDDB':'', 'CONDDB':'123456', 'DQTag':'',
                                         'fileTypesIn':['SDST'],
                                         'fileTypesOut':['BHADRON.DST', 'CALIBRATION.DST']}] )

  def test__applyOptionalCorrections( self ):

    stepMC = {'ApplicationName': 'Gauss', 'Usable': 'Yes', 'StepId': 246, 'ApplicationVersion': 'v28r3p1',
              'ExtraPackages': 'AppConfig.v3r104', 'StepName': 'Stripping14-Merging',
              'ProcessingPass': 'MC', 'Visible': 'N', 'DDDB': 'head-20110302',
              'OptionFiles': '$APPCONFIGOPTS/Merging/DV-Stripping14-Merging.py', 'CONDDB': 'head-20110407',
              'fileTypesIn': [],
              'fileTypesOut': ['ALLSTREAMS.DST']}
    stepStripp = {'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': 123, 'ApplicationVersion': 'v28r3p1',
                  'ExtraPackages': 'AppConfig.v3r104', 'StepName': 'Stripping14-Merging',
                  'ProcessingPass': 'Merging', 'Visible': 'N', 'DDDB': 'head-20110302',
                  'OptionFiles': '$APPCONFIGOPTS/Merging/DV-Stripping14-Merging.py', 'CONDDB': 'head-20110407',
                  'fileTypesIn': ['SDST'],
                  'fileTypesOut': ['BHADRON.DST', 'CALIBRATION.DST', 'CHARM.MDST', 'CHARMCOMPLETEEVENT.DST']}
    mergeStep = {'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': 456, 'ApplicationVersion': 'v28r3p1',
                 'ExtraPackages': 'AppConfig.v3r104', 'StepName': 'Stripping14-Merging',
                 'ProcessingPass': 'Merging', 'Visible': 'N', 'DDDB': 'head-20110302',
                 'OptionFiles': '$APPCONFIGOPTS/Merging/DV-Stripping14-Merging.py', 'CONDDB': 'head-20110407',
                 'fileTypesIn': ['BHADRON.DST', 'CALIBRATION.DST', 'PID.MDST'],
                 'fileTypesOut': ['BHADRON.DST', 'CALIBRATION.DST', 'PID.MDST']}
    mergeStepBHADRON = {'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': 456, 'ApplicationVersion': 'v28r3p1',
                        'ExtraPackages': 'AppConfig.v3r104', 'StepName': 'Stripping14-Merging',
                        'ProcessingPass': 'Merging', 'Visible': 'N', 'DDDB': 'head-20110302',
                        'OptionFiles': '$APPCONFIGOPTS/Merging/DV-Stripping14-Merging.py', 'CONDDB': 'head-20110407',
                        'fileTypesIn': ['BHADRON.DST'],
                        'fileTypesOut': ['BHADRON.DST']}
    mergeStepCALIBRA = {'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': 456, 'ApplicationVersion': 'v28r3p1',
                        'ExtraPackages': 'AppConfig.v3r104', 'StepName': 'Stripping14-Merging',
                        'ProcessingPass': 'Merging', 'Visible': 'N', 'DDDB': 'head-20110302',
                        'OptionFiles': '$APPCONFIGOPTS/Merging/DV-Stripping14-Merging.py', 'CONDDB': 'head-20110407',
                        'fileTypesIn': ['CALIBRATION.DST'],
                        'fileTypesOut': ['CALIBRATION.DST']}
    mergeStepPIDMDST = {'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': 456, 'ApplicationVersion': 'v28r3p1',
                        'ExtraPackages': 'AppConfig.v3r104', 'StepName': 'Stripping14-Merging',
                        'ProcessingPass': 'Merging', 'Visible': 'N', 'DDDB': 'head-20110302',
                        'OptionFiles': '$APPCONFIGOPTS/Merging/DV-Stripping14-Merging.py', 'CONDDB': 'head-20110407',
                        'fileTypesIn': ['PID.MDST'],
                        'fileTypesOut': ['PID.MDST']}

    pr = ProductionRequest( self.bkClientMock, self.diracProdIn )
    pr.prodsTypeList = ['DataStripping', 'Merge']
    pr.plugins = ['ByRun', 'BySize']
    pr.stepsListDict = [stepStripp, mergeStep]
    pr.stepsInProds = [[1], [2]]
    pr.outputSEs = [ 'Tier1-DST', 'Tier1-M-DST']
    pr.priorities = [1, 4]
    pr.cpus = [10, 100]
    pr.bkFileType = 'TYPE'
    pr._applyOptionalCorrections()
    prodsTypeListExpected = ['DataStripping', 'Merge', 'Merge', 'Merge']
    pluginsExpected = ['ByRun', 'BySize', 'BySize', 'BySize']
    stepsListDictExpected = [stepStripp, mergeStepBHADRON, mergeStepCALIBRA, mergeStepPIDMDST ]
    stepsInProdExpected = [[1], [2], [3], [4]]
    outputSEsExpected = ['Tier1-DST', 'Tier1-M-DST', 'Tier1-M-DST', 'Tier1-M-DST']
    prioritiesExpected = [1, 4, 4, 4]
    cpusExpected = [10, 100, 100, 100]
    bkQueriesExpected = ['Full', 'fromPreviousProd', 'fromPreviousProd', 'fromPreviousProd']
    previousProdsExpected = [None, 1, 1, 1]
    self.assertEqual( pr.prodsTypeList, prodsTypeListExpected )
    self.assertEqual( pr.plugins, pluginsExpected )
    self.assertEqual( pr.stepsListDict, stepsListDictExpected )
    self.assertEqual( pr.stepsInProds, stepsInProdExpected )
    self.assertEqual( pr.outputSEs, outputSEsExpected )
    self.assertEqual( pr.priorities, prioritiesExpected )
    self.assertEqual( pr.cpus, cpusExpected )
    self.assertEqual( pr.bkQueries, bkQueriesExpected )
    self.assertEqual( pr.previousProds, previousProdsExpected )

    pr = ProductionRequest( self.bkClientMock, self.diracProdIn )
    pr.prodsTypeList = ['DataStripping', 'Merge']
    pr.plugins = ['ByRun', 'ByRunFileTypeSizeWithFlush']
    pr.stepsListDict = [stepStripp, mergeStep]
    pr.stepsInProds = [[1], [2]]
    pr.outputSEs = [ 'Tier1-DST', 'Tier1-M-DST']
    pr.priorities = [1, 4]
    pr.cpus = [10, 100]
    pr.bkFileType = 'TYPE'
    pr._applyOptionalCorrections()
    prodsTypeListExpected = ['DataStripping', 'Merge']
    pluginsExpected = ['ByRun', 'ByRunFileTypeSizeWithFlush']
    stepsListDictExpected = [stepStripp, mergeStep ]
    stepsInProdExpected = [[1], [2]]
    outputSEsExpected = ['Tier1-DST', 'Tier1-M-DST']
    prioritiesExpected = [1, 4]
    cpusExpected = [10, 100]
    bkQueriesExpected = ['Full', 'fromPreviousProd']
    previousProdsExpected = [None, 1]
    self.assertEqual( pr.prodsTypeList, prodsTypeListExpected )
    self.assertEqual( pr.plugins, pluginsExpected )
    self.assertEqual( pr.stepsListDict, stepsListDictExpected )
    self.assertEqual( pr.stepsInProds, stepsInProdExpected )
    self.assertEqual( pr.outputSEs, outputSEsExpected )
    self.assertEqual( pr.priorities, prioritiesExpected )
    self.assertEqual( pr.cpus, cpusExpected )
    self.assertEqual( pr.bkQueries, bkQueriesExpected )
    self.assertEqual( pr.previousProds, previousProdsExpected )

    pr = ProductionRequest( self.bkClientMock, self.diracProdIn )
    pr.prodsTypeList = ['Merge', 'Merge']
    pr.plugins = ['BySize', 'ByRunFileTypeSizeWithFlush']
    pr.stepsListDict = [mergeStep, mergeStep]
    pr.stepsInProds = [[1], [2]]
    pr.outputSEs = [ 'Tier1-DST', 'Tier1-M-DST']
    pr.priorities = [1, 4]
    pr.cpus = [10, 100]
    pr._applyOptionalCorrections()
    prodsTypeListExpected = ['Merge', 'Merge', 'Merge', 'Merge']
    pluginsExpected = ['BySize', 'BySize', 'BySize', 'ByRunFileTypeSizeWithFlush']
    stepsListDictExpected = [mergeStepBHADRON, mergeStepCALIBRA, mergeStepPIDMDST, mergeStep ]
    stepsInProdExpected = [[1], [2], [3], [4]]
    outputSEsExpected = ['Tier1-DST', 'Tier1-DST', 'Tier1-DST', 'Tier1-M-DST']
    prioritiesExpected = [1, 1, 1, 4]
    cpusExpected = [10, 10, 10, 100]
    bkQueriesExpected = ['', '', '', 'fromPreviousProd']
    previousProdsExpected = [None, None, None, 1]
    self.assertEqual( pr.prodsTypeList, prodsTypeListExpected )
    self.assertEqual( pr.plugins, pluginsExpected )
    self.assertEqual( pr.stepsListDict, stepsListDictExpected )
    self.assertEqual( pr.stepsInProds, stepsInProdExpected )
    self.assertEqual( pr.outputSEs, outputSEsExpected )
    self.assertEqual( pr.priorities, prioritiesExpected )
    self.assertEqual( pr.cpus, cpusExpected )
    self.assertEqual( pr.bkQueries, bkQueriesExpected )
    self.assertEqual( pr.previousProds, previousProdsExpected )

    pr = ProductionRequest( self.bkClientMock, self.diracProdIn )
    pr.prodsTypeList = ['DataStripping']
    pr.plugins = ['ByRun']
    pr.stepsListDict = [stepStripp, mergeStep]
    pr.stepsInProds = [[1, 2]]
    pr.priorities = [1]
    pr.outputSEs = [ 'Tier1-DST']
    pr.cpus = [10]
    pr.bkFileType = 'TYPE'
    pr._applyOptionalCorrections()
    prodsTypeListExpected = ['DataStripping']
    pluginsExpected = ['ByRun']
    stepsListDictExpected = [stepStripp, mergeStep ]
    stepsInProdExpected = [[1, 2]]
    outputSEsExpected = ['Tier1-DST']
    prioritiesExpected = [1]
    cpusExpected = [10]
    bkQueriesExpected = ['Full']
    previousProdsExpected = [None]
    self.assertEqual( pr.prodsTypeList, prodsTypeListExpected )
    self.assertEqual( pr.plugins, pluginsExpected )
    self.assertEqual( pr.stepsListDict, stepsListDictExpected )
    self.assertEqual( pr.stepsInProds, stepsInProdExpected )
    self.assertEqual( pr.outputSEs, outputSEsExpected )
    self.assertEqual( pr.priorities, prioritiesExpected )
    self.assertEqual( pr.cpus, cpusExpected )
    self.assertEqual( pr.bkQueries, bkQueriesExpected )
    self.assertEqual( pr.previousProds, previousProdsExpected )

    pr = ProductionRequest( self.bkClientMock, self.diracProdIn )
    pr.prodsTypeList = ['DataStripping', 'Merge', 'Merge']
    pr.plugins = ['ByRun', 'BySize', 'ByRunFileTypeSizeWithFlush']
    pr.stepsListDict = [stepStripp, mergeStep, mergeStep]
    pr.stepsInProds = [[1], [2], [3]]
    pr.outputSEs = [ 'Tier1-DST', 'Tier1-M-DST', 'Tier1-M-DST']
    pr.priorities = [1, 4, 5]
    pr.cpus = [10, 100, 1000]
    pr.bkFileType = 'TYPE'
    pr._applyOptionalCorrections()
    prodsTypeListExpected = ['DataStripping', 'Merge', 'Merge', 'Merge', 'Merge']
    pluginsExpected = ['ByRun', 'BySize', 'BySize', 'BySize', 'ByRunFileTypeSizeWithFlush']
    stepsListDictExpected = [stepStripp, mergeStepBHADRON, mergeStepCALIBRA, mergeStepPIDMDST, mergeStep ]
    stepsInProdExpected = [[1], [2], [3], [4], [5]]
    outputSEsExpected = ['Tier1-DST', 'Tier1-M-DST', 'Tier1-M-DST', 'Tier1-M-DST', 'Tier1-M-DST']
    prioritiesExpected = [1, 4, 4, 4, 5]
    cpusExpected = [10, 100, 100, 100, 1000]
    bkQueriesExpected = ['Full', 'fromPreviousProd', 'fromPreviousProd', 'fromPreviousProd', 'fromPreviousProd']
    previousProdsExpected = [None, 1, 1, 1, 2]
    self.assertEqual( pr.prodsTypeList, prodsTypeListExpected )
    self.assertEqual( pr.plugins, pluginsExpected )
    self.assertEqual( pr.stepsListDict, stepsListDictExpected )
    self.assertEqual( pr.stepsInProds, stepsInProdExpected )
    self.assertEqual( pr.outputSEs, outputSEsExpected )
    self.assertEqual( pr.priorities, prioritiesExpected )
    self.assertEqual( pr.cpus, cpusExpected )
    self.assertEqual( pr.bkQueries, bkQueriesExpected )
    self.assertEqual( pr.previousProds, previousProdsExpected )

    pr = ProductionRequest( self.bkClientMock, self.diracProdIn )
    pr.prodsTypeList = ['DataStripping', 'Merge']
    pr.plugins = ['ByRun', 'BySize']
    pr.stepsListDict = [stepStripp, mergeStep]
    pr.stepsInProds = [[3], [4]]
    pr.outputSEs = [ 'Tier1-DST', 'Tier1-M-DST']
    pr.priorities = [1, 4]
    pr.cpus = [10, 100]
    pr.bkFileType = 'TYPE'
    pr._applyOptionalCorrections()
    prodsTypeListExpected = ['DataStripping', 'Merge', 'Merge', 'Merge']
    pluginsExpected = ['ByRun', 'BySize', 'BySize', 'BySize']
    stepsListDictExpected = [stepStripp, mergeStepBHADRON, mergeStepCALIBRA, mergeStepPIDMDST ]
    stepsInProdExpected = [[3], [4], [5], [6]]
    outputSEsExpected = ['Tier1-DST', 'Tier1-M-DST', 'Tier1-M-DST', 'Tier1-M-DST']
    prioritiesExpected = [1, 4, 4, 4]
    cpusExpected = [10, 100, 100, 100]
    bkQueriesExpected = ['Full', 'fromPreviousProd', 'fromPreviousProd', 'fromPreviousProd']
    previousProdsExpected = [None, 1, 1, 1]
    self.assertEqual( pr.prodsTypeList, prodsTypeListExpected )
    self.assertEqual( pr.plugins, pluginsExpected )
    self.assertEqual( pr.stepsListDict, stepsListDictExpected )
    self.assertEqual( pr.stepsInProds, stepsInProdExpected )
    self.assertEqual( pr.outputSEs, outputSEsExpected )
    self.assertEqual( pr.priorities, prioritiesExpected )
    self.assertEqual( pr.cpus, cpusExpected )
    self.assertEqual( pr.bkQueries, bkQueriesExpected )
    self.assertEqual( pr.previousProds, previousProdsExpected )

    pr = ProductionRequest( self.bkClientMock, self.diracProdIn )
    pr.prodsTypeList = ['DataStripping']
    pr.plugins = ['ByRun']
    pr.stepsListDict = [stepStripp, mergeStep]
    pr.stepsInProds = [[3, 4]]
    pr.priorities = [1]
    pr.outputSEs = [ 'Tier1-DST']
    pr.cpus = [10]
    pr.bkFileType = 'TYPE'
    pr._applyOptionalCorrections()
    prodsTypeListExpected = ['DataStripping']
    pluginsExpected = ['ByRun']
    stepsListDictExpected = [stepStripp, mergeStep ]
    stepsInProdExpected = [[3, 4]]
    outputSEsExpected = ['Tier1-DST']
    prioritiesExpected = [1]
    cpusExpected = [10]
    bkQueriesExpected = ['Full']
    previousProdsExpected = [None]
    self.assertEqual( pr.prodsTypeList, prodsTypeListExpected )
    self.assertEqual( pr.plugins, pluginsExpected )
    self.assertEqual( pr.stepsListDict, stepsListDictExpected )
    self.assertEqual( pr.stepsInProds, stepsInProdExpected )
    self.assertEqual( pr.outputSEs, outputSEsExpected )
    self.assertEqual( pr.priorities, prioritiesExpected )
    self.assertEqual( pr.cpus, cpusExpected )
    self.assertEqual( pr.bkQueries, bkQueriesExpected )
    self.assertEqual( pr.previousProds, previousProdsExpected )

    pr = ProductionRequest( self.bkClientMock, self.diracProdIn )
    pr.prodsTypeList = ['Merge', 'Merge']
    pr.plugins = ['BySize', 'ByRunFileTypeSizeWithFlush']
    pr.stepsListDict = [mergeStep, mergeStep]
    pr.stepsInProds = [[3], [4]]
    pr.outputSEs = [ 'Tier1-DST', 'Tier1-M-DST']
    pr.priorities = [1, 4]
    pr.cpus = [10, 100]
    pr._applyOptionalCorrections()
    prodsTypeListExpected = ['Merge', 'Merge', 'Merge', 'Merge']
    pluginsExpected = ['BySize', 'BySize', 'BySize', 'ByRunFileTypeSizeWithFlush']
    stepsListDictExpected = [mergeStepBHADRON, mergeStepCALIBRA, mergeStepPIDMDST, mergeStep ]
    stepsInProdExpected = [[3], [4], [5], [6]]
    outputSEsExpected = ['Tier1-DST', 'Tier1-DST', 'Tier1-DST', 'Tier1-M-DST']
    prioritiesExpected = [1, 1, 1, 4]
    cpusExpected = [10, 10, 10, 100]
    bkQueriesExpected = ['', '', '', 'fromPreviousProd']
    previousProdsExpected = [None, None, None, 1]
    self.assertEqual( pr.prodsTypeList, prodsTypeListExpected )
    self.assertEqual( pr.plugins, pluginsExpected )
    self.assertEqual( pr.stepsListDict, stepsListDictExpected )
    self.assertEqual( pr.stepsInProds, stepsInProdExpected )
    self.assertEqual( pr.outputSEs, outputSEsExpected )
    self.assertEqual( pr.priorities, prioritiesExpected )
    self.assertEqual( pr.cpus, cpusExpected )
    self.assertEqual( pr.bkQueries, bkQueriesExpected )
    self.assertEqual( pr.previousProds, previousProdsExpected )

    pr = ProductionRequest( self.bkClientMock, self.diracProdIn )
    pr.prodsTypeList = ['MCSimulation', 'Merge']
    pr.plugins = ['', 'ByRunFileTypeSizeWithFlush']
    pr.stepsListDict = [stepMC, mergeStep]
    pr.stepsInProds = [[1], [2]]
    pr.outputSEs = [ 'Tier1-DST', 'Tier1-M-DST']
    pr.priorities = [1, 4]
    pr.cpus = [10, 100]
    pr._applyOptionalCorrections()
    prodsTypeListExpected = ['MCSimulation', 'Merge']
    pluginsExpected = ['', 'ByRunFileTypeSizeWithFlush']
    stepsListDictExpected = [stepMC, mergeStep ]
    stepsInProdExpected = [[1], [2]]
    outputSEsExpected = ['Tier1-DST', 'Tier1-M-DST']
    prioritiesExpected = [1, 4]
    cpusExpected = [10, 100]
    bkQueriesExpected = ['', 'fromPreviousProd']
    previousProdsExpected = [None, 1]
    self.assertEqual( pr.prodsTypeList, prodsTypeListExpected )
    self.assertEqual( pr.plugins, pluginsExpected )
    self.assertEqual( pr.stepsListDict, stepsListDictExpected )
    self.assertEqual( pr.stepsInProds, stepsInProdExpected )
    self.assertEqual( pr.outputSEs, outputSEsExpected )
    self.assertEqual( pr.priorities, prioritiesExpected )
    self.assertEqual( pr.cpus, cpusExpected )
    self.assertEqual( pr.bkQueries, bkQueriesExpected )
    self.assertEqual( pr.previousProds, previousProdsExpected )

  def test_getProdsDescriptionDict( self ):
    pr = ProductionRequest( self.bkClientMock, self.diracProdIn )
    pr.stepsList = [123, 456, 789, 987]
    pr.prodsTypeList = ['DataStripping', 'Merge', 'Merge']
    pr.removeInputsFlags = [False, True, True]
    pr.inputs = [[], [], []]
    pr.targets = ['', '', '']
    pr.groupSizes = [1, 1, 2]
    pr.inputDataPolicies = ['download', 'download', 'download']
    pr.outputFileMasks = ['', '', '']
    pr.stepsInProds = [[1, 2], [3], [4]]
    pr.bkQueries = ['Full', 'fromPreviousProd', 'fromPreviousProd']
    pr.outputSEs = ['Tier1-BUFFER', 'Tier1-DST', 'Tier1-DST']
    pr.priorities = [5, 8, 9]
    pr.cpus = [1000000, 300000, 10000]
    pr.plugins = ['ByRun', 'BySize', 'BySize']
    pr.previousProds = [None, 1, 1]

    res = pr._getProdsDescriptionDict()

    resExpected = {1:{
                      'productionType':'DataStripping',
                      'stepsInProd':[123, 456],
                      'bkQuery': 'Full',
                      'removeInputsFlag': False,
                      'tracking':0,
                      'outputSE': 'Tier1-BUFFER',
                      'priority': 5,
                      'cpu': 1000000,
                      'outputFileMask':'',
                      'input': [],
                      'target':'',
                      'groupSize': 1,
                      'plugin': 'ByRun',
                      'inputDataPolicy':'download',
                      'derivedProduction':0,
                      'transformationFamily':0,
                      'previousProd': None
                     },

                   2:{
                      'productionType':'Merge',
                      'stepsInProd':[789],
                      'bkQuery': 'fromPreviousProd',
                      'removeInputsFlag': True,
                      'tracking':1,
                      'outputSE': 'Tier1-DST',
                      'priority': 8,
                      'cpu': 300000,
                      'outputFileMask':'',
                      'input': [],
                      'target':'',
                      'groupSize': 1,
                      'plugin': 'BySize',
                      'inputDataPolicy':'download',
                      'derivedProduction':0,
                      'transformationFamily':0,
                      'previousProd': 1
                      },

                   3:{
                      'productionType':'Merge',
                      'stepsInProd':[987],
                      'bkQuery': 'fromPreviousProd',
                      'removeInputsFlag': True,
                      'tracking':1,
                      'outputSE': 'Tier1-DST',
                      'priority': 9,
                      'cpu': 10000,
                      'outputFileMask':'',
                      'input': [],
                      'target':'',
                      'groupSize': 2,
                      'plugin': 'BySize',
                      'inputDataPolicy':'download',
                      'derivedProduction':0,
                      'transformationFamily':0,
                      'previousProd': 1
                      }
                   }
    self.maxDiff = None
    self.assertEqual( res, resExpected )


  def test__getBKKQuery( self ):

    pr = ProductionRequest( self.bkClientMock, self.diracProdIn )
    pr.dataTakingConditions = 'dataTC'
    pr.processingPass = 'procePass'
    pr.dqFlag = 'OK,AA, BB'
    pr.startRun = '123'
    pr.endRun = '456'
    res = pr._getBKKQuery()

    resExpected = {'DataTakingConditions':'dataTC',
                   'ProcessingPass':'procePass',
                   'FileType':'',
                   'EventType':'',
                   'ConfigName':'test',
                   'ConfigVersion':'certification',
                   'DataQualityFlag':'OK;;;AA;;;BB',
                   'StartRun':123,
                   'EndRun':456
                   }
    self.assertEqual( res, resExpected )

    pr = ProductionRequest( self.bkClientMock, self.diracProdIn )
    pr.dqFlag = 'OK,AA, BB'
    res = pr._getBKKQuery( 'fromPreviousProd', 'type', 123 )
    resExpected = {'ProductionID':123,
                   'FileType':'type',
                   'EventType':'',
                   'DataQualityFlag':'OK;;;AA;;;BB'}
    self.assertEqual( res, resExpected )

  def test__splitIntoProductionSteps( self ):

    stepStripp = {'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': 13718, 'ApplicationVersion': 'v28r3p1',
                  'ExtraPackages': 'AppConfig.v3r104', 'StepName': 'Stripping14-Merging',
                  'ProcessingPass': 'Merging', 'Visible': 'N', 'DDDB': 'head-20110302',
                  'OptionFiles': '$APPCONFIGOPTS/Merging/DV-Stripping14-Merging.py', 'CONDDB': 'head-20110407',
                  'fileTypesIn': ['SDST'],
                  'fileTypesOut': ['BHADRON.DST', 'CALIBRATION.DST', 'CHARM.MDST', 'CHARMCOMPLETEEVENT.DST']}


    r = _splitIntoProductionSteps( stepStripp )

    r_exp = [{'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': 13718, 'ApplicationVersion': 'v28r3p1',
              'ExtraPackages': 'AppConfig.v3r104', 'StepName': 'Stripping14-Merging',
              'ProcessingPass': 'Merging', 'Visible': 'N', 'DDDB': 'head-20110302',
              'OptionFiles': '$APPCONFIGOPTS/Merging/DV-Stripping14-Merging.py', 'CONDDB': 'head-20110407',
              'fileTypesIn': ['SDST'],
              'fileTypesOut': ['BHADRON.DST', 'CALIBRATION.DST', 'CHARM.MDST', 'CHARMCOMPLETEEVENT.DST']}]


    self.assertEqual( r, r_exp )

    stepMerge = {'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': 13718, 'ApplicationVersion': 'v28r3p1',
                 'ExtraPackages': 'AppConfig.v3r104', 'StepName': 'Stripping14-Merging',
                 'ProcessingPass': 'Merging', 'Visible': 'N', 'DDDB': 'head-20110302',
                 'OptionFiles': '$APPCONFIGOPTS/Merging/DV-Stripping14-Merging.py', 'CONDDB': 'head-20110407',
                 'fileTypesIn': ['BHADRON.DST', 'CALIBRATION.DST'],
                 'fileTypesOut': ['BHADRON.DST', 'CALIBRATION.DST']}


    r = _splitIntoProductionSteps( stepMerge )

    r_exp = [{'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': 13718, 'ApplicationVersion': 'v28r3p1',
              'ExtraPackages': 'AppConfig.v3r104', 'StepName': 'Stripping14-Merging',
              'ProcessingPass': 'Merging', 'Visible': 'N', 'DDDB': 'head-20110302',
              'OptionFiles': '$APPCONFIGOPTS/Merging/DV-Stripping14-Merging.py', 'CONDDB': 'head-20110407',
              'fileTypesIn': ['BHADRON.DST'],
              'fileTypesOut': ['BHADRON.DST']},
             {'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': 13718, 'ApplicationVersion': 'v28r3p1',
              'ExtraPackages': 'AppConfig.v3r104', 'StepName': 'Stripping14-Merging',
              'ProcessingPass': 'Merging', 'Visible': 'N', 'DDDB': 'head-20110302',
              'OptionFiles': '$APPCONFIGOPTS/Merging/DV-Stripping14-Merging.py', 'CONDDB': 'head-20110407',
              'fileTypesIn': ['CALIBRATION.DST'],
              'fileTypesOut': ['CALIBRATION.DST']}
             ]

    self.assertEqual( r, r_exp )

class ProductionRequestFailure( ClientTestCase ):

  def test__getBKKQuery( self ):

    pr = ProductionRequest( self.bkClientMock, self.diracProdIn )

    pr.runsList = ['1', '2']
    self.assertRaises( ValueError, pr._getBKKQuery )

  def test_resolveStepsFailure( self ):
    pr = ProductionRequest( self.bkClientMock, self.diracProdIn )
    pr.stepsList = ['123']
    self.bkClientMock.getAvailableSteps.return_value = {'OK': False,
                                                        'Message': 'error'}
    self.assertRaises( ValueError, pr.resolveSteps )

    self.bkClientMock.getStepInputFiles.return_value = {'OK': False,
                                                        'Message': 'error'}
    self.assertRaises( ValueError, pr.resolveSteps )


class ProductionRequestFullChain( ClientTestCase ):

  def test_full( self ):

    pr = ProductionRequest( self.bkClientFake, self.diracProdIn )
    pr.logger.setLevel( 'VERBOSE' )

    stepsList = [ '123' ]
    stepsList.append( '456' )
    stepsList.append( '' )
    stepsList.append( '' )
    stepsList.append( '' )
    pr.stepsList = stepsList
    pr.resolveSteps()

    pr.appendName = '1'
    pr.configName = 'MC'
    pr.configVersion = 'MC11a'

    pr.events = '100'
    pr.sysConfig = 'i686-slc5-gcc43-opt'

    pr.extend = '100'

    pr.eventType = '11124001'
    pr.parentRequestID = '34'
    pr.requestID = '35'

    pr.prodGroup = 'Sim05/Trig0x40760037Flagged/Reco12a/Stripping17Flagged'
    pr.dataTakingConditions = 'Beam3500GeV-2011-MagDown-Nu2-EmNoCuts'

    pr.prodsTypeList = ['Stripping', 'Merge']
    pr.outputSEs = ['Tier1_MC-DST', 'Tier1_MC-DST']
    pr.stepsInProds = [range( 1, len( pr.stepsList ) ), [len( pr.stepsList )]]
    pr.removeInputsFlags = [False, True]
    pr.priorities = [1, 6]
    pr.cpus = [1000, 100]
    pr.outputFileMasks = ['FOO', '']
    pr.targets = ['Tier2', '']
    pr.groupSizes = [1, 5]
    pr.plugins = ['', 'BySize']
    pr.inputDataPolicies = ['', 'protocol']

    res = pr.buildAndLaunchRequest()

    self.assertEqual( res, {'OK':True, 'Value': [321, 321]} )

#############################################################################
# Test Suite run 
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ClientTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProductionRequestSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProductionRequestFailure ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProductionRequestFullChain ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
