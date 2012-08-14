import unittest
from mock import Mock

from LHCbDIRAC.ProductionManagementSystem.Client.ProductionRequest import ProductionRequest, _splitIntoProductionSteps

class ClientTestCase( unittest.TestCase ):
  """ Base class for the Client test cases
  """
  def setUp( self ):

    self.bkClientMock = Mock()
    self.diracProdIn = Mock()

#############################################################################
# TemplatesUtilities.py
#############################################################################

class ProductionRequestSuccess( ClientTestCase ):

  def test_resolveStepsSuccess( self ):


    self.bkClientMock.getAvailableSteps.return_value = {'OK': True,
                                                        'Value': {'TotalRecords': 1,
                                                                  'ParameterNames': ['StepId', 'beta', 'gamma'],
                                                                  'Records': [[13698, 'Stripping14-Stripping', 'DaVinci']]}}
    self.bkClientMock.getStepInputFiles.return_value = {'OK': True,
                                                        'Value': {'TotalRecords': 7,
                                                                  'ParameterNames': ['FileType', 'Visible'],
                                                                  'Records': [['BHADRON.DST', 'Y'], ['CALIBRATION.DST', 'Y']]}}
    self.bkClientMock.getStepOutputFiles.return_value = {'OK': True,
                                                        'Value': {'TotalRecords': 7,
                                                                  'ParameterNames': ['FileType', 'Visible'],
                                                                  'Records': [['SDST', 'Y'], ['CALIBRATION.DST', 'Y']]}}

    pr = ProductionRequest( self.bkClientMock, self.diracProdIn )
    pr.stepsList = ['123']
    pr.resolveSteps()
    self.assertEqual( pr.stepsListDict, [{'StepId': 13698, 'beta':'Stripping14-Stripping', 'gamma':'DaVinci',
                            'fileTypesIn':['BHADRON.DST', 'CALIBRATION.DST'],
                            'fileTypesOut':['SDST', 'CALIBRATION.DST']}] )
    pr = ProductionRequest( self.bkClientMock, self.diracProdIn )
    pr.stepsList = ['123', '456']
    pr.resolveSteps()
    self.assertEqual( pr.stepsListDict, [{'StepId': 13698, 'beta':'Stripping14-Stripping', 'gamma':'DaVinci',
                            'fileTypesIn':['BHADRON.DST', 'CALIBRATION.DST'],
                            'fileTypesOut':['SDST', 'CALIBRATION.DST']},
                           {'StepId': 13698, 'beta':'Stripping14-Stripping', 'gamma':'DaVinci',
                            'fileTypesIn':['BHADRON.DST', 'CALIBRATION.DST'],
                            'fileTypesOut':['SDST', 'CALIBRATION.DST']}
                           ] )
    pr = ProductionRequest( self.bkClientMock, self.diracProdIn )
    pr.stepsList = ['123', '456', '', '']
    pr.resolveSteps()
    self.assertEqual( pr.stepsListDict, [{'StepId': 13698, 'beta':'Stripping14-Stripping', 'gamma':'DaVinci',
                            'fileTypesIn':['BHADRON.DST', 'CALIBRATION.DST'],
                            'fileTypesOut':['SDST', 'CALIBRATION.DST']},
                           {'StepId': 13698, 'beta':'Stripping14-Stripping', 'gamma':'DaVinci',
                            'fileTypesIn':['BHADRON.DST', 'CALIBRATION.DST'],
                            'fileTypesOut':['SDST', 'CALIBRATION.DST']}
                           ] )

    self.bkClientMock.getStepInputFiles.return_value = {'OK': True,
                                                        'Value': {'TotalRecords': 7,
                                                                  'ParameterNames': ['FileType', 'Visible'],
                                                                  'Records': [['BHADRON.DST', 'Y']]}}
    pr = ProductionRequest( self.bkClientMock, self.diracProdIn )
    pr.stepsList = ['123']
    pr.resolveSteps()
    self.assertEqual( pr.stepsListDict, [{'StepId': 13698, 'beta':'Stripping14-Stripping', 'gamma':'DaVinci',
                            'fileTypesIn':['BHADRON.DST'],
                            'fileTypesOut':['SDST', 'CALIBRATION.DST']}] )

    self.bkClientMock.getStepInputFiles.return_value = {'OK': True,
                                                        'Value': {'TotalRecords': 0,
                                                                  'ParameterNames': ['FileType', 'Visible'],
                                                                  'Records': []}}
    pr = ProductionRequest( self.bkClientMock, self.diracProdIn )
    pr.stepsList = ['123']
    pr.resolveSteps()
    self.assertEqual( pr.stepsListDict, [{'StepId': 13698, 'beta':'Stripping14-Stripping', 'gamma':'DaVinci',
                            'fileTypesIn':[],
                            'fileTypesOut':['SDST', 'CALIBRATION.DST']}] )

  def test_resolveStepsFailure( self ):
    pr = ProductionRequest( self.bkClientMock, self.diracProdIn )
    pr.stepsList = ['123']
    self.bkClientMock.getAvailableSteps.return_value = {'OK': False,
                                                        'Message': 'error'}
    self.assertRaises( ValueError, pr.resolveSteps )

    self.bkClientMock.getStepInputFiles.return_value = {'OK': False,
                                                        'Message': 'error'}
    self.assertRaises( ValueError, pr.resolveSteps )

  def test__applyOptionalCorrections( self ):

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
    pr._applyOptionalCorrections()
    prodsTypeListExpected = ['DataStripping', 'Merge', 'Merge', 'Merge']
    pluginsExpected = ['ByRun', 'BySize', 'BySize', 'BySize']
    stepsListDictExpected = [stepStripp, mergeStepBHADRON, mergeStepCALIBRA, mergeStepPIDMDST ]
    stepsInProdExpected = [[1], [2], [3], [4]]
    outputSEsExpected = ['Tier1-DST', 'Tier1-M-DST', 'Tier1-M-DST', 'Tier1-M-DST']
    prioritiesExpected = [1, 4, 4, 4]
    cpusExpected = [10, 100, 100, 100]
    self.assertEqual( pr.prodsTypeList, prodsTypeListExpected )
    self.assertEqual( pr.plugins, pluginsExpected )
    self.assertEqual( pr.stepsListDict, stepsListDictExpected )
    self.assertEqual( pr.stepsInProds, stepsInProdExpected )
    self.assertEqual( pr.outputSEs, outputSEsExpected )
    self.assertEqual( pr.priorities, prioritiesExpected )
    self.assertEqual( pr.cpus, cpusExpected )

    pr.prodsTypeList = ['DataStripping', 'Merge']
    pr.plugins = ['ByRun', 'ByRunFileTypeSizeWithFlush']
    pr.stepsListDict = [stepStripp, mergeStep]
    pr.stepsInProds = [[1], [2]]
    pr.outputSEs = [ 'Tier1-DST', 'Tier1-M-DST']
    pr.priorities = [1, 4]
    pr.cpus = [10, 100]
    pr._applyOptionalCorrections()
    prodsTypeListExpected = ['DataStripping', 'Merge']
    pluginsExpected = ['ByRun', 'ByRunFileTypeSizeWithFlush']
    stepsListDictExpected = [stepStripp, mergeStep ]
    stepsInProdExpected = [[1], [2]]
    outputSEsExpected = ['Tier1-DST', 'Tier1-M-DST']
    prioritiesExpected = [1, 4]
    cpusExpected = [10, 100]
    self.assertEqual( pr.prodsTypeList, prodsTypeListExpected )
    self.assertEqual( pr.plugins, pluginsExpected )
    self.assertEqual( pr.stepsListDict, stepsListDictExpected )
    self.assertEqual( pr.stepsInProds, stepsInProdExpected )
    self.assertEqual( pr.outputSEs, outputSEsExpected )
    self.assertEqual( pr.priorities, prioritiesExpected )
    self.assertEqual( pr.cpus, cpusExpected )

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
    self.assertEqual( pr.prodsTypeList, prodsTypeListExpected )
    self.assertEqual( pr.plugins, pluginsExpected )
    self.assertEqual( pr.stepsListDict, stepsListDictExpected )
    self.assertEqual( pr.stepsInProds, stepsInProdExpected )
    self.assertEqual( pr.outputSEs, outputSEsExpected )
    self.assertEqual( pr.priorities, prioritiesExpected )
    self.assertEqual( pr.cpus, cpusExpected )

    pr.prodsTypeList = ['DataStripping']
    pr.plugins = ['ByRun']
    pr.stepsListDict = [stepStripp, mergeStep]
    pr.stepsInProds = [[1, 2]]
    pr.priorities = [1]
    pr.outputSEs = [ 'Tier1-DST']
    pr.cpus = [10]
    pr._applyOptionalCorrections()
    prodsTypeListExpected = ['DataStripping']
    pluginsExpected = ['ByRun']
    stepsListDictExpected = [stepStripp, mergeStep ]
    stepsInProdExpected = [[1, 2]]
    outputSEsExpected = ['Tier1-DST']
    prioritiesExpected = [1]
    cpusExpected = [10]
    self.assertEqual( pr.prodsTypeList, prodsTypeListExpected )
    self.assertEqual( pr.plugins, pluginsExpected )
    self.assertEqual( pr.stepsListDict, stepsListDictExpected )
    self.assertEqual( pr.stepsInProds, stepsInProdExpected )
    self.assertEqual( pr.outputSEs, outputSEsExpected )
    self.assertEqual( pr.priorities, prioritiesExpected )
    self.assertEqual( pr.cpus, cpusExpected )

    pr.prodsTypeList = ['DataStripping', 'Merge', 'Merge']
    pr.plugins = ['ByRun', 'BySize', 'ByRunFileTypeSizeWithFlush']
    pr.stepsListDict = [stepStripp, mergeStep, mergeStep]
    pr.stepsInProds = [[1], [2], [3]]
    pr.outputSEs = [ 'Tier1-DST', 'Tier1-M-DST', 'Tier1-M-DST']
    pr.priorities = [1, 4, 5]
    pr.cpus = [10, 100, 1000]
    pr._applyOptionalCorrections()
    prodsTypeListExpected = ['DataStripping', 'Merge', 'Merge', 'Merge', 'Merge']
    pluginsExpected = ['ByRun', 'BySize', 'BySize', 'BySize', 'ByRunFileTypeSizeWithFlush']
    stepsListDictExpected = [stepStripp, mergeStepBHADRON, mergeStepCALIBRA, mergeStepPIDMDST, mergeStep ]
    stepsInProdExpected = [[1], [2], [3], [4], [5]]
    outputSEsExpected = ['Tier1-DST', 'Tier1-M-DST', 'Tier1-M-DST', 'Tier1-M-DST', 'Tier1-M-DST']
    prioritiesExpected = [1, 4, 4, 4, 5]
    cpusExpected = [10, 100, 100, 100, 1000]
    self.assertEqual( pr.prodsTypeList, prodsTypeListExpected )
    self.assertEqual( pr.plugins, pluginsExpected )
    self.assertEqual( pr.stepsListDict, stepsListDictExpected )
    self.assertEqual( pr.stepsInProds, stepsInProdExpected )
    self.assertEqual( pr.outputSEs, outputSEsExpected )
    self.assertEqual( pr.priorities, prioritiesExpected )
    self.assertEqual( pr.cpus, cpusExpected )

    pr = ProductionRequest( self.bkClientMock, self.diracProdIn )
    pr.prodsTypeList = ['DataStripping', 'Merge']
    pr.plugins = ['ByRun', 'BySize']
    pr.stepsListDict = [stepStripp, mergeStep]
    pr.stepsInProds = [[3], [4]]
    pr.outputSEs = [ 'Tier1-DST', 'Tier1-M-DST']
    pr.priorities = [1, 4]
    pr.cpus = [10, 100]
    pr._applyOptionalCorrections()
    prodsTypeListExpected = ['DataStripping', 'Merge', 'Merge', 'Merge']
    pluginsExpected = ['ByRun', 'BySize', 'BySize', 'BySize']
    stepsListDictExpected = [stepStripp, mergeStepBHADRON, mergeStepCALIBRA, mergeStepPIDMDST ]
    stepsInProdExpected = [[3], [4], [5], [6]]
    outputSEsExpected = ['Tier1-DST', 'Tier1-M-DST', 'Tier1-M-DST', 'Tier1-M-DST']
    prioritiesExpected = [1, 4, 4, 4]
    cpusExpected = [10, 100, 100, 100]
    self.assertEqual( pr.prodsTypeList, prodsTypeListExpected )
    self.assertEqual( pr.plugins, pluginsExpected )
    self.assertEqual( pr.stepsListDict, stepsListDictExpected )
    self.assertEqual( pr.stepsInProds, stepsInProdExpected )
    self.assertEqual( pr.outputSEs, outputSEsExpected )
    self.assertEqual( pr.priorities, prioritiesExpected )
    self.assertEqual( pr.cpus, cpusExpected )

    pr.prodsTypeList = ['DataStripping']
    pr.plugins = ['ByRun']
    pr.stepsListDict = [stepStripp, mergeStep]
    pr.stepsInProds = [[3, 4]]
    pr.priorities = [1]
    pr.outputSEs = [ 'Tier1-DST']
    pr.cpus = [10]
    pr._applyOptionalCorrections()
    prodsTypeListExpected = ['DataStripping']
    pluginsExpected = ['ByRun']
    stepsListDictExpected = [stepStripp, mergeStep ]
    stepsInProdExpected = [[3, 4]]
    outputSEsExpected = ['Tier1-DST']
    prioritiesExpected = [1]
    cpusExpected = [10]
    self.assertEqual( pr.prodsTypeList, prodsTypeListExpected )
    self.assertEqual( pr.plugins, pluginsExpected )
    self.assertEqual( pr.stepsListDict, stepsListDictExpected )
    self.assertEqual( pr.stepsInProds, stepsInProdExpected )
    self.assertEqual( pr.outputSEs, outputSEsExpected )
    self.assertEqual( pr.priorities, prioritiesExpected )
    self.assertEqual( pr.cpus, cpusExpected )

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
    self.assertEqual( pr.prodsTypeList, prodsTypeListExpected )
    self.assertEqual( pr.plugins, pluginsExpected )
    self.assertEqual( pr.stepsListDict, stepsListDictExpected )
    self.assertEqual( pr.stepsInProds, stepsInProdExpected )
    self.assertEqual( pr.outputSEs, outputSEsExpected )
    self.assertEqual( pr.priorities, prioritiesExpected )
    self.assertEqual( pr.cpus, cpusExpected )

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
    pr.bkQuery = {'P':1, 'Q':'abc'}
    pr.outputSEs = ['Tier1-BUFFER', 'Tier1-DST', 'Tier1-DST']
    pr.priorities = [5, 8, 9]
    pr.cpus = [1000000, 300000, 10000]
    pr.plugins = ['ByRun', 'BySize', 'BySize']

    res = pr._getProdsDescriptionDict()

    resExpected = {1:{
                      'productionType':'DataStripping',
                      'stepsInProd':[123, 456],
                      'bkQuery': pr.bkQuery,
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
                      'transformationFamily':0
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
                      'transformationFamily':0
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
                      'transformationFamily':0
                      }
                   }
    self.maxDiff = None
    self.assertEqual( res, resExpected )


  def test__buildFullBKKQuery( self ):

    pr = ProductionRequest( self.bkClientMock, self.diracProdIn )
    pr.dataTakingConditions = 'dataTC'
    pr.processingPass = 'procePass'
    pr.dqFlag = 'OK,AA, BB'
    pr.startRun = '123'
    pr.endRun = '456'
    pr._buildFullBKKQuery()

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
    self.assertEqual( pr.bkQuery, resExpected )

    pr.runsList = ['1', '2']
    self.assertRaises( ValueError, pr._buildFullBKKQuery )

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

#############################################################################
# Test Suite run 
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ClientTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProductionRequestSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
