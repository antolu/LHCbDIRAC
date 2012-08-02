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



  def test_getProdsDescriptionDict( self ):
    pr = ProductionRequest( self.bkClientMock, self.diracProdIn )
    pr.stepsList = [123, 456, 789]
    pr.prodsTypeList = ['DataStripping', 'Merge']
    pr.stepsInProds = [[1, 2], [3]]
    pr.bkQuery = {'P':1, 'Q':'abc'}
    pr.outputSEs = ['Tier1-BUFFER', 'Tier1-DST']
    pr.priorities = [5, 8]
    pr.CPUs = [1000000, 300000]
    pr.plugins = ['ByRun', 'BySize']

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
                      'derivedProduction':0
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
                      'derivedProduction':0
                      }
                   }

    self.assertEqual( res, resExpected )


  def test__buildFullBKKQuery( self ):

    pr = ProductionRequest( self.bkClientMock, self.diracProdIn )
    pr.dataTakingConditions = 'dataTC'
    pr.processingPass = 'procePass'
    pr.DQFlag = 'OK,AA, BB'
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

    stepStripp = [{'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': 13718, 'ApplicationVersion': 'v28r3p1',
                  'ExtraPackages': 'AppConfig.v3r104', 'StepName': 'Stripping14-Merging',
                  'ProcessingPass': 'Merging', 'Visible': 'N', 'DDDB': 'head-20110302',
                  'OptionFiles': '$APPCONFIGOPTS/Merging/DV-Stripping14-Merging.py', 'CONDDB': 'head-20110407',
                  'fileTypesIn': ['SDST'],
                  'fileTypesOut': ['BHADRON.DST', 'CALIBRATION.DST', 'CHARM.MDST', 'CHARMCOMPLETEEVENT.DST']}
                  ]

    r = _splitIntoProductionSteps( stepStripp )

    r_exp = [{'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': 13718, 'ApplicationVersion': 'v28r3p1',
              'ExtraPackages': 'AppConfig.v3r104', 'StepName': 'Stripping14-Merging',
              'ProcessingPass': 'Merging', 'Visible': 'N', 'DDDB': 'head-20110302',
              'OptionFiles': '$APPCONFIGOPTS/Merging/DV-Stripping14-Merging.py', 'CONDDB': 'head-20110407',
              'fileTypesIn': ['SDST'],
              'fileTypesOut': ['BHADRON.DST', 'CALIBRATION.DST', 'CHARM.MDST', 'CHARMCOMPLETEEVENT.DST']}
             ]

    self.assertEqual( r, r_exp )

    stepMerge = [{'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': 13718, 'ApplicationVersion': 'v28r3p1',
                 'ExtraPackages': 'AppConfig.v3r104', 'StepName': 'Stripping14-Merging',
                 'ProcessingPass': 'Merging', 'Visible': 'N', 'DDDB': 'head-20110302',
                 'OptionFiles': '$APPCONFIGOPTS/Merging/DV-Stripping14-Merging.py', 'CONDDB': 'head-20110407',
                 'fileTypesIn': ['BHADRON.DST', 'CALIBRATION.DST'],
                 'fileTypesOut': ['BHADRON.DST', 'CALIBRATION.DST']}
                 ]

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

    stepsList = [{'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': 13718, 'ApplicationVersion': 'v28r3p1',
                  'ExtraPackages': 'AppConfig.v3r104', 'StepName': 'Stripping14-Merging',
                  'ProcessingPass': 'Merging', 'Visible': 'N', 'DDDB': 'head-20110302',
                  'OptionFiles': '$APPCONFIGOPTS/Merging/DV-Stripping14-Merging.py', 'CONDDB': 'head-20110407',
                  'fileTypesIn': ['SDST'],
                  'fileTypesOut': ['BHADRON.DST', 'CALIBRATION.DST']},
                 {'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': 13718, 'ApplicationVersion': 'v28r3p1',
                 'ExtraPackages': 'AppConfig.v3r104', 'StepName': 'Stripping14-Merging',
                 'ProcessingPass': 'Merging', 'Visible': 'N', 'DDDB': 'head-20110302',
                 'OptionFiles': '$APPCONFIGOPTS/Merging/DV-Stripping14-Merging.py', 'CONDDB': 'head-20110407',
                 'fileTypesIn': ['BHADRON.DST', 'CALIBRATION.DST'],
                 'fileTypesOut': ['BHADRON.DST', 'CALIBRATION.DST']}
                 ]

    r = _splitIntoProductionSteps( stepsList )

    r_exp = [{'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': 13718, 'ApplicationVersion': 'v28r3p1',
              'ExtraPackages': 'AppConfig.v3r104', 'StepName': 'Stripping14-Merging',
              'ProcessingPass': 'Merging', 'Visible': 'N', 'DDDB': 'head-20110302',
              'OptionFiles': '$APPCONFIGOPTS/Merging/DV-Stripping14-Merging.py', 'CONDDB': 'head-20110407',
              'fileTypesIn': ['SDST'],
              'fileTypesOut': ['BHADRON.DST', 'CALIBRATION.DST']},
             {'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': 13718, 'ApplicationVersion': 'v28r3p1',
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
