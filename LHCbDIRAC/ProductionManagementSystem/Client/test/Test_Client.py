import unittest
from mock import Mock

from LHCbDIRAC.ProductionManagementSystem.Client.ProductionRequest import ProductionRequest

class ClientTestCase( unittest.TestCase ):
  """ Base class for the Client test cases
  """
  def setUp( self ):

    self.bkClientMock = Mock()

#############################################################################
# TemplatesUtilities.py
#############################################################################

class ProductionRequestSuccess( ClientTestCase ):

  def test_resolveStepsSuccess( self ):


    self.bkClientMock.getAvailableSteps.return_value = {'OK': True,
                                                        'Value': {'TotalRecords': 1,
                                                                  'ParameterNames': ['alfa', 'beta', 'gamma'],
                                                                  'Records': [[13698, 'Stripping14-Stripping', 'DaVinci']]}}
    self.bkClientMock.getStepInputFiles.return_value = {'OK': True,
                                                        'Value': {'TotalRecords': 7,
                                                                  'ParameterNames': ['FileType', 'Visible'],
                                                                  'Records': [['BHADRON.DST', 'Y'], ['CALIBRATION.DST', 'Y']]}}
    self.bkClientMock.getStepOutputFiles.return_value = {'OK': True,
                                                        'Value': {'TotalRecords': 7,
                                                                  'ParameterNames': ['FileType', 'Visible'],
                                                                  'Records': [['SDST', 'Y'], ['CALIBRATION.DST', 'Y']]}}

    pr = ProductionRequest( self.bkClientMock )
    pr.stepsList = ['123']
    res = pr.resolveSteps()
    self.assertEqual( res, [{'alfa': 13698, 'beta':'Stripping14-Stripping', 'gamma':'DaVinci',
                            'fileTypesIn':['BHADRON.DST', 'CALIBRATION.DST'],
                            'fileTypesOut':['SDST', 'CALIBRATION.DST']}] )
    pr.stepsList = ['123', '456']
    res = pr.resolveSteps()
    self.assertEqual( res, [{'alfa': 13698, 'beta':'Stripping14-Stripping', 'gamma':'DaVinci',
                            'fileTypesIn':['BHADRON.DST', 'CALIBRATION.DST'],
                            'fileTypesOut':['SDST', 'CALIBRATION.DST']},
                           {'alfa': 13698, 'beta':'Stripping14-Stripping', 'gamma':'DaVinci',
                            'fileTypesIn':['BHADRON.DST', 'CALIBRATION.DST'],
                            'fileTypesOut':['SDST', 'CALIBRATION.DST']}
                           ] )
    pr.stepsList = ['123', '456', '', '']
    res = pr.resolveSteps()
    self.assertEqual( res, [{'alfa': 13698, 'beta':'Stripping14-Stripping', 'gamma':'DaVinci',
                            'fileTypesIn':['BHADRON.DST', 'CALIBRATION.DST'],
                            'fileTypesOut':['SDST', 'CALIBRATION.DST']},
                           {'alfa': 13698, 'beta':'Stripping14-Stripping', 'gamma':'DaVinci',
                            'fileTypesIn':['BHADRON.DST', 'CALIBRATION.DST'],
                            'fileTypesOut':['SDST', 'CALIBRATION.DST']}
                           ] )

    self.bkClientMock.getStepInputFiles.return_value = {'OK': True,
                                                        'Value': {'TotalRecords': 7,
                                                                  'ParameterNames': ['FileType', 'Visible'],
                                                                  'Records': [['BHADRON.DST', 'Y']]}}
    pr.stepsList = ['123']
    res = pr.resolveSteps()
    self.assertEqual( res, [{'alfa': 13698, 'beta':'Stripping14-Stripping', 'gamma':'DaVinci',
                            'fileTypesIn':['BHADRON.DST'],
                            'fileTypesOut':['SDST', 'CALIBRATION.DST']}] )

    self.bkClientMock.getStepInputFiles.return_value = {'OK': True,
                                                        'Value': {'TotalRecords': 0,
                                                                  'ParameterNames': ['FileType', 'Visible'],
                                                                  'Records': []}}
    res = pr.resolveSteps()
    self.assertEqual( res, [{'alfa': 13698, 'beta':'Stripping14-Stripping', 'gamma':'DaVinci',
                            'fileTypesIn':[],
                            'fileTypesOut':['SDST', 'CALIBRATION.DST']}] )

  def test_resolveStepsFailure( self ):
    pr = ProductionRequest( self.bkClientMock )
    pr.stepsList = ['123']
    self.bkClientMock.getAvailableSteps.return_value = {'OK': False,
                                                        'Message': 'error'}
    self.assertRaises( ValueError, pr.resolveSteps )

    self.bkClientMock.getStepInputFiles.return_value = {'OK': False,
                                                        'Message': 'error'}
    self.assertRaises( ValueError, pr.resolveSteps )



  def test_getProdsDescriptionDict( self ):
    pr = ProductionRequest( self.bkClientMock )
    pr.stepsList = [123, 456, 789]
    pr.prodsTypeList = ['DataStripping', 'Merge']
    pr.stepsInProds = [[1, 2], [3]]
    pr.bkQuery = {'P':1, 'Q':'abc'}
    pr.outputSEs = ['Tier1-BUFFER', 'Tier1-DST']
    pr.priorities = [5, 8]
    pr.CPUs = [1000000, 300000]

    res = pr._getProdsDescriptionDict()

    resExpected = {'DataStripping':{
                                    'stepsInProd':[123, 456],
                                    'bkQuery': pr.bkQuery,
                                    'removeInputsFlag': False,
                                    'tracking':0,
                                    'outputSE': 'Tier1-BUFFER',
                                    'priority': 5,
                                    'cpu': 1000000,
                                    'outputFileMask':'',
                                    'input': [],
                                    'target':''
                                    },

                   'Merge':{
                            'stepsInProd':[789],
                            'bkQuery': 'fromPreviousProd',
                            'removeInputsFlag': True,
                            'tracking':1,
                            'outputSE': 'Tier1-DST',
                            'priority': 8,
                            'cpu': 300000,
                            'outputFileMask':'',
                            'input': [],
                            'target':''
                            }
                   }

    self.assertEqual( res, resExpected )

#############################################################################
# Test Suite run 
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ClientTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProductionRequestSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
