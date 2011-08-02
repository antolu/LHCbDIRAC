import unittest
from DIRAC.ResourceStatusSystem.Utilities.mock import Mock
from LHCbDIRAC.Workflow.Templates.TemplatesUtilities import resolveSteps, _splitIntoProductionSteps

class TemplatesTestCase( unittest.TestCase ):
  """ Base class for the Templates test cases
  """
  def setUp( self ):

    self.bkClientMock = Mock()

#############################################################################
# TemplatesUtilities.py
#############################################################################

class TemplatesUtilitiesBaseSuccess( TemplatesTestCase ):

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

    res = resolveSteps( ['123'], BKKClientIn = self.bkClientMock )
    self.assertEqual( res, [{'alfa': 13698, 'beta':'Stripping14-Stripping', 'gamma':'DaVinci',
                            'fileTypesIn':['BHADRON.DST', 'CALIBRATION.DST'],
                            'fileTypesOut':['SDST', 'CALIBRATION.DST']}] )
    res = resolveSteps( ['123', '456'], BKKClientIn = self.bkClientMock )
    self.assertEqual( res, [{'alfa': 13698, 'beta':'Stripping14-Stripping', 'gamma':'DaVinci',
                            'fileTypesIn':['BHADRON.DST', 'CALIBRATION.DST'],
                            'fileTypesOut':['SDST', 'CALIBRATION.DST']},
                           {'alfa': 13698, 'beta':'Stripping14-Stripping', 'gamma':'DaVinci',
                            'fileTypesIn':['BHADRON.DST', 'CALIBRATION.DST'],
                            'fileTypesOut':['SDST', 'CALIBRATION.DST']}
                           ] )
    res = resolveSteps( ['123', '456', '', ''], BKKClientIn = self.bkClientMock )
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
    res = resolveSteps( ['123'], BKKClientIn = self.bkClientMock )
    self.assertEqual( res, [{'alfa': 13698, 'beta':'Stripping14-Stripping', 'gamma':'DaVinci',
                            'fileTypesIn':['BHADRON.DST'],
                            'fileTypesOut':['SDST', 'CALIBRATION.DST']}] )

    self.bkClientMock.getStepInputFiles.return_value = {'OK': True,
                                                        'Value': {'TotalRecords': 0,
                                                                  'ParameterNames': ['FileType', 'Visible'],
                                                                  'Records': []}}
    res = resolveSteps( ['123'], BKKClientIn = self.bkClientMock )
    self.assertEqual( res, [{'alfa': 13698, 'beta':'Stripping14-Stripping', 'gamma':'DaVinci',
                            'fileTypesIn':[],
                            'fileTypesOut':['SDST', 'CALIBRATION.DST']}] )

  def test_resolveStepsFailure( self ):
    self.bkClientMock.getAvailableSteps.return_value = {'OK': False,
                                                        'Message': 'error'}
    self.assertRaises( ValueError, resolveSteps, ['123'], self.bkClientMock )

    self.bkClientMock.getStepInputFiles.return_value = {'OK': False,
                                                        'Message': 'error'}
    self.assertRaises( ValueError, resolveSteps, ['123'], self.bkClientMock )

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
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TemplatesTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TemplatesUtilitiesBaseSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
