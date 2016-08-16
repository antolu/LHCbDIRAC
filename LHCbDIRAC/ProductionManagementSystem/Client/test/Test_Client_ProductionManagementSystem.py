""" Test of the ProductionRequest and Production modules
"""

import unittest
from mock import Mock, MagicMock

from LHCbDIRAC.ProductionManagementSystem.Client.ProductionRequest import ProductionRequest, _splitIntoProductionSteps
from LHCbDIRAC.ProductionManagementSystem.Client.Production import Production

#pylint: disable=protected-access

class bkClientFake(object):
  def getAvailableSteps( self, stepID ):
    if stepID == {'StepId':123}:
      return {'OK': True,
              'Value': {'TotalRecords': 1,
                        'ParameterNames': ['StepId', 'StepName', 'ApplicationName', 'ApplicationVersion',
                                           'OptionFiles', 'Visible', 'ExtraPackages', 'ProcessingPass', 'OptionsFormat',
                                           'DDDB', 'CONDDB', 'DQTag', 'SystemConfig'],
                        'Records': [[123, 'Stripping14-Stripping', 'DaVinci', 'v2r2',
                                     'optsFiles', 'Yes', 'eps', 'procPass', '',
                                     '', '123456', '', '']]}}
    elif stepID in ( {'StepId':456}, {'StepId':789}, {'StepId':987} ):
      return {'OK': True,
              'Value': {'TotalRecords': 1,
                        'ParameterNames': ['StepId', 'StepName', 'ApplicationName', 'ApplicationVersion',
                                           'OptionFiles', 'Visible', 'ExtraPackages', 'ProcessingPass', 'OptionsFormat',
                                           'DDDB', 'CONDDB', 'DQTag', 'SystemConfig'],
                        'Records': [[456, 'Merge', 'LHCb', 'v1r2',
                                     'optsFiles', 'Yes', 'eps', 'procPass', '',
                                     '', 'fromPreviousStep', '', 'x86']]}}
    elif stepID == {'StepId':125080}:
      return {'OK': True,
              'Value': {'TotalRecords': 1,
                        'ParameterNames': ['StepId', 'StepName', 'ApplicationName', 'ApplicationVersion',
                                           'ExtraPackages', 'ProcessingPass', 'Visible', 'Usable',
                                           'DDDB', 'CONDDB', 'DQTag', 'OptionsFormat', 'OptionFiles',
                                           'isMulticore', 'SystemConfig', 'mcTCK', 'ExtraOptions'],
                        'Records': [[125080, 'Sim08a', 'Gauss', 'v45r3', 'AppConfig.v3r171', 'Sim08a', 'Y',
                                     'Yes', 'Sim08-20130503-1', 'Sim08-20130503-1-vc-mu100', '', '',
                                     '$APPCONFIGOPTS/Gauss/Sim08-Beam4000GeV-mu100-2012-nu2.5.py;$DECFILESROOT/options/11102400.py;$LBPYTHIA8ROOT/options/Pythia8.py;$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py',
                                     'N', 'x86_64-slc5-gcc43-opt', '', '']]}}
    elif stepID == {'StepId':124620}:
      return {'OK': True,
              'Value' : {'TotalRecords': 1,
                         'ParameterNames' : ['StepId', 'StepName', 'ApplicationName', 'ApplicationVersion',
                                             'ExtraPackages', 'ProcessingPass', 'Visible', 'Usable',
                                             'DDDB', 'CONDDB', 'DQTag', 'OptionsFormat', 'OptionFiles',
                                             'isMulticore', 'SystemConfig', 'mcTCK', 'ExtraOptions'],
                         'Records': [[124620, 'Digi13', 'Boole', 'v26r3', 'AppConfig.v3r164', 'Digi13', 'N',
                                      'Yes', 'Sim08-20130503-1', 'Sim08-20130503-1-vc-mu100', '', '',
                                      '$APPCONFIGOPTS/Boole/Default.py;$APPCONFIGOPTS/Boole/DataType-2012.py;$APPCONFIGOPTS/Boole/Boole-SiG4EnergyDeposit.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py',
                                      'N', 'x86_64-slc5-gcc43-opt', '', '']]}}

  def getStepInputFiles( self, stepID ):
    if stepID == 123:
      return {'OK': True, 'Value': {'TotalRecords': 7,
                                    'ParameterNames': ['FileType', 'Visible'],
                                    'Records': [['SDST', 'Y']]}}

    if stepID == 456:
      return {'OK': True, 'Value': {'TotalRecords': 7,
                                    'ParameterNames': ['FileType', 'Visible'],
                                    'Records': [['BHADRON.DST', 'Y'], ['CALIBRATION.DST', 'Y']]}}
    if stepID == 125080:
      return {'OK': True, 'Value': {'TotalRecords': 7,
                                    'ParameterNames': ['FileType', 'Visible'],
                                    'Records': [['', 'Y']]}}
    if stepID == 124620:
      return {'OK': True, 'Value': {'TotalRecords': 7,
                                    'ParameterNames': ['FileType', 'Visible'],
                                    'Records': [['SIM', 'N']]}}

  def getStepOutputFiles( self, stepID ):
    if stepID == 123:
      return {'OK': True, 'Value': {'TotalRecords': 7,
                                    'ParameterNames': ['FileType', 'Visible'],
                                    'Records': [['BHADRON.DST', 'Y'], ['CALIBRATION.DST', 'Y']]}}
    if stepID == 456:
      return {'OK': True, 'Value': {'TotalRecords': 7,
                                    'ParameterNames': ['FileType', 'Visible'],
                                    'Records': [['BHADRON.DST', 'Y'], ['CALIBRATION.DST', 'Y']]}}
    if stepID == 125080:
      return {'OK': True, 'Value': {'TotalRecords': 7,
                                    'ParameterNames': ['FileType', 'Visible'],
                                    'Records': [['SIM', 'Y']]}}
    if stepID == 124620:
      return {'OK': True, 'Value': {'TotalRecords': 7,
                                    'ParameterNames': ['FileType', 'Visible'],
                                    'Records': [['DIGI', 'N']]}}

class ClientTestCase( unittest.TestCase ):
  """ Base class for the Client test cases
  """
  def setUp( self ):

    self.bkClientMock = Mock()
    self.diracProdIn = Mock()
    self.diracProdIn.launchProduction.return_value = {'OK': True, 'Value': 321}

    self.bkClientFake = bkClientFake()

    self.maxDiff = None

#############################################################################
# Production.py
#############################################################################

class ProductionSuccess( ClientTestCase ):


  def test__constructOutputFilesList( self ):
    prod = Production()
    res = prod._constructOutputFilesList( ['T1', 'T2'] )
    resExpected = [{'outputDataType': 't1', 'outputDataName': '@{STEP_ID}.t1'},
                   {'outputDataType': 't2', 'outputDataName': '@{STEP_ID}.t2'}]
    self.assertEqual( res, resExpected )

    res = prod._constructOutputFilesList( ['T1', 'HIST'] )
    resExpected = [{'outputDataType': 't1', 'outputDataName': '@{STEP_ID}.t1'},
                   {'outputDataType': 'hist', 'outputDataName': '@{applicationName}_@{STEP_ID}.Hist.root'}]
    self.assertEqual( res, resExpected )

    res = prod._constructOutputFilesList( ['T1', 'HIST'] )
    resExpected = [{'outputDataType': 't1', 'outputDataName': '@{STEP_ID}.t1'},
                   {'outputDataType': 'hist', 'outputDataName': '@{applicationName}_@{STEP_ID}.Hist.root'}]
    self.assertEqual( res, resExpected )


#############################################################################
# ProductionRequest.py
#############################################################################

stepMC = {'StepId': 123, 'StepName':'Stripping14-Stripping',
          'ApplicationName':'Gauss', 'ApplicationVersion':'v2r2', 'ExtraOptions': '',
          'OptionFiles':'optsFiles', 'Visible':'Yes', 'ExtraPackages':'eps',
          'ProcessingPass':'procPass', 'OptionsFormat':'',
          'prodStepID': "123['SDST']", 'SystemConfig':'',
          'DDDB':'', 'CONDDB':'123456', 'DQTag':'', 'isMulticore': 'N',
          'mcTCK': '',
          'fileTypesIn':[],
          'fileTypesOut':['SIM']}

stepMC2 = {'StepId': 123, 'StepName':'Stripping14-Stripping',
           'ApplicationName':'DaVinci', 'ApplicationVersion':'v2r2', 'ExtraOptions': '',
           'OptionFiles':'optsFiles', 'Visible':'Yes', 'ExtraPackages':'eps',
           'ProcessingPass':'procPass', 'OptionsFormat':'',
           'prodStepID': "123['SDST']", 'SystemConfig':'',
           'DDDB':'', 'CONDDB':'123456', 'DQTag':'', 'isMulticore': 'N',
           'mcTCK': '',
           'fileTypesIn':['SIM'],
           'fileTypesOut':['DST']}

step1Dict = {'StepId': 123, 'StepName':'Stripping14-Stripping',
             'ApplicationName':'DaVinci', 'ApplicationVersion':'v2r2', 'ExtraOptions': '',
             'OptionFiles':'optsFiles', 'Visible':'Yes', 'ExtraPackages':'eps',
             'ProcessingPass':'procPass', 'OptionsFormat':'',
             'prodStepID': "123['SDST']", 'SystemConfig':'',
             'DDDB':'', 'CONDDB':'123456', 'DQTag':'', 'isMulticore': 'N',
             'mcTCK': '',
             'fileTypesIn':['SDST'],
             'fileTypesOut':['BHADRON.DST', 'CALIBRATION.DST']}

step2Dict = {'StepId': 456, 'StepName':'Merge',
             'ApplicationName':'LHCb', 'ApplicationVersion':'v1r2', 'ExtraOptions': '',
             'OptionFiles':'optsFiles', 'Visible':'Yes', 'ExtraPackages':'eps',
             'ProcessingPass':'procPass', 'OptionsFormat':'', 'SystemConfig':'x86',
             'prodStepID': "456['BHADRON.DST', 'CALIBRATION.DST']",
             'DDDB':'', 'CONDDB':'123456', 'DQTag':'', 'isMulticore': 'N',
             'mcTCK': '',
             'fileTypesIn':['BHADRON.DST', 'CALIBRATION.DST'],
             'fileTypesOut':['BHADRON.DST', 'CALIBRATION.DST']}

class ProductionRequestSuccess( ClientTestCase ):

  def test__mcSpecialCase( self ):
    pr = ProductionRequest( self.bkClientFake, self.diracProdIn )
    pr.tc = MagicMock()
    # prepare the test case
    prod = Production()
    prod.setParameter( 'ProcessingType', 'JDL', 'Test', 'ProductionGroupOrType' )
    prod.addApplicationStep( stepDict = stepMC,
                             modulesList = ['GaudiApplication', 'AnalyseLogFile'] )
    prod.addApplicationStep( stepDict = stepMC2,
                             inputData = 'previousStep',
                             modulesList = ['GaudiApplication', 'AnalyseLogFile'] )
    prod.priority = '1'
    prod.addFinalizationStep()
    prod.setFileMask( '', '4' )

    pr._modifyAndLaunchMCXML( prod, {'tracking':0} )
    for par in prod.LHCbJob.workflow.parameters:
      if par.getName() == 'Site':
        self.assertEqual( par.value, 'DIRAC.Test.ch' )
      if par.getName() == 'Numberofevents':
        self.assertEqual( par.value, '500' )
      if par.getName() == 'listoutput':
        self.assert_( 'gausshist' in dict( par.value ).values() )
      if par.getName() == 'outputDataStep':
        self.assertEqual( par.value, '1;4' )
      if par.getName() == 'outputDataFileMask':
        self.assertEqual( par.value, 'GAUSSHIST;DST' )

    # re-prepare the test case
    prod = Production()
    prod.setParameter( 'ProcessingType', 'JDL', 'Test', 'ProductionGroupOrType' )
    prod.addApplicationStep( stepDict = step1Dict,
                             inputData = 'previousStep',
                             modulesList = ['GaudiApplication', 'AnalyseLogFile'] )
    prod.addFinalizationStep()
    prod.setFileMask( '', ['4'] )

    pr._mcSpecialCase( prod, {'tracking':0} )
    for par in prod.LHCbJob.workflow.parameters:
      if par.getName() == 'Site':
        self.assertEqual( par.value, 'ANY' )
      if par.getName() == 'listoutput':
        self.assert_( 'gausshist' not in dict( par.value ).values() )
      if par.getName() == 'outputDataStep':
        self.assertEqual( par.value, '4' )
      if par.getName() == 'outputDataFileMask':
        self.assertEqual( par.value, '' )
      if par.getName() == 'Priority':
        self.assertEqual( par.value, '1' )

    # And again re-prepare the test case
    prod = Production()
    prod.setParameter( 'ProcessingType', 'JDL', 'Test', 'ProductionGroupOrType' )
    prod.addApplicationStep( stepDict = step1Dict,
                             inputData = 'previousStep',
                             modulesList = ['GaudiApplication', 'AnalyseLogFile'] )
    prod.addFinalizationStep()
    prod.setFileMask( '' )

    pr._mcSpecialCase( prod, {'tracking':0} )
    for par in prod.LHCbJob.workflow.parameters:
      if par.getName() == 'Site':
        self.assertEqual( par.value, 'ANY' )
      if par.getName() == 'listoutput':
        self.assert_( 'gausshist' not in dict( par.value ).values() )
      if par.getName() == 'outputDataFileMask':
        self.assertEqual( par.value, '' )
      if par.getName() == 'Priority':
        self.assertEqual( par.value, '1' )


  def test_resolveStepsSuccess( self ):

    pr = ProductionRequest( self.bkClientFake, self.diracProdIn )
    pr.stepsList = ['123']
    pr.resolveSteps()
    self.assertEqual( pr.stepsListDict, [step1Dict] )
    pr = ProductionRequest( self.bkClientFake, self.diracProdIn )
    pr.stepsList = ['123', '456']
    pr.resolveSteps()
    self.assertEqual( pr.stepsListDict, [step1Dict, step2Dict] )
    pr = ProductionRequest( self.bkClientFake, self.diracProdIn )
    pr.stepsList = ['123', '456', '', '']
    pr.resolveSteps()
    self.assertEqual( pr.stepsListDict, [{'StepId': 123, 'StepName':'Stripping14-Stripping',
                                          'ApplicationName':'DaVinci', 'ApplicationVersion':'v2r2', 'ExtraOptions': '',
                                          'OptionFiles':'optsFiles', 'Visible':'Yes', 'ExtraPackages':'eps',
                                          'ProcessingPass':'procPass', 'OptionsFormat':'', 'SystemConfig':'',
                                          'DDDB':'', 'CONDDB':'123456', 'DQTag':'', 'isMulticore': 'N',
                                          'prodStepID': "123['SDST']", 'mcTCK': '',
                                          'fileTypesIn':['SDST'],
                                          'fileTypesOut':['BHADRON.DST', 'CALIBRATION.DST']},
                                         {'StepId': 456, 'StepName':'Merge',
                                          'ApplicationName':'LHCb', 'ApplicationVersion':'v1r2', 'ExtraOptions': '',
                                          'OptionFiles':'optsFiles', 'Visible':'Yes', 'ExtraPackages':'eps',
                                          'ProcessingPass':'procPass', 'OptionsFormat':'', 'SystemConfig':'x86',
                                          'prodStepID': "456['BHADRON.DST', 'CALIBRATION.DST']",
                                          'DDDB':'', 'CONDDB':'123456', 'DQTag':'', 'isMulticore': 'N', 'mcTCK': '',
                                          'fileTypesIn':['BHADRON.DST', 'CALIBRATION.DST'],
                                          'fileTypesOut':['BHADRON.DST', 'CALIBRATION.DST']}
                                        ] )
    pr = ProductionRequest( self.bkClientFake, self.diracProdIn )
    pr.stepsList = ['123']
    pr.resolveSteps()
    self.assertEqual( pr.stepsListDict, [{'StepId': 123, 'StepName':'Stripping14-Stripping',
                                          'ApplicationName':'DaVinci', 'ApplicationVersion':'v2r2', 'ExtraOptions': '',
                                          'OptionFiles':'optsFiles', 'Visible':'Yes', 'ExtraPackages':'eps',
                                          'ProcessingPass':'procPass', 'OptionsFormat':'', 'SystemConfig':'',
                                          'DDDB':'', 'CONDDB':'123456', 'DQTag':'', 'isMulticore': 'N',
                                          'prodStepID': "123['SDST']", 'mcTCK': '',
                                          'fileTypesIn':['SDST'],
                                          'fileTypesOut':['BHADRON.DST', 'CALIBRATION.DST']}] )
    pr = ProductionRequest( self.bkClientFake, self.diracProdIn )
    pr.stepsList = ['123']
    pr.resolveSteps()
    self.assertEqual( pr.stepsListDict, [{'StepId': 123, 'StepName':'Stripping14-Stripping',
                                          'ApplicationName':'DaVinci', 'ApplicationVersion':'v2r2', 'ExtraOptions': '',
                                          'OptionFiles':'optsFiles', 'Visible':'Yes', 'ExtraPackages':'eps',
                                          'ProcessingPass':'procPass', 'OptionsFormat':'', 'SystemConfig':'',
                                          'prodStepID': "123['SDST']", 'mcTCK': '',
                                          'DDDB':'', 'CONDDB':'123456', 'DQTag':'', 'isMulticore': 'N',
                                          'fileTypesIn':['SDST'],
                                          'fileTypesOut':['BHADRON.DST', 'CALIBRATION.DST']}] )

  def test__determineOutputSEs( self ):
    pr = ProductionRequest( self.bkClientFake, self.diracProdIn )
    pr.stepsList = ['123', '456']
    pr.resolveSteps()
    pr.outputSEs = ['SE1', 'SE2']
    pr.specialOutputSEs = [{}, {}]
    pr._determineOutputSEs()
    self.assertEqual( pr.outputSEsPerFileType, [{'CALIBRATION.DST': 'SE1', 'BHADRON.DST': 'SE1'},
                                                {'CALIBRATION.DST': 'SE2', 'BHADRON.DST': 'SE2'}] )

    pr = ProductionRequest( self.bkClientFake, self.diracProdIn )
    pr.stepsList = ['123', '456']
    pr.resolveSteps()
    pr.outputSEs = ['SE1', 'SE2']
    pr.specialOutputSEs = [{'CALIBRATION.DST': 'SE3'}, {}]
    pr._determineOutputSEs()
    self.assertEqual( pr.outputSEsPerFileType, [{'CALIBRATION.DST': 'SE3', 'BHADRON.DST': 'SE1'},
                                                {'CALIBRATION.DST': 'SE2', 'BHADRON.DST': 'SE2'}] )

  def test__applyOptionalCorrections( self ):

    stepMC = {'ApplicationName': 'Gauss', 'Usable': 'Yes', 'StepId': 246, 'ApplicationVersion': 'v28r3p1',
              'ExtraPackages': 'AppConfig.v3r104', 'StepName': 'Stripping14-Merging', 'SystemConfig': '',
              'ProcessingPass': 'MC', 'Visible': 'N', 'DDDB': 'head-20110302', 'mcTCK': '',
              'OptionFiles': '$APPCONFIGOPTS/Merging/DV-Stripping14-Merging.py', 'CONDDB': 'head-20110407',
              'fileTypesIn': [],
              'fileTypesOut': ['ALLSTREAMS.DST']}
    stepStripp = {'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': 123, 'ApplicationVersion': 'v28r3p1',
                  'ExtraPackages': 'AppConfig.v3r104', 'StepName': 'Stripping14-Merging', 'SystemConfig': '',
                  'ProcessingPass': 'Merging', 'Visible': 'N', 'DDDB': 'head-20110302', 'mcTCK': '',
                  'OptionFiles': '$APPCONFIGOPTS/Merging/DV-Stripping14-Merging.py', 'CONDDB': 'head-20110407',
                  'fileTypesIn': ['SDST'],
                  'fileTypesOut': ['BHADRON.DST', 'CALIBRATION.DST', 'CHARM.MDST', 'CHARMCOMPLETEEVENT.DST']}
    mergeStep = {'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': 456, 'ApplicationVersion': 'v28r3p1',
                 'ExtraPackages': 'AppConfig.v3r104', 'StepName': 'Stripping14-Merging', 'SystemConfig': '',
                 'ProcessingPass': 'Merging', 'Visible': 'N', 'DDDB': 'head-20110302', 'mcTCK': '',
                 'OptionFiles': '$APPCONFIGOPTS/Merging/DV-Stripping14-Merging.py', 'CONDDB': 'head-20110407',
                 'fileTypesIn': ['BHADRON.DST', 'CALIBRATION.DST', 'PID.MDST'],
                 'fileTypesOut': ['BHADRON.DST', 'CALIBRATION.DST', 'PID.MDST']}
    mergeStepBHADRON = {'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': 456, 'ApplicationVersion': 'v28r3p1',
                        'ExtraPackages': 'AppConfig.v3r104', 'StepName': 'Stripping14-Merging', 'SystemConfig': '',
                        'prodStepID': "456['BHADRON.DST']",
                        'ProcessingPass': 'Merging', 'Visible': 'N', 'DDDB': 'head-20110302', 'mcTCK': '',
                        'OptionFiles': '$APPCONFIGOPTS/Merging/DV-Stripping14-Merging.py', 'CONDDB': 'head-20110407',
                        'fileTypesIn': ['BHADRON.DST'],
                        'fileTypesOut': ['BHADRON.DST']}
    mergeStepCALIBRA = {'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': 456, 'ApplicationVersion': 'v28r3p1',
                        'ExtraPackages': 'AppConfig.v3r104', 'StepName': 'Stripping14-Merging', 'SystemConfig': '',
                        'prodStepID': "456['CALIBRATION.DST']",
                        'ProcessingPass': 'Merging', 'Visible': 'N', 'DDDB': 'head-20110302', 'mcTCK': '',
                        'OptionFiles': '$APPCONFIGOPTS/Merging/DV-Stripping14-Merging.py', 'CONDDB': 'head-20110407',
                        'fileTypesIn': ['CALIBRATION.DST'],
                        'fileTypesOut': ['CALIBRATION.DST']}
    mergeStepPIDMDST = {'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': 456, 'ApplicationVersion': 'v28r3p1',
                        'ExtraPackages': 'AppConfig.v3r104', 'StepName': 'Stripping14-Merging', 'SystemConfig': '',
                        'prodStepID': "456['PID.MDST']",
                        'ProcessingPass': 'Merging', 'Visible': 'N', 'DDDB': 'head-20110302', 'mcTCK': '',
                        'OptionFiles': '$APPCONFIGOPTS/Merging/DV-Stripping14-Merging.py', 'CONDDB': 'head-20110407',
                        'fileTypesIn': ['PID.MDST'],
                        'fileTypesOut': ['PID.MDST']}

    pr = ProductionRequest( self.bkClientFake, self.diracProdIn )
    pr.prodsTypeList = ['DataStripping', 'Merge']
    pr.plugins = ['ByRun', 'BySize']
    pr.stepsListDict = [stepStripp, mergeStep]
    pr.stepsList = [123, 456]
    pr.stepsInProds = [[1], [2]]
    pr.outputSEs = [ 'Tier1-DST', 'Tier1-M-DST']
    pr.specialOutputSEs = [{}, {}]
    pr.priorities = [1, 4]
    pr.cpus = [10, 100]
    pr.groupSizes = [1, 2]
    pr.bkFileType = 'TYPE'
    pr.removeInputsFlags = [False, True]
    pr.outputFileMasks = ['', 'dst']
    pr.outputFileSteps = ['', '2']
    pr.inputs = [['aa'], []]
    pr.inputDataPolicies = ['dl', 'pr']
    pr.events = [-1, -1]
    pr.bkQueries = ['Full', 'fromPreviousProd']
    pr.targets = ['Target1', 'Target2']
    pr.multicore = ['False', 'True']
    pr.outputModes = ['Local', 'Any']
    pr.ancestorDepths = [0, 1]
    pr._applyOptionalCorrections()
    prodsTypeListExpected = ['DataStripping', 'Merge', 'Merge', 'Merge']
    pluginsExpected = ['ByRun', 'BySize', 'BySize', 'BySize']
    stepsListDictExpected = [stepStripp, mergeStepBHADRON, mergeStepCALIBRA, mergeStepPIDMDST ]
    stepsInProdExpected = [[1], [2], [3], [4]]
    outputSEsExpected = ['Tier1-DST', 'Tier1-M-DST', 'Tier1-M-DST', 'Tier1-M-DST']
    specialOutputSEsExpected = [{}, {}, {}, {}]
    prioritiesExpected = [1, 4, 4, 4]
    cpusExpected = [10, 100, 100, 100]
    bkQueriesExpected = ['Full', 'fromPreviousProd', 'fromPreviousProd', 'fromPreviousProd']
    groupSizesExpected = [1, 2, 2, 2]
    previousProdsExpected = [None, 1, 1, 1]
    removeInputsFlagExpected = [False, True, True, True]
    outputFileMasksExpected = ['', 'dst', 'dst', 'dst']
    outputFileStepsExpected = ['', '2', '2', '2']
    inputsExpected = [['aa'], [], [], []]
    inputDataPoliciesExpected = ['dl', 'pr', 'pr', 'pr']
    eventsExpected = [-1, -1, -1, -1]
    targetsExpected = ['Target1', 'Target2', 'Target2', 'Target2']
    multicoreExpected = ['False', 'True', 'True', 'True']
    outputModeExpected = ['Local', 'Any', 'Any', 'Any']
    ancestorDepthsExpected = [0, 1, 1, 1]
    self.assertEqual( pr.prodsTypeList, prodsTypeListExpected )
    self.assertEqual( pr.plugins, pluginsExpected )
    self.assertEqual( pr.stepsListDict, stepsListDictExpected )
    self.assertEqual( pr.stepsInProds, stepsInProdExpected )
    self.assertEqual( pr.outputSEs, outputSEsExpected )
    self.assertEqual( pr.specialOutputSEs, specialOutputSEsExpected )
    self.assertEqual( pr.priorities, prioritiesExpected )
    self.assertEqual( pr.cpus, cpusExpected )
    self.assertEqual( pr.groupSizes, groupSizesExpected )
    self.assertEqual( pr.bkQueries, bkQueriesExpected )
    self.assertEqual( pr.previousProds, previousProdsExpected )
    self.assertEqual( pr.removeInputsFlags, removeInputsFlagExpected )
    self.assertEqual( pr.outputFileMasks, outputFileMasksExpected )
    self.assertEqual( pr.outputFileSteps, outputFileStepsExpected )
    self.assertEqual( pr.inputs, inputsExpected )
    self.assertEqual( pr.inputDataPolicies, inputDataPoliciesExpected )
    self.assertEqual( pr.events, eventsExpected )
    self.assertEqual( pr.targets, targetsExpected )
    self.assertEqual( pr.multicore, multicoreExpected )
    self.assertEqual( pr.outputModes, outputModeExpected )
    self.assertEqual( pr.ancestorDepths, ancestorDepthsExpected )

    pr = ProductionRequest( self.bkClientFake, self.diracProdIn )
    pr.prodsTypeList = ['DataStripping', 'Merge']
    pr.plugins = ['ByRun', 'ByRunFileTypeSizeWithFlush']
    pr.stepsListDict = [stepStripp, mergeStep]
    pr.stepsList = [123, 456]
    pr.stepsInProds = [[1], [2]]
    pr.outputSEs = [ 'Tier1-DST', 'Tier1-M-DST']
    pr.specialOutputSEs = [{'Type1': 'SE1'}, {}]
    pr.priorities = [1, 4]
    pr.cpus = [10, 100]
    pr.groupSizes = [1, 2]
    pr.bkFileType = ['Type1', 'Type2']
    pr.inputs = [[], ['aa']]
    pr.removeInputsFlags = [False, True]
    pr.outputFileMasks = ['', 'dst']
    pr.outputFileSteps = ['', '2']
    pr.inputDataPolicies = ['pr', 'dl']
    pr.events = [-1, -1]
    pr.bkQueries = ['Full', 'fromPreviousProd']
    pr.targets = ['Target1', 'Target2']
    pr.multicore = ['False', 'True']
    pr.outputModes = ['Local', 'Any']
    pr.ancestorDepths = [0, 0]
    pr._applyOptionalCorrections()
    prodsTypeListExpected = ['DataStripping', 'Merge']
    pluginsExpected = ['ByRun', 'ByRunFileTypeSizeWithFlush']
    stepsListDictExpected = [stepStripp, mergeStep ]
    stepsInProdExpected = [[1], [2]]
    outputSEsExpected = ['Tier1-DST', 'Tier1-M-DST']
    specialOutputSEsExpected = [{'Type1': 'SE1'}, {}]
    prioritiesExpected = [1, 4]
    cpusExpected = [10, 100]
    groupSizesExpected = [1, 2]
    bkQueriesExpected = ['Full', 'fromPreviousProd']
    previousProdsExpected = [None, 1]
    removeInputsFlagExpected = [False, True]
    outputFileMasksExpected = ['', 'dst']
    outputFileStepsExpected = ['', '2']
    inputsExpected = [[], ['aa']]
    inputDataPoliciesExpected = ['pr', 'dl']
    eventsExpected = [-1, -1]
    targetsExpected = ['Target1', 'Target2']
    multicoreExpected = ['False', 'True']
    outputModeExpected = ['Local', 'Any']
    ancestorDepthsExpected = [0, 0]
    self.assertEqual( pr.prodsTypeList, prodsTypeListExpected )
    self.assertEqual( pr.plugins, pluginsExpected )
    self.assertEqual( pr.stepsListDict, stepsListDictExpected )
    self.assertEqual( pr.stepsInProds, stepsInProdExpected )
    self.assertEqual( pr.outputSEs, outputSEsExpected )
    self.assertEqual( pr.specialOutputSEs, specialOutputSEsExpected )
    self.assertEqual( pr.priorities, prioritiesExpected )
    self.assertEqual( pr.cpus, cpusExpected )
    self.assertEqual( pr.groupSizes, groupSizesExpected )
    self.assertEqual( pr.bkQueries, bkQueriesExpected )
    self.assertEqual( pr.previousProds, previousProdsExpected )
    self.assertEqual( pr.removeInputsFlags, removeInputsFlagExpected )
    self.assertEqual( pr.outputFileMasks, outputFileMasksExpected )
    self.assertEqual( pr.outputFileSteps, outputFileStepsExpected )
    self.assertEqual( pr.inputs, inputsExpected )
    self.assertEqual( pr.inputDataPolicies, inputDataPoliciesExpected )
    self.assertEqual( pr.events, eventsExpected )
    self.assertEqual( pr.targets, targetsExpected )
    self.assertEqual( pr.multicore, multicoreExpected )
    self.assertEqual( pr.outputModes, outputModeExpected )
    self.assertEqual( pr.ancestorDepths, ancestorDepthsExpected )

    pr = ProductionRequest( self.bkClientFake, self.diracProdIn )
    pr.prodsTypeList = ['Merge', 'Merge']
    pr.plugins = ['BySize', 'ByRunFileTypeSizeWithFlush']
    pr.stepsListDict = [mergeStep, mergeStep]
    pr.stepsList = [456, 456]
    pr.stepsInProds = [[1], [2]]
    pr.outputSEs = [ 'Tier1-DST', 'Tier1-M-DST']
    pr.specialOutputSEs = [{'Type1':'SE1'}, {}]
    pr.priorities = [1, 4]
    pr.cpus = [10, 100]
    pr.groupSizes = [1, 2]
    pr.removeInputsFlags = [True, True]
    pr.outputFileMasks = ['', 'dst']
    pr.outputFileSteps = ['', '2']
    pr.inputs = [[], ['aa']]
    pr.inputDataPolicies = ['dl', 'pr']
    pr.events = [-1, -1]
    pr.bkQueries = ['fromPreviousProd', 'fromPreviousProd']
    pr.targets = ['Target1', 'Target2']
    pr.multicore = ['False', 'True']
    pr.outputModes = ['Local', 'Any']
    pr.ancestorDepths = [1, 0]
    pr._applyOptionalCorrections()
    prodsTypeListExpected = ['Merge', 'Merge', 'Merge', 'Merge']
    pluginsExpected = ['BySize', 'BySize', 'BySize', 'ByRunFileTypeSizeWithFlush']
    stepsListDictExpected = [mergeStepBHADRON, mergeStepCALIBRA, mergeStepPIDMDST, mergeStep ]
    stepsInProdExpected = [[1], [2], [3], [4]]
    outputSEsExpected = ['Tier1-DST', 'Tier1-DST', 'Tier1-DST', 'Tier1-M-DST']
    specialOutputSEsExpected = [{'Type1':'SE1'}, {'Type1':'SE1'}, {'Type1':'SE1'}, {}]
    prioritiesExpected = [1, 1, 1, 4]
    cpusExpected = [10, 10, 10, 100]
    groupSizesExpected = [1, 1, 1, 2]
    bkQueriesExpected = ['fromPreviousProd', 'fromPreviousProd', 'fromPreviousProd', 'fromPreviousProd']
    previousProdsExpected = [None, None, None, 1]
    removeInputsFlagExpected = [True, True, True, True]
    outputFileMasksExpected = ['', '', '', 'dst']
    outputFileStepsExpected = ['', '', '', '2']
    inputsExpected = [[], [], [], ['aa']]
    inputDataPoliciesExpected = ['dl', 'dl', 'dl', 'pr']
    eventsExpected = [-1, -1, -1, -1]
    targetsExpected = ['Target1', 'Target1', 'Target1', 'Target2']
    multicoreExpected = ['False', 'False', 'False', 'True']
    outputModeExpected = ['Local', 'Local', 'Local', 'Any']
    ancestorDepthsExpected = [1, 1, 1, 0]
    self.assertEqual( pr.prodsTypeList, prodsTypeListExpected )
    self.assertEqual( pr.plugins, pluginsExpected )
    self.assertEqual( pr.stepsListDict, stepsListDictExpected )
    self.assertEqual( pr.stepsInProds, stepsInProdExpected )
    self.assertEqual( pr.outputSEs, outputSEsExpected )
    self.assertEqual( pr.specialOutputSEs, specialOutputSEsExpected )
    self.assertEqual( pr.priorities, prioritiesExpected )
    self.assertEqual( pr.cpus, cpusExpected )
    self.assertEqual( pr.groupSizes, groupSizesExpected )
    self.assertEqual( pr.bkQueries, bkQueriesExpected )
    self.assertEqual( pr.previousProds, previousProdsExpected )
    self.assertEqual( pr.removeInputsFlags, removeInputsFlagExpected )
    self.assertEqual( pr.outputFileMasks, outputFileMasksExpected )
    self.assertEqual( pr.outputFileSteps, outputFileStepsExpected )
    self.assertEqual( pr.inputs, inputsExpected )
    self.assertEqual( pr.inputDataPolicies, inputDataPoliciesExpected )
    self.assertEqual( pr.events, eventsExpected )
    self.assertEqual( pr.targets, targetsExpected )
    self.assertEqual( pr.multicore, multicoreExpected )
    self.assertEqual( pr.outputModes, outputModeExpected )
    self.assertEqual( pr.ancestorDepths, ancestorDepthsExpected )

    pr = ProductionRequest( self.bkClientFake, self.diracProdIn )
    pr.prodsTypeList = ['DataStripping']
    pr.plugins = ['ByRun']
    pr.stepsListDict = [stepStripp, mergeStep]
    pr.stepsList = [123]
    pr.stepsInProds = [[1, 2]]
    pr.priorities = [1]
    pr.outputSEs = [ 'Tier1-DST']
    pr.cpus = [10]
    pr.groupSizes = [1]
    pr.bkFileType = 'TYPE'
    pr.removeInputsFlags = [False]
    pr.outputFileMasks = ['']
    pr.outputFileSteps = ['2']
    pr.inputs = [[]]
    pr.inputDataPolicies = ['']
    pr.events = [-1]
    pr.bkQueries = ['Full']
    pr.targets = ['Target1']
    pr.multicore = ['False']
    pr.outputModes = ['Local']
    pr.ancestorDepths = [1]
    pr._applyOptionalCorrections()
    prodsTypeListExpected = ['DataStripping']
    pluginsExpected = ['ByRun']
    stepsListDictExpected = [stepStripp, mergeStep ]
    stepsInProdExpected = [[1, 2]]
    outputSEsExpected = ['Tier1-DST']
    prioritiesExpected = [1]
    cpusExpected = [10]
    groupSizesExpected = [1]
    bkQueriesExpected = ['Full']
    previousProdsExpected = [None]
    removeInputsFlagExpected = [False]
    outputFileMasksExpected = ['']
    outputFileStepsExpected = ['2']
    inputsExpected = [[]]
    inputDataPoliciesExpected = ['']
    eventsExpected = [-1]
    targetsExpected = ['Target1']
    multicoreExpected = ['False']
    outputModeExpected = ['Local']
    ancestorDepthsExpected = [1]
    self.assertEqual( pr.prodsTypeList, prodsTypeListExpected )
    self.assertEqual( pr.plugins, pluginsExpected )
    self.assertEqual( pr.stepsListDict, stepsListDictExpected )
    self.assertEqual( pr.stepsInProds, stepsInProdExpected )
    self.assertEqual( pr.outputSEs, outputSEsExpected )
    self.assertEqual( pr.priorities, prioritiesExpected )
    self.assertEqual( pr.cpus, cpusExpected )
    self.assertEqual( pr.groupSizes, groupSizesExpected )
    self.assertEqual( pr.bkQueries, bkQueriesExpected )
    self.assertEqual( pr.previousProds, previousProdsExpected )
    self.assertEqual( pr.removeInputsFlags, removeInputsFlagExpected )
    self.assertEqual( pr.outputFileMasks, outputFileMasksExpected )
    self.assertEqual( pr.outputFileSteps, outputFileStepsExpected )
    self.assertEqual( pr.inputs, inputsExpected )
    self.assertEqual( pr.inputDataPolicies, inputDataPoliciesExpected )
    self.assertEqual( pr.events, eventsExpected )
    self.assertEqual( pr.targets, targetsExpected )
    self.assertEqual( pr.multicore, multicoreExpected )
    self.assertEqual( pr.outputModes, outputModeExpected )
    self.assertEqual( pr.ancestorDepths, ancestorDepthsExpected )

    pr = ProductionRequest( self.bkClientFake, self.diracProdIn )
    pr.prodsTypeList = ['DataStripping', 'Merge', 'Merge']
    pr.plugins = ['ByRun', 'BySize', 'ByRunFileTypeSizeWithFlush']
    pr.stepsListDict = [stepStripp, mergeStep, mergeStep]
    pr.stepsList = [123, 456, 456]
    pr.stepsInProds = [[1], [2], [3]]
    pr.outputSEs = [ 'Tier1-DST', 'Tier1-M-DST', 'Tier1-M-DST']
    pr.priorities = [1, 4, 5]
    pr.cpus = [10, 100, 1000]
    pr.groupSizes = [1, 2, 3]
    pr.bkFileType = 'TYPE'
    pr.removeInputsFlags = [False, True, True]
    pr.outputFileMasks = ['', 'dst', '']
    pr.outputFileSteps = ['', '2', '']
    pr.inputs = [[], ['aa'], ['bb']]
    pr.inputDataPolicies = ['', 'dl', 'pr']
    pr.bkQueries = ['Full', 'fromPreviousProd', 'fromPreviousProd']
    pr.events = [-1, -1, -1]
    pr.targets = ['Target1', 'Target2', 'Target3']
    pr.multicore = ['False', 'True', 'False']
    pr.outputModes = ['Local', 'Any', 'Any']
    pr.ancestorDepths = [1, 2, 3]
    pr._applyOptionalCorrections()
    prodsTypeListExpected = ['DataStripping', 'Merge', 'Merge', 'Merge', 'Merge']
    pluginsExpected = ['ByRun', 'BySize', 'BySize', 'BySize', 'ByRunFileTypeSizeWithFlush']
    stepsListDictExpected = [stepStripp, mergeStepBHADRON, mergeStepCALIBRA, mergeStepPIDMDST, mergeStep ]
    stepsInProdExpected = [[1], [2], [3], [4], [5]]
    outputSEsExpected = ['Tier1-DST', 'Tier1-M-DST', 'Tier1-M-DST', 'Tier1-M-DST', 'Tier1-M-DST']
    prioritiesExpected = [1, 4, 4, 4, 5]
    groupSizesExpected = [1, 2, 2, 2, 3]
    cpusExpected = [10, 100, 100, 100, 1000]
    bkQueriesExpected = ['Full', 'fromPreviousProd', 'fromPreviousProd', 'fromPreviousProd', 'fromPreviousProd']
    previousProdsExpected = [None, 1, 1, 1, 2]
    removeInputsFlagExpected = [False, True, True, True, True]
    outputFileMasksExpected = ['', 'dst', 'dst', 'dst', '']
    outputFileStepsExpected = ['', '2', '2', '2', '']
    inputsExpected = [[], ['aa'], ['aa'], ['aa'], ['bb']]
    inputDataPoliciesExpected = ['', 'dl', 'dl', 'dl', 'pr']
    eventsExpected = [-1, -1, -1, -1, -1]
    targetsExpected = ['Target1', 'Target2', 'Target2', 'Target2', 'Target3']
    multicoreExpected = ['False', 'True', 'True', 'True', 'False']
    outputModeExpected = ['Local', 'Any', 'Any', 'Any', 'Any']
    ancestorDepthsExpected = [1, 2, 2, 2, 3]
    self.assertEqual( pr.prodsTypeList, prodsTypeListExpected )
    self.assertEqual( pr.plugins, pluginsExpected )
    self.assertEqual( pr.stepsListDict, stepsListDictExpected )
    self.assertEqual( pr.stepsInProds, stepsInProdExpected )
    self.assertEqual( pr.outputSEs, outputSEsExpected )
    self.assertEqual( pr.priorities, prioritiesExpected )
    self.assertEqual( pr.cpus, cpusExpected )
    self.assertEqual( pr.groupSizes, groupSizesExpected )
    self.assertEqual( pr.bkQueries, bkQueriesExpected )
    self.assertEqual( pr.previousProds, previousProdsExpected )
    self.assertEqual( pr.removeInputsFlags, removeInputsFlagExpected )
    self.assertEqual( pr.outputFileMasks, outputFileMasksExpected )
    self.assertEqual( pr.outputFileSteps, outputFileStepsExpected )
    self.assertEqual( pr.inputs, inputsExpected )
    self.assertEqual( pr.inputDataPolicies, inputDataPoliciesExpected )
    self.assertEqual( pr.events, eventsExpected )
    self.assertEqual( pr.targets, targetsExpected )
    self.assertEqual( pr.multicore, multicoreExpected )
    self.assertEqual( pr.outputModes, outputModeExpected )
    self.assertEqual( pr.ancestorDepths, ancestorDepthsExpected )

    pr = ProductionRequest( self.bkClientFake, self.diracProdIn )
    pr.prodsTypeList = ['DataStripping', 'Merge']
    pr.plugins = ['ByRun', 'BySize']
    pr.stepsListDict = [stepStripp, mergeStep]
    pr.stepsList = [123, 456]
    pr.stepsInProds = [[3], [4]]
    pr.outputSEs = [ 'Tier1-DST', 'Tier1-M-DST']
    pr.priorities = [1, 4]
    pr.cpus = [10, 100]
    pr.groupSizes = [1, 2]
    pr.bkFileType = 'TYPE'
    pr.removeInputsFlags = [False, True]
    pr.outputFileMasks = ['', 'dst']
    pr.outputFileSteps = ['', '2']
    pr.inputs = [[], []]
    pr.inputDataPolicies = ['', 'dl']
    pr.events = [-1, -1]
    pr.bkQueries = ['Full', 'fromPreviousProd']
    pr.targets = ['Target1', 'Target2']
    pr.multicore = ['False', 'True']
    pr.outputModes = ['Local', 'Any']
    pr.ancestorDepths = [1, 0]
    pr._applyOptionalCorrections()
    prodsTypeListExpected = ['DataStripping', 'Merge', 'Merge', 'Merge']
    pluginsExpected = ['ByRun', 'BySize', 'BySize', 'BySize']
    stepsListDictExpected = [stepStripp, mergeStepBHADRON, mergeStepCALIBRA, mergeStepPIDMDST ]
    stepsInProdExpected = [[3], [4], [5], [6]]
    outputSEsExpected = ['Tier1-DST', 'Tier1-M-DST', 'Tier1-M-DST', 'Tier1-M-DST']
    prioritiesExpected = [1, 4, 4, 4]
    cpusExpected = [10, 100, 100, 100]
    groupSizesExpected = [1, 2, 2, 2]
    bkQueriesExpected = ['Full', 'fromPreviousProd', 'fromPreviousProd', 'fromPreviousProd']
    previousProdsExpected = [None, 1, 1, 1]
    removeInputsFlagExpected = [False, True, True, True]
    outputFileMasksExpected = ['', 'dst', 'dst', 'dst']
    outputFileStepsExpected = ['', '2', '2', '2']
    inputsExpected = [[], [], [], []]
    inputDataPoliciesExpected = ['', 'dl', 'dl', 'dl']
    eventsExpected = [-1, -1, -1, -1]
    targetsExpected = ['Target1', 'Target2', 'Target2', 'Target2']
    multicoreExpected = ['False', 'True', 'True', 'True']
    outputModeExpected = ['Local', 'Any', 'Any', 'Any']
    ancestorDepthsExpected = [1, 0, 0, 0]
    self.assertEqual( pr.prodsTypeList, prodsTypeListExpected )
    self.assertEqual( pr.plugins, pluginsExpected )
    self.assertEqual( pr.stepsListDict, stepsListDictExpected )
    self.assertEqual( pr.stepsInProds, stepsInProdExpected )
    self.assertEqual( pr.outputSEs, outputSEsExpected )
    self.assertEqual( pr.priorities, prioritiesExpected )
    self.assertEqual( pr.cpus, cpusExpected )
    self.assertEqual( pr.groupSizes, groupSizesExpected )
    self.assertEqual( pr.bkQueries, bkQueriesExpected )
    self.assertEqual( pr.previousProds, previousProdsExpected )
    self.assertEqual( pr.removeInputsFlags, removeInputsFlagExpected )
    self.assertEqual( pr.outputFileMasks, outputFileMasksExpected )
    self.assertEqual( pr.outputFileSteps, outputFileStepsExpected )
    self.assertEqual( pr.inputs, inputsExpected )
    self.assertEqual( pr.inputDataPolicies, inputDataPoliciesExpected )
    self.assertEqual( pr.events, eventsExpected )
    self.assertEqual( pr.targets, targetsExpected )
    self.assertEqual( pr.multicore, multicoreExpected )
    self.assertEqual( pr.outputModes, outputModeExpected )
    self.assertEqual( pr.ancestorDepths, ancestorDepthsExpected )

    pr = ProductionRequest( self.bkClientFake, self.diracProdIn )
    pr.prodsTypeList = ['DataStripping']
    pr.plugins = ['ByRun']
    pr.stepsListDict = [stepStripp, mergeStep]
    pr.stepsList = [123, 456]
    pr.stepsInProds = [[3, 4]]
    pr.priorities = [1]
    pr.outputSEs = [ 'Tier1-DST']
    pr.cpus = [10]
    pr.groupSizes = [1]
    pr.removeInputsFlags = [False]
    pr.outputFileMasks = ['dst']
    pr.outputFileSteps = ['2']
    pr.bkFileType = 'TYPE'
    pr.inputs = [[]]
    pr.inputDataPolicies = ['dl']
    pr.events = [-1]
    pr.bkQueries = ['Full']
    pr.targets = ['Target1']
    pr.multicore = ['False']
    pr.outputModes = ['Local']
    pr.ancestorDepths = [1]
    pr._applyOptionalCorrections()
    prodsTypeListExpected = ['DataStripping']
    pluginsExpected = ['ByRun']
    stepsListDictExpected = [stepStripp, mergeStep ]
    stepsInProdExpected = [[3, 4]]
    outputSEsExpected = ['Tier1-DST']
    prioritiesExpected = [1]
    cpusExpected = [10]
    groupSizesExpected = [1]
    bkQueriesExpected = ['Full']
    previousProdsExpected = [None]
    removeInputsFlagExpected = [False]
    outputFileMasksExpected = ['dst']
    outputFileStepsExpected = ['2']
    inputsExpected = [[]]
    inputDataPoliciesExpected = ['dl']
    eventsExpected = [-1]
    targetsExpected = ['Target1']
    multicoreExpected = ['False']
    outputModeExpected = ['Local']
    ancestorDepthsExpected = [1]
    self.assertEqual( pr.prodsTypeList, prodsTypeListExpected )
    self.assertEqual( pr.plugins, pluginsExpected )
    self.assertEqual( pr.stepsListDict, stepsListDictExpected )
    self.assertEqual( pr.stepsInProds, stepsInProdExpected )
    self.assertEqual( pr.outputSEs, outputSEsExpected )
    self.assertEqual( pr.priorities, prioritiesExpected )
    self.assertEqual( pr.cpus, cpusExpected )
    self.assertEqual( pr.groupSizes, groupSizesExpected )
    self.assertEqual( pr.bkQueries, bkQueriesExpected )
    self.assertEqual( pr.previousProds, previousProdsExpected )
    self.assertEqual( pr.removeInputsFlags, removeInputsFlagExpected )
    self.assertEqual( pr.outputFileMasks, outputFileMasksExpected )
    self.assertEqual( pr.outputFileSteps, outputFileStepsExpected )
    self.assertEqual( pr.inputs, inputsExpected )
    self.assertEqual( pr.inputDataPolicies, inputDataPoliciesExpected )
    self.assertEqual( pr.events, eventsExpected )
    self.assertEqual( pr.targets, targetsExpected )
    self.assertEqual( pr.multicore, multicoreExpected )
    self.assertEqual( pr.outputModes, outputModeExpected )
    self.assertEqual( pr.ancestorDepths, ancestorDepthsExpected )

    pr = ProductionRequest( self.bkClientFake, self.diracProdIn )
    pr.prodsTypeList = ['Merge', 'Merge']
    pr.plugins = ['BySize', 'ByRunFileTypeSizeWithFlush']
    pr.stepsListDict = [mergeStep, mergeStep]
    pr.stepsList = [456, 456]
    pr.stepsInProds = [[3], [4]]
    pr.outputSEs = [ 'Tier1-DST', 'Tier1-M-DST']
    pr.priorities = [1, 4]
    pr.cpus = [10, 100]
    pr.groupSizes = [1, 2]
    pr.removeInputsFlags = [True, True]
    pr.outputFileMasks = ['', 'dst']
    pr.outputFileSteps = ['', '2']
    pr.inputs = [['aa'], ['bb']]
    pr.inputDataPolicies = ['dl', 'pr']
    pr.events = [-1, -1]
    pr.bkQueries = ['Full', 'fromPreviousProd']
    pr.targets = ['Target1', 'Target2']
    pr.multicore = ['False', 'True']
    pr.outputModes = ['Local', 'Any']
    pr.ancestorDepths = [1, 0]
    pr._applyOptionalCorrections()
    prodsTypeListExpected = ['Merge', 'Merge', 'Merge', 'Merge']
    pluginsExpected = ['BySize', 'BySize', 'BySize', 'ByRunFileTypeSizeWithFlush']
    stepsListDictExpected = [mergeStepBHADRON, mergeStepCALIBRA, mergeStepPIDMDST, mergeStep ]
    stepsInProdExpected = [[3], [4], [5], [6]]
    outputSEsExpected = ['Tier1-DST', 'Tier1-DST', 'Tier1-DST', 'Tier1-M-DST']
    prioritiesExpected = [1, 1, 1, 4]
    cpusExpected = [10, 10, 10, 100]
    groupSizesExpected = [1, 1, 1, 2]
    bkQueriesExpected = ['Full', 'Full', 'Full', 'fromPreviousProd']
    previousProdsExpected = [None, None, None, 1]
    removeInputsFlagExpected = [True, True, True, True]
    outputFileMasksExpected = ['', '', '', 'dst']
    outputFileStepsExpected = ['', '', '', '2']
    inputsExpected = [['aa'], ['aa'], ['aa'], ['bb']]
    inputDataPoliciesExpected = ['dl', 'dl', 'dl', 'pr']
    eventsExpected = [-1, -1, -1, -1]
    targetsExpected = ['Target1', 'Target1', 'Target1', 'Target2']
    multicoreExpected = ['False', 'False', 'False', 'True']
    outputModeExpected = ['Local', 'Local', 'Local', 'Any']
    ancestorDepthsExpected = [1, 1, 1, 0]
    self.assertEqual( pr.prodsTypeList, prodsTypeListExpected )
    self.assertEqual( pr.plugins, pluginsExpected )
    self.assertEqual( pr.stepsListDict, stepsListDictExpected )
    self.assertEqual( pr.stepsInProds, stepsInProdExpected )
    self.assertEqual( pr.outputSEs, outputSEsExpected )
    self.assertEqual( pr.priorities, prioritiesExpected )
    self.assertEqual( pr.cpus, cpusExpected )
    self.assertEqual( pr.groupSizes, groupSizesExpected )
    self.assertEqual( pr.bkQueries, bkQueriesExpected )
    self.assertEqual( pr.previousProds, previousProdsExpected )
    self.assertEqual( pr.removeInputsFlags, removeInputsFlagExpected )
    self.assertEqual( pr.outputFileMasks, outputFileMasksExpected )
    self.assertEqual( pr.outputFileSteps, outputFileStepsExpected )
    self.assertEqual( pr.inputs, inputsExpected )
    self.assertEqual( pr.inputDataPolicies, inputDataPoliciesExpected )
    self.assertEqual( pr.events, eventsExpected )
    self.assertEqual( pr.targets, targetsExpected )
    self.assertEqual( pr.multicore, multicoreExpected )
    self.assertEqual( pr.outputModes, outputModeExpected )
    self.assertEqual( pr.ancestorDepths, ancestorDepthsExpected )

    pr = ProductionRequest( self.bkClientFake, self.diracProdIn )
    pr.prodsTypeList = ['MCSimulation', 'Merge']
    pr.plugins = ['', 'ByRunFileTypeSizeWithFlush']
    pr.stepsListDict = [stepMC, mergeStep]
    pr.stepsList = [123, 456]
    pr.stepsInProds = [[1], [2]]
    pr.outputSEs = [ 'Tier1-DST', 'Tier1-M-DST']
    pr.priorities = [1, 4]
    pr.cpus = [10, 100]
    pr.groupSizes = [1, 2]
    pr.removeInputsFlags = [False, True]
    pr.outputFileMasks = ['', 'dst']
    pr.outputFileSteps = ['', '2']
    pr.inputs = [[], []]
    pr.inputDataPolicies = ['', 'dl']
    pr.events = [100, -1]
    pr.bkQueries = ['', 'fromPreviousProd']
    pr.targets = ['Target1', 'Target2']
    pr.multicore = ['False', 'True']
    pr.outputModes = ['Local', 'Any']
    pr.ancestorDepths = [1, 0]
    pr._applyOptionalCorrections()
    prodsTypeListExpected = ['MCSimulation', 'Merge']
    pluginsExpected = ['', 'ByRunFileTypeSizeWithFlush']
    stepsListDictExpected = [stepMC, mergeStep ]
    stepsInProdExpected = [[1], [2]]
    outputSEsExpected = ['Tier1-DST', 'Tier1-M-DST']
    prioritiesExpected = [1, 4]
    cpusExpected = [10, 100]
    groupSizesExpected = [1, 2]
    bkQueriesExpected = ['', 'fromPreviousProd']
    previousProdsExpected = [None, 1]
    removeInputsFlagExpected = [False, True]
    outputFileMasksExpected = ['', 'dst']
    outputFileStepsExpected = ['', '2']
    inputsExpected = [[], []]
    inputDataPoliciesExpected = ['', 'dl']
    eventsExpected = [100, -1]
    targetsExpected = ['Target1', 'Target2']
    multicoreExpected = ['False', 'True']
    outputModeExpected = ['Local', 'Any']
    ancestorDepthsExpected = [1, 0]
    self.assertEqual( pr.prodsTypeList, prodsTypeListExpected )
    self.assertEqual( pr.plugins, pluginsExpected )
    self.assertEqual( pr.stepsListDict, stepsListDictExpected )
    self.assertEqual( pr.stepsInProds, stepsInProdExpected )
    self.assertEqual( pr.outputSEs, outputSEsExpected )
    self.assertEqual( pr.priorities, prioritiesExpected )
    self.assertEqual( pr.cpus, cpusExpected )
    self.assertEqual( pr.groupSizes, groupSizesExpected )
    self.assertEqual( pr.bkQueries, bkQueriesExpected )
    self.assertEqual( pr.previousProds, previousProdsExpected )
    self.assertEqual( pr.removeInputsFlags, removeInputsFlagExpected )
    self.assertEqual( pr.outputFileMasks, outputFileMasksExpected )
    self.assertEqual( pr.outputFileSteps, outputFileStepsExpected )
    self.assertEqual( pr.inputs, inputsExpected )
    self.assertEqual( pr.inputDataPolicies, inputDataPoliciesExpected )
    self.assertEqual( pr.events, eventsExpected )
    self.assertEqual( pr.targets, targetsExpected )
    self.assertEqual( pr.multicore, multicoreExpected )
    self.assertEqual( pr.outputModes, outputModeExpected )
    self.assertEqual( pr.ancestorDepths, ancestorDepthsExpected )

  def test_getProdsDescriptionDict( self ):
    pr = ProductionRequest( self.bkClientFake, self.diracProdIn )
    pr.stepsList = [123, 456, 456, 456]
    pr.prodsTypeList = ['DataStripping', 'Merge', 'Merge']
    pr.removeInputsFlags = [False, True, True]
    pr.inputs = [[], [], []]
    pr.targets = ['', '', '']
    pr.groupSizes = [1, 1, 2]
    pr.inputDataPolicies = ['download', 'download', 'download']
    pr.outputFileMasks = ['', '', '']
    pr.outputFileSteps = ['', '', '']
    pr.stepsInProds = [[1, 2], [3], [4]]
    pr.bkQueries = ['Full', 'fromPreviousProd', 'fromPreviousProd']
#     pr.outputSEs = ['Tier1-BUFFER', 'Tier1-DST', 'Tier1-DST']
#     pr.specialOutputSEs = [{}, {}, {}]
    pr.outputSEsPerFileType = [{'T1':'SE1'}, {'T1':'SE1'}, {'T1':'SE1'}]
    pr.priorities = [5, 8, 9]
    pr.cpus = [1000000, 300000, 10000]
    pr.plugins = ['ByRun', 'BySize', 'BySize']
    pr.previousProds = [None, 1, 1]
    pr.events = [-1, -1, -1]
    pr.multicore = ['False', 'False', 'True']
    pr.outputModes = ['Any', 'Local', 'Any']
    pr.ancestorDepths = [0, 0, 0]

    pr.stepsListDict = [{'StepId': 123, 'StepName':'Stripping14-Stripping',
                         'ApplicationName':'DaVinci', 'ApplicationVersion':'v2r2',
                         'OptionFiles':'optsFiles', 'Visible':'Yes', 'ExtraPackages':'eps',
                         'ProcessingPass':'procPass', 'OptionsFormat':'',
                         'DDDB':'', 'CONDDB':'123456', 'DQTag':'', 'isMulticore': 'False',
                         'fileTypesIn':['SDST'],
                         'fileTypesOut':['BHADRON.DST', 'CALIBRATION.DST']},
                        {'StepId': 456, 'StepName':'Merge',
                         'ApplicationName':'LHCb', 'ApplicationVersion':'v1r2',
                         'OptionFiles':'optsFiles', 'Visible':'Yes', 'ExtraPackages':'eps',
                         'ProcessingPass':'procPass', 'OptionsFormat':'',
                         'DDDB':'', 'CONDDB':'123456', 'DQTag':'', 'isMulticore': 'False',
                         'prodStepID': "456['BHADRON.DST']",
                         'fileTypesIn':['BHADRON.DST'],
                         'fileTypesOut':['BHADRON.DST']},
                        {'StepId': 456, 'StepName':'Merge',
                         'ApplicationName':'LHCb', 'ApplicationVersion':'v1r2',
                         'OptionFiles':'optsFiles', 'Visible':'Yes', 'ExtraPackages':'eps',
                         'ProcessingPass':'procPass', 'OptionsFormat':'',
                         'DDDB':'', 'CONDDB':'123456', 'DQTag':'', 'isMulticore': 'False',
                         'prodStepID': "456['CALIBRATION.DST']",
                         'fileTypesIn':['CALIBRATION.DST'],
                         'fileTypesOut':['CALIBRATION.DST']},
                        {'StepId': 456, 'StepName':'Merge',
                         'ApplicationName':'LHCb', 'ApplicationVersion':'v1r2',
                         'OptionFiles':'optsFiles', 'Visible':'Yes', 'ExtraPackages':'eps',
                         'ProcessingPass':'procPass', 'OptionsFormat':'',
                         'DDDB':'', 'CONDDB':'123456', 'DQTag':'', 'isMulticore': 'False',
                         'prodStepID': "456['PID.MDST']",
                         'fileTypesIn':['PID.MDST'],
                         'fileTypesOut':['PID.MDST']},
                       ]
    res = pr._getProdsDescriptionDict()

    resExpected = {1:{'productionType':'DataStripping',
                      'stepsInProd':[123, 456],
                      'bkQuery': 'Full',
                      'removeInputsFlag': False,
                      'tracking':0,
                      'outputSE': {'T1':'SE1'},
                      'priority': 5,
                      'cpu': 1000000,
                      'outputFileMask':'',
                      'outputFileStep':'',
                      'input': [],
                      'target':'',
                      'groupSize': 1,
                      'plugin': 'ByRun',
                      'inputDataPolicy':'download',
                      'derivedProduction':0,
                      'transformationFamily':0,
                      'previousProd': None,
                      'stepsInProd-ProdName': ["123['SDST']", "456['BHADRON.DST']"],
                      'events':-1,
                      'multicore': 'False',
                      'outputMode': 'Any',
                      'ancestorDepth': 0
                     },

                   2:{'productionType':'Merge',
                      'stepsInProd':[456],
                      'bkQuery': 'fromPreviousProd',
                      'removeInputsFlag': True,
                      'tracking':1,
                      'outputSE': {'T1':'SE1'},
                      'priority': 8,
                      'cpu': 300000,
                      'outputFileMask':'',
                      'outputFileStep':'',
                      'input': [],
                      'target':'',
                      'groupSize': 1,
                      'plugin': 'BySize',
                      'inputDataPolicy':'download',
                      'derivedProduction':0,
                      'transformationFamily':0,
                      'previousProd': 1,
                      'stepsInProd-ProdName': ["456['CALIBRATION.DST']"],
                      'events':-1,
                      'multicore': 'False',
                      'outputMode': 'Local',
                      'ancestorDepth': 0
                     },

                   3:{'productionType':'Merge',
                      'stepsInProd':[456],
                      'bkQuery': 'fromPreviousProd',
                      'removeInputsFlag': True,
                      'tracking':1,
                      'outputSE': {'T1':'SE1'},
                      'priority': 9,
                      'cpu': 10000,
                      'outputFileMask':'',
                      'outputFileStep':'',
                      'input': [],
                      'target':'',
                      'groupSize': 2,
                      'plugin': 'BySize',
                      'inputDataPolicy':'download',
                      'derivedProduction':0,
                      'transformationFamily':0,
                      'previousProd': 1,
                      'stepsInProd-ProdName': ["456['PID.MDST']"],
                      'events':-1,
                      'multicore': 'True',
                      'outputMode': 'Any',
                      'ancestorDepth': 0
                     }
                  }
    self.maxDiff = None
    self.assertEqual( res, resExpected )


  def test__getBKKQuery( self ):

    pr = ProductionRequest( self.bkClientFake, self.diracProdIn )
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

    pr = ProductionRequest( self.bkClientFake, self.diracProdIn )
    pr.dataTakingConditions = 'dataTC'
    pr.processingPass = 'procePass'
    pr.dqFlag = 'OK,AA, BB'
    pr.bkFileType = ['type1', 'type2']
    pr.startRun = '123'
    pr.endRun = '456'
    res = pr._getBKKQuery()
    resExpected = {'DataTakingConditions':'dataTC',
                   'ProcessingPass':'procePass',
                   'FileType':'type1;;;type2',
                   'EventType':'',
                   'ConfigName':'test',
                   'ConfigVersion':'certification',
                   'DataQualityFlag':'OK;;;AA;;;BB',
                   'StartRun':123,
                   'EndRun':456
                  }
    self.assertEqual( res, resExpected )

    pr = ProductionRequest( self.bkClientFake, self.diracProdIn )
    pr.dataTakingConditions = 'dataTC'
    pr.processingPass = 'procePass'
    pr.dqFlag = 'OK,AA, BB'
    pr.bkFileType = ['type1']
    pr.startRun = '123'
    pr.endRun = '456'
    res = pr._getBKKQuery()
    resExpected = {'DataTakingConditions':'dataTC',
                   'ProcessingPass':'procePass',
                   'FileType':'type1',
                   'EventType':'',
                   'ConfigName':'test',
                   'ConfigVersion':'certification',
                   'DataQualityFlag':'OK;;;AA;;;BB',
                   'StartRun':123,
                   'EndRun':456
                  }
    self.assertEqual( res, resExpected )

    pr = ProductionRequest( self.bkClientFake, self.diracProdIn )
    pr.dataTakingConditions = 'dataTC'
    pr.processingPass = 'procePass'
    pr.dqFlag = 'OK,AA, BB'
    pr.bkFileType = 'type1'
    pr.startRun = '123'
    pr.endRun = '456'
    res = pr._getBKKQuery()
    resExpected = {'DataTakingConditions':'dataTC',
                   'ProcessingPass':'procePass',
                   'FileType':'type1',
                   'EventType':'',
                   'ConfigName':'test',
                   'ConfigVersion':'certification',
                   'DataQualityFlag':'OK;;;AA;;;BB',
                   'StartRun':123,
                   'EndRun':456
                  }
    self.assertEqual( res, resExpected )

    pr = ProductionRequest( self.bkClientFake, self.diracProdIn )
    pr.dataTakingConditions = 'dataTC'
    pr.processingPass = 'procePass'
    pr.dqFlag = 'OK,AA, BB'
    pr.bkFileType = 'type1, type2'
    pr.startRun = '123'
    pr.endRun = '456'
    res = pr._getBKKQuery()
    resExpected = {'DataTakingConditions':'dataTC',
                   'ProcessingPass':'procePass',
                   'FileType':'type1;;;type2',
                   'EventType':'',
                   'ConfigName':'test',
                   'ConfigVersion':'certification',
                   'DataQualityFlag':'OK;;;AA;;;BB',
                   'StartRun':123,
                   'EndRun':456
                  }
    self.assertEqual( res, resExpected )

    pr = ProductionRequest( self.bkClientFake, self.diracProdIn )
    pr.dqFlag = 'OK,AA, BB'
    res = pr._getBKKQuery( 'fromPreviousProd', ['type'], 123 )
    resExpected = {'ProductionID':123,
                   'FileType':'type',
                   'DataQualityFlag':'OK;;;AA;;;BB'}
    self.assertEqual( res, resExpected )

    pr = ProductionRequest( self.bkClientFake, self.diracProdIn )
    pr.dqFlag = 'OK,AA, BB'
    res = pr._getBKKQuery( 'fromPreviousProd', ['type'], 123 )
    resExpected = {'ProductionID':123,
                   'FileType':'type',
                   'DataQualityFlag':'OK;;;AA;;;BB'}
    self.assertEqual( res, resExpected )

    pr = ProductionRequest( self.bkClientFake, self.diracProdIn )
    pr.dqFlag = 'OK,AA, BB'
    res = pr._getBKKQuery( 'fromPreviousProd', ['type1', 'type2'], 123 )
    resExpected = {'ProductionID':123,
                   'FileType':'type1;;;type2',
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
              'prodStepID': "13718['BHADRON.DST']",
              'ProcessingPass': 'Merging', 'Visible': 'N', 'DDDB': 'head-20110302',
              'OptionFiles': '$APPCONFIGOPTS/Merging/DV-Stripping14-Merging.py', 'CONDDB': 'head-20110407',
              'fileTypesIn': ['BHADRON.DST'],
              'fileTypesOut': ['BHADRON.DST']},
             {'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': 13718, 'ApplicationVersion': 'v28r3p1',
              'ExtraPackages': 'AppConfig.v3r104', 'StepName': 'Stripping14-Merging',
              'prodStepID': "13718['CALIBRATION.DST']",
              'ProcessingPass': 'Merging', 'Visible': 'N', 'DDDB': 'head-20110302',
              'OptionFiles': '$APPCONFIGOPTS/Merging/DV-Stripping14-Merging.py', 'CONDDB': 'head-20110407',
              'fileTypesIn': ['CALIBRATION.DST'],
              'fileTypesOut': ['CALIBRATION.DST']}
            ]

    self.assertEqual( r, r_exp )

class ProductionRequestFailure( ClientTestCase ):

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

    pr.events = ['100', '-1']

    pr.eventsToProduce = 10000

    pr.eventType = '11124001'
    pr.parentRequestID = '34'
    pr.requestID = '35'

    pr.prodGroup = 'Sim05/Trig0x40760037Flagged/Reco12a/Stripping17Flagged'
    pr.dataTakingConditions = 'Beam3500GeV-2011-MagDown-Nu2-EmNoCuts'

    pr.prodsTypeList = ['Stripping', 'Merge']
    pr.outputSEs = ['Tier1_MC-DST', 'Tier1_MC-DST']
    pr.specialOutputSEs = [{}, {}]
    pr.stepsInProds = [range( 1, len( pr.stepsList ) ), [len( pr.stepsList )]]
    pr.removeInputsFlags = [False, True]
    pr.priorities = [1, 6]
    pr.cpus = [1000, 100]
    pr.outputModes = ['Local', 'Any']
    pr.outputFileMasks = ['FOO', '']
    pr.outputFileSteps = ['2', '']
    pr.targets = ['Tier2', '']
    pr.groupSizes = [1, 5]
    pr.plugins = ['', 'BySize']
    pr.inputDataPolicies = ['', 'protocol']
    pr.inputs = [[], []]

    res = pr.buildAndLaunchRequest()

    self.assertEqual( res, {'OK':True, 'Value': [321, 321, 321]} )

#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ClientTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProductionSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProductionRequestSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProductionRequestFailure ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProductionRequestFullChain ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
