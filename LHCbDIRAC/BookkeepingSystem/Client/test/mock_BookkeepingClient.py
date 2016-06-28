""" A mock of the BookkeepingClient, used for testing purposes
"""

class BookkeepingClientFake(object):
  """ a fake BookkeepingClient - replicating some of the methods
  """
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
