""" A mock of the BookkeepingClient, used for testing purposes
"""

#pylint: disable=protected-access, missing-docstring, invalid-name, line-too-long

from mock import MagicMock

bkc_mock = MagicMock()
bkc_mock.sendBookkeeping.return_value = {'OK': True, 'Value': ''}
bkc_mock.getFileTypes.return_value = {'OK': True,
                                      'rpcStub': ( ( 'Bookkeeping/BookkeepingManager',
                                                     {'skipCACheck': False, 'delegatedGroup': 'diracAdmin',
                                                      'timeout': 3600} ), 'getFileTypes', ( {}, ) ),
                                      'Value': {'TotalRecords': 48, 'ParameterNames': ['FileTypes'],
                                                'Records': [['DAVINCIHIST'], ['DIELECTRON.DST'], ['BU2JPSIK.MDST'],
                                                            ['SIM'], ['BD2JPSIKS.MDST'],
                                                            ['BU2JPSIK.DST'], ['BUBDBSSELECTION.DST'],
                                                            ['LAMBDA.DST'], ['BSMUMUBLIND.DST'], ['HADRONIC.DST']]}}
bkc_mock.getFileMetadata.return_value = {'OK': True,
                                         'Value': {'Successful':{'foo': {'ADLER32': None,
                                                                         'FileType': 'SDST',
                                                                         'FullStat': None,
                                                                         'GotReplica': 'Yes',
                                                                         'RunNumber': 93718},
                                                                 'bar': {'ADLER32': None,
                                                                         'FileType': 'SDST',
                                                                         'FullStat': None,
                                                                         'GotReplica': 'Yes',
                                                                         'RunNumber': 93720}}},
                                         'rpcStub': ( ( 'Bookkeeping/BookkeepingManager', ) )}
bkc_mock.getFileTypeVersion.return_value = {'OK': True,
                                            'Value': {'lfn1': 'ROOT',
                                                      'lfn2': 'MDF'}}
bkc_mock.getFileDescendants.return_value = { 'OK': True,
                                             'Value': {'Failed': [],
                                                       'NotProcessed': [],
                                                       'Successful': {'aa.raw': ['bb.raw', 'bb.log']},
                                                       'WithMetadata': {'aa.raw': {'bb.raw': {'FileType': 'RAW',
                                                                                              'RunNumber': 97019,
                                                                                              'GotReplica':'Yes'},
                                                                                   'bb.log': {'FileType': 'LOG',
                                                                                              'GotReplica':'Yes'}
                                                                                  }
                                                                       }
                                                      }
                                           }


class BookkeepingClientFake(object):
  """ a fake BookkeepingClient - replicating some of the methods
  """
  def getAvailableSteps( self, stepID ):
    if stepID == {'StepId':123}:
      return {'OK': True,
              'Value': {'TotalRecords': 1,
                        'ParameterNames': [ 'StepId', 'StepName', 'ApplicationName', 'ApplicationVersion',
                                            'OptionFiles', 'Visible', 'ExtraPackages', 'ProcessingPass', 'OptionsFormat',
                                            'DDDB', 'CONDDB', 'DQTag', 'SystemConfig'],
                        'Records': [[123, 'Stripping14-Stripping', 'DaVinci', 'v2r2',
                                     'optsFiles', 'Yes', 'eps', 'procPass', '',
                                     '', '123456', '', '']]}}
    elif stepID in ( {'StepId':456}, {'StepId':987} ):
      return {'OK': True,
              'Value': {'TotalRecords': 1,
                        'ParameterNames': [ 'StepId', 'StepName', 'ApplicationName', 'ApplicationVersion',
                                            'OptionFiles', 'Visible', 'ExtraPackages', 'ProcessingPass', 'OptionsFormat',
                                            'DDDB', 'CONDDB', 'DQTag', 'SystemConfig'],
                        'Records': [[456, 'Merge', 'LHCb', 'v1r2',
                                     'optsFiles', 'Yes', 'eps', 'procPass', '',
                                     '', 'fromPreviousStep', '', 'x86']]}}
    elif stepID == {'StepId': 789}:
      return {'OK': True,
              'Value': {'TotalRecords': 1,
                        'ParameterNames': [ 'StepId', 'StepName', 'ApplicationName', 'ApplicationVersion',
                                            'OptionFiles', 'Visible', 'ExtraPackages', 'ProcessingPass', 'OptionsFormat',
                                            'DDDB', 'CONDDB', 'DQTag', 'SystemConfig'],
                        'Records': [[789, 'MergeHisto', 'Noether', 'v1r2',
                                     'optsFiles', 'Yes', 'eps', 'procPass', '',
                                     '', 'fromPreviousStep', '', 'x86']]}}
    elif stepID == {'StepId':125080}:
      return {'OK': True,
              'Value': {'TotalRecords': 1,
                        'ParameterNames': [ 'StepId', 'StepName', 'ApplicationName', 'ApplicationVersion',
                                            'ExtraPackages', 'ProcessingPass', 'Visible', 'Usable',
                                            'DDDB', 'CONDDB', 'DQTag', 'OptionsFormat', 'OptionFiles',
                                            'isMulticore', 'SystemConfig', 'mcTCK', 'ExtraOptions'],
                        'Records': [[ 125080, 'Sim08a', 'Gauss', 'v45r3', 'AppConfig.v3r171', 'Sim08a', 'Y',
                                      'Yes', 'Sim08-20130503-1', 'Sim08-20130503-1-vc-mu100', '', '',
                                      '$APPCONFIGOPTS/Gauss/Sim08-Beam4000GeV-mu100-2012-nu2.5.py;$DECFILESROOT/options/11102400.py;$LBPYTHIA8ROOT/options/Pythia8.py;$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py',
                                      'N', 'x86_64-slc5-gcc43-opt', '', '']]}}
    elif stepID == {'StepId':124620}:
      return {'OK': True,
              'Value' : { 'TotalRecords': 1,
                          'ParameterNames' : ['StepId', 'StepName', 'ApplicationName', 'ApplicationVersion',
                                              'ExtraPackages', 'ProcessingPass', 'Visible', 'Usable',
                                              'DDDB', 'CONDDB', 'DQTag', 'OptionsFormat', 'OptionFiles',
                                              'isMulticore', 'SystemConfig', 'mcTCK', 'ExtraOptions'],
                          'Records': [[ 124620, 'Digi13', 'Boole', 'v26r3', 'AppConfig.v3r164', 'Digi13', 'N',
                                        'Yes', 'Sim08-20130503-1', 'Sim08-20130503-1-vc-mu100', '', '',
                                        '$APPCONFIGOPTS/Boole/Default.py;$APPCONFIGOPTS/Boole/DataType-2012.py;$APPCONFIGOPTS/Boole/Boole-SiG4EnergyDeposit.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py',
                                        'N', 'x86_64-slc5-gcc43-opt', '', '']]}}
    elif stepID == {'StepId':999}:
      return {'OK': True,
             'Value' : { 'TotalRecords': 1,
                          'ParameterNames' : ['StepId', 'OptionFiles', 'ExtraOptions', 'SystemConfig', 'mcTCK', 'isMulticore', 'StepName',
                          'ApplicationName', 'ApplicationVersion', 'Visible', 'Usable', 'ProcessingPass', 'ExtraPackages', 'DDDB', 'CONDDB', 'DQTag'],
                          'Records': [[ 999, '$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py', '', 'x86_64-slc6-gcc49-opt', '', 'N',
                          'Stripping28', 'DaVinci', 'v41r3', 'Yes', 'Yes', 'Stripping28', 'AppConfig.v3r306', 'dddb-20150724', 'cond-20161011', '']]}}
    elif stepID == {'StepId':998}:
      return {'OK': True,
             'Value' : { 'TotalRecords': 1,
                          'ParameterNames' : ['StepId', 'OptionFiles', 'ExtraOptions', 'SystemConfig', 'mcTCK', 'isMulticore', 'StepName',
                          'ApplicationName', 'ApplicationVersion', 'Visible', 'Usable', 'ProcessingPass', 'ExtraPackages', 'DDDB', 'CONDDB', 'DQTag'],
                          'Records': [[ 998, '', '', 'x86_64-slc6-gcc49-opt', '', 'N',
                          'Stripping28', 'DaVinci', 'v41r3', 'Yes', 'Yes', 'Stripping28', 'AppConfig.v3r306', 'dddb-20150724', 'cond-20161011', '']]}}

  def getStepInputFiles( self, stepID ):
    if stepID == 123:
      return {'OK': True,
              'Value': {'TotalRecords': 7,
                        'ParameterNames': ['FileType', 'Visible'],
                        'Records': [['SDST', 'Y']]}}

    if stepID == 456:
      return {'OK': True,
              'Value': {'TotalRecords': 7,
                        'ParameterNames': ['FileType', 'Visible'],
                        'Records': [['BHADRON.DST', 'Y'], ['CALIBRATION.DST', 'Y']]}}
    if stepID == 789:
      return {'OK': True,
              'Value': {'TotalRecords': 7,
                        'ParameterNames': ['FileType', 'Visible'],
                        'Records': [['DAVINCIHIST', 'Y'], ['BRUNELHIST', 'Y']]}}
    if stepID == 125080:
      return {'OK': True,
              'Value': {'TotalRecords': 7,
                        'ParameterNames': ['FileType', 'Visible'],
                        'Records': [['', 'Y']]}}
    if stepID == 124620:
      return {'OK': True,
              'Value': {'TotalRecords': 7,
                        'ParameterNames': ['FileType', 'Visible'],
                        'Records': [['SIM', 'N']]}}
    if stepID == 999 or stepID == 998:
      return {'OK': True,
              'Value': {'TotalRecords': 7,
                        'ParameterNames': ['FileType', 'Visible'],
                        'Records': [['SDST', 'Y']]}}


  def getStepOutputFiles( self, stepID ):
    if stepID == 123:
      return {'OK': True,
              'Value': {'TotalRecords': 7,
                        'ParameterNames': ['FileType', 'Visible'],
                        'Records': [['BHADRON.DST', 'Y'], ['CALIBRATION.DST', 'Y']]}}
    if stepID == 456:
      return {'OK': True,
              'Value': {'TotalRecords': 7,
                        'ParameterNames': ['FileType', 'Visible'],
                        'Records': [['BHADRON.DST', 'Y'], ['CALIBRATION.DST', 'Y']]}}
    if stepID == 789:
      return {'OK': True,
              'Value': {'TotalRecords': 7,
                        'ParameterNames': ['FileType', 'Visible'],
                        'Records': [['ROOT', 'Y']]}}
    if stepID == 125080:
      return {'OK': True,
              'Value': {'TotalRecords': 7,
                        'ParameterNames': ['FileType', 'Visible'],
                        'Records': [['SIM', 'Y']]}}
    if stepID == 124620:
      return {'OK': True,
              'Value': {'TotalRecords': 7,
                        'ParameterNames': ['FileType', 'Visible'],
                        'Records': [['DIGI', 'N']]}}
    if stepID == 999 or stepID == 998:
      return {'OK': True,
              'Value': {'TotalRecords': 7,
                        'ParameterNames': ['FileType', 'Visible'],
                        'Records': [['BHADRON.DST', 'Y'], ['CALIBRATION.DST', 'Y']]}}


# Some steps definitions, for testing purposes

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

stepHistoMergingDict = {'StepId': 789, 'StepName':'MergeHisto',
                        'ApplicationName':'Noether', 'ApplicationVersion':'v1r2', 'ExtraOptions': '',
                        'OptionFiles':'optsFiles', 'Visible':'Yes', 'ExtraPackages':'eps',
                        'ProcessingPass':'procPass', 'OptionsFormat':'', 'SystemConfig':'x86',
                        'prodStepID': "789['DAVINCIHIST', 'BRUNELHIST']",
                        'DDDB':'', 'CONDDB':'123456', 'DQTag':'', 'isMulticore': 'N',
                        'mcTCK': '',
                        'fileTypesIn':['DAVINCIHIST', 'BRUNELHIST'],
                        'fileTypesOut':['ROOT']}
