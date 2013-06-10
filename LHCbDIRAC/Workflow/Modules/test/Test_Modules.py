import unittest, itertools, os, copy, shutil

from mock import Mock, patch

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers import Resources

from LHCbDIRAC.Workflow.Modules.ModulesUtilities import lowerExtension, getEventsToProduce, getCPUNormalizationFactorAvg

class ModulesTestCase( unittest.TestCase ):
  """ Base class for the Modules test cases
  """
  def setUp( self ):

    gLogger.setLevel( 'DEBUG' )
#    import sys
#    sys.modules["DIRAC"] = DIRAC.ResourceStatusSystem.test.fake_Logger
#    sys.modules["DIRAC.ResourceStatusSystem.Utilities.CS"] = DIRAC.ResourceStatusSystem.test.fake_Logger

    jr_mock = Mock()
    jr_mock.setApplicationStatus.return_value = {'OK': True, 'Value': ''}
    jr_mock.generateForwardDISET.return_value = {'OK': True, 'Value': 'pippo'}
    jr_mock.setJobParameter.return_value = {'OK': True, 'Value': 'pippo'}
#    jr_mock.setJobApplicationStatus.return_value = {'OK': True, 'Value': 'pippo'}

    self.fr_mock = Mock()
    self.fr_mock.getFiles.return_value = {}
    self.fr_mock.setFileStatus.return_value = {'OK': True, 'Value': ''}
    self.fr_mock.commit.return_value = {'OK': True, 'Value': ''}
    self.fr_mock.generateForwardDISET.return_value = {'OK': True, 'Value': ''}

    rc_mock = Mock()
    rc_mock.update.return_value = {'OK': True, 'Value': ''}
    rc_mock.setDISETRequest.return_value = {'OK': True, 'Value': ''}
    rc_mock.isEmpty.return_value = {'OK': True, 'Value': ''}
    rc_mock.toXML.return_value = {'OK': True, 'Value': ''}
    rc_mock.getDigest.return_value = {'OK': True, 'Value': ''}
    self.rc_mock = rc_mock

    ar_mock = Mock()
    ar_mock.commit.return_value = {'OK': True, 'Value': ''}

    self.rm_mock = Mock()
    self.rm_mock.getReplicas.return_value = {'OK': True, 'Value':{'Successful':{'pippo':'metadataPippo'},
                                                                  'Failed':None}}
    self.rm_mock.getCatalogFileMetadata.return_value = {'OK': True, 'Value':{'Successful':{'pippo':'metadataPippo'},
                                                                             'Failed':None}}
    self.rm_mock.removeFile.return_value = {'OK': True, 'Value': {'Failed':False}}
    self.rm_mock.putStorageDirectory.return_value = {'OK': True, 'Value': {'Failed':False}}
    self.rm_mock.addCatalogFile.return_value = {'OK': True, 'Value': {'Failed':False}}
    self.rm_mock.putAndRegister.return_value = {'OK': True, 'Value': {'Failed':False}}
    self.rm_mock.getFile.return_value = {'OK': True, 'Value': {'Failed':False}}

    self.jsu_mock = Mock()
    self.jsu_mock.setJobApplicationStatus.return_value = {'OK': True, 'Value': ''}

    self.jsu_mock = Mock()
    self.jsu_mock.setJobApplicationStatus.return_value = {'OK': True, 'Value': ''}

    request_mock = Mock()
    request_mock.addSubRequest.return_value = {'OK': True, 'Value': ''}
    request_mock.setSubRequestFiles.return_value = {'OK': True, 'Value': ''}
    request_mock.getNumSubRequests.return_value = {'OK': True, 'Value': ''}
    request_mock._getLastOrder.return_value = 1

    self.ft_mock = Mock()
    self.ft_mock.transferAndRegisterFile.return_value = {'OK': True, 'Value': {'uploadedSE':''}}
    self.ft_mock.transferAndRegisterFileFailover.return_value = {'OK': True, 'Value': {}}
    self.ft_mock.getRequestObject.return_value = {'OK': True, 'Value': request_mock}

    self.bkc_mock = Mock()
    self.bkc_mock.sendBookkeeping.return_value = {'OK': True, 'Value': ''}
    self.bkc_mock.getFileTypes.return_value = {'OK': True,
                                               'rpcStub': ( ( 'Bookkeeping/BookkeepingManager',
                                                            {'skipCACheck': False, 'delegatedGroup': 'diracAdmin',
                                                            'timeout': 3600} ), 'getFileTypes', ( {}, ) ),
                                               'Value': {'TotalRecords': 48, 'ParameterNames': ['FileTypes'],
                                                         'Records': [['DAVINCIHIST'], ['DIELECTRON.DST'], ['BU2JPSIK.MDST'],
                                                                     ['SIM'], ['BD2JPSIKS.MDST'],
                                                                     ['BU2JPSIK.DST'], ['BUBDBSSELECTION.DST'],
                                                                     ['LAMBDA.DST'], ['BSMUMUBLIND.DST'], ['HADRONIC.DST']]}}
    self.bkc_mock.getFileMetadata.return_value = {'OK': True,
                                                  'Value': {'Successful':{
                                                                          'foo': {'ADLER32': None,
                                                                                  'FileType': 'SDST',
                                                                                  'FullStat': None,
                                                                                  'GotReplica': 'Yes',
                                                                                  'RunNumber': 93718},
                                                                          'bar': {'ADLER32': None,
                                                                                  'FileType': 'SDST',
                                                                                  'FullStat': None,
                                                                                  'GotReplica': 'Yes',
                                                                                  'RunNumber': 93720}}
                                                            },
                                                  'rpcStub': ( ( 'Bookkeeping/BookkeepingManager', ) )
                                                  }

    self.nc_mock = Mock()
    self.nc_mock.sendMail.return_value = {'OK': True, 'Value': ''}

    self.xf_o_mock = Mock()
    self.xf_o_mock.inputFileStats = {'a':1, 'b':2}
    self.xf_o_mock.outputFileStats = {'a':1, 'b':2}
    self.xf_o_mock.analyse.return_value = {'OK': True, 'Value': ''}

    self.jobStep_mock = Mock()
    self.jobStep_mock.commit.return_value = {'OK': True, 'Value': ''}
    self.jobStep_mock.setValuesFromDict.return_value = {'OK': True, 'Value': ''}
    self.jobStep_mock.checkValues.return_value = {'OK': True, 'Value': ''}

    self.version = 'someVers'
    self.prod_id = '123'
    self.prod_job_id = '00000456'
    self.wms_job_id = '00012345'
    self.workflowStatus = {'OK':True}
    self.stepStatus = {'OK':True}
    self.wf_commons = [
                       {'PRODUCTION_ID': self.prod_id, 'JOB_ID': self.prod_job_id, 'eventType': '123456789', 'jobType': 'merge',
                        'configName': 'aConfigName', 'configVersion': 'aConfigVersion', 'outputDataFileMask':'',
                        'BookkeepingLFNs':'aa', 'ProductionOutputData':'ProductionOutputData', 'numberOfEvents':'100',
                        'JobReport':jr_mock, 'Request':rc_mock, 'AccountingReport': ar_mock, 'FileReport':self.fr_mock,
                        'SystemConfig':'sys_config', 'runNumber':'Unknown', 'gaudiSteps': ['someApp_1']},
                       {'PRODUCTION_ID': self.prod_id, 'JOB_ID': self.prod_job_id,
                        'configName': 'aConfigName', 'configVersion': 'aConfigVersion', 'outputDataFileMask':'', 'jobType': 'merge',
                        'BookkeepingLFNs':'aa', 'ProductionOutputData':'ProductionOutputData', 'numberOfEvents':'100',
                        'JobReport':jr_mock, 'Request':rc_mock, 'AccountingReport': ar_mock, 'FileReport':self.fr_mock,
                        'SystemConfig':'sys_config', 'LogFilePath':'someDir', 'runNumber':'Unknown',
                        'gaudiSteps': ['someApp_1']},
                       {'PRODUCTION_ID': self.prod_id, 'JOB_ID': self.prod_job_id,
                        'configName': 'aConfigName', 'configVersion': 'aConfigVersion', 'outputDataFileMask':'', 'jobType': 'merge',
                        'BookkeepingLFNs':'aa', 'ProductionOutputData':'ProductionOutputData', 'numberOfEvents':'100',
                        'JobReport':jr_mock, 'Request':rc_mock, 'AccountingReport': ar_mock, 'FileReport':self.fr_mock,
                        'SystemConfig':'sys_config', 'LogFilePath':'someDir', 'LogTargetPath':'someOtherDir',
                        'runNumber':'Unknown', 'gaudiSteps': ['someApp_1']},
                       {'PRODUCTION_ID': self.prod_id, 'JOB_ID': self.prod_job_id,
                        'configName': 'aConfigName', 'configVersion': 'aConfigVersion', 'outputDataFileMask':'', 'jobType': 'merge',
                        'BookkeepingLFNs':'aa', 'ProductionOutputData':'ProductionOutputData', 'numberOfEvents':'100',
                        'JobReport':jr_mock, 'Request':rc_mock, 'AccountingReport': ar_mock, 'FileReport':self.fr_mock,
                        'SystemConfig':'sys_config', 'LogFilePath':'someDir', 'LogTargetPath':'someOtherDir',
                        'runNumber':'Unknown', 'gaudiSteps': ['someApp_1'] },
                       {'PRODUCTION_ID': self.prod_id, 'JOB_ID': self.prod_job_id,
                        'configName': 'aConfigName', 'configVersion': 'aConfigVersion', 'outputDataFileMask':'', 'jobType': 'reco',
                        'BookkeepingLFNs':'aa', 'ProductionOutputData':'ProductionOutputData',
                        'JobReport':jr_mock, 'Request':rc_mock, 'AccountingReport': ar_mock, 'FileReport':self.fr_mock,
                        'SystemConfig':'sys_config', 'runNumber':'Unknown', 'gaudiSteps': ['someApp_1']},
                       {'PRODUCTION_ID': self.prod_id, 'JOB_ID': self.prod_job_id,
                        'configName': 'aConfigName', 'configVersion': 'aConfigVersion', 'outputDataFileMask':'', 'jobType': 'reco',
                        'BookkeepingLFNs':'aa', 'ProductionOutputData':'ProductionOutputData',
                        'JobReport':jr_mock, 'Request':rc_mock, 'AccountingReport': ar_mock, 'FileReport':self.fr_mock,
                        'SystemConfig':'sys_config', 'LogFilePath':'someDir', 'runNumber':'Unknown',
                        'gaudiSteps': ['someApp_1']},
                       {'PRODUCTION_ID': self.prod_id, 'JOB_ID': self.prod_job_id,
                        'configName': 'aConfigName', 'configVersion': 'aConfigVersion', 'outputDataFileMask':'', 'jobType': 'reco',
                        'BookkeepingLFNs':'aa', 'ProductionOutputData':'ProductionOutputData',
                        'JobReport':jr_mock, 'Request':rc_mock, 'AccountingReport': ar_mock, 'FileReport':self.fr_mock,
                        'SystemConfig':'sys_config', 'LogFilePath':'someDir', 'LogTargetPath':'someOtherDir',
                        'runNumber':'Unknown', 'gaudiSteps': ['someApp_1']},
                       {'PRODUCTION_ID': self.prod_id, 'JOB_ID': self.prod_job_id,
                        'configName': 'aConfigName', 'configVersion': 'aConfigVersion', 'outputDataFileMask':'', 'jobType': 'reco',
                        'BookkeepingLFNs':'aa', 'ProductionOutputData':'ProductionOutputData',
                        'JobReport':jr_mock, 'Request':rc_mock, 'AccountingReport': ar_mock, 'FileReport':self.fr_mock,
                        'SystemConfig':'sys_config', 'LogFilePath':'someDir', 'LogTargetPath':'someOtherDir',
                        'runNumber':'Unknown', 'gaudiSteps': ['someApp_1']},
                       {'PRODUCTION_ID': self.prod_id, 'JOB_ID': self.prod_job_id,
                        'configName': 'aConfigName', 'configVersion': 'aConfigVersion', 'outputDataFileMask':'', 'jobType': 'reco',
                        'BookkeepingLFNs':'aa', 'ProductionOutputData':'ProductionOutputData',
                        'JobReport':jr_mock, 'Request':rc_mock, 'AccountingReport': ar_mock, 'FileReport':self.fr_mock,
                        'SystemConfig':'sys_config', 'LogFilePath':'someDir', 'LogTargetPath':'someOtherDir',
                        'runNumber':'Unknown', 'InputData': '', 'gaudiSteps': ['someApp_1'] },
                       {'PRODUCTION_ID': self.prod_id, 'JOB_ID': self.prod_job_id,
                        'configName': 'aConfigName', 'configVersion': 'aConfigVersion', 'outputDataFileMask':'', 'jobType': 'reco',
                        'BookkeepingLFNs':'aa', 'ProductionOutputData':'ProductionOutputData',
                        'JobReport':jr_mock, 'Request':rc_mock, 'AccountingReport': ar_mock, 'FileReport':self.fr_mock,
                        'SystemConfig':'sys_config', 'LogFilePath':'someDir', 'LogTargetPath':'someOtherDir',
                        'runNumber':'Unknown', 'InputData': 'foo;bar', 'gaudiSteps': ['someApp_1'] },
                       {'PRODUCTION_ID': self.prod_id, 'JOB_ID': self.prod_job_id,
                        'configName': 'aConfigName', 'configVersion': 'aConfigVersion', 'outputDataFileMask':'', 'jobType': 'reco',
                        'BookkeepingLFNs':'aa', 'ProductionOutputData':'ProductionOutputData',
                        'JobReport':jr_mock, 'Request':rc_mock, 'AccountingReport': ar_mock, 'FileReport':self.fr_mock,
                        'SystemConfig':'sys_config', 'LogFilePath':'someDir', 'LogTargetPath':'someOtherDir',
                        'runNumber':'Unknown', 'InputData': 'foo;bar', 'ParametricInputData':'' ,
                        'gaudiSteps': ['someApp_1']},
                       {'PRODUCTION_ID': self.prod_id, 'JOB_ID': self.prod_job_id,
                        'configName': 'aConfigName', 'configVersion': 'aConfigVersion', 'outputDataFileMask':'', 'jobType': 'reco',
                        'BookkeepingLFNs':'aa', 'ProductionOutputData':'ProductionOutputData',
                        'JobReport':jr_mock, 'Request':rc_mock, 'AccountingReport': ar_mock, 'FileReport':self.fr_mock,
                        'SystemConfig':'sys_config', 'LogFilePath':'someDir', 'LogTargetPath':'someOtherDir',
                        'runNumber':'Unknown', 'InputData': 'foo;bar', 'ParametricInputData':'pid1;pid2;pid3',
                        'gaudiSteps': ['someApp_1']},
                       ]
    self.step_commons = [
                         {'applicationName':'someApp', 'applicationVersion':'v1r0', 'eventType': '123456789',
                         'applicationLog':'appLog', 'extraPackages':'', 'XMLSummary':'XMLSummaryFile',
                         'numberOfEvents':'100', 'BKStepID':'123', 'StepProcPass':'Sim123', 'outputFilePrefix':'pref_',
                         'STEP_INSTANCE_NAME':'someApp_1',
                         'listoutput':[{'outputDataName':self.prod_id + '_' + self.prod_job_id + '_', 'outputDataSE':'aaa',
                                       'outputDataType':'bbb'}]},
                         {'applicationName':'someApp', 'applicationVersion':'v1r0', 'eventType': '123456789',
                         'applicationLog':'appLog', 'extraPackages':'', 'XMLSummary':'XMLSummaryFile',
                         'numberOfEvents':'100', 'BKStepID':'123', 'StepProcPass':'Sim123', 'outputFilePrefix':'pref_',
                         'optionsLine': '',
                         'STEP_INSTANCE_NAME':'someApp_1',
                         'listoutput':[{'outputDataName':self.prod_id + '_' + self.prod_job_id + '_', 'outputDataSE':'aaa',
                                       'outputDataType':'bbb'}]},
                         {'applicationName':'someApp', 'applicationVersion':'v1r0', 'eventType': '123456789',
                         'applicationLog':'appLog', 'extraPackages':'', 'XMLSummary':'XMLSummaryFile',
                         'numberOfEvents':'100', 'BKStepID':'123', 'StepProcPass':'Sim123', 'outputFilePrefix':'pref_',
                         'extraOptionsLine': 'blaBla',
                         'STEP_INSTANCE_NAME':'someApp_1',
                         'listoutput':[{'outputDataName':self.prod_id + '_' + self.prod_job_id + '_', 'outputDataSE':'aaa',
                                       'outputDataType':'bbb'}]}
                         ]
    self.step_number = '321'
    self.step_id = '%s_%s_%s' % ( self.prod_id, self.prod_job_id, self.step_number )



    from LHCbDIRAC.Workflow.Modules.ModuleBase import ModuleBase
    self.mb = ModuleBase( bkClientIn = self.bkc_mock, rm = self.rm_mock )

    from LHCbDIRAC.Workflow.Modules.AnalyseLogFile import AnalyseLogFile
    self.alf = AnalyseLogFile( bkClient = self.bkc_mock, rm = self.rm_mock )

    from LHCbDIRAC.Workflow.Modules.AnalyseXMLSummary import AnalyseXMLSummary
    self.axlf = AnalyseXMLSummary( bkClient = self.bkc_mock, rm = self.rm_mock )

    from LHCbDIRAC.Workflow.Modules.GaudiApplication import GaudiApplication
    self.ga = GaudiApplication( bkClient = self.bkc_mock, rm = self.rm_mock )

    from LHCbDIRAC.Workflow.Modules.GaudiApplicationScript import GaudiApplicationScript
    self.gas = GaudiApplicationScript( bkClient = self.bkc_mock, rm = self.rm_mock )

    from LHCbDIRAC.Workflow.Modules.BookkeepingReport import BookkeepingReport
    self.bkr = BookkeepingReport( bkClient = self.bkc_mock, rm = self.rm_mock )

    from LHCbDIRAC.Workflow.Modules.ErrorLogging import ErrorLogging
    self.el = ErrorLogging( bkClient = self.bkc_mock, rm = self.rm_mock )

    from LHCbDIRAC.Workflow.Modules.FailoverRequest import FailoverRequest
    self.fr = FailoverRequest( bkClient = self.bkc_mock, rm = self.rm_mock )

    from LHCbDIRAC.Workflow.Modules.MergeMDF import MergeMDF
    self.mm = MergeMDF( bkClient = self.bkc_mock, rm = self.rm_mock )

    from LHCbDIRAC.Workflow.Modules.ProtocolAccessTest import ProtocolAccessTest
    self.pat = ProtocolAccessTest( bkClient = self.bkc_mock, rm = self.rm_mock )

    from LHCbDIRAC.Workflow.Modules.RemoveInputData import RemoveInputData
    self.rid = RemoveInputData( bkClient = self.bkc_mock, rm = self.rm_mock )

    from LHCbDIRAC.Workflow.Modules.SendBookkeeping import SendBookkeeping
    self.sb = SendBookkeeping( bkClient = self.bkc_mock, rm = self.rm_mock )

    from LHCbDIRAC.Workflow.Modules.UploadOutputData import UploadOutputData
    self.uod = UploadOutputData( bkClient = self.bkc_mock, rm = self.rm_mock )

    from LHCbDIRAC.Workflow.Modules.UserJobFinalization import UserJobFinalization
    self.ujf = UserJobFinalization( bkClient = self.bkc_mock, rm = self.rm_mock )
    self.ujf.bkClient = self.bkc_mock

    from LHCbDIRAC.Workflow.Modules.StepAccounting import StepAccounting
    self.sa = StepAccounting( bkClient = self.bkc_mock, rm = self.rm_mock )

    from LHCbDIRAC.Workflow.Modules.UploadLogFile import UploadLogFile
    self.ulf = UploadLogFile( bkClient = self.bkc_mock, rm = self.rm_mock )

    from LHCbDIRAC.Workflow.Modules.FileUsage import FileUsage
    self.fu = FileUsage( bkClient = self.bkc_mock, rm = self.rm_mock )

    from LHCbDIRAC.Workflow.Modules.CreateDataFile import CreateDataFile
    self.cdf = CreateDataFile( bkClient = self.bkc_mock, rm = self.rm_mock )

  def tearDown( self ):
    for fileProd in ['appLog', 'foo.txt', 'aaa.Bhadron.dst', 'bbb.Calibration.dst',
                     'ccc.charm.mdst', 'prova.txt', 'foo.txt', 'BAR.txt', 'FooBAR.ext.txt', 'foo_1.txt', 'bar_2.py',
                     'ErrorLogging_Step1_coredump.log', '123_00000456_request.xml', 'lfn1', 'lfn2',
                     'aaa.bhadron.dst', 'bbb.calibration.dst', 'ProductionOutputData', 'data.py',
                     '00000123_00000456.tar', 'someOtherDir', 'DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK',
                     ]:
      try:
        os.remove( fileProd )
      except OSError:
        continue

    for directory in ['./job', 'job']:
      try:
        shutil.rmtree( directory )
      except:
        continue

#############################################################################
# ModuleBase.py
#############################################################################

class ModuleBaseSuccess( ModulesTestCase ):

  #################################################

  def test__checkLocalExistance( self ):

    self.assertRaises( OSError, self.mb._checkLocalExistance, ['aaa', 'bbb'] )

  #################################################

  def test__applyMask( self ):

    candidateFiles = {
                      '00012345_00012345_4.dst':
                        {'lfn': '/lhcb/MC/2010/DST/00012345/0001/00012345_00012345_4.dst',
                         'type': 'dst',
                         'workflowSE': 'Tier1_MC_M-DST'},
                      '00012345_00012345_2.digi': {'type': 'digi', 'workflowSE': 'Tier1-RDST'},
                      '00012345_00012345_3.digi': {'type': 'digi', 'workflowSE': 'Tier1-RDST'},
                      '00012345_00012345_5.AllStreams.dst':
                        {'lfn': '/lhcb/MC/2010/DST/00012345/0001/00012345_00012345_5.AllStreams.dst',
                         'type': 'allstreams.dst',
                         'workflowSE': 'Tier1_MC_M-DST'},
                      '00012345_00012345_1.sim': {'type': 'sim', 'workflowSE': 'Tier1-RDST'}}


    fileMasks = ( ['dst'], 'dst', ['sim'], ['digi'], ['digi', 'sim'], 'allstreams.dst' )
    stepMasks = ( '', '5', '', ['2'], ['1', '3'], '' )

    results = ( 
               {
                '00012345_00012345_4.dst':
                  {'lfn': '/lhcb/MC/2010/DST/00012345/0001/00012345_00012345_4.dst',
                   'type': 'dst',
                   'workflowSE': 'Tier1_MC_M-DST'}
                },
               {},
                {
                 '00012345_00012345_1.sim': {'type': 'sim', 'workflowSE': 'Tier1-RDST'}
                },
                {
                 '00012345_00012345_2.digi': {'type': 'digi', 'workflowSE': 'Tier1-RDST'},
                },
                {
                 '00012345_00012345_3.digi': {'type': 'digi', 'workflowSE': 'Tier1-RDST'},
                 '00012345_00012345_1.sim': {'type': 'sim', 'workflowSE': 'Tier1-RDST'}
                },
                {
                 '00012345_00012345_5.AllStreams.dst':
                  {'lfn': '/lhcb/MC/2010/DST/00012345/0001/00012345_00012345_5.AllStreams.dst',
                   'type': 'allstreams.dst',
                   'workflowSE': 'Tier1_MC_M-DST'}
                }
               )



    for fileMask, result, stepMask in itertools.izip( fileMasks, results, stepMasks ):
      res = self.mb._applyMask( candidateFiles, fileMask, stepMask )
      self.assertEqual( res, result )

  #################################################

  def test__checkSanity( self ):

    candidateFiles = {
                      '00012345_00012345_4.dst':
                        {'lfn': '/lhcb/MC/2010/DST/00012345/0001/00012345_00012345_4.dst',
                         'type': 'dst',
                         'workflowSE': 'Tier1_MC_M-DST'},
                      '00012345_00012345_2.digi': {'type': 'digi', 'workflowSE': 'Tier1-RDST'},
                      '00012345_00012345_3.digi': {'type': 'digi', 'workflowSE': 'Tier1-RDST'},
                      '00012345_00012345_5.AllStreams.dst':
                        {'lfn': '/lhcb/MC/2010/DST/00012345/0001/00012345_00012345_5.AllStreams.dst',
                         'type': 'DST',
                         'workflowSE': 'Tier1_MC_M-DST'},
                      '00012345_00012345_1.sim': {'type': 'sim', 'workflowSE': 'Tier1-RDST'}}

    self.assertRaises( ValueError, self.mb._checkSanity, candidateFiles )

  #################################################

  def test_getCandidateFiles( self ):
    # this needs to avoid the "checkLocalExistance"

    open( 'foo_1.txt', 'w' ).close()
    open( 'bar_2.py', 'w' ).close()

    outputList = [{'outputDataType': 'txt', 'outputDataSE': 'Tier1-RDST', 'outputDataName': 'foo_1.txt'},
                  {'outputDataType': 'py', 'outputDataSE': 'Tier1-RDST', 'outputDataName': 'bar_2.py'}]
    outputLFNs = ['/lhcb/MC/2010/DST/00012345/0001/foo_1.txt', '/lhcb/MC/2010/DST/00012345/0001/bar_2.py']
    fileMask = 'txt'
    stepMask = ''
    result = {'foo_1.txt': {'lfn': '/lhcb/MC/2010/DST/00012345/0001/foo_1.txt',
                            'type': outputList[0]['outputDataType'],
                            'workflowSE': outputList[0]['outputDataSE']}}

    res = self.mb.getCandidateFiles( outputList, outputLFNs, fileMask, stepMask )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], result )

    fileMask = ['txt', 'py']
    stepMask = None
    result = {'foo_1.txt': {'lfn': '/lhcb/MC/2010/DST/00012345/0001/foo_1.txt',
                            'type': outputList[0]['outputDataType'],
                            'workflowSE': outputList[0]['outputDataSE']},
              'bar_2.py': {'lfn': '/lhcb/MC/2010/DST/00012345/0001/bar_2.py',
                           'type': outputList[1]['outputDataType'],
                           'workflowSE': outputList[1]['outputDataSE']},
              }
    res = self.mb.getCandidateFiles( outputList, outputLFNs, fileMask, stepMask )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], result )

    fileMask = ['aa']
    stepMask = None
    res = self.mb.getCandidateFiles( outputList, outputLFNs, fileMask, stepMask )
    result = {}
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], result )

    fileMask = ''
    stepMask = '2'
    result = {'bar_2.py': {'lfn': '/lhcb/MC/2010/DST/00012345/0001/bar_2.py',
                           'type': outputList[1]['outputDataType'],
                           'workflowSE': outputList[1]['outputDataSE']}}

    res = self.mb.getCandidateFiles( outputList, outputLFNs, fileMask, stepMask )

    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], result )

    fileMask = ''
    stepMask = 2
    result = {'bar_2.py': {'lfn': '/lhcb/MC/2010/DST/00012345/0001/bar_2.py',
                           'type': outputList[1]['outputDataType'],
                           'workflowSE': outputList[1]['outputDataSE']}}

    res = self.mb.getCandidateFiles( outputList, outputLFNs, fileMask, stepMask )

    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], result )


    fileMask = ''
    stepMask = ['2', '3']
    result = {'bar_2.py': {'lfn': '/lhcb/MC/2010/DST/00012345/0001/bar_2.py',
                           'type': outputList[1]['outputDataType'],
                           'workflowSE': outputList[1]['outputDataSE']}}

    res = self.mb.getCandidateFiles( outputList, outputLFNs, fileMask, stepMask )

    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], result )

    fileMask = ''
    stepMask = ['3']
    result = {}

    res = self.mb.getCandidateFiles( outputList, outputLFNs, fileMask, stepMask )

    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], result )

  def test__enableModule( self ):

    self.mb.execute( self.version, self.prod_id, self.prod_job_id, self.wms_job_id,
                     self.workflowStatus, self.stepStatus,
                     self.wf_commons, self.step_commons[0],
                     self.step_number, self.step_id )
    self.assertTrue( self.mb._enableModule() )

  def test__determineStepInputData( self ):

    self.mb.stepName = 'DaVinci_2'

    inputData = 'previousStep'
    self.mb.gaudiSteps = ['Brunel_1', 'DaVinci_2']
    self.mb.workflow_commons = {'outputList': [{'stepName': 'Brunel_1',
                                               'outputDataType': 'brunelhist',
                                               'outputBKType': 'BRUNELHIST',
                                               'outputDataSE': 'CERN-HIST',
                                               'outputDataName': 'Brunel_00012345_00006789_1_Hist.root'},
                                              {'stepName': 'Brunel_1',
                                               'outputDataType': 'sdst',
                                               'outputBKType': 'SDST',
                                               'outputDataSE': 'Tier1-BUFFER',
                                               'outputDataName': '00012345_00006789_1.sdst'}
                                              ]
                                }
    self.mb.inputDataType = 'SDST'

    first = self.mb._determineStepInputData( inputData )
    second = ['00012345_00006789_1.sdst']
    self.assertEqual( first, second )

    inputData = 'previousStep'
    self.mb.gaudiSteps = ['Brunel_1', 'DaVinci_2']
    self.mb.workflow_commons['outputList'] = [{'stepName': 'Brunel_1',
                                               'outputDataType': 'brunelhist',
                                               'outputBKType': 'BRUNELHIST',
                                               'outputDataSE': 'CERN-HIST',
                                               'outputDataName': 'Brunel_00012345_00006789_1_Hist.root'},
                                              {'stepName': 'Brunel_1',
                                               'outputDataType': 'sdst',
                                               'outputBKType': 'SDST',
                                               'outputDataSE': 'Tier1-BUFFER',
                                               'outputDataName': 'some.sdst'},
                                              {'stepName': 'Brunel_1',
                                               'outputDataType': 'sdst',
                                               'outputBKType': 'SDST',
                                               'outputDataSE': 'Tier1-BUFFER',
                                               'outputDataName': '00012345_00006789_1.sdst'}
                                              ]
    self.mb.inputDataType = 'SDST'
    first = self.mb._determineStepInputData( inputData )
    second = ['some.sdst', '00012345_00006789_1.sdst']
    self.assertEqual( first, second )

    inputData = 'LFN:123.raw'
    first = self.mb._determineStepInputData( inputData )
    second = ['123.raw']
    self.assertEqual( first, second )

  def test__determineOutputs( self ):
    self.mb.stepInputData = ['foo', 'bar']

    self.mb.jobType = 'merge'
    self.mb.step_id = '00000123_00000456_1'
    for step_commons in self.step_commons:
      self.mb.step_commons = step_commons
      self.mb.step_commons['listoutput'] = [{'outputDataType': 'bhadron.dst;sdst',
                                              'outputDataSE': 'Tier1_M-DST',
                                              'outputDataName': '00000123_00000456_1.bhadron.dst;sdst'}]
      outF, outft, histos = self.mb._determineOutputs()
      self.assertEqual( outF, [{'outputDataType': 'sdst',
                                'outputDataName': '00000123_00000456_1.sdst',
                                'outputDataSE': 'Tier1_M-DST',
                                'outputBKType': 'SDST'}] )
      self.assertEqual( outft, ['sdst'] )
      self.assertFalse( histos )

    self.mb.jobType = 'reco'
    for step_commons in self.step_commons:
      self.mb.step_commons = step_commons
      self.mb.step_commons['listoutput'] = [{'outputDataType': 'sdst',
                                             'outputDataSE': 'Tier1_M-DST',
                                             'outputDataName': '00000123_00000456_1.sdst',
                                             'outputBKType': 'SDST'}]
      outF, outft, histos = self.mb._determineOutputs()
      self.assertEqual( outF, [{'outputDataType': 'sdst',
                                'outputDataName': '00000123_00000456_1.sdst',
                                'outputDataSE': 'Tier1_M-DST',
                                'outputBKType': 'SDST'}] )
      self.assertEqual( outft, ['sdst'] )
      self.assertFalse( histos )

  def test__findOutputs( self ):
    open( 'aaa.Bhadron.dst', 'w' ).close()
    open( 'bbb.Calibration.dst', 'w' ).close()
    open( 'ccc.charm.mdst', 'w' ).close()
    open( 'prova.txt', 'w' ).close()

    stepOutput = [{'outputDataType': 'BHADRON.DST', 'outputDataSE': 'Tier1-DST', 'outputDataName': 'aaa.bhadron.dst'},
                  {'outputDataType': 'CALIBRATION.DST', 'outputDataSE': 'Tier1-DST', 'outputDataName': 'bbb.calibration.dst'},
                  {'outputDataType': 'CHARM.MDST', 'outputDataSE': 'Tier1-DST', 'outputDataName': 'ccc.charm.mdst'},
                  {'outputDataType': 'CHARMCONTROL.DST', 'outputDataSE': 'Tier1-DST', 'outputDataName': '00012345_00012345_2.CHARMCONTROL.DST'},
                  {'outputDataType': 'CHARMFULL.DST', 'outputDataSE': 'Tier1-DST', 'outputDataName': '00012345_00012345_2.CHARMFULL.DST'},
                  {'outputDataType': 'LEPTONIC.MDST', 'outputDataSE': 'Tier1-DST', 'outputDataName': '00012345_00012345_2.LEPTONIC.MDST'},
                  {'outputDataType': 'LEPTONICFULL.DST', 'outputDataSE': 'Tier1-DST', 'outputDataName': '00012345_00012345_2.LEPTONICFULL.DST'},
                  {'outputDataType': 'MINIBIAS.DST', 'outputDataSE': 'Tier1-DST', 'outputDataName': '00012345_00012345_2.MINIBIAS.DST'},
                  {'outputDataType': 'RADIATIVE.DST', 'outputDataSE': 'Tier1-DST', 'outputDataName': '00012345_00012345_2.RADIATIVE.DST'},
                  {'outputDataType': 'SEMILEPTONIC.DST', 'outputDataSE': 'Tier1-DST', 'outputDataName': '00012345_00012345_2.SEMILEPTONIC.DST'},
                  {'outputDataType': 'HIST', 'outputDataSE': 'CERN-HIST', 'outputDataName': 'DaVinci_00012345_00012345_2_Hist.root'}]

    self.assertRaises( IOError, self.mb._findOutputs, stepOutput )

    stepOutput = [{'outputDataType': 'BHADRON.DST', 'outputDataSE': 'Tier1-DST', 'outputDataName': 'aaa.bhadron.dst'}]
    outExp = [{'outputDataType': 'bhadron.dst', 'outputBKType': 'BHADRON.DST',
               'outputDataSE': 'Tier1-DST', 'outputDataName': 'aaa.Bhadron.dst',
               'stepName': 'someApp_1'}]
    bkExp = ['BHADRON.DST']

    self.mb.stepName = 'someApp_1'
    out, bk = self.mb._findOutputs( stepOutput )

    self.assertEqual( out, outExp )
    self.assertEqual( bk, bkExp )


  def test_getFileMetadata( self ):
    open( 'foo_1.txt', 'w' ).close()
    open( 'bar_2.py', 'w' ).close()

    candidateFiles = {'foo_1.txt': {'lfn': '/lhcb/MC/2010/DST/00012345/0001/foo_1.txt',
                                    'type': 'txt',
                                    'workflowSE': 'SE1'},
                      'bar_2.py': {'lfn': '/lhcb/MC/2010/DST/00012345/0001/bar_2.py',
                                   'type': 'py',
                                   'workflowSE': 'SE2'},
                      }

    result = self.mb.getFileMetadata( candidateFiles )
    self.assertTrue( result['OK'] )

    expectedResult = {'bar_2.py': {'filedict': {'Status': 'Waiting',
                                                'LFN': '/lhcb/MC/2010/DST/00012345/0001/bar_2.py',
                                                'GUID': 'D41D8CD9-8F00-B204-E980-0998ECF8427E',
                                                'Adler': '001',
                                                'Size': 0},
                                   'lfn': '/lhcb/MC/2010/DST/00012345/0001/bar_2.py',
                                   'localpath': os.getcwd() + '/bar_2.py',
                                   'workflowSE': 'SE2',
                                   'guid': 'D41D8CD9-8F00-B204-E980-0998ECF8427E',
                                   'type': 'py'},
                      'foo_1.txt': {'filedict': {'Status': 'Waiting',
                                                 'LFN': '/lhcb/MC/2010/DST/00012345/0001/foo_1.txt',
                                                 'GUID': 'D41D8CD9-8F00-B204-E980-0998ECF8427E',
                                                 'Adler': '001',
                                                 'Size': 0},
                                    'lfn': '/lhcb/MC/2010/DST/00012345/0001/foo_1.txt',
                                    'localpath': os.getcwd() + '/foo_1.txt',
                                    'workflowSE': 'SE1',
                                    'guid': 'D41D8CD9-8F00-B204-E980-0998ECF8427E',
                                    'type': 'txt'}
                      }

    self.assertEqual( result['Value'], expectedResult )


#############################################################################
# GaudiApplication.py
#############################################################################

class GaudiApplicationSuccess( ModulesTestCase ):

  #################################################

#  def test_execute( self ):
# FIXME: difficult to mock
#
#    #no errors, no input data
#    for wf_commons in copy.deepcopy( self.wf_commons ):
#      self.assertTrue( self.ga.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
#                                        self.workflowStatus, self.stepStatus,
#                                        wf_commons, self.step_commons,
#                                        self.step_number, self.step_id,
#                                        Mock() )['OK'] )

#############################################################################
# GaudiApplicationScript.py
#############################################################################

# class GaudiApplicationScriptSuccess( ModulesTestCase ):
#
#  #################################################
#
#  def test_execute( self ):
# #FIXME: difficult to mock
#
#    self.step_commons['script'] = 'cat'
#    #no errors, no input data
#    for wf_commons in copy.deepcopy( self.wf_commons ):
#      self.assertTrue( self.gas.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
#                                        self.workflowStatus, self.stepStatus,
#                                        wf_commons, self.step_commons,
#                                        self.step_number, self.step_id,
#                                        ['aa', 'bb'] )['OK'] )
  pass


#############################################################################
# ModulesUtilities.py
#############################################################################

class ModulesUtilitiesSuccess( ModulesTestCase ):

  #################################################

  def test_lowerExtension( self ):

    open( 'foo.tXt', 'w' ).close()
    open( 'BAR.txt', 'w' ).close()
    open( 'FooBAR.eXT.TXT', 'w' ).close()

    lowerExtension()

    self.assert_( 'foo.txt' in os.listdir( '.' ) )
    self.assert_( 'BAR.txt' in os.listdir( '.' ) )
    self.assert_( 'FooBAR.ext.txt' in os.listdir( '.' ) )

  #################################################

  def test_getEventsToProduce( self ):

    CPUe = 2.0
    CPUTime = 1000000.0
    CPUNormalizationFactor = 0.5

    out = getEventsToProduce( CPUe, CPUTime, CPUNormalizationFactor )
    outExp = 250000

    self.assertEqual( out, outExp )

  def test_getCPUNormalizationFactorAvg( self ):

    with patch.object( gConfig, 'getSections' ) as mockGetSections:  # @UndefinedVariable
      with patch.object( Resources, 'getQueues' ) as mockGetQueues:  # @UndefinedVariable

        # gConfig.getSection error
        mockGetSections.return_value = S_ERROR()
        self.assertRaises( RuntimeError, getCPUNormalizationFactorAvg )

        # Resources.getQueues error
        mockGetSections.return_value = S_OK( ['LCG.CERN.ch'] )
        mockGetQueues.return_value = S_ERROR()
        self.assertRaises( RuntimeError, getCPUNormalizationFactorAvg )

        # no queues
        mockGetQueues.return_value = S_OK( {'LCG.CERN.ch' : {}} )
        self.assertRaises( RuntimeError, getCPUNormalizationFactorAvg )

        # success
        mockGetQueues.return_value = S_OK( {'LCG.CERN.ch':
          {'ce201.cern.ch': {'CEType': 'CREAM',
          'OS': 'ScientificCERNSLC_Boron_5.5',
          'Pilot': 'True',
          'Queues': {'cream-lsf-grid_2nh_lhcb': {'MaxTotalJobs': '1000',
            'MaxWaitingJobs': '20',
            'SI00': '1000',
            'maxCPUTime': '120'},
           'cream-lsf-grid_lhcb': {'MaxTotalJobs': '1000',
            'MaxWaitingJobs': '100',
            'SI00': '1000',
            'WaitingToRunningRatio': '0.2',
            'maxCPUTime': '10080'}},
          'SI00': '5242',
          'SubmissionMode': 'Direct',
          'architecture': 'x86_64',
          'wnTmpDir': '.'},
         'ce202.cern.ch': {'CEType': 'CREAM',
          'OS': 'ScientificCERNSLC_Boron_5.8',
          'Pilot': 'True',
          'Queues': {'cream-lsf-grid_2nh_lhcb': {'MaxTotalJobs': '1000',
            'MaxWaitingJobs': '20',
            'SI00': '1000',
            'maxCPUTime': '120'},
           'cream-lsf-grid_lhcb': {'MaxTotalJobs': '1000',
            'MaxWaitingJobs': '100',
            'SI00': '1000',
            'WaitingToRunningRatio': '0.2',
            'maxCPUTime': '10080'}},
          'SI00': '5242',
          'SubmissionMode': 'Direct',
          'architecture': 'x86_64',
          'wnTmpDir': '.'}}} )
        outExp = 250000
        out = getCPUNormalizationFactorAvg()
        self.assertEqual( out, outExp )


#############################################################################
# AnalyseXMLSummary.py
#############################################################################

class AnalyseXMLSummarySuccess( ModulesTestCase ):

  #################################################

  def test_execute( self ):

    self.axlf.stepInputData = ['some.sdst', '00012345_00006789_1.sdst']
    self.axlf.jobType = 'merge'


    logAnalyser = Mock()

    logAnalyser.return_value = {'OK':True, 'Value':''}

    # no errors, no input data
    for wf_commons in copy.deepcopy( self.wf_commons ):
      for step_commons in self.step_commons:
        self.assertTrue( self.axlf.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                            self.workflowStatus, self.stepStatus,
                                            wf_commons, step_commons,
                                            self.step_number, self.step_id,
                                            self.nc_mock, logAnalyser, self.xf_o_mock )['OK'] )


    # logAnalyser gives errors
    self.axlf.jobType = 'reco'

    logAnalyser.return_value = {'OK':False, 'Message':'a mess'}

    for wf_commons in copy.deepcopy( self.wf_commons ):
      for step_commons in self.step_commons:
        if wf_commons.has_key( 'AnalyseLogFilePreviouslyFinalized' ):
          continue
        self.assertFalse( self.axlf.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                            self.workflowStatus, self.stepStatus,
                                            wf_commons, step_commons,
                                            self.step_number, self.step_id,
                                            self.nc_mock, logAnalyser, self.xf_o_mock )['OK'] )


  def test__updateFileStatus( self ):
    inputs = [{'i1':'OK', 'i2':'OK'},
              {'i1':'OK', 'i2':'Unused'},
              {'i1':'Unused', 'i2':'Unused'}
              ]
    for inp in inputs:
      self.axlf._updateFileStatus( inp, 'Processed', self.prod_id, self.fr_mock )

#############################################################################
# AnalyseLogFile.py
#############################################################################

class AnalyseLogFileSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self ):

    self.alf.stepInputData = ['some.sdst', '00012345_00006789_1.sdst']
    self.alf.jobType = 'merge'

    logAnalyser = Mock()
    logAnalyser.return_value = {'OK':True, 'Value':''}
#    no errors, no input data
    for wf_commons in copy.deepcopy( self.wf_commons ):
      for step_commons in self.step_commons:
        self.assertTrue( self.alf.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                            self.workflowStatus, self.stepStatus,
                                            wf_commons, step_commons,
                                            self.step_number, self.step_id,
                                            self.nc_mock, logAnalyser )['OK'] )


    self.alf.jobType = 'reco'

    # logAnalyser gives errors
    logAnalyser.return_value = {'OK':False, 'Message':'a mess'}

    for wf_commons in copy.deepcopy( self.wf_commons ):
      for step_commons in copy.deepcopy( self.step_commons ):
        if wf_commons.has_key( 'AnalyseLogFilePreviouslyFinalized' ):
          continue
        self.assertFalse( self.alf.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                          self.workflowStatus, self.stepStatus,
                                          wf_commons, step_commons,
                                          self.step_number, self.step_id,
                                          self.nc_mock, logAnalyser )['OK'] )

    # there's a core dump
    logAnalyser.return_value = {'OK':True, 'Message':''}
    open( 'ErrorLogging_Step1_coredump.log', 'w' ).close()
    for wf_commons in copy.deepcopy( self.wf_commons ):
      for step_commons in self.step_commons:
        self.assertFalse( self.alf.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                          self.workflowStatus, self.stepStatus,
                                          wf_commons, step_commons,
                                          self.step_number, self.step_id,
                                          self.nc_mock, logAnalyser )['OK'] )


  def test__updateFileStatus( self ):
    inputs = [{'i1':'OK', 'i2':'OK'},
              {'i1':'OK', 'i2':'Unused'},
              {'i1':'Unused', 'i2':'Unused'}
              ]
    for inp in inputs:
      self.axlf._updateFileStatus( inp, 'Processed', self.prod_id, self.fr_mock )


#############################################################################
# BookkeepingReport.py
#############################################################################

class BookkeepingReportSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self ):

    # no errors, no input data
    for wf_commons in copy.deepcopy( self.wf_commons ):
      for step_commons in self.step_commons:
        self.assertTrue( self.bkr.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                         self.workflowStatus, self.stepStatus,
                                         wf_commons, step_commons,
                                         self.step_number, self.step_id, False,
                                         self.xf_o_mock )['OK'] )


class BookkeepingReportFailure( ModulesTestCase ):

  #################################################

  def test_execute( self ):

    for wf_commons in copy.deepcopy( self.wf_commons ):
      for step_commons in self.step_commons:
        self.assertFalse( self.bkr.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                          self.workflowStatus, self.stepStatus,
                                          wf_commons, step_commons,
                                          self.step_number, self.step_id, False )['OK'] )
      step_commons_1 = copy.deepcopy( step_commons )
      step_commons_1.pop( 'XMLSummary' )
      self.assertFalse( self.bkr.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                          self.workflowStatus, self.stepStatus,
                                          wf_commons, step_commons_1,
                                          self.step_number, self.step_id, False )['OK'] )


##############################################################################
# # ErrorLogging.py
##############################################################################

class ErrorLoggingSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self ):

    # no errors, no input data
    for wf_commons in copy.deepcopy( self.wf_commons ):
      for step_commons in self.step_commons:
        self.el.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                       self.workflowStatus, self.stepStatus,
                       wf_commons, step_commons,
                       self.step_number, self.step_id )

    # TODO: make a real test (this one always exits with "Application log file from previous module not found locally")

#############################################################################
# FailoverRequest.py
#############################################################################

class FailoverRequestSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self ):

    self.fr.jobType = 'merge'
    self.fr.stepInputData = ['foo', 'bar']

    # no errors, no input data
    for wf_commons in copy.deepcopy( self.wf_commons ):
      for step_commons in self.step_commons:
        self.assertTrue( self.fr.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                        self.workflowStatus, self.stepStatus,
                                        wf_commons, step_commons,
                                        self.step_number, self.step_id )['OK'] )

#############################################################################
# MergeMDF.py
#############################################################################

# class MergeMDFSuccess( ModulesTestCase ):
#
#  #################################################
#
#  def test_execute( self ):
#
#    #no errors, no input data
#    for wf_commons in copy.deepcopy( self.wf_commons ):
#      self.assertFalse( self.mm.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
#                                        self.workflowStatus, self.stepStatus,
#                                        wf_commons, self.step_commons,
#                                        self.step_number, self.step_id,
#                                        self.rm_mock )['OK'] )
#      wf_commons['InputData'] = ['lfn1', 'lfn2']
#      open( 'lfn1', 'w' ).close()
#      open( 'lfn2', 'w' ).close()
#      self.assertTrue( self.mm.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
#                                        self.workflowStatus, self.stepStatus,
#                                        wf_commons, self.step_commons,
#                                        self.step_number, self.step_id,
#                                        self.rm_mock )['OK'] )

##############################################################################
# # ProtocolAccessTest.py
##############################################################################

class ProtocolAccessTestSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self ):

    step_commons = copy.deepcopy( self.step_commons )
    # no errors, no input data
    for wf_commons in copy.deepcopy( self.wf_commons ):
      for step_commons_1 in step_commons:
        self.assertFalse( self.pat.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                          self.workflowStatus, self.stepStatus,
                                          wf_commons, step_commons_1,
                                          self.step_number, self.step_id )['OK'] )
      step_commons_1['protocols'] = ['pr1', 'pr2']
      self.assertFalse( self.pat.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                          self.workflowStatus, self.stepStatus,
                                          wf_commons, step_commons_1,
                                          self.step_number, self.step_id )['OK'] )
      step_commons_1['applicationVersion'] = 'v1'
#      wf_commons['InputData'] = ['lfn1', 'lfn2']
#      self.assertTrue( self.pat.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
#                                          self.workflowStatus, self.stepStatus,
#                                          wf_commons, step_commons,
#                                          self.step_number, self.step_id )['OK'] )

    # TODO: make others cases tests!

##############################################################################
# # RemoveInputData.py
##############################################################################

class RemoveInputDataSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self ):

    # no errors, no input data
    for wf_commons in copy.deepcopy( self.wf_commons ):
      if 'InputData' in wf_commons.keys():
        continue
      for step_commons in self.step_commons:
        self.assertTrue( self.rid.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                         self.workflowStatus, self.stepStatus,
                                         wf_commons, step_commons,
                                         self.step_number, self.step_id )['OK'] )

    # no errors, input data
    for wf_commons in copy.deepcopy( self.wf_commons ):
      if 'InputData' not in wf_commons.keys():
        continue
      for step_commons in self.step_commons:
        self.assertTrue( self.rid.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                         self.workflowStatus, self.stepStatus,
                                         wf_commons, step_commons,
                                         self.step_number, self.step_id )['OK'] )



##############################################################################
# # SendBookkeeping.py
##############################################################################

class SendBookkeepingSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self ):

    # no errors, no input data
    for wf_commons in copy.deepcopy( self.wf_commons ):
      for step_commons in self.step_commons:
        self.assertTrue( self.sb.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                        self.workflowStatus, self.stepStatus,
                                        wf_commons, step_commons,
                                        self.step_number, self.step_id )['OK'] )

#############################################################################
# StepAccounting.py
#############################################################################

class StepAccountingSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self ):

    self.sa.jobType = 'merge'
    self.sa.stepInputData = ['foo', 'bar']

    for wf_commons in copy.deepcopy( self.wf_commons ):
      for step_commons in self.step_commons:
        self.assertTrue( self.sa.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                        self.workflowStatus, self.stepStatus,
                                        wf_commons, step_commons,
                                        self.step_number, self.step_id,
                                        self.jobStep_mock, self.xf_o_mock )['OK'] )

#############################################################################
# UploadLogFile.py
#############################################################################

class UploadLogFileSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self ):

    # no errors, no input data
#    for wf_commons in copy.deepcopy( self.wf_commons ):
#      for step_commons in self.step_commons:
#        self.assertTrue( self.ulf.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
#                                           self.workflowStatus, self.stepStatus,
#                                           wf_commons, step_commons,
#                                           self.step_number, self.step_id,
#                                           self.rm_mock, self.ft_mock,
#                                           self.bkc_mock )['OK'] )

    # putStorageDirectory returns False

    rm_mock = copy.deepcopy( self.rm_mock )
    rm_mock.putStorageDirectory.return_value = {'OK':False, 'Message':'bih'}
    ft_mock = copy.deepcopy( self.ft_mock )
    request_mock = Mock()
    request_mock.addSubRequest.return_value = {'OK': True, 'Value': ''}
    request_mock.setSubRequestFiles.return_value = {'OK': True, 'Value': ''}
    request_mock.getNumSubRequests.return_value = {'OK': True, 'Value': ''}
    request_mock._getLastOrder.return_value = 1
    ft_mock.getRequestObject.return_value = {'OK': True, 'Value': request_mock}
    for wf_commons in copy.deepcopy( self.wf_commons ):
      for step_commons in self.step_commons:
        self.assertTrue( self.ulf.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                           self.workflowStatus, self.stepStatus,
                                           wf_commons, step_commons,
                                           self.step_number, self.step_id,
                                           ft_mock )['OK'] )
#        self.assertTrue( self.ulf.finalize( rm_mock, self.ft_mock )['OK'] )


##############################################################################
# # UploadOutputData.py
##############################################################################

class UploadOutputDataSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self ):

    # no errors, no input data
    for wf_commons in copy.deepcopy( self.wf_commons ):
      if wf_commons.has_key( 'InputData' ):
        continue
      for step_commons in copy.deepcopy( self.step_commons ):
        fileDescendants = {}
        self.assertTrue( self.uod.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                           self.workflowStatus, self.stepStatus,
                                           wf_commons, step_commons,
                                           self.step_number, self.step_id,
                                           self.ft_mock, SEs = ['SomeSE'],
                                           fileDescendants = fileDescendants )['OK'] )


    # no errors, input data
    for wf_commons in copy.deepcopy( self.wf_commons ):
      for step_commons in self.step_commons:
        for transferAndRegisterFile in ( {'OK': True, 'Value': {'uploadedSE':''}}, {'OK': False, 'Message': 'error'} ):
#          for transferAndRegisterFileFailover in ( {'OK': True, 'Value': {}}, {'OK': False, 'Message': 'error'} ):
          self.ft_mock.transferAndRegisterFile.return_value = transferAndRegisterFile
#            self.ft_mock.transferAndRegisterFileFailover.return_value = transferAndRegisterFileFailover
          open( 'foo.txt', 'w' ).close()
          open( 'bar.txt', 'w' ).close()
          if not wf_commons.has_key( 'InputData' ):
            continue
          if wf_commons['InputData'] == '':
            continue
          wf_commons['outputList'] = [{'outputDataType': 'txt', 'outputDataSE': 'Tier1-RDST', 'outputDataName': 'foo.txt'},
                                      {'outputDataType': 'txt', 'outputDataSE': 'Tier1-RAW', 'outputDataName': 'bar.txt'},
                                      ]
          wf_commons['ProductionOutputData'] = ['/lhcb/MC/2010/DST/00012345/0001/foo.txt',
                                                '/lhcb/MC/2010/DST/00012345/0001/bar.txt' ]
#          self.bkc_mock.getFileDescendants.return_value = {'OK': False,
#                                                           'rpcStub': ( ( 'Bookkeeping/BookkeepingManager',
#                                                                        {'skipCACheck': False,
#                                                                         'timeout': 3600} ),
#                                                                       'getFileDescendants', ( ['foo'], 9, 0, True ) ),
#                                                           'Value': {'Successful': {'foo.txt': ['baaar']},
#                                                                     'Failed': [],
#                                                                     'NotProcessed': []}}
          fileDescendants = {'foo.txt': ['baaar']}
          self.assertFalse( self.uod.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                             self.workflowStatus, self.stepStatus,
                                             wf_commons, step_commons,
                                             self.step_number, self.step_id,
                                             self.ft_mock, SEs = ['SomeSE'],
                                             fileDescendants = fileDescendants )['OK'] )
#          self.bkc_mock.getFileDescendants.return_value = {'OK': True,
#                                                           'rpcStub': ( ( 'Bookkeeping/BookkeepingManager',
#                                                                        {'skipCACheck': False,
#                                                                         'timeout': 3600} ),
#                                                                       'getFileDescendants', ( ['foo'], 9, 0, True ) ),
#                                                           'Value': {'Successful': {},
#                                                                     'Failed': [],
#                                                                     'NotProcessed': []}}
          if wf_commons['Request'] == '':
            continue
          fileDescendants = {}
          res = self.uod.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                  self.workflowStatus, self.stepStatus,
                                  wf_commons, step_commons,
                                  self.step_number, self.step_id,
                                  self.ft_mock, SEs = ['SomeSE'],
                                  fileDescendants = fileDescendants )
          print res, transferAndRegisterFile
          self.assertTrue( res['OK'] )
#            if transferAndRegisterFileFailover['OK']:
#              self.assertTrue( res['OK'] )
#            else:
#              self.assertFalse( res['OK'] )
          os.remove( 'foo.txt' )
          os.remove( 'bar.txt' )

##############################################################################
# # UserJobFinalization.py
##############################################################################

class UserJobFinalizationSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self ):

    # no errors, no input data
    for wf_commons in copy.deepcopy( self.wf_commons ):
      wf_commons['TotalSteps'] = self.step_number
      for step_commons in self.step_commons:
        self.assertTrue( self.ujf.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                         self.workflowStatus, self.stepStatus,
                                         wf_commons, step_commons,
                                         self.step_number, self.step_id,
                                         self.ft_mock )['OK'] )
      # with input data - would require correct CS settings...
#      wf_commons['UserOutputData'] = ['i1', 'i2']
#      wf_commons['OwnerName'] = 'fstagni'
#      open( 'i1', 'w' ).close()
#      open( 'i2', 'w' ).close()
#      self.assertTrue( self.ujf.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
#                                         self.workflowStatus, self.stepStatus,
#                                         wf_commons, self.step_commons,
#                                         self.step_number, self.step_id,
#                                         self.ft_mock )['OK'] )
#      os.remove( 'i1' )
#      os.remove( 'i2' )
    # TODO: make others cases tests!

#############################################################################
# FileUsage.py
#############################################################################

class FileUsageSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self ):

    # no errors, no input files to report
    wfC = copy.deepcopy( self.wf_commons )
    for wf_commons in wfC:  # copy.deepcopy( self.wf_commons ):
      for step_commons in self.step_commons:
        self.assertTrue( self.fu.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                          self.workflowStatus, self.stepStatus,
                                          wf_commons, step_commons,
                                          self.step_number, self.step_id )['OK'] )
    wfC = copy.deepcopy( self.wf_commons )
    for wf_commons in wfC:  # copy.deepcopy( self.wf_commons ):
      wf_commons['ParametricInputData'] = ['LFN:/lhcb/data/2010/EW.DST/00008380/0000/00008380_00000287_1.ew.dst',
                                           'LFN:/lhcb/data/2010/EW.DST/00008380/0000/00008380_00000285_1.ew.dst',
                                           'LFN:/lhcb/data/2010/EW.DST/00008380/0000/00008380_00000281_1.ew.dst']
      for step_commons in self.step_commons:
        self.assertTrue( self.fu.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                          self.workflowStatus, self.stepStatus,
                                          wf_commons, step_commons,
                                          self.step_number, self.step_id )['OK'] )
    wfC = copy.deepcopy( self.wf_commons )
    for wf_commons in wfC:  # copy.deepcopy( self.wf_commons ):
      wf_commons['ParametricInputData'] = ['LFN:/lhcb/data/2010/EW.DST/00008380/0000/00008380_00000287_1.ew.dst',
                                           'LFN:/lhcb/data/2010/EW.DST/00008380/0000/00008380_00000285_1.ew.dst',
                                           'LFN:/lhcb/data/2010/PIPPO/00008380/0000/00008380_00000281_1.pippo.dst']
      for step_commons in self.step_commons:
        self.assertTrue( self.fu.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                          self.workflowStatus, self.stepStatus,
                                          wf_commons, step_commons,
                                          self.step_number, self.step_id )['OK'] )

    # workflow status not ok
    wfC = copy.deepcopy( self.wf_commons )
    for wf_commons in wfC:  # copy.deepcopy( self.wf_commons ):
      self.workflowStatus = {'OK':False, 'Message':'Mess'}
      for step_commons in self.step_commons:
        self.assertFalse( self.fu.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                           self.workflowStatus, self.stepStatus,
                                           wf_commons, step_commons,
                                           self.step_number, self.step_id )['OK'] )


#############################################################################
# FileUsage.py
#############################################################################

class CreateDataFileSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self ):

    self.cdf.jobType = 'merge'
    self.cdf.stepInputData = ['foo', 'bar']

    for wf_commons in copy.deepcopy( self.wf_commons ):
      for step_commons in self.step_commons:
        self.assertTrue( self.cdf.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                            self.workflowStatus, self.stepStatus,
                                            wf_commons, step_commons,
                                            self.step_number, self.step_id )['OK'] )

#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ModulesTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ModuleBaseSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( AnalyseXMLSummarySuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( AnalyseLogFileSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( GaudiApplicationSuccess ) )
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( GaudiApplicationScriptSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ModulesUtilitiesSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( BookkeepingReportSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( BookkeepingReportFailure ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ErrorLoggingSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( FailoverRequestSuccess ) )
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MergeMDFSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProtocolAccessTestSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( RemoveInputDataSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( SendBookkeepingSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( StepAccountingSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( UploadLogFileSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( UploadOutputDataSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( UserJobFinalizationSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( FileUsageSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( CreateDataFileSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
