import unittest, itertools, os, copy

#import DIRAC.ResourceStatusSystem.test.fake_Logger
from mock import Mock

from DIRAC import gLogger

from LHCbDIRAC.Workflow.Modules.ModulesUtilities import lowerExtension

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
    jr_mock.generateRequest.return_value = {'OK': True, 'Value': 'pippo'}
    jr_mock.setJobParameter.return_value = {'OK': True, 'Value': 'pippo'}
#    jr_mock.setJobApplicationStatus.return_value = {'OK': True, 'Value': 'pippo'}

    self.fr_mock = Mock()
    self.fr_mock.getFiles.return_value = {}
    self.fr_mock.setFileStatus.return_value = {'OK': True, 'Value': ''}
    self.fr_mock.commit.return_value = {'OK': True, 'Value': ''}
    self.fr_mock.generateRequest.return_value = {'OK': True, 'Value': ''}

    rc_mock = Mock()
    rc_mock.update.return_value = {'OK': True, 'Value': ''}
    rc_mock.setDISETRequest.return_value = {'OK': True, 'Value': ''}
    rc_mock.isEmpty.return_value = {'OK': True, 'Value': ''}
    rc_mock.toXML.return_value = {'OK': True, 'Value': ''}
    rc_mock.getDigest.return_value = {'OK': True, 'Value': ''}

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

    self.ft_mock = Mock()
    self.ft_mock.transferAndRegisterFile.return_value = {'OK': True, 'Value': ''}
    self.ft_mock.transferAndRegisterFileFailover.return_value = {'OK': True, 'Value': ''}
    self.ft_mock.getRequestObject.return_value = {'OK': True, 'Value': ''}

    self.bkc_mock = Mock()
    self.bkc_mock.sendBookkeeping.return_value = {'OK': True, 'Value': ''}

    self.nc_mock = Mock()
    self.nc_mock.sendMail.return_value = {'OK': True, 'Value': ''}

    self.version = 'someVers'
    self.prod_id = '123'
    self.prod_job_id = '00000456'
    self.wms_job_id = '00012345'
    self.workflowStatus = {'OK':True}
    self.stepStatus = {'OK':True}
    self.wf_commons = [
                       {'PRODUCTION_ID': self.prod_id, 'JOB_ID': self.prod_job_id,
                        'configName': 'aConfigName', 'configVersion': 'aConfigVersion', 'outputDataFileMask':'',
                        'BookkeepingLFNs':'aa', 'ProductionOutputData':'ProductionOutputData',
                        'JobReport':jr_mock, 'Request':rc_mock, 'AccountingReport': ar_mock, 'FileReport':self.fr_mock,
                        'SystemConfig':'sys_config'},
                       {'PRODUCTION_ID': self.prod_id, 'JOB_ID': self.prod_job_id,
                        'configName': 'aConfigName', 'configVersion': 'aConfigVersion', 'outputDataFileMask':'',
                        'BookkeepingLFNs':'aa', 'ProductionOutputData':'ProductionOutputData',
                        'JobReport':jr_mock, 'Request':rc_mock, 'AccountingReport': ar_mock, 'FileReport':self.fr_mock,
                        'SystemConfig':'sys_config', 'LogFilePath':'someDir'},
                       {'PRODUCTION_ID': self.prod_id, 'JOB_ID': self.prod_job_id,
                        'configName': 'aConfigName', 'configVersion': 'aConfigVersion', 'outputDataFileMask':'',
                        'BookkeepingLFNs':'aa', 'ProductionOutputData':'ProductionOutputData',
                        'JobReport':jr_mock, 'Request':rc_mock, 'AccountingReport': ar_mock, 'FileReport':self.fr_mock,
                        'SystemConfig':'sys_config', 'LogFilePath':'someDir', 'LogTargetPath':'someOtherDir'},
                       {'PRODUCTION_ID': self.prod_id, 'JOB_ID': self.prod_job_id,
                        'configName': 'aConfigName', 'configVersion': 'aConfigVersion', 'outputDataFileMask':'',
                        'BookkeepingLFNs':'aa', 'ProductionOutputData':'ProductionOutputData',
                        'JobReport':jr_mock, 'Request':rc_mock, 'AccountingReport': ar_mock, 'FileReport':self.fr_mock,
                        'SystemConfig':'sys_config', 'LogFilePath':'someDir', 'LogTargetPath':'someOtherDir' }
                       ]
    self.step_commons = {'applicationName':'someApp', 'applicationVersion':'v1r0',
                         'applicationLog':'appLog', 'extraPackages':'',
                         'listoutput':[{'outputDataName':self.prod_id + '_' + self.prod_job_id + '_', 'outputDataSE':'aaa',
                                       'outputDataType':'bbb'}]}
    self.step_number = '321'
    self.step_id = '%s_%s_%s' % ( self.prod_id, self.prod_job_id, self.step_number )



    from LHCbDIRAC.Workflow.Modules.ModuleBase import ModuleBase
    self.mb = ModuleBase()

    from LHCbDIRAC.Workflow.Modules.AnalyseLogFile import AnalyseLogFile
    self.alf = AnalyseLogFile()

    from LHCbDIRAC.Workflow.Modules.AnalyseXMLSummary import AnalyseXMLSummary
    self.axlf = AnalyseXMLSummary()

    from LHCbDIRAC.Workflow.Modules.GaudiApplication import GaudiApplication
    self.ga = GaudiApplication()

    from LHCbDIRAC.Workflow.Modules.GaudiApplicationScript import GaudiApplicationScript
    self.gas = GaudiApplicationScript()

    from LHCbDIRAC.Workflow.Modules.BookkeepingReport import BookkeepingReport
    self.bkr = BookkeepingReport()

    from LHCbDIRAC.Workflow.Modules.ErrorLogging import ErrorLogging
    self.el = ErrorLogging()

    from LHCbDIRAC.Workflow.Modules.FailoverRequest import FailoverRequest
    self.fr = FailoverRequest()

    from LHCbDIRAC.Workflow.Modules.MergeMDF import MergeMDF
    self.mm = MergeMDF()

    from LHCbDIRAC.Workflow.Modules.ProtocolAccessTest import ProtocolAccessTest
    self.pat = ProtocolAccessTest()

    from LHCbDIRAC.Workflow.Modules.RemoveInputData import RemoveInputData
    self.rid = RemoveInputData()

    from LHCbDIRAC.Workflow.Modules.SendBookkeeping import SendBookkeeping
    self.sb = SendBookkeeping()

    from LHCbDIRAC.Workflow.Modules.UploadOutputData import UploadOutputData
    self.uod = UploadOutputData()

    from LHCbDIRAC.Workflow.Modules.UserJobFinalization import UserJobFinalization
    self.ujf = UserJobFinalization()

#    from LHCbDIRAC.Workflow.Modules.StepAccounting import StepAccounting
#    self.sa = StepAccounting()
#
    from LHCbDIRAC.Workflow.Modules.UploadLogFile import UploadLogFile
    self.ulf = UploadLogFile()

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
    #this needs to avoid the "checkLocalExistance"

    open( 'foo.txt', 'w' ).close()

    outputList = [{'outputDataType': 'txt', 'outputDataSE': 'Tier1-RDST', 'outputDataName': 'foo.txt'}]
    outputLFNs = ['/lhcb/MC/2010/DST/00012345/0001/foo.txt']
    fileMask = 'txt'
    stepMask = ''
    result = {'foo.txt': {'lfn': '/lhcb/MC/2010/DST/00012345/0001/foo.txt', 'type': outputList[0]['outputDataType'], 'workflowSE': outputList[0]['outputDataSE']}}

    res = self.mb.getCandidateFiles( outputList, outputLFNs, fileMask, stepMask )

    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], result )

    os.remove( 'foo.txt' )

  def test__enableModule( self ):

    self.mb.execute( self.version, self.prod_id, self.prod_job_id, self.wms_job_id,
                     self.workflowStatus, self.stepStatus,
                     self.wf_commons, self.step_commons,
                     self.step_number, self.step_id )
    self.assertTrue( self.mb._enableModule() )

#############################################################################
# GaudiApplication.py
#############################################################################

class GaudiApplicationSuccess( ModulesTestCase ):

  #################################################

#  def test_execute( self ): 
#FIXME: difficult to mock
#
#    #no errors, no input data
#    for wf_commons in copy.deepcopy( self.wf_commons ):
#      self.assertTrue( self.ga.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
#                                        self.workflowStatus, self.stepStatus,
#                                        wf_commons, self.step_commons,
#                                        self.step_number, self.step_id,
#                                        Mock() )['OK'] )

  #################################################

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

    outExp = [{'outputDataType': 'bhadron.dst', 'outputBKType': 'BHADRON.DST', 'outputDataSE': 'Tier1-DST', 'outputDataName': 'aaa.Bhadron.dst'},
              {'outputDataType': 'calibration.dst', 'outputBKType': 'CALIBRATION.DST', 'outputDataSE': 'Tier1-DST', 'outputDataName': 'bbb.Calibration.dst'},
              {'outputDataType': 'charm.mdst', 'outputBKType': 'CHARM.MDST', 'outputDataSE': 'Tier1-DST', 'outputDataName': 'ccc.charm.mdst'}]
    bkExp = ['BHADRON.DST', 'CALIBRATION.DST', 'CHARM.MDST']


    out, bk = self.ga._findOutputs( stepOutput )

    self.assert_( out == outExp )
    self.assert_( bk == bkExp )


    stepOutput = [{'outputDataType': 'BHADRON.DST', 'outputDataSE': 'Tier1-DST', 'outputDataName': 'aaa.bhadron.dst'}]
    outExp = [{'outputDataType': 'bhadron.dst', 'outputBKType': 'BHADRON.DST', 'outputDataSE': 'Tier1-DST', 'outputDataName': 'aaa.Bhadron.dst'}]
    bkExp = ['BHADRON.DST']

    out, bk = self.ga._findOutputs( stepOutput )

    self.assert_( out == outExp )
    self.assert_( bk == bkExp )



    os.remove( 'aaa.Bhadron.dst' )
    os.remove( 'bbb.Calibration.dst' )
    os.remove( 'ccc.charm.mdst' )
    os.remove( 'prova.txt' )


#############################################################################
# GaudiApplicationScript.py
#############################################################################

#class GaudiApplicationScriptSuccess( ModulesTestCase ):
#
#  #################################################
#
#  def test_execute( self ):
##FIXME: difficult to mock
#
#    self.step_commons['script'] = 'cat'
#    #no errors, no input data
#    for wf_commons in copy.deepcopy( self.wf_commons ):
#      self.assertTrue( self.gas.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
#                                        self.workflowStatus, self.stepStatus,
#                                        wf_commons, self.step_commons,
#                                        self.step_number, self.step_id,
#                                        ['aa', 'bb'] )['OK'] )


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

    os.remove( 'foo.txt' )
    os.remove( 'BAR.txt' )
    os.remove( 'FooBAR.ext.txt' )

  #################################################

#############################################################################
# AnalyseXMLSummary.py
#############################################################################

class AnalyseXMLSummarySuccess( ModulesTestCase ):

  #################################################

  def test_execute( self ):

    logAnalyser = Mock()

    logAnalyser.return_value = {'OK':True, 'Value':''}

    #no errors, no input data
    for wf_commons in copy.deepcopy( self.wf_commons ):
      self.assertTrue( self.axlf.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                          self.workflowStatus, self.stepStatus,
                                          wf_commons, self.step_commons,
                                          self.step_number, self.step_id,
                                          self.nc_mock, self.rm_mock, logAnalyser )['OK'] )


    #logAnalyser gives errors
    logAnalyser.return_value = {'OK':False, 'Message':'a mess'}

    for wf_commons in copy.deepcopy( self.wf_commons ):
      self.assertFalse( self.axlf.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                          self.workflowStatus, self.stepStatus,
                                          wf_commons, self.step_commons,
                                          self.step_number, self.step_id,
                                          self.nc_mock, self.rm_mock, logAnalyser )['OK'] )

    #there's a core dump
    logAnalyser.return_value = {'OK':True, 'Message':''}
    open( 'ErrorLogging_Step1_coredump.log', 'w' ).close()
    for wf_commons in copy.deepcopy( self.wf_commons ):
      self.assertFalse( self.axlf.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                          self.workflowStatus, self.stepStatus,
                                          wf_commons, self.step_commons,
                                          self.step_number, self.step_id,
                                          self.nc_mock, self.rm_mock, logAnalyser )['OK'] )
    os.remove( 'ErrorLogging_Step1_coredump.log' )

  def test__updateFileStatus( self ):
    inputs = [{'i1':'OK', 'i2':'OK'},
              {'i1':'OK', 'i2':'ApplicationCrash'},
              {'i1':'Unused', 'i2':'ApplicationCrash'}
              ]
    for input in inputs:
      self.axlf._updateFileStatus( input, 'Processed', self.prod_id, self.rm_mock, self.fr_mock )


#############################################################################
# BookkeepingReport.py
#############################################################################

class BookkeepingReportSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self ):

    #no errors, no input data
    for wf_commons in copy.deepcopy( self.wf_commons ):
      self.assertTrue( self.bkr.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                         self.workflowStatus, self.stepStatus,
                                         wf_commons, self.step_commons,
                                         self.step_number, self.step_id, False )['OK'] )

    #TODO: make other tests (how?)!

##############################################################################
## ErrorLogging.py
##############################################################################

class ErrorLoggingSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self ):

    #no errors, no input data
    for wf_commons in copy.deepcopy( self.wf_commons ):
      self.el.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                       self.workflowStatus, self.stepStatus,
                       wf_commons, self.step_commons,
                       self.step_number, self.step_id )

    #TODO: make a real test (this one always exits with "Application log file from previous module not found locally")

#############################################################################
# FailoverRequest.py
#############################################################################

class FailoverRequestSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self ):

    #no errors, no input data
    for wf_commons in copy.deepcopy( self.wf_commons ):
      self.assertTrue( self.fr.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                        self.workflowStatus, self.stepStatus,
                                        wf_commons, self.step_commons,
                                        self.step_number, self.step_id )['OK'] )

#############################################################################
# MergeMDF.py
#############################################################################

class MergeMDFSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self ):

    #no errors, no input data
    for wf_commons in copy.deepcopy( self.wf_commons ):
      self.assertFalse( self.mm.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                        self.workflowStatus, self.stepStatus,
                                        wf_commons, self.step_commons,
                                        self.step_number, self.step_id,
                                        self.rm_mock )['OK'] )
      wf_commons['InputData'] = ['lfn1', 'lfn2']
      open( 'lfn1', 'w' ).close()
      open( 'lfn2', 'w' ).close()
      self.assertTrue( self.mm.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                        self.workflowStatus, self.stepStatus,
                                        wf_commons, self.step_commons,
                                        self.step_number, self.step_id,
                                        self.rm_mock )['OK'] )
      os.remove( 'lfn1' )
      os.remove( 'lfn2' )

##############################################################################
## ProtocolAccessTest.py
##############################################################################

class ProtocolAccessTestSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self ):

    step_commons = copy.deepcopy( self.step_commons )
    #no errors, no input data
    for wf_commons in copy.deepcopy( self.wf_commons ):
      self.assertFalse( self.pat.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                          self.workflowStatus, self.stepStatus,
                                          wf_commons, step_commons,
                                          self.step_number, self.step_id, self.rm_mock )['OK'] )
      step_commons['protocols'] = ['pr1', 'pr2']
      self.assertFalse( self.pat.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                          self.workflowStatus, self.stepStatus,
                                          wf_commons, step_commons,
                                          self.step_number, self.step_id, self.rm_mock )['OK'] )
      step_commons['applicationVersion'] = 'v1'
#      wf_commons['InputData'] = ['lfn1', 'lfn2']
#      self.assertTrue( self.pat.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
#                                          self.workflowStatus, self.stepStatus,
#                                          wf_commons, step_commons,
#                                          self.step_number, self.step_id, self.rm_mock )['OK'] )

    #TODO: make others cases tests!

##############################################################################
## RemoveInputData.py
##############################################################################

class RemoveInputDataSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self ):

    #no errors, no input data
    for wf_commons in copy.deepcopy( self.wf_commons ):
      self.assertFalse( self.rid.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                         self.workflowStatus, self.stepStatus,
                                         wf_commons, self.step_commons,
                                         self.step_number, self.step_id, self.rm_mock )['OK'] )
      wf_commons['InputData'] = ['lfn1', 'lfn2']
      self.assertTrue( self.rid.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                         self.workflowStatus, self.stepStatus,
                                         wf_commons, self.step_commons,
                                         self.step_number, self.step_id, self.rm_mock )['OK'] )

    #TODO: make others cases tests!


##############################################################################
## SendBookkeeping.py
##############################################################################

class SendBookkeepingSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self ):

    #no errors, no input data
    for wf_commons in copy.deepcopy( self.wf_commons ):
      self.assertTrue( self.sb.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                        self.workflowStatus, self.stepStatus,
                                        wf_commons, self.step_commons,
                                        self.step_number, self.step_id, self.bkc_mock )['OK'] )

    #TODO: make others cases tests!

##############################################################################
## StepAccounting.py
##############################################################################
#
#class StepAccountingSuccess( ModulesTestCase ):
#
#  #################################################
#
#  def test_execute( self ):
#
#    self.assertTrue( self.sa.execute() )
#
#    #TODO: make others cases tests!
#
##############################################################################
## UploadLogFile.py
##############################################################################

class UploadLogFileSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self ):

    #no errors, no input data
    for wf_commons in copy.deepcopy( self.wf_commons ):
      self.assertTrue( self.ulf.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                         self.workflowStatus, self.stepStatus,
                                         wf_commons, self.step_commons,
                                         self.step_number, self.step_id,
                                         self.rm_mock, self.ft_mock )['OK'] )

    #TODO: make others cases tests!

##############################################################################
## UploadOutputData.py
##############################################################################

class UploadOutputDataSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self ):

    #no errors, no input data
    for wf_commons in copy.deepcopy( self.wf_commons ):
      self.assertTrue( self.uod.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                         self.workflowStatus, self.stepStatus,
                                         wf_commons, self.step_commons,
                                         self.step_number, self.step_id,
                                         self.rm_mock, self.ft_mock, self.bkc_mock )['OK'] )

    #TODO: make others cases tests! e.g. why does not check for file existance?

##############################################################################
## UserJobFinalization.py
##############################################################################

class UserJobFinalizationSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self ):

    #no errors, no input data
    for wf_commons in copy.deepcopy( self.wf_commons ):
      wf_commons['TotalSteps'] = self.step_number
      self.assertTrue( self.ujf.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                         self.workflowStatus, self.stepStatus,
                                         wf_commons, self.step_commons,
                                         self.step_number, self.step_id,
                                         self.rm_mock, self.ft_mock )['OK'] )
      #with input data - would require correct CS settings...
#      wf_commons['UserOutputData'] = ['i1', 'i2']
#      wf_commons['OwnerName'] = 'fstagni'
#      open( 'i1', 'w' ).close()
#      open( 'i2', 'w' ).close()
#      self.assertTrue( self.ujf.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
#                                         self.workflowStatus, self.stepStatus,
#                                         wf_commons, self.step_commons,
#                                         self.step_number, self.step_id,
#                                         self.rm_mock, self.ft_mock )['OK'] )
#      os.remove( 'i1' )
#      os.remove( 'i2' )
    #TODO: make others cases tests!

#############################################################################
# Test Suite run 
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ModulesTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ModuleBaseSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( AnalyseXMLSummarySuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( GaudiApplicationSuccess ) )
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( GaudiApplicationScriptSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ModulesUtilitiesSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( BookkeepingReportSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ErrorLoggingSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( FailoverRequestSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MergeMDFSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProtocolAccessTestSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( RemoveInputDataSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( SendBookkeepingSuccess ) )
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( StepAccountingSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( UploadLogFileSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( UploadOutputDataSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( UserJobFinalizationSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
