import unittest, itertools, os, copy

#import DIRAC.ResourceStatusSystem.test.fake_Logger
from DIRAC.ResourceStatusSystem.Utilities.mock import Mock

from LHCbDIRAC.Workflow.Modules.ModulesUtilities import lowerExtension

class ModulesTestCase( unittest.TestCase ):
  """ Base class for the Modules test cases
  """
  def setUp( self ):

    self.maxDiff = None

#    import sys
#    sys.modules["DIRAC"] = DIRAC.ResourceStatusSystem.test.fake_Logger
#    sys.modules["DIRAC.ResourceStatusSystem.Utilities.CS"] = DIRAC.ResourceStatusSystem.test.fake_Logger

    jr_mock = Mock()
    jr_mock.setApplicationStatus.return_value = {'OK': True, 'Value': ''}
    jr_mock.generateRequest.return_value = {'OK': True, 'Value': 'pippo'}
    jr_mock.setJobParameter.return_value = {'OK': True, 'Value': 'pippo'}

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
    self.rm_mock.getReplicas.return_value = {'OK': True, 'Value':{'Successful':{'pippo':'metadataPippo'}}}
    self.rm_mock.getCatalogFileMetadata.return_value = {'OK': True, 'Value':{'Successful':{'pippo':'metadataPippo'}}}
    self.rm_mock.removeFile.return_value = {'OK': True, 'Value': {'Failed':False}}
    self.rm_mock.putStorageDirectory.return_value = {'OK': True, 'Value': {'Failed':False}}
    self.rm_mock.addCatalogFile.return_value = {'OK': True, 'Value': {'Failed':False}}
    self.rm_mock.putAndRegister.return_value = {'OK': True, 'Value': {'Failed':False}}

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
                        'JobReport':jr_mock, 'Request':rc_mock, 'AccountingReport': ar_mock,
                        'SystemConfig':'sys_config'},
                       {'PRODUCTION_ID': self.prod_id, 'JOB_ID': self.prod_job_id,
                        'configName': 'aConfigName', 'configVersion': 'aConfigVersion', 'outputDataFileMask':'',
                        'BookkeepingLFNs':'aa', 'ProductionOutputData':'ProductionOutputData',
                        'JobReport':jr_mock, 'Request':rc_mock, 'AccountingReport': ar_mock,
                        'SystemConfig':'sys_config', 'LogFilePath':'someDir'},
                       {'PRODUCTION_ID': self.prod_id, 'JOB_ID': self.prod_job_id,
                        'configName': 'aConfigName', 'configVersion': 'aConfigVersion', 'outputDataFileMask':'',
                        'BookkeepingLFNs':'aa', 'ProductionOutputData':'ProductionOutputData',
                        'JobReport':jr_mock, 'Request':rc_mock, 'AccountingReport': ar_mock,
                        'SystemConfig':'sys_config', 'LogFilePath':'someDir', 'LogTargetPath':'someOtherDir'},
                       {'PRODUCTION_ID': self.prod_id, 'JOB_ID': self.prod_job_id,
                        'configName': 'aConfigName', 'configVersion': 'aConfigVersion', 'outputDataFileMask':'',
                        'BookkeepingLFNs':'aa', 'ProductionOutputData':'ProductionOutputData',
                        'JobReport':jr_mock, 'Request':rc_mock, 'AccountingReport': ar_mock,
                        'SystemConfig':'sys_config', 'LogFilePath':'someDir', 'LogTargetPath':'someOtherDir' }
                       ]
    self.step_commons = {'applicationName':'DaVinci', 'applicationVersion':'v1r0',
                         'applicationLog':'appLog',
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

    from LHCbDIRAC.Workflow.Modules.BookkeepingReport import BookkeepingReport
    self.bkr = BookkeepingReport()

    from LHCbDIRAC.Workflow.Modules.ErrorLogging import ErrorLogging
    self.el = ErrorLogging()

    from LHCbDIRAC.Workflow.Modules.FailoverRequest import FailoverRequest
    self.fr = FailoverRequest()

    from LHCbDIRAC.Workflow.Modules.ProtocolAccessTest import ProtocolAccessTest
    self.pat = ProtocolAccessTest()

    from LHCbDIRAC.Workflow.Modules.RemoveInputData import RemoveInputData
    self.rid = RemoveInputData()

    from LHCbDIRAC.Workflow.Modules.SendBookkeeping import SendBookkeeping
    self.sb = SendBookkeeping()

    from LHCbDIRAC.Workflow.Modules.UploadOutputData import UploadOutputData
    self.uod = UploadOutputData()

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
#
#    self.assertTrue( self.ga.execute() )

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
# AnalyseLogFile.py
#############################################################################

#class AnalyseLogFileSuccess( ModulesTestCase ):
#
#  #################################################
#
#  def test_execute( self ):
#
#    logAnalyser = Mock()
#    logAnalyser.return_value = {'OK':True, 'Value':''}
#
#    self.assertTrue( self.alf.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
#                                       self.workflowStatus, self.stepStatus,
#                                       self.wf_commons, self.step_commons,
#                                       self.step_number, self.step_id,
#                                       self.nc_mock, self.rm_mock, logAnalyser ) )
#
#    logAnalyser.return_value = {'OK':False, 'Message':'a mess'}
#
#    self.assertTrue( self.alf.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
#                                       self.workflowStatus, self.stepStatus,
#                                       self.wf_commons, self.step_commons,
#                                       self.step_number, self.step_id,
#                                       self.nc_mock, self.rm_mock, logAnalyser ) )

    #TODO: make others cases tests!


#############################################################################
# AnalyseLogFile.py
#############################################################################

class AnalyseXMLSummarySuccess( ModulesTestCase ):

  #################################################

  def test_execute( self ):

    logAnalyser = Mock()

    logAnalyser.return_value = {'OK':True, 'Value':''}

    #no errors, no input data
    for wf_commons in self.wf_commons:
      self.assertTrue( self.axlf.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                          self.workflowStatus, self.stepStatus,
                                          wf_commons, self.step_commons,
                                          self.step_number, self.step_id,
                                          self.nc_mock, self.rm_mock, logAnalyser )['OK'] )


    #logAnalyser gives errors
    logAnalyser.return_value = {'OK':False, 'Message':'a mess'}

    for wf_commons in self.wf_commons:
      self.assertFalse( self.axlf.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                          self.workflowStatus, self.stepStatus,
                                          wf_commons, self.step_commons,
                                          self.step_number, self.step_id,
                                          self.nc_mock, self.rm_mock, logAnalyser )['OK'] )



    #there's a core dump


#  def test__finalizeWithErrors( self ):
#    self.axlf._finalizeWithErrors( 'A Subject', self.nc_mock, self.rm_mock )


  def test__updateFileStatus( self ):
    inputs = [{'i1':'OK', 'i2':'OK'},
              {'i1':'OK', 'i2':'ApplicationCrash'},
              {'i1':'Unused', 'i2':'ApplicationCrash'}
              ]
    for input in inputs:
      self.assertTrue( self.axlf._updateFileStatus( input, 'Processed', self.prod_id, self.rm_mock, self.fr_mock )['OK'] )

  #TODO: make others cases tests!



#############################################################################
# BookkeepingReport.py
#############################################################################

#class BookkeepingReportSuccess( ModulesTestCase ):
#
#  #################################################
#
#  def test_execute( self ):
#
#    self.bkr.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
#                      self.workflowStatus, self.stepStatus,
#                      self.wf_commons, self.step_commons,
#                      self.step_number, self.step_id, False )
#
#    #TODO: make a real test!
#
##############################################################################
## ErrorLogging.py
##############################################################################
#
#class ErrorLoggingSuccess( ModulesTestCase ):
#
#  #################################################
#
#  def test_execute( self ):
#    pass
#
##    self.el.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
##                     self.workflowStatus, self.stepStatus,
##                     self.wf_commons, self.step_commons,
##                     self.step_number, self.step_id )
#
#    #TODO: make a real test!
#
##############################################################################
## FailoverRequest.py
##############################################################################
#
#class FailoverRequestSuccess( ModulesTestCase ):
#
#  #################################################
#
#  def test_execute( self ):
#
#    self.assertTrue( self.fr.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
#                                      self.workflowStatus, self.stepStatus,
#                                      self.wf_commons, self.step_commons,
#                                      self.step_number, self.step_id ) )
#
#    #TODO: make others cases tests!
#
##############################################################################
## ProtocolAccessTest.py
##############################################################################
#
#class ProtocolAccessTestSuccess( ModulesTestCase ):
#
#  #################################################
#
#  def test_execute( self ):
#
#    self.assertTrue( self.pat.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
#                                       self.workflowStatus, self.stepStatus,
#                                       self.wf_commons, self.step_commons,
#                                       self.step_number, self.step_id, self.rm_mock ) )
#
#    #TODO: make others cases tests!
#
##############################################################################
## RemoveInputData.py
##############################################################################
#
#class RemoveInputDataSuccess( ModulesTestCase ):
#
#  #################################################
#
#  def test_execute( self ):
#
#    self.assertTrue( self.rid.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
#                                       self.workflowStatus, self.stepStatus,
#                                       self.wf_commons, self.step_commons,
#                                       self.step_number, self.step_id, self.rm_mock ) )
#
#    #TODO: make others cases tests!
#
##############################################################################
## SendBookkeeping.py
##############################################################################
#
#class SendBookkeepingSuccess( ModulesTestCase ):
#
#  #################################################
#
#  def test_execute( self ):
#
#    self.assertTrue( self.sb.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
#                                      self.workflowStatus, self.stepStatus,
#                                      self.wf_commons, self.step_commons,
#                                      self.step_number, self.step_id, self.bkc_mock ) )
#
#    #TODO: make others cases tests!
#
##############################################################################
## StepAccounting.py
##############################################################################
#
##class StepAccountingSuccess( ModulesTestCase ):
##
##  #################################################
##
##  def test_execute( self ):
##
##    self.assertTrue( self.sa.execute() )
##
##    #TODO: make others cases tests!
#
##############################################################################
## UploadLogFile.py
##############################################################################
#
#class UploadLogFileSuccess( ModulesTestCase ):
#
#  #################################################
#
#  def test_execute( self ):
#
#    self.assertTrue( self.ulf.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
#                                       self.workflowStatus, self.stepStatus,
#                                       self.wf_commons, self.step_commons,
#                                       self.step_number, self.step_id,
#                                       self.rm_mock, self.ft_mock ) )
#
#    #TODO: make others cases tests!
#
##############################################################################
## UploadOutputData.py
##############################################################################
#
#class UploadOutputDataSuccess( ModulesTestCase ):
#
#  #################################################
#
#  def test_execute( self ):
#
#    self.assertTrue( self.uod.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
#                                       self.workflowStatus, self.stepStatus,
#                                       self.wf_commons, self.step_commons,
#                                       self.step_number, self.step_id,
#                                       self.rm_mock, self.ft_mock, self.bkc_mock ) )
#
#    #TODO: make others cases tests!

#############################################################################
# Test Suite run 
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ModulesTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ModuleBaseSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( AnalyseLogFileSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( AnalyseXMLSummarySuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( GaudiApplicationSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ModulesUtilitiesSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( BookkeepingReportSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ErrorLoggingSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( FailoverRequestSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProtocolAccessTestSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( RemoveInputDataSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( SendBookkeepingSuccess ) )
##  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( StepAccountingSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( UploadLogFileSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( UploadOutputDataSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
