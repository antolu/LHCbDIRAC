""" Unit tests for Workflow Modules
"""

#pylint: disable=protected-access, missing-docstring, invalid-name, line-too-long

import unittest
import itertools
import os
import copy
import shutil

from mock import MagicMock, patch

from DIRAC.DataManagementSystem.Client.test.mock_DM import dm_mock
from DIRAC.Resources.Catalog.test.mock_FC import fc_mock

from DIRAC import gLogger
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.TransformationSystem.Client.FileReport import FileReport
from LHCbDIRAC.Core.Utilities.XMLSummaries import XMLSummary


# mocks
from LHCbDIRAC.Workflow.Modules.test.mock_Commons import prod_id, prod_job_id, wms_job_id, \
                                                         workflowStatus, stepStatus, step_id, step_number,\
                                                         step_commons, wf_commons,\
                                                         rc_mock
from LHCbDIRAC.BookkeepingSystem.Client.test.mock_BookkeepingClient import bkc_mock



# sut
from LHCbDIRAC.Workflow.Modules.AnalyseXMLSummary import AnalyseXMLSummary
from LHCbDIRAC.Workflow.Modules.AnalyseLogFile import AnalyseLogFile
from LHCbDIRAC.Workflow.Modules.BookkeepingReport import BookkeepingReport
from LHCbDIRAC.Workflow.Modules.FailoverRequest import FailoverRequest
from LHCbDIRAC.Workflow.Modules.RemoveInputData import RemoveInputData
from LHCbDIRAC.Workflow.Modules.SendBookkeeping import SendBookkeeping
from LHCbDIRAC.Workflow.Modules.UploadOutputData import UploadOutputData
from LHCbDIRAC.Workflow.Modules.UserJobFinalization import UserJobFinalization
from LHCbDIRAC.Workflow.Modules.StepAccounting import StepAccounting
from LHCbDIRAC.Workflow.Modules.UploadLogFile import UploadLogFile
from LHCbDIRAC.Workflow.Modules.FileUsage import FileUsage
from LHCbDIRAC.Workflow.Modules.CreateDataFile import CreateDataFile

getDestinationSEListMock = MagicMock()
getDestinationSEListMock.return_value = []
getDestinationSEListMockCNAF = MagicMock()
getDestinationSEListMockCNAF.return_value = ['CNAF']


__RCSID__ = "$Id$"

class ModulesTestCase( unittest.TestCase ):
  """ Base class for the Modules test cases
  """
  def setUp( self ):

    gLogger.setLevel( 'DEBUG' )
    self.maxDiff = None

    self.jsu_mock = MagicMock()
    self.jsu_mock.setJobApplicationStatus.return_value = {'OK': True, 'Value': ''}

    self.jsu_mock = MagicMock()
    self.jsu_mock.setJobApplicationStatus.return_value = {'OK': True, 'Value': ''}

    self.ft_mock = MagicMock()
    self.ft_mock.transferAndRegisterFile.return_value = {'OK': True, 'Value': {'uploadedSE':''}}
    self.ft_mock.transferAndRegisterFileFailover.return_value = {'OK': True, 'Value': {}}
    self.ft_mock.request = rc_mock
    self.ft_mock.FileCatalog = fc_mock

    self.nc_mock = MagicMock()
    self.nc_mock.sendMail.return_value = {'OK': True, 'Value': ''}

    self.xf_o_mock = MagicMock()
    self.xf_o_mock.inputFileStats = {'a':1, 'b':2}
    self.xf_o_mock.outputFileStats = {'a':1, 'b':2}
    self.xf_o_mock.analyse.return_value = True

    self.jobStep_mock = MagicMock()
    self.jobStep_mock.commit.return_value = {'OK': True, 'Value': ''}
    self.jobStep_mock.setValuesFromDict.return_value = {'OK': True, 'Value': ''}
    self.jobStep_mock.checkValues.return_value = {'OK': True, 'Value': ''}

  def tearDown( self ):
    for fileProd in ['appLog', 'foo.txt', 'aaa.Bhadron.dst', 'bbb.Calibration.dst', 'bar.py', 'aLongLog.log',
                     'bookkeeping_123_00000456_321.xml',
                     'aLongLog.log.gz', 'ccc.charm.mdst', 'ccc.charm.mdst', 'prova.txt', 'aLog.log',
                     'BAR.txt', 'FooBAR.ext.txt', 'foo_1.txt', 'bar_2.py', 'bar.txt',
                     'ErrorLogging_Step1_coredump.log', '123_00000456_request.xml', 'lfn1', 'lfn2', 'XMLSummaryFile',
                     'aaa.bhadron.dst', 'bbb.calibration.dst', 'ProductionOutputData', 'data.py', '123_00000456_request.json',
                     '00000123_00000456.tar', 'someOtherDir', 'DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK', 'myfoo.blah',
                     'prodConf_someApp__.py', 'prodConf_someApp___.py']:
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
# AnalyseXMLSummary.py
#############################################################################

@patch( "LHCbDIRAC.Workflow.Modules.ModuleBase.RequestValidator", side_effect = MagicMock() )
class AnalyseXMLSummarySuccess( ModulesTestCase ):

  #################################################

  def test_execute( self, _patch ):

    axlf = AnalyseXMLSummary( bkClient = bkc_mock, dm = dm_mock )
    axlf.stepInputData = ['some.sdst', '00012345_00006789_1.sdst']
    axlf.jobType = 'merge'

    logAnalyser = MagicMock()

    logAnalyser.return_value = True
    axlf.logAnalyser = logAnalyser
    axlf.XMLSummary_o = self.xf_o_mock
    axlf.nc = self.nc_mock
    axlf.XMLSummary = 'XMLSummaryFile'
    with open( axlf.XMLSummary, 'w' ) as f:
      f.write( """<?xml version="1.0" encoding="UTF-8"?>

  <summary version="1.0" xsi:noNamespaceSchemaLocation="$XMLSUMMARYBASEROOT/xml/XMLSummary.xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
          <success>True</success>
          <step>finalize</step>
          <usage>
                  <stat unit="KB" useOf="MemoryMaximum">866104.0</stat>
          </usage>
          <input>
                  <file GUID="CCE96707-4BE9-E011-81CD-003048F35252" name="LFN:00012478_00000532_1.sim" status="full">200</file>
          </input>
          <output>
                  <file GUID="229BBEF1-66E9-E011-BBD0-003048F35252" name="PFN:00012478_00000532_2.xdigi" status="full">200</file>
          </output>
  </summary>
  """ )

    # no errors, all ok
    for wf_cs in copy.deepcopy( wf_commons ):
      for s_cs in step_commons:
        self.assertTrue( axlf.execute( prod_id, prod_job_id, wms_job_id,
                                       workflowStatus, stepStatus,
                                       wf_cs, s_cs,
                                       step_number, step_id )['OK'] )

    # logAnalyser gives errors
    axlf.jobType = 'reco'

    logAnalyser.return_value = False
    axlf.logAnalyser = logAnalyser

    for wf_cs in copy.deepcopy( wf_commons ):
      for s_cs in step_commons:
        if wf_cs.has_key( 'AnalyseLogFilePreviouslyFinalized' ):
          continue
        self.assertTrue( axlf.execute( prod_id, prod_job_id, wms_job_id,
                                       workflowStatus, stepStatus,
                                       wf_cs, s_cs,
                                       step_number, step_id )['OK'] )

  def test__basicSuccess( self, _patch ):

    axlf = AnalyseXMLSummary( bkClient = bkc_mock, dm = dm_mock )

    f = open( 'XMLSummaryFile', 'w' )
    f.write( """<?xml version="1.0" encoding="UTF-8"?>

<summary version="1.0" xsi:noNamespaceSchemaLocation="$XMLSUMMARYBASEROOT/xml/XMLSummary.xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
        <success>True</success>
        <step>finalize</step>
        <usage>
                <stat unit="KB" useOf="MemoryMaximum">866104.0</stat>
        </usage>
        <input>
                <file GUID="CCE96707-4BE9-E011-81CD-003048F35252" name="LFN:00012478_00000532_1.sim" status="full">200</file>
        </input>
        <output>
                <file GUID="229BBEF1-66E9-E011-BBD0-003048F35252" name="PFN:00012478_00000532_2.xdigi" status="full">200</file>
        </output>
</summary>
""" )
    f.close()
    axlf.XMLSummary_o = XMLSummary( 'XMLSummaryFile' )
    res = axlf._basicSuccess()
    self.assertFalse( res )

    axlf.XMLSummary_o.inputFileStats = {'full':2, 'part':1, 'fail':0, 'other':0}
    axlf.XMLSummary_o.inputStatus = [( 'aa/1.txt', 'full' ), ( 'aa/2.txt', 'part' )]
    axlf.inputDataList = ['aa/1.txt', 'aa/2.txt']
    axlf.numberOfEvents = -1
    axlf.fileReport = FileReport()
    axlf.production_id = '123'
    res = axlf._basicSuccess()
    self.assertTrue( res )
    self.assertEqual( axlf.fileReport.statusDict, {'aa/2.txt': 'Problematic'} )

    axlf.XMLSummary_o.inputFileStats = {'full':2, 'part':0, 'fail':1, 'other':0}
    axlf.XMLSummary_o.inputStatus = [( 'aa/1.txt', 'fail' ), ( 'aa/2.txt', 'full' )]
    axlf.inputDataList = ['aa/1.txt', 'aa/2.txt']
    axlf.numberOfEvents = -1
    axlf.fileReport = FileReport()
    axlf.production_id = '123'
    res = axlf._basicSuccess()
    self.assertTrue( res )
    self.assertEqual( axlf.fileReport.statusDict, {'aa/1.txt': 'Problematic'} )

    axlf.XMLSummary_o.inputFileStats = {'full':2, 'part':0, 'fail':1, 'other':0}
    axlf.XMLSummary_o.inputStatus = [( 'aa/1.txt', 'fail' ), ( 'aa/2.txt', 'full' )]
    axlf.inputDataList = ['aa/3.txt']
    axlf.numberOfEvents = -1
    axlf.fileReport = FileReport()
    axlf.production_id = '123'
    res = axlf._basicSuccess()
    self.assertFalse( res )
    self.assertEqual( axlf.fileReport.statusDict, {} )

    axlf.XMLSummary_o.inputFileStats = {'full':2, 'part':1, 'fail':1, 'other':0}
    axlf.XMLSummary_o.inputStatus = [( 'aa/1.txt', 'fail' ), ( 'aa/2.txt', 'part' )]
    axlf.inputDataList = ['aa/1.txt', 'aa/2.txt']
    axlf.numberOfEvents = -1
    axlf.fileReport = FileReport()
    axlf.production_id = '123'
    res = axlf._basicSuccess()
    self.assertTrue( res )
    self.assertEqual( axlf.fileReport.statusDict, {'aa/1.txt': 'Problematic', 'aa/2.txt': 'Problematic'} )

    axlf.XMLSummary_o.inputFileStats = {'full':2, 'part':1, 'fail':1, 'other':0}
    axlf.XMLSummary_o.inputStatus = [( 'aa/1.txt', 'fail' ), ( 'aa/2.txt', 'part' )]
    axlf.inputDataList = ['aa/1.txt', 'aa/2.txt']
    axlf.numberOfEvents = '10'
    axlf.fileReport = FileReport()
    axlf.production_id = '123'
    res = axlf._basicSuccess()
    self.assertTrue( res )
    self.assertEqual( axlf.fileReport.statusDict, {'aa/1.txt': 'Problematic'} )


#############################################################################
# AnalyseLogFile.py
#############################################################################

@patch( "LHCbDIRAC.Workflow.Modules.ModuleBase.RequestValidator", side_effect = MagicMock() )
class AnalyseLogFileSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self, _patch ):

    alf = AnalyseLogFile( bkClient = bkc_mock, dm = dm_mock )
    alf.stepInputData = ['some.sdst', '00012345_00006789_1.sdst']
    alf.jobType = 'merge'
    alf.nc = self.nc_mock

    logAnalyser = MagicMock()
    logAnalyser.return_value = True
    alf.logAnalyser = logAnalyser
#    no errors, no input data
    for wf_cs in copy.deepcopy( wf_commons ):
      for s_cs in step_commons:
        self.assertTrue( alf.execute( prod_id, prod_job_id, wms_job_id,
                                      workflowStatus, stepStatus,
                                      wf_cs, s_cs,
                                      step_number, step_id )['OK'] )


    alf.jobType = 'reco'

    # logAnalyser gives errors
    logAnalyser.return_value = False
    alf.logAnalyser = logAnalyser

    for wf_cs in copy.deepcopy( wf_commons ):
      for s_cs in copy.deepcopy( step_commons ):
        if wf_cs.has_key( 'AnalyseLogFilePreviouslyFinalized' ):
          continue
        self.assertFalse( alf.execute( prod_id, prod_job_id, wms_job_id,
                                       workflowStatus, stepStatus,
                                       wf_cs, s_cs,
                                       step_number, step_id )['OK'] )

    # there's a core dump
    logAnalyser.return_value = False
    alf.logAnalyser = logAnalyser
    open( 'ErrorLogging_Step1_coredump.log', 'w' ).close()
    for wf_cs in copy.deepcopy( wf_commons ):
      for s_cs in step_commons:
        if not wf_cs.has_key( 'AnalyseLogFilePreviouslyFinalized' ):
          self.assertFalse( alf.execute( prod_id, prod_job_id, wms_job_id,
                                         workflowStatus, stepStatus,
                                         wf_cs, s_cs,
                                         step_number, step_id )['OK'] )
        else:
          self.assertTrue( alf.execute( prod_id, prod_job_id, wms_job_id,
                                        workflowStatus, stepStatus,
                                        wf_cs, s_cs,
                                        step_number, step_id )['OK'] )


#############################################################################
# FailoverRequest.py
#############################################################################

@patch( "LHCbDIRAC.Workflow.Modules.ModuleBase.RequestValidator", side_effect = MagicMock() )
class FailoverRequestSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self, _patch ):

    fr = FailoverRequest( bkClient = bkc_mock, dm = dm_mock )
    fr.jobType = 'merge'
    fr.stepInputData = ['foo', 'bar']
    fr.requestValidator = MagicMock()

    # no errors, no input data
    for wf_cs in copy.deepcopy( wf_commons ):
      for s_cs in step_commons:
        self.assertTrue( fr.execute( prod_id, prod_job_id, wms_job_id,
                                     workflowStatus, stepStatus,
                                     wf_cs, s_cs,
                                     step_number, step_id )['OK'] )


##############################################################################
# # RemoveInputData.py
##############################################################################

@patch( "LHCbDIRAC.Workflow.Modules.ModuleBase.RequestValidator", side_effect = MagicMock() )
class RemoveInputDataSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self, _patch ):

    rid = RemoveInputData( bkClient = bkc_mock, dm = dm_mock )
    # no errors, no input data
    for wf_cs in copy.deepcopy( wf_commons ):
      if 'InputData' in wf_cs.keys():
        continue
      for s_cs in step_commons:
        self.assertTrue( rid.execute( prod_id, prod_job_id, wms_job_id,
                                      workflowStatus, stepStatus,
                                      wf_cs, s_cs,
                                      step_number, step_id )['OK'] )

    # no errors, input data
    for wf_cs in copy.deepcopy( wf_commons ):
      if 'InputData' not in wf_cs.keys():
        continue
      for s_cs in step_commons:
        self.assertTrue( rid.execute( prod_id, prod_job_id, wms_job_id,
                                      workflowStatus, stepStatus,
                                      wf_cs, s_cs,
                                      step_number, step_id )['OK'] )



##############################################################################
# # SendBookkeeping.py
##############################################################################

@patch( "LHCbDIRAC.Workflow.Modules.ModuleBase.RequestValidator", side_effect = MagicMock() )
class SendBookkeepingSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self, _patch ):

    # no errors, no input data
    for wf_cs in copy.deepcopy( wf_commons ):
      for s_cs in step_commons:
        sb = SendBookkeeping( bkClient = bkc_mock, dm = dm_mock )
        self.assertTrue( sb.execute( prod_id, prod_job_id, wms_job_id,
                                     workflowStatus, stepStatus,
                                     wf_cs, s_cs,
                                     step_number, step_id )['OK'] )

#############################################################################
# StepAccounting.py
#############################################################################

@patch( "LHCbDIRAC.Workflow.Modules.ModuleBase.RequestValidator", side_effect = MagicMock() )
class StepAccountingSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self, _patch ):

    sa = StepAccounting( bkClient = bkc_mock, dm = dm_mock )
    sa.jobType = 'merge'
    sa.stepInputData = ['foo', 'bar']
    sa.siteName = 'DIRAC.Test.ch'

    for wf_cs in copy.deepcopy( wf_commons ):
      for s_cs in step_commons:
        self.assertTrue( sa.execute( prod_id, prod_job_id, wms_job_id,
                                     workflowStatus, stepStatus,
                                     wf_cs, s_cs,
                                     step_number, step_id,
                                     self.jobStep_mock, self.xf_o_mock )['OK'] )

#############################################################################
# UploadLogFile.py
#############################################################################

@patch( "LHCbDIRAC.Workflow.Modules.ModuleBase.RequestValidator", side_effect = MagicMock() )
class UploadLogFileSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self, _patch ):

    # no errors, no input data
#    for wf_commons in copy.deepcopy( wf_commons ):
#      for step_commons in step_commons:
#        self.assertTrue( ulf.execute( prod_id, prod_job_id, wms_job_id,
#                                           workflowStatus, stepStatus,
#                                           wf_commons, step_commons,
#                                           step_number, step_id,
#                                           dm_mock, self.ft_mock,
#                                           bkc_mock )['OK'] )

    # putStorageDirectory returns False

    rm_mock = copy.deepcopy( dm_mock )
    rm_mock.putStorageDirectory.return_value = {'OK':False, 'Message':'bih'}
    ft_mock = copy.deepcopy( self.ft_mock )
    ulf = UploadLogFile( bkClient = bkc_mock, dm = dm_mock )
    ulf.request = Request()
    ulf.failoverTransfer = ft_mock
    for wf_cs in copy.deepcopy( wf_commons ):
      for s_cs in step_commons:
        self.assertTrue( ulf.execute( prod_id, prod_job_id, 0,
                                      workflowStatus, stepStatus,
                                      wf_cs, s_cs,
                                      step_number, step_id )['OK'] )
#        self.assertTrue( ulf.finalize( rm_mock, self.ft_mock )['OK'] )

  def test__uploadLogToFailoverSE( self, _patch ):
    open( 'foo.txt', 'w' ).close()
    tarFileName = 'foo.txt'
    ulf = UploadLogFile( bkClient = bkc_mock, dm = dm_mock )
    ulf.request = Request()
    ulf.failoverTransfer = self.ft_mock
    ulf.logLFNPath = '/an/lfn/foo.txt'
    ulf.failoverSEs = ['SE1', 'SE2']
    ulf._uploadLogToFailoverSE( tarFileName )

  def test__determinRelevantFiles( self, _patch ):
    for fileN in ['foo.txt', 'aLog.log', 'aLongLog.log', 'aLongLog.log.gz']:
      try:
        os.remove( fileN )
      except OSError:
        continue

    open( 'foo.txt', 'w' ).close()
    open( 'bar.py', 'w' ).close()
    open( 'aLog.log', 'w' ).close()
    ulf = UploadLogFile( bkClient = bkc_mock, dm = dm_mock )
    res = ulf._determineRelevantFiles()
    self.assertTrue( res['OK'] )
    expected = ['foo.txt', 'aLog.log']
    if 'pylint.txt' in os.listdir( '.' ):
      expected.append( 'pylint.txt' )
    if 'nosetests.xml' in os.listdir( '.' ):
      expected.append( 'nosetests.xml' )
    self.assert_( set( res['Value'] ) >= set( expected ) )

    fd = open( 'aLongLog.log', 'w' )
    for _x in xrange( 2500 ):
      fd.writelines( "Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum" )
    fd.close()
    res = ulf._determineRelevantFiles()
    self.assertTrue( res['OK'] )
    expected = ['foo.txt', 'aLog.log', 'aLongLog.log.gz']
    if 'pylint.txt' in os.listdir( '.' ):
      expected.append( 'pylint.txt' )
    if 'nosetests.xml' in os.listdir( '.' ):
      expected.append( 'nosetests.xml' )
    self.assert_( set( res['Value'] ) >= set( expected ) )

    open( 'foo.txt', 'w' ).close()
    fd = open( 'aLongLog.log', 'w' )
    for _x in xrange( 2500 ):
      fd.writelines( "Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum" )
    fd.close()
    open( 'bar.py', 'w' ).close()
    res = ulf._determineRelevantFiles()
    expected = ['foo.txt', 'aLog.log', 'aLongLog.log.gz']
    if 'pylint.txt' in os.listdir( '.' ):
      expected.append( 'pylint.txt' )
    if 'nosetests.xml' in os.listdir( '.' ):
      expected.append( 'nosetests.xml' )
    self.assertTrue( res['OK'] )
    self.assert_( set( res['Value'] ) >= set( expected ) )

##############################################################################
# # UploadOutputData.py
##############################################################################

@patch( "LHCbDIRAC.Workflow.Modules.ModuleBase.RequestValidator", side_effect = MagicMock() )
class UploadOutputDataSuccess( ModulesTestCase ):

  #################################################

  @patch( "LHCbDIRAC.Workflow.Modules.UploadOutputData.FileCatalog", side_effect = fc_mock )
  @patch( "LHCbDIRAC.Core.Utilities.ResolveSE.gConfig", side_effect = MagicMock() )
  def test_execute( self, _p, _patch, _patched ):

    uod = UploadOutputData( bkClient = bkc_mock, dm = dm_mock )
    uod.siteName = 'DIRAC.Test.ch'
    uod.failoverTransfer = self.ft_mock

    # no errors, no input data
    for wf_cs in copy.deepcopy( wf_commons ):
      if wf_cs.has_key( 'InputData' ):
        continue
      for s_cs in copy.deepcopy( step_commons ):
        fileDescendants = {}
        self.assertTrue( uod.execute( prod_id, prod_job_id, wms_job_id,
                                      workflowStatus, stepStatus,
                                      wf_cs, s_cs,
                                      step_number, step_id,
                                      SEs = ['SomeSE'],
                                      fileDescendants = fileDescendants )['OK'] )


    # no errors, input data
    for wf_cs in copy.deepcopy( wf_commons ):
      for s_cs in step_commons:
        for transferAndRegisterFile in ( {'OK': True, 'Value': {'uploadedSE':''}}, {'OK': False, 'Message': 'error'} ):
#          for transferAndRegisterFileFailover in ( {'OK': True, 'Value': {}}, {'OK': False, 'Message': 'error'} ):
          self.ft_mock.transferAndRegisterFile.return_value = transferAndRegisterFile
#            self.ft_mock.transferAndRegisterFileFailover.return_value = transferAndRegisterFileFailover
          open( 'foo.txt', 'w' ).close()
          open( 'bar.txt', 'w' ).close()
          if not wf_cs.has_key( 'InputData' ):
            continue
          if wf_cs['InputData'] == '':
            continue
          wf_cs['outputList'] = [{'outputDataType': 'txt', 'outputDataName': 'foo.txt'},
                                 {'outputDataType': 'txt', 'outputDataName': 'bar.txt'},
                                ]
          wf_cs['ProductionOutputData'] = ['/lhcb/MC/2010/DST/00012345/0001/foo.txt',
                                           '/lhcb/MC/2010/DST/00012345/0001/bar.txt' ]
#          bkc_mock.getFileDescendants.return_value = {'OK': False,
#                                                           'rpcStub': ( ( 'Bookkeeping/BookkeepingManager',
#                                                                        {'skipCACheck': False,
#                                                                         'timeout': 3600} ),
#                                                                       'getFileDescendants', ( ['foo'], 9, 0, True ) ),
#                                                           'Value': {'Successful': {'foo.txt': ['baaar']},
#                                                                     'Failed': [],
#                                                                     'NotProcessed': []}}
          fileDescendants = {'foo.txt': ['baaar']}
          self.assertFalse( uod.execute( prod_id, prod_job_id, wms_job_id,
                                         workflowStatus, stepStatus,
                                         wf_cs, s_cs,
                                         step_number, step_id,
                                         SEs = ['SomeSE'],
                                         fileDescendants = fileDescendants )['OK'] )
#          bkc_mock.getFileDescendants.return_value = {'OK': True,
#                                                           'rpcStub': ( ( 'Bookkeeping/BookkeepingManager',
#                                                                        {'skipCACheck': False,
#                                                                         'timeout': 3600} ),
#                                                                       'getFileDescendants', ( ['foo'], 9, 0, True ) ),
#                                                           'Value': {'Successful': {},
#                                                                     'Failed': [],
#                                                                     'NotProcessed': []}}
          if wf_cs['Request'] == '':
            continue
          fileDescendants = {}
          res = uod.execute( prod_id, prod_job_id, wms_job_id,
                             workflowStatus, stepStatus,
                             wf_cs, s_cs,
                             step_number, step_id,
                             SEs = ['SomeSE'],
                             fileDescendants = fileDescendants )
          self.assertTrue( res['OK'] )
#            if transferAndRegisterFileFailover['OK']:
#              self.assertTrue( res['OK'] )
#            else:
#              self.assertFalse( res['OK'] )
          os.remove( 'foo.txt' )
          os.remove( 'bar.txt' )

  def test__cleanUp( self, _patch ):
    f1 = File()
    f1.LFN = '/a/1.txt'
    f2 = File()
    f2.LFN = '/a/2.txt'
    f3 = File()
    f3.LFN = '/a/3.txt'

    o1 = Operation()
    o1.Type = 'RegisterFile'
    o1.addFile( f1 )
    o2 = Operation()
    o2.Type = 'RegisterFile'
    o2.addFile( f2 )
    o3 = Operation()
    o3.Type = 'ForwardDISET'
    o4 = Operation()
    o4.Type = 'RegisterFile'
    o4.addFile( f1 )
    o4.addFile( f3 )

    r = Request()
    r.addOperation( o4 )
    r.addOperation( o1 )
    r.addOperation( o2 )
    r.addOperation( o3 )

    uod = UploadOutputData( bkClient = bkc_mock, dm = dm_mock )
    uod.failoverTransfer = self.ft_mock
    uod.request = r

    expected = Request()
    expected.addOperation( o3 )
    removeOp = Operation()
    removeOp.Type = 'RemoveFile'
    fileRemove1 = File()
    fileRemove1.LFN = '/a/1.txt'
    fileRemove2 = File()
    fileRemove2.LFN = '/a/2.txt'
    fileRemove3 = File()
    fileRemove3.LFN = '/a/notPresent.txt'
    removeOp.addFile( fileRemove1 )
    removeOp.addFile( fileRemove3 )
    removeOp.addFile( fileRemove2 )
    expected.addOperation( removeOp )

    uod._cleanUp( {'1.txt':{'lfn':'/a/1.txt'},
                   '2.txt':{'lfn':'/a/2.txt'},
                   'notPresent.txt':{'lfn':'/a/notPresent.txt' } } )

    for opsR, opsE in itertools.izip( uod.request, expected ):
      self.assertEqual( str( opsR ), str( opsE ) )


##############################################################################
# # UserJobFinalization.py
##############################################################################

@patch( "LHCbDIRAC.Workflow.Modules.ModuleBase.RequestValidator", side_effect = MagicMock() )
class UserJobFinalizationSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self, _patch ):

    ujf = UserJobFinalization( bkClient = bkc_mock, dm = dm_mock )
    ujf.bkClient = bkc_mock
    ujf.failoverTransfer = self.ft_mock
    ujf.requestValidator = MagicMock()
    ujf.requestValidator.validate.return_value = {'OK':True}

    # no errors, no input data
    for wf_cs in copy.deepcopy( wf_commons ):
      wf_cs['TotalSteps'] = step_number
      for s_cs in step_commons:
        self.assertTrue( ujf.execute( prod_id, prod_job_id, wms_job_id,
                                      workflowStatus, stepStatus,
                                      wf_cs, s_cs,
                                      step_number, step_id )['OK'] )

    for wf_cs in copy.deepcopy( wf_commons ):
      wf_cs['TotalSteps'] = step_number
      for s_cs in step_commons:
        wf_cs['UserOutputData'] = ['i1', 'i2']
        wf_cs['UserOutputSE'] = ['MySE']
        wf_cs['OwnerName'] = 'fstagni'
        open( 'i1', 'w' ).close()
        open( 'i2', 'w' ).close()
        self.assertTrue( ujf.execute( prod_id, prod_job_id, wms_job_id,
                                      workflowStatus, stepStatus,
                                      wf_cs, s_cs,
                                      step_number, step_id, orderedSEs = ['MySE1', 'MySE2'] )['OK'] )
      os.remove( 'i1' )
      os.remove( 'i2' )

  @patch( "LHCbDIRAC.Workflow.Modules.UserJobFinalization.getDestinationSEList", side_effect = getDestinationSEListMock )
  def test__getOrderedSEsList( self, _patch, _patched ):

    ujf = UserJobFinalization( bkClient = bkc_mock, dm = dm_mock )

    ujf.userOutputSE = ['userSE']
    res = ujf._getOrderedSEsList()
    self.assertEqual( res, ['userSE'] )

    ujf.defaultOutputSE = ['CERN']
    res = ujf._getOrderedSEsList()
    self.assertEqual( res, ['userSE', 'CERN'] )

  @patch( "LHCbDIRAC.Workflow.Modules.UserJobFinalization.getDestinationSEList", side_effect = getDestinationSEListMockCNAF )
  def test__getOrderedSEsListCNAF( self, _patch, _patched ):

    ujf = UserJobFinalization( bkClient = bkc_mock, dm = dm_mock )
    res = ujf._getOrderedSEsList()
    self.assertEqual( res, ['CNAF'] )

#############################################################################
# FileUsage.py
#############################################################################

@patch( "LHCbDIRAC.Workflow.Modules.ModuleBase.RequestValidator", side_effect = MagicMock() )
class FileUsageSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self, _patch ):

    # no errors, no input files to report
    wfStatus = copy.deepcopy(workflowStatus)
    wfC = copy.deepcopy( wf_commons )
    for wf_cs in wfC:  # copy.deepcopy( wf_commons ):
      for s_cs in step_commons:
        fu = FileUsage( bkClient = bkc_mock, dm = dm_mock )
        self.assertTrue( fu.execute( prod_id, prod_job_id, wms_job_id,
                                     wfStatus, stepStatus,
                                     wf_cs, s_cs,
                                     step_number, step_id )['OK'] )
    wfC = copy.deepcopy( wf_commons )
    for wf_cs in wfC:  # copy.deepcopy( wf_commons ):
      wf_cs['ParametricInputData'] = ['LFN:/lhcb/data/2010/EW.DST/00008380/0000/00008380_00000287_1.ew.dst',
                                      'LFN:/lhcb/data/2010/EW.DST/00008380/0000/00008380_00000285_1.ew.dst',
                                      'LFN:/lhcb/data/2010/EW.DST/00008380/0000/00008380_00000281_1.ew.dst']
      for s_cs in step_commons:
        fu = FileUsage( bkClient = bkc_mock, dm = dm_mock )
        self.assertTrue( fu.execute( prod_id, prod_job_id, wms_job_id,
                                     workflowStatus, stepStatus,
                                     wf_cs, s_cs,
                                     step_number, step_id )['OK'] )
    wfC = copy.deepcopy( wf_commons )
    for wf_cs in wfC:  # copy.deepcopy( wf_commons ):
      wf_cs['ParametricInputData'] = ['LFN:/lhcb/data/2010/EW.DST/00008380/0000/00008380_00000287_1.ew.dst',
                                      'LFN:/lhcb/data/2010/EW.DST/00008380/0000/00008380_00000285_1.ew.dst',
                                      'LFN:/lhcb/data/2010/PIPPO/00008380/0000/00008380_00000281_1.pippo.dst']
      for s_cs in step_commons:
        fu = FileUsage( bkClient = bkc_mock, dm = dm_mock )
        self.assertTrue( fu.execute( prod_id, prod_job_id, wms_job_id,
                                     workflowStatus, stepStatus,
                                     wf_cs, s_cs,
                                     step_number, step_id )['OK'] )

    # workflow status not ok
    wfC = copy.deepcopy( wf_commons )
    for wf_cs in wfC:  # copy.deepcopy( wf_commons ):
      wfStatus = {'OK':False, 'Message':'Mess'}
      for s_cs in step_commons:
        fu = FileUsage( bkClient = bkc_mock, dm = dm_mock )
        self.assertTrue( fu.execute( prod_id, prod_job_id, wms_job_id,
                                     wfStatus, stepStatus,
                                     wf_cs, s_cs,
                                     step_number, step_id )['OK'] )


#############################################################################
# FileUsage.py
#############################################################################

@patch( "LHCbDIRAC.Workflow.Modules.ModuleBase.RequestValidator", side_effect = MagicMock() )
class CreateDataFileSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self, _patch ):

    cdf = CreateDataFile( bkClient = bkc_mock, dm = dm_mock )
    cdf.jobType = 'merge'
    cdf.stepInputData = ['foo', 'bar']

    for wf_cs in copy.deepcopy( wf_commons ):
      for s_cs in step_commons:
        self.assertTrue( cdf.execute( prod_id, prod_job_id, wms_job_id,
                                      workflowStatus, stepStatus,
                                      wf_cs, s_cs,
                                      step_number, step_id )['OK'] )


#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  unittest.main()

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#