""" Unit tests for Workflow Modules
"""

#pylint: disable=protected-access, missing-docstring, invalid-name, line-too-long

import unittest
import itertools
import os
import shutil

from mock import MagicMock, patch

from DIRAC.DataManagementSystem.Client.test.mock_DM import dm_mock
from DIRAC.Resources.Catalog.test.mock_FC import fc_mock

from DIRAC import gLogger

from LHCbDIRAC.Workflow.Modules.test.mock_Commons import version, prod_id, prod_job_id, wms_job_id, \
                                                         workflowStatus, stepStatus, step_id, step_number,\
                                                         step_commons, wf_commons,\
                                                         rc_mock
from LHCbDIRAC.BookkeepingSystem.Client.test.mock_BookkeepingClient import bkc_mock

# sut
from LHCbDIRAC.Workflow.Modules.ModuleBase import ModuleBase

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
# ModuleBase.py
#############################################################################

@patch( "LHCbDIRAC.Workflow.Modules.ModuleBase.RequestValidator", side_effect = MagicMock() )
class ModuleBaseSuccess( ModulesTestCase ):

  #################################################

  def test__checkLocalExistance( self, _patch ):

    mb = ModuleBase( bkClientIn = bkc_mock, dm = dm_mock )
    self.assertRaises( OSError, mb._checkLocalExistance, ['aaa', 'bbb'] )

    #################################################

  def test__applyMask( self, _patch ):

    candidateFiles = {'00012345_00012345_4.dst': {'lfn': '/lhcb/MC/2010/DST/00012345/0001/00012345_00012345_4.dst',
                                                  'type': 'dst'},
                      '00012345_00012345_2.digi': {'type': 'digi'},
                      '00012345_00012345_3.digi': {'type': 'digi'},
                      '00012345_00012345_5.AllStreams.dst': {'lfn': '/lhcb/MC/2010/DST/00012345/0001/00012345_00012345_5.AllStreams.dst',
                                                             'type': 'allstreams.dst'},
                      '00012345_00012345_1.sim': {'type': 'sim'},
                      'Gauss_HIST_1.root': {'type':'GAUSSHIST'},
                      '00038941_00000004_6.B2D0Pi_D2KKPiPi.Strip.dst': {'lfn': '/lhcb/MC/2012/B2D0PI_D2KKPIPI.STRIP.DST/00038941/0000/00038941_00000004_6.B2D0Pi_D2KKPiPi.Strip.dst',
                                                                        'type': 'b2d0pi_d2kkpipi.strip.dst'}}


    fileMasks = ( ['dst'], 'dst', ['sim'], ['digi'], ['digi', 'sim'], 'allstreams.dst',
                  'b2d0pi_d2kkpipi.strip.dst', [], ['b2d0pi_d2kkpipi.strip.dst', 'digi'],
                  ['gausshist', 'digi'],
                )
    stepMasks = ( '', '5', '', ['2'], ['1', '3'], '', '', ['6'], [],
                  ['1', '3'], )

    results = ({'00012345_00012345_4.dst':{'lfn': '/lhcb/MC/2010/DST/00012345/0001/00012345_00012345_4.dst',
                                           'type': 'dst'}},
               {},
               {'00012345_00012345_1.sim': {'type': 'sim'}},
               {'00012345_00012345_2.digi': {'type': 'digi'},},
               {'00012345_00012345_3.digi': {'type': 'digi'},
                '00012345_00012345_1.sim': {'type': 'sim'}},
               {'00012345_00012345_5.AllStreams.dst':{'lfn': '/lhcb/MC/2010/DST/00012345/0001/00012345_00012345_5.AllStreams.dst',
                                                      'type': 'allstreams.dst'}},
               {'00038941_00000004_6.B2D0Pi_D2KKPiPi.Strip.dst':
                {'lfn': '/lhcb/MC/2012/B2D0PI_D2KKPIPI.STRIP.DST/00038941/0000/00038941_00000004_6.B2D0Pi_D2KKPiPi.Strip.dst',
                 'type': 'b2d0pi_d2kkpipi.strip.dst'}},
               {'00038941_00000004_6.B2D0Pi_D2KKPiPi.Strip.dst':
                {'lfn': '/lhcb/MC/2012/B2D0PI_D2KKPIPI.STRIP.DST/00038941/0000/00038941_00000004_6.B2D0Pi_D2KKPiPi.Strip.dst',
                 'type': 'b2d0pi_d2kkpipi.strip.dst'}},
               {'00012345_00012345_2.digi': {'type': 'digi'},
                '00012345_00012345_3.digi': {'type': 'digi'},
                '00038941_00000004_6.B2D0Pi_D2KKPiPi.Strip.dst':
                {'lfn': '/lhcb/MC/2012/B2D0PI_D2KKPIPI.STRIP.DST/00038941/0000/00038941_00000004_6.B2D0Pi_D2KKPiPi.Strip.dst',
                 'type': 'b2d0pi_d2kkpipi.strip.dst'}},
               {'00012345_00012345_3.digi': {'type': 'digi'},
                'Gauss_HIST_1.root':{'type':'GAUSSHIST'}},)

    for fileMask, result, stepMask in itertools.izip( fileMasks, results, stepMasks ):
      mb = ModuleBase( bkClientIn = bkc_mock, dm = dm_mock )
      res = mb._applyMask( candidateFiles, fileMask, stepMask )
      self.assertEqual( res, result )

  #################################################

  #################################################

  def test__checkSanity( self, _patch ):

    candidateFiles = {'00012345_00012345_4.dst':
                      {'lfn': '/lhcb/MC/2010/DST/00012345/0001/00012345_00012345_4.dst',
                       'type': 'dst'},
                      '00012345_00012345_2.digi': {'type': 'digi'},
                      '00012345_00012345_3.digi': {'type': 'digi'},
                      '00012345_00012345_5.AllStreams.dst':
                      {'lfn': '/lhcb/MC/2010/DST/00012345/0001/00012345_00012345_5.AllStreams.dst',
                       'type': 'DST'},
                      '00012345_00012345_1.sim': {'type': 'sim'}}

    mb = ModuleBase( bkClientIn = bkc_mock, dm = dm_mock )
    self.assertRaises( ValueError, mb._checkSanity, candidateFiles )

  #################################################

  def test_getCandidateFiles( self, _patch ):
    # this needs to avoid the "checkLocalExistance"

    mb = ModuleBase( bkClientIn = bkc_mock, dm = dm_mock )
    mb.outputSEs = {'txt':'SE1', 'py': 'SE', 'HIST':'HIST', 'blah':'SE2'}

    open( 'foo_1.txt', 'w' ).close()
    open( 'bar_2.py', 'w' ).close()
    open( 'myfoo.blah', 'w' ).close()

    outputList = [{'outputDataType': 'txt', 'outputDataName': 'foo_1.txt'},
                  {'outputDataType': 'py', 'outputDataName': 'bar_2.py'},
                  {'outputDataType': 'blah', 'outputDataName': 'myfoo.blah'}]
    outputLFNs = ['/lhcb/MC/2010/DST/00012345/0001/foo_1.txt',
                  '/lhcb/MC/2010/DST/00012345/0001/bar_2.py',
                  '/lhcb/user/f/fstagni/2015_06/prepend_myfoo.blah']
    fileMask = 'txt'
    stepMask = ''
    result = {'foo_1.txt': {'lfn': '/lhcb/MC/2010/DST/00012345/0001/foo_1.txt',
                            'type': outputList[0]['outputDataType'],
                            'workflowSE': 'SE1'}}

    res = mb.getCandidateFiles( outputList, outputLFNs, fileMask, stepMask )
    self.assertEqual( res, result )

    fileMask = ['txt', 'py']
    stepMask = None
    result = {'foo_1.txt': {'lfn': '/lhcb/MC/2010/DST/00012345/0001/foo_1.txt',
                            'type': outputList[0]['outputDataType'],
                            'workflowSE': 'SE1'},
              'bar_2.py': {'lfn': '/lhcb/MC/2010/DST/00012345/0001/bar_2.py',
                           'type': outputList[1]['outputDataType'],
                           'workflowSE': 'SE'}}
    res = mb.getCandidateFiles( outputList, outputLFNs, fileMask, stepMask )
    self.assertEqual( res, result )

    fileMask = ['blah']
    stepMask = None
    result = {'myfoo.blah': {'lfn': '/lhcb/user/f/fstagni/2015_06/prepend_myfoo.blah',
                             'type': outputList[2]['outputDataType'],
                             'workflowSE': 'SE2'}}
    res = mb.getCandidateFiles( outputList, outputLFNs, fileMask, stepMask )
    self.assertEqual( res, result )

    fileMask = ['aa']
    stepMask = None
    res = mb.getCandidateFiles( outputList, outputLFNs, fileMask, stepMask )
    result = {}
    self.assertEqual( res, result )

    fileMask = ''
    stepMask = '2'
    result = {'bar_2.py': {'lfn': '/lhcb/MC/2010/DST/00012345/0001/bar_2.py',
                           'type': outputList[1]['outputDataType'],
                           'workflowSE': 'SE'}}

    res = mb.getCandidateFiles( outputList, outputLFNs, fileMask, stepMask )

    self.assertEqual( res, result )

    fileMask = ''
    stepMask = 2
    result = {'bar_2.py': {'lfn': '/lhcb/MC/2010/DST/00012345/0001/bar_2.py',
                           'type': outputList[1]['outputDataType'],
                           'workflowSE': 'SE'}}

    res = mb.getCandidateFiles( outputList, outputLFNs, fileMask, stepMask )

    self.assertEqual( res, result )


    fileMask = ''
    stepMask = ['2', '3']
    result = {'bar_2.py': {'lfn': '/lhcb/MC/2010/DST/00012345/0001/bar_2.py',
                           'type': outputList[1]['outputDataType'],
                           'workflowSE': 'SE'}}

    res = mb.getCandidateFiles( outputList, outputLFNs, fileMask, stepMask )

    self.assertEqual( res, result )

    fileMask = ''
    stepMask = ['3']
    result = {}

    res = mb.getCandidateFiles( outputList, outputLFNs, fileMask, stepMask )

    self.assertEqual( res, result )

  def test__enableModule( self, _patch ):

    mb = ModuleBase( bkClientIn = bkc_mock, dm = dm_mock )
    mb.execute( version, prod_id, prod_job_id, wms_job_id,
                workflowStatus, stepStatus,
                wf_commons, step_commons[0],
                step_number, step_id )
    self.assertTrue( mb._enableModule() )

  def test__determineStepInputData( self, _patch ):

    mb = ModuleBase( bkClientIn = bkc_mock, dm = dm_mock )
    mb.stepName = 'DaVinci_2'

    inputData = 'previousStep'
    mb.gaudiSteps = ['Brunel_1', 'DaVinci_2']
    mb.workflow_commons = {'outputList': [{'stepName': 'Brunel_1',
                                                'outputDataType': 'brunelhist',
                                                'outputBKType': 'BRUNELHIST',
                                                'outputDataName': 'Brunel_00012345_00006789_1_Hist.root'},
                                               {'stepName': 'Brunel_1',
                                                'outputDataType': 'sdst',
                                                'outputBKType': 'SDST',
                                                'outputDataName': '00012345_00006789_1.sdst'}
                                              ]
                               }
    mb.inputDataType = 'SDST'

    first = mb._determineStepInputData( inputData )
    second = ['00012345_00006789_1.sdst']
    self.assertEqual( first, second )

    inputData = 'previousStep'
    mb.gaudiSteps = ['Brunel_1', 'DaVinci_2']
    mb.workflow_commons['outputList'] = [{'stepName': 'Brunel_1',
                                               'outputDataType': 'brunelhist',
                                               'outputBKType': 'BRUNELHIST',
                                               'outputDataName': 'Brunel_00012345_00006789_1_Hist.root'},
                                              {'stepName': 'Brunel_1',
                                               'outputDataType': 'sdst',
                                               'outputBKType': 'SDST',
                                               'outputDataName': 'some.sdst'},
                                              {'stepName': 'Brunel_1',
                                               'outputDataType': 'sdst',
                                               'outputBKType': 'SDST',
                                               'outputDataName': '00012345_00006789_1.sdst'}
                                             ]
    mb.inputDataType = 'SDST'
    first = mb._determineStepInputData( inputData )
    second = ['some.sdst', '00012345_00006789_1.sdst']
    self.assertEqual( first, second )

    inputData = 'LFN:123.raw'
    first = mb._determineStepInputData( inputData )
    second = ['123.raw']
    self.assertEqual( first, second )

  def test__determineOutputs( self, _patch ):
    mb = ModuleBase( bkClientIn = bkc_mock, dm = dm_mock )
    mb.stepInputData = ['foo', 'bar']

    mb.jobType = 'merge'
    mb.step_id = '00000123_00000456_1'
    for s_cs in list( step_commons ):
      mb.step_commons = dict( s_cs )
      mb.step_commons['listoutput'] = [{'outputDataType': 'bhadron.dst;sdst',
                                             'outputDataName': '00000123_00000456_1.bhadron.dst;sdst'}]
      outF, outft, histos = mb._determineOutputs()
      self.assertEqual( outF, [{'outputDataType': 'sdst',
                                'outputDataName': '00000123_00000456_1.sdst',
                                'outputBKType': 'SDST'}] )
      self.assertEqual( outft, ['sdst'] )
      self.assertFalse( histos )

      mb.step_commons['listoutput'] = [{'outputDataType': 'root',
                                             'outputDataName': '00000123_00000456_1.root',
                                             'outputBKType': 'ROOT'}]
      outF, outft, histos = mb._determineOutputs()
      self.assertEqual( outF, [{'outputDataType': 'root',
                                'outputDataName': '00000123_00000456_1.root',
                                'outputBKType': 'ROOT'}] )
      self.assertEqual( outft, ['root'] )
      self.assertFalse( histos )

    mb.jobType = 'reco'
    for s_cs in step_commons:
      mb.step_commons = s_cs
      mb.step_commons['listoutput'] = [{'outputDataType': 'sdst',
                                             'outputDataName': '00000123_00000456_1.sdst',
                                             'outputBKType': 'SDST'}]
      outF, outft, histos = mb._determineOutputs()
      self.assertEqual( outF, [{'outputDataType': 'sdst',
                                'outputDataName': '00000123_00000456_1.sdst',
                                'outputBKType': 'SDST'}] )
      self.assertEqual( outft, ['sdst'] )
      self.assertFalse( histos )

  def test__findOutputs( self, _patch ):
    open( 'aaa.Bhadron.dst', 'w' ).close()
    open( 'bbb.Calibration.dst', 'w' ).close()
    open( 'ccc.charm.mdst', 'w' ).close()
    open( 'prova.txt', 'w' ).close()

    stepOutput = [{'outputDataType': 'BHADRON.DST', 'outputDataName': 'aaa.bhadron.dst'},
                  {'outputDataType': 'CALIBRATION.DST', 'outputDataName': 'bbb.calibration.dst'},
                  {'outputDataType': 'CHARM.MDST', 'outputDataName': 'ccc.charm.mdst'},
                  {'outputDataType': 'CHARMCONTROL.DST', 'outputDataName': '00012345_00012345_2.CHARMCONTROL.DST'},
                  {'outputDataType': 'CHARMFULL.DST', 'outputDataName': '00012345_00012345_2.CHARMFULL.DST'},
                  {'outputDataType': 'LEPTONIC.MDST', 'outputDataName': '00012345_00012345_2.LEPTONIC.MDST'},
                  {'outputDataType': 'LEPTONICFULL.DST', 'outputDataName': '00012345_00012345_2.LEPTONICFULL.DST'},
                  {'outputDataType': 'MINIBIAS.DST', 'outputDataName': '00012345_00012345_2.MINIBIAS.DST'},
                  {'outputDataType': 'RADIATIVE.DST', 'outputDataName': '00012345_00012345_2.RADIATIVE.DST'},
                  {'outputDataType': 'SEMILEPTONIC.DST', 'outputDataName': '00012345_00012345_2.SEMILEPTONIC.DST'},
                  {'outputDataType': 'HIST', 'outputDataName': 'DaVinci_00012345_00012345_2_Hist.root'}]

    mb = ModuleBase( bkClientIn = bkc_mock, dm = dm_mock )
    self.assertRaises( IOError, mb._findOutputs, stepOutput )

    stepOutput = [{'outputDataType': 'BHADRON.DST', 'outputDataName': 'aaa.bhadron.dst'}]
    outExp = [{'outputDataType': 'bhadron.dst', 'outputBKType': 'BHADRON.DST', 'outputDataName': 'aaa.Bhadron.dst',
               'stepName': 'someApp_1'}]
    bkExp = ['BHADRON.DST']

    mb.stepName = 'someApp_1'
    out, bk = mb._findOutputs( stepOutput )

    self.assertEqual( out, outExp )
    self.assertEqual( bk, bkExp )


  def test_getFileMetadata( self, _patch ):
    open( 'foo_1.txt', 'w' ).close()
    open( 'bar_2.py', 'w' ).close()

    candidateFiles = {'foo_1.txt': {'lfn': '/lhcb/MC/2010/DST/00012345/0001/foo_1.txt',
                                    'type': 'txt',
                                    'workflowSE': 'SE1'},
                      'bar_2.py': {'lfn': '/lhcb/MC/2010/DST/00012345/0001/bar_2.py',
                                   'type': 'py',
                                   'workflowSE': 'SE2'},
                     }

    expectedResult = {'bar_2.py': {'filedict': {'Status': 'Waiting',
                                                'LFN': '/lhcb/MC/2010/DST/00012345/0001/bar_2.py',
                                                'GUID': 'D41D8CD9-8F00-B204-E980-0998ECF8427E',
                                                'Checksum': '001',
                                                'ChecksumType': 'ADLER32',
                                                'Size': 0},
                                   'lfn': '/lhcb/MC/2010/DST/00012345/0001/bar_2.py',
                                   'workflowSE': 'SE2',
                                   'localpath': os.getcwd() + '/bar_2.py',
                                   'guid': 'D41D8CD9-8F00-B204-E980-0998ECF8427E',
                                   'type': 'py'},
                      'foo_1.txt': {'filedict': {'Status': 'Waiting',
                                                 'LFN': '/lhcb/MC/2010/DST/00012345/0001/foo_1.txt',
                                                 'GUID': 'D41D8CD9-8F00-B204-E980-0998ECF8427E',
                                                 'Checksum': '001',
                                                 'ChecksumType': 'ADLER32',
                                                 'Size': 0},
                                    'lfn': '/lhcb/MC/2010/DST/00012345/0001/foo_1.txt',
                                    'workflowSE': 'SE1',
                                    'localpath': os.getcwd() + '/foo_1.txt',
                                    'guid': 'D41D8CD9-8F00-B204-E980-0998ECF8427E',
                                    'type': 'txt'}
                     }

    mb = ModuleBase( bkClientIn = bkc_mock, dm = dm_mock )
    result = mb.getFileMetadata( candidateFiles )
    self.assertEqual( result, expectedResult )

  def test_createProdConfFile( self, _patch ):
#     mb.applicationName = 'myApp'
    for wf_cs in wf_commons:
      mb = ModuleBase( bkClientIn = bkc_mock, dm = dm_mock )
      mb.workflow_commons = wf_cs
      for s_cs in step_commons:
        mb.step_commons = s_cs
        mb._resolveInputVariables()
        mb._resolveInputStep()
        res = mb.createProdConfFile( ['DST', 'GAUSSHIST'], True, 123, 1 )
        print res

@patch( "LHCbDIRAC.Workflow.Modules.ModuleBase.RequestValidator", side_effect = MagicMock() )
class ModuleBaseFailure( ModulesTestCase ):

  def test_getCandidateFiles( self, _patch ):
    # this needs to avoid the "checkLocalExistance"

    mb = ModuleBase( bkClientIn = bkc_mock, dm = dm_mock )
    mb.outputSEs = {'txt':'SE1', 'py': 'SE', 'HIST':'HIST', 'blah':'SE2'}

    open( 'foo_1.txt', 'w' ).close()
    open( 'bar_2.py', 'w' ).close()
    open( 'myfoo.blah', 'w' ).close()

    outputList = [{'outputDataType': 'txt', 'outputDataName': 'foo_1.txt'},
                  {'outputDataType': 'py', 'outputDataName': 'bar_2.py'},
                  {'outputDataType': 'blah', 'outputDataName': 'myfoo.blah'}]
    outputLFNs = ['/lhcb/MC/2010/DST/00012345/0001/foo_1.txt',
                  '/lhcb/MC/2010/DST/00012345/0001/bar_2.py',
                  '/lhcb/user/f/fstagni/2015_06/prepend_myfoo_1.blah']
    fileMask = 'blah'
    stepMask = ''

    self.assertRaises( ValueError, mb.getCandidateFiles, outputList, outputLFNs, fileMask, stepMask )

#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  unittest.main()

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
