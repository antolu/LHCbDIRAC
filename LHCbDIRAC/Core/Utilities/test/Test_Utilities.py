import unittest, itertools, copy

from LHCbDIRAC.Core.Utilities.ProductionData import constructProductionLFNs, _makeProductionLFN, _applyMask

class UtilitiesTestCase( unittest.TestCase ):
  """ Base class for the Utilities test cases
  """
  def setUp( self ):

    pass

#############################################################################
# ModuleBase.py
#############################################################################

class ProductionDataSuccess( UtilitiesTestCase ):

  #################################################

  def test_constructProductionLFNs( self ):

    # + test with InputData

    paramDict = { 'PRODUCTION_ID':'12345',
                  'JOB_ID':'54321',
                  'dataType':'MC',
                  'configVersion':'test',
                  'configName':'certification',
                  'JobType':'MCSimulation',
                  'outputList':[  {'outputDataType': 'sim', 'outputDataSE': 'Tier1-RDST', 'outputDataName': '00012345_00012345_1.sim'},
                                  {'outputDataType': 'digi', 'outputDataSE': 'Tier1-RDST', 'outputDataName': '00012345_00012345_2.digi'},
                                  {'outputDataType': 'dst', 'outputDataSE': 'Tier1_MC_M-DST', 'outputDataName': '00012345_00012345_4.dst'},
                                  {'outputDataType': 'ALLSTREAMS.DST', 'outputBKType': 'ALLSTREAMS.DST', 'outputDataSE': 'Tier1_MC_M-DST', 'outputDataName': '00012345_00012345_5.AllStreams.dst'}],
#                  'outputDataFileMask': ''
                 }

    reslist = [
                { 'LogTargetPath': ['/lhcb/certification/test/LOG/00012345/0005/00012345_00054321.tar'],
                  'LogFilePath': ['/lhcb/certification/test/LOG/00012345/0005/00054321'],
                  'DebugLFNs': ['/lhcb/debug/test/SIM/00012345/0005/00012345_00054321_1.sim',
                               '/lhcb/debug/test/DIGI/00012345/0005/00012345_00054321_2.digi',
                               '/lhcb/debug/test/DST/00012345/0005/00012345_00054321_4.dst',
                               '/lhcb/debug/test/ALLSTREAMS.DST/00012345/0005/00012345_00054321_5.allstreams.dst',
                               '/lhcb/debug/test/CORE/00012345/0005/00054321_core'],
                  'BookkeepingLFNs': ['/lhcb/certification/test/SIM/00012345/0005/00012345_00054321_1.sim',
                                      '/lhcb/certification/test/DIGI/00012345/0005/00012345_00054321_2.digi',
                                      '/lhcb/certification/test/DST/00012345/0005/00012345_00054321_4.dst',
                                      '/lhcb/certification/test/ALLSTREAMS.DST/00012345/0005/00012345_00054321_5.allstreams.dst'],
                  'ProductionOutputData': ['/lhcb/certification/test/SIM/00012345/0005/00012345_00054321_1.sim',
                                           '/lhcb/certification/test/DIGI/00012345/0005/00012345_00054321_2.digi',
                                           '/lhcb/certification/test/DST/00012345/0005/00012345_00054321_4.dst',
                                           '/lhcb/certification/test/ALLSTREAMS.DST/00012345/0005/00012345_00054321_5.allstreams.dst']},
                { 'LogTargetPath': ['/lhcb/certification/test/LOG/00012345/0005/00012345_00054321.tar'],
                  'LogFilePath': ['/lhcb/certification/test/LOG/00012345/0005/00054321'],
                  'DebugLFNs': ['/lhcb/debug/test/SIM/00012345/0005/00012345_00054321_1.sim',
                               '/lhcb/debug/test/DIGI/00012345/0005/00012345_00054321_2.digi',
                               '/lhcb/debug/test/DST/00012345/0005/00012345_00054321_4.dst',
                               '/lhcb/debug/test/ALLSTREAMS.DST/00012345/0005/00012345_00054321_5.allstreams.dst',
                               '/lhcb/debug/test/CORE/00012345/0005/00054321_core'],
                  'BookkeepingLFNs': ['/lhcb/certification/test/SIM/00012345/0005/00012345_00054321_1.sim',
                                      '/lhcb/certification/test/DIGI/00012345/0005/00012345_00054321_2.digi',
                                      '/lhcb/certification/test/DST/00012345/0005/00012345_00054321_4.dst',
                                      '/lhcb/certification/test/ALLSTREAMS.DST/00012345/0005/00012345_00054321_5.allstreams.dst'],
                  'ProductionOutputData': ['/lhcb/certification/test/DST/00012345/0005/00012345_00054321_4.dst']},
                { 'LogTargetPath': ['/lhcb/certification/test/LOG/00012345/0005/00012345_00054321.tar'],
                  'LogFilePath': ['/lhcb/certification/test/LOG/00012345/0005/00054321'],
                  'DebugLFNs': ['/lhcb/debug/test/SIM/00012345/0005/00012345_00054321_1.sim',
                               '/lhcb/debug/test/DIGI/00012345/0005/00012345_00054321_2.digi',
                               '/lhcb/debug/test/DST/00012345/0005/00012345_00054321_4.dst',
                               '/lhcb/debug/test/ALLSTREAMS.DST/00012345/0005/00012345_00054321_5.allstreams.dst',
                               '/lhcb/debug/test/CORE/00012345/0005/00054321_core'],
                  'BookkeepingLFNs': ['/lhcb/certification/test/SIM/00012345/0005/00012345_00054321_1.sim',
                                      '/lhcb/certification/test/DIGI/00012345/0005/00012345_00054321_2.digi',
                                      '/lhcb/certification/test/DST/00012345/0005/00012345_00054321_4.dst',
                                      '/lhcb/certification/test/ALLSTREAMS.DST/00012345/0005/00012345_00054321_5.allstreams.dst'],
                  'ProductionOutputData': ['/lhcb/certification/test/DST/00012345/0005/00012345_00054321_4.dst']},
                { 'LogTargetPath': ['/lhcb/certification/test/LOG/00012345/0005/00012345_00054321.tar'],
                  'LogFilePath': ['/lhcb/certification/test/LOG/00012345/0005/00054321'],
                  'DebugLFNs': ['/lhcb/debug/test/SIM/00012345/0005/00012345_00054321_1.sim',
                               '/lhcb/debug/test/DIGI/00012345/0005/00012345_00054321_2.digi',
                               '/lhcb/debug/test/DST/00012345/0005/00012345_00054321_4.dst',
                               '/lhcb/debug/test/ALLSTREAMS.DST/00012345/0005/00012345_00054321_5.allstreams.dst',
                               '/lhcb/debug/test/CORE/00012345/0005/00054321_core'],
                  'BookkeepingLFNs': ['/lhcb/certification/test/SIM/00012345/0005/00012345_00054321_1.sim',
                                      '/lhcb/certification/test/DIGI/00012345/0005/00012345_00054321_2.digi',
                                      '/lhcb/certification/test/DST/00012345/0005/00012345_00054321_4.dst',
                                      '/lhcb/certification/test/ALLSTREAMS.DST/00012345/0005/00012345_00054321_5.allstreams.dst'],
                  'ProductionOutputData': ['/lhcb/certification/test/DIGI/00012345/0005/00012345_00054321_2.digi',
                                           '/lhcb/certification/test/DST/00012345/0005/00012345_00054321_4.dst']},
                { 'LogTargetPath': ['/lhcb/certification/test/LOG/00012345/0005/00012345_00054321.tar'],
                  'LogFilePath': ['/lhcb/certification/test/LOG/00012345/0005/00054321'],
                  'DebugLFNs': ['/lhcb/debug/test/SIM/00012345/0005/00012345_00054321_1.sim',
                               '/lhcb/debug/test/DIGI/00012345/0005/00012345_00054321_2.digi',
                               '/lhcb/debug/test/DST/00012345/0005/00012345_00054321_4.dst',
                               '/lhcb/debug/test/ALLSTREAMS.DST/00012345/0005/00012345_00054321_5.allstreams.dst',
                               '/lhcb/debug/test/CORE/00012345/0005/00054321_core'],
                  'BookkeepingLFNs': ['/lhcb/certification/test/SIM/00012345/0005/00012345_00054321_1.sim',
                                      '/lhcb/certification/test/DIGI/00012345/0005/00012345_00054321_2.digi',
                                      '/lhcb/certification/test/DST/00012345/0005/00012345_00054321_4.dst',
                                      '/lhcb/certification/test/ALLSTREAMS.DST/00012345/0005/00012345_00054321_5.allstreams.dst'],
                  'ProductionOutputData': ['/lhcb/certification/test/ALLSTREAMS.DST/00012345/0005/00012345_00054321_5.allstreams.dst']},
                { 'LogTargetPath': ['/lhcb/certification/test/LOG/00012345/0005/00012345_00054321.tar'],
                  'LogFilePath': ['/lhcb/certification/test/LOG/00012345/0005/00054321'],
                  'DebugLFNs': ['/lhcb/debug/test/SIM/00012345/0005/00012345_00054321_1.sim',
                               '/lhcb/debug/test/DIGI/00012345/0005/00012345_00054321_2.digi',
                               '/lhcb/debug/test/DST/00012345/0005/00012345_00054321_4.dst',
                               '/lhcb/debug/test/ALLSTREAMS.DST/00012345/0005/00012345_00054321_5.allstreams.dst',
                               '/lhcb/debug/test/CORE/00012345/0005/00054321_core'],
                  'BookkeepingLFNs': ['/lhcb/certification/test/SIM/00012345/0005/00012345_00054321_1.sim',
                                      '/lhcb/certification/test/DIGI/00012345/0005/00012345_00054321_2.digi',
                                      '/lhcb/certification/test/DST/00012345/0005/00012345_00054321_4.dst',
                                      '/lhcb/certification/test/ALLSTREAMS.DST/00012345/0005/00012345_00054321_5.allstreams.dst'],
                  'ProductionOutputData': ['/lhcb/certification/test/DST/00012345/0005/00012345_00054321_4.dst',
                                           '/lhcb/certification/test/ALLSTREAMS.DST/00012345/0005/00012345_00054321_5.allstreams.dst']},
                ]

    outputDataFileMasks = ( '', 'dst', 'DST', ['digi', 'dst'], 'ALLSTREAMS.DST', ['dst', 'allstreams.dst'] )

    for outputDataFileMask, resL in itertools.izip( outputDataFileMasks, reslist ):
      paramDict['outputDataFileMask'] = outputDataFileMask

      res = constructProductionLFNs( paramDict )

      self.assert_( res['OK'] )
      self.assertEqual( res['Value'], resL )


  #################################################

  def test__makeProductionLFN( self ):

    JOB_ID = '00054321'
    LFN_ROOT = '/lhcb/MC/MC10'
    filetuple = ( ( '00012345_00054321_1.sim', 'sim' ), ( '00012345_00054321_1.SIM', 'SIM' ) )
    mode = ( 'certification', 'Certification' )
    prodstring = '00012345'

    for ft, m in itertools.izip( filetuple, mode ):
      res = _makeProductionLFN( JOB_ID, LFN_ROOT, ft, m, prodstring )
      self.assertEqual( res, '/lhcb/MC/MC10/SIM/00012345/0005/00012345_00054321_1.sim' )

  #################################################

  def test__applymask( self ):

    dtl = [( '00012345_00054321_1.sim', 'sim' ),
           ( '00012345_00054321_4.dst', 'dst' ),
           ( '00012345_00054321_2.digi', 'digi' ),
           ( '00012345_00054321_5.AllStreams.dst', 'ALLSTREAMS.DST' )]

    wfMask = ( '', 'dst', 'ALLSTREAMS.DST', ['dst', 'digi'], ['DIGI', 'allstreams.dst'] )

    dtlM = ( [
               ( '00012345_00054321_1.sim', 'sim' ),
               ( '00012345_00054321_4.dst', 'dst' ),
               ( '00012345_00054321_2.digi', 'digi' ),
               ( '00012345_00054321_5.AllStreams.dst', 'ALLSTREAMS.DST' )
              ],
              [( '00012345_00054321_4.dst', 'dst' )],
              [( '00012345_00054321_5.AllStreams.dst', 'ALLSTREAMS.DST' )],
              [
               ( '00012345_00054321_4.dst', 'dst' ),
               ( '00012345_00054321_2.digi', 'digi' )
              ],
              [
               ( '00012345_00054321_2.digi', 'digi' ),
               ( '00012345_00054321_5.AllStreams.dst', 'ALLSTREAMS.DST' )
              ]
            )

    for mask, res in itertools.izip( wfMask, dtlM ):
      r = _applyMask( mask, dtl )

      self.assertEqual( r, res )

#############################################################################
# Test Suite run 
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( UtilitiesTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProductionDataSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
