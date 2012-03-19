__RCSID__ = "$Id:  $"

import unittest, itertools, os

from mock import Mock

from LHCbDIRAC.Core.Utilities.ProductionData import constructProductionLFNs, _makeProductionLFN, _applyMask, getLogPath
from LHCbDIRAC.Core.Utilities.InputDataResolution import InputDataResolution
from LHCbDIRAC.Core.Utilities.ProdConf import ProdConf
from LHCbDIRAC.Core.Utilities.ProductionEnvironment import getProjectCommand
from LHCbDIRAC.Core.Utilities.CombinedSoftwareInstallation import _getAreas, _getApp

class UtilitiesTestCase( unittest.TestCase ):
  """ Base class for the Utilities test cases
  """
  def setUp( self ):

    self.bkClientMock = Mock()
    self.bkClientMock.getFileTypes.return_value = {'OK': True, 'rpcStub': ( ( 'Bookkeeping/BookkeepingManager',
                                                                            {'skipCACheck': False, 'delegatedGroup': 'diracAdmin', } ),
                                                                           'getFileTypes', ( {'': ''}, ) ),
                                                   'Value': {'TotalRecords': 48, 'ParameterNames': ['FileTypes'],
                                                             'Records': [['SDST'], ['PID.MDST'], ['GEN'],
                                                                         ['LEPTONIC.MDST'], ['EW.DST'], ['CHARM.DST']]}}
    self.bkClientMock.getTypeVersion.return_value = {'OK': True, 'rpcStub': ( ( 'Bookkeeping/BookkeepingManager',
                                                                            {'skipCACheck': False, 'delegatedGroup': 'diracAdmin', } ),
                                                                           'getFileTypes', ( {'': ''}, ) ),
                                                     'Value': {'lfn1':'ROOT', 'lfn2':'MDF'}}

    self.IDR = InputDataResolution( {}, self.bkClientMock )

    self.pc = ProdConf()

  def tearDown( self ):
    for fileProd in ['prodConf.py']:
      try:
        os.remove( fileProd )
      except OSError:
        continue


#################################################

class ProductionEnvironmentSuccess( UtilitiesTestCase ):

  def test_getProjectCommand( self ):
    expected = [
                [ ['AppConfig.v110'], '/buf/setupProject.sh --debug --use="AppConfig v110"  --tag_add=Pythia Gauss v40r0 --runtime-project Brunel v2r1  bof' ],
                [ ['AppConfig.v110', 'pippo.v1'], '/buf/setupProject.sh --debug --use="AppConfig v110"  --use="pippo v1"  --tag_add=Pythia Gauss v40r0 --runtime-project Brunel v2r1  bof' ],
                [ ['AppConfig.v110', 'pippo.v1', 'ProdConf'], '/buf/setupProject.sh --debug --use="AppConfig v110"  --use="pippo v1"  --use="ProdConf"  --tag_add=Pythia Gauss v40r0 --runtime-project Brunel v2r1  bof' ],
                ]

    for ep in expected:
      ret = getProjectCommand( '/buf/setupProject.sh', 'Gauss', 'v40r0', ep[0],
                              'Pythia', 'DIRAC.Test.ch', 'Brunel', 'v2r1', 'bof' )
      self.assertEqual( ret['Value'], ep[1] )


#################################################

class ProdConfSuccess( UtilitiesTestCase ):

  def test__buildOptions( self ):
    ret = self.pc._buildOptions( {'Application':'DaVinci', 'InputFiles':['foo', 'bar']} )
    self.assertEquals( ret, {'Application':'DaVinci', 'InputFiles':['foo', 'bar']} )

    self.pc.whatsIn = {'AppVersion': 'v30r0'}
    ret = self.pc._buildOptions( {'Application':'DaVinci', 'InputFiles':['foo', 'bar']} )
    self.assertEquals( ret, {'Application':'DaVinci', 'InputFiles':['foo', 'bar'], 'AppVersion': 'v30r0'} )

    ret = self.pc._buildOptions( {'AppVersion':'v31r0', 'InputFiles':['foo', 'bar']} )
    self.assertEquals( ret, {'InputFiles':['foo', 'bar'], 'AppVersion': 'v31r0'} )

  def test__getOptionsString( self ):
    ret = self.pc._getOptionsString( {'Application':'DaVinci', 'InputFiles':['foo', 'bar']} )
    self.assertEquals( ret, "from ProdConf import ProdConf\n\nProdConf(\n  Application='DaVinci',\n  InputFiles=['foo', 'bar'],\n)" )

  def test_complete( self ):
    try:
      os.remove( 'prodConf.py' )
    except OSError:
      pass
    pc1 = ProdConf()
    self.assertEquals( pc1.whatsIn, {} )
    pc1.putOptionsIn( {'Application':'DaVinci'} )
    self.assertEquals( pc1.whatsIn, {'Application':'DaVinci'} )

    try:
      os.remove( 'prodConf.py' )
    except OSError:
      pass
    pc1 = ProdConf()
    pc1.putOptionsIn( {'InputFiles':['foo', 'bar']} )
    self.assertEquals( pc1.whatsIn, {'InputFiles':['foo', 'bar']} )

    try:
      os.remove( 'prodConf.py' )
    except OSError:
      pass
    pc1 = ProdConf()
    pc1.putOptionsIn( {'RunNumber':12345} )
    self.assertEquals( pc1.whatsIn, {'RunNumber':12345} )
    fopen = open( 'prodConf.py', 'r' )
    fileString = fopen.read()
    fopen.close()
    string = "from ProdConf import ProdConf\n\nProdConf(\n  RunNumber=12345,\n)"
    self.assertEquals( string, fileString )

    try:
      os.remove( 'prodConf.py' )
    except OSError:
      pass
    pc1 = ProdConf()
    pc1.putOptionsIn( {'Application':'DaVinci', 'InputFiles':['foo', 'bar']} )
    self.assertEquals( pc1.whatsIn, {'Application':'DaVinci', 'InputFiles':['foo', 'bar']} )

    try:
      os.remove( 'prodConf.py' )
    except OSError:
      pass
    pc1 = ProdConf()
    pc1.putOptionsIn( {'Application':'DaVinci'} )
    self.assertEquals( pc1.whatsIn, {'Application':'DaVinci'} )
    pc1.putOptionsIn( {'Application':'LHCb'} )
    self.assertEquals( pc1.whatsIn, {'Application':'LHCb'} )

    try:
      os.remove( 'prodConf.py' )
    except OSError:
      pass
    pc1 = ProdConf()
    pc1.putOptionsIn( {'Application':'DaVinci', 'InputFiles':['foo', 'bar']} )
    self.assertEquals( pc1.whatsIn, {'Application':'DaVinci', 'InputFiles':['foo', 'bar']} )
    pc1.putOptionsIn( {'Application':'LHCb'} )
    self.assertEquals( pc1.whatsIn, {'Application':'LHCb', 'InputFiles':['foo', 'bar']} )
    pc1.putOptionsIn( {'RunNumber':12345} )
    self.assertEquals( pc1.whatsIn, {'Application':'LHCb', 'InputFiles':['foo', 'bar'], 'RunNumber':12345} )

    try:
      os.remove( 'prodConf.py' )
    except OSError:
      pass
    pc1 = ProdConf()

    pc1.putOptionsIn( {'Application':'DaVinci', 'InputFiles':['foo', 'bar'], 'AppVersion':'v30r0'} )
    self.assertEquals( pc1.whatsIn, {'Application':'DaVinci', 'InputFiles':['foo', 'bar'], 'AppVersion':'v30r0'} )
    fopen = open( 'prodConf.py', 'r' )
    fileString = fopen.read()
    fopen.close()
    string = "from ProdConf import ProdConf\n\nProdConf(\n  Application='DaVinci',\n  InputFiles=['foo', 'bar'],\n  AppVersion='v30r0',\n)"
    self.assertEquals( string, fileString )

    pc1.putOptionsIn( {'Application':'LHCb'} )
    self.assertEquals( pc1.whatsIn, {'Application':'LHCb', 'InputFiles':['foo', 'bar'], 'AppVersion':'v30r0'} )
    fopen = open( 'prodConf.py', 'r' )
    fileString = fopen.read()
    fopen.close()
    string = "from ProdConf import ProdConf\n\nProdConf(\n  Application='LHCb',\n  InputFiles=['foo', 'bar'],\n  AppVersion='v30r0',\n)"
    self.assertEquals( string, fileString )

    pc1.putOptionsIn( {'InputFiles':['pippo', 'pluto']} )
    self.assertEquals( pc1.whatsIn, {'Application': 'LHCb', 'InputFiles': ['pippo', 'pluto'], 'AppVersion': 'v30r0'} )
    fopen = open( 'prodConf.py', 'r' )
    fileString = fopen.read()
    fopen.close()
    string = "from ProdConf import ProdConf\n\nProdConf(\n  Application='LHCb',\n  InputFiles=['pippo', 'pluto'],\n  AppVersion='v30r0',\n)"
    self.assertEquals( string, fileString )

    pc1.putOptionsIn( {'InputFiles':[]} )
    self.assertEquals( pc1.whatsIn, {'Application': 'LHCb', 'InputFiles': [], 'AppVersion': 'v30r0'} )
    fopen = open( 'prodConf.py', 'r' )
    fileString = fopen.read()
    fopen.close()
    string = "from ProdConf import ProdConf\n\nProdConf(\n  Application='LHCb',\n  InputFiles=[],\n  AppVersion='v30r0',\n)"
    self.assertEquals( string, fileString )

    pc1.putOptionsIn( {'Application':'', 'RunNumber':12345} )
    self.assertEquals( pc1.whatsIn, {'Application': '', 'InputFiles': [], 'AppVersion': 'v30r0', 'RunNumber':12345} )
    fopen = open( 'prodConf.py', 'r' )
    fileString = fopen.read()
    fopen.close()
    string = "from ProdConf import ProdConf\n\nProdConf(\n  Application='',\n  InputFiles=[],\n  AppVersion='v30r0',\n  RunNumber=12345,\n)"
    self.assertEquals( string, fileString )


#################################################

class ProductionDataSuccess( UtilitiesTestCase ):

  #################################################

  def test_constructProductionLFNs( self ):

    # + test with InputData

    paramDict = { 'PRODUCTION_ID':'12345',
                  'JOB_ID':'54321',
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
                               '/lhcb/debug/test/ALLSTREAMS.DST/00012345/0005/00012345_00054321_5.AllStreams.dst',
                               '/lhcb/debug/test/CORE/00012345/0005/00054321_core'],
                  'BookkeepingLFNs': ['/lhcb/certification/test/SIM/00012345/0005/00012345_00054321_1.sim',
                                      '/lhcb/certification/test/DIGI/00012345/0005/00012345_00054321_2.digi',
                                      '/lhcb/certification/test/DST/00012345/0005/00012345_00054321_4.dst',
                                      '/lhcb/certification/test/ALLSTREAMS.DST/00012345/0005/00012345_00054321_5.AllStreams.dst'],
                  'ProductionOutputData': ['/lhcb/certification/test/SIM/00012345/0005/00012345_00054321_1.sim',
                                           '/lhcb/certification/test/DIGI/00012345/0005/00012345_00054321_2.digi',
                                           '/lhcb/certification/test/DST/00012345/0005/00012345_00054321_4.dst',
                                           '/lhcb/certification/test/ALLSTREAMS.DST/00012345/0005/00012345_00054321_5.AllStreams.dst']},
                { 'LogTargetPath': ['/lhcb/certification/test/LOG/00012345/0005/00012345_00054321.tar'],
                  'LogFilePath': ['/lhcb/certification/test/LOG/00012345/0005/00054321'],
                  'DebugLFNs': ['/lhcb/debug/test/SIM/00012345/0005/00012345_00054321_1.sim',
                               '/lhcb/debug/test/DIGI/00012345/0005/00012345_00054321_2.digi',
                               '/lhcb/debug/test/DST/00012345/0005/00012345_00054321_4.dst',
                               '/lhcb/debug/test/ALLSTREAMS.DST/00012345/0005/00012345_00054321_5.AllStreams.dst',
                               '/lhcb/debug/test/CORE/00012345/0005/00054321_core'],
                  'BookkeepingLFNs': ['/lhcb/certification/test/SIM/00012345/0005/00012345_00054321_1.sim',
                                      '/lhcb/certification/test/DIGI/00012345/0005/00012345_00054321_2.digi',
                                      '/lhcb/certification/test/DST/00012345/0005/00012345_00054321_4.dst',
                                      '/lhcb/certification/test/ALLSTREAMS.DST/00012345/0005/00012345_00054321_5.AllStreams.dst'],
                  'ProductionOutputData': ['/lhcb/certification/test/DST/00012345/0005/00012345_00054321_4.dst']},
                { 'LogTargetPath': ['/lhcb/certification/test/LOG/00012345/0005/00012345_00054321.tar'],
                  'LogFilePath': ['/lhcb/certification/test/LOG/00012345/0005/00054321'],
                  'DebugLFNs': ['/lhcb/debug/test/SIM/00012345/0005/00012345_00054321_1.sim',
                               '/lhcb/debug/test/DIGI/00012345/0005/00012345_00054321_2.digi',
                               '/lhcb/debug/test/DST/00012345/0005/00012345_00054321_4.dst',
                               '/lhcb/debug/test/ALLSTREAMS.DST/00012345/0005/00012345_00054321_5.AllStreams.dst',
                               '/lhcb/debug/test/CORE/00012345/0005/00054321_core'],
                  'BookkeepingLFNs': ['/lhcb/certification/test/SIM/00012345/0005/00012345_00054321_1.sim',
                                      '/lhcb/certification/test/DIGI/00012345/0005/00012345_00054321_2.digi',
                                      '/lhcb/certification/test/DST/00012345/0005/00012345_00054321_4.dst',
                                      '/lhcb/certification/test/ALLSTREAMS.DST/00012345/0005/00012345_00054321_5.AllStreams.dst'],
                  'ProductionOutputData': ['/lhcb/certification/test/DST/00012345/0005/00012345_00054321_4.dst']},
                { 'LogTargetPath': ['/lhcb/certification/test/LOG/00012345/0005/00012345_00054321.tar'],
                  'LogFilePath': ['/lhcb/certification/test/LOG/00012345/0005/00054321'],
                  'DebugLFNs': ['/lhcb/debug/test/SIM/00012345/0005/00012345_00054321_1.sim',
                               '/lhcb/debug/test/DIGI/00012345/0005/00012345_00054321_2.digi',
                               '/lhcb/debug/test/DST/00012345/0005/00012345_00054321_4.dst',
                               '/lhcb/debug/test/ALLSTREAMS.DST/00012345/0005/00012345_00054321_5.AllStreams.dst',
                               '/lhcb/debug/test/CORE/00012345/0005/00054321_core'],
                  'BookkeepingLFNs': ['/lhcb/certification/test/SIM/00012345/0005/00012345_00054321_1.sim',
                                      '/lhcb/certification/test/DIGI/00012345/0005/00012345_00054321_2.digi',
                                      '/lhcb/certification/test/DST/00012345/0005/00012345_00054321_4.dst',
                                      '/lhcb/certification/test/ALLSTREAMS.DST/00012345/0005/00012345_00054321_5.AllStreams.dst'],
                  'ProductionOutputData': ['/lhcb/certification/test/DIGI/00012345/0005/00012345_00054321_2.digi',
                                           '/lhcb/certification/test/DST/00012345/0005/00012345_00054321_4.dst']},
                { 'LogTargetPath': ['/lhcb/certification/test/LOG/00012345/0005/00012345_00054321.tar'],
                  'LogFilePath': ['/lhcb/certification/test/LOG/00012345/0005/00054321'],
                  'DebugLFNs': ['/lhcb/debug/test/SIM/00012345/0005/00012345_00054321_1.sim',
                               '/lhcb/debug/test/DIGI/00012345/0005/00012345_00054321_2.digi',
                               '/lhcb/debug/test/DST/00012345/0005/00012345_00054321_4.dst',
                               '/lhcb/debug/test/ALLSTREAMS.DST/00012345/0005/00012345_00054321_5.AllStreams.dst',
                               '/lhcb/debug/test/CORE/00012345/0005/00054321_core'],
                  'BookkeepingLFNs': ['/lhcb/certification/test/SIM/00012345/0005/00012345_00054321_1.sim',
                                      '/lhcb/certification/test/DIGI/00012345/0005/00012345_00054321_2.digi',
                                      '/lhcb/certification/test/DST/00012345/0005/00012345_00054321_4.dst',
                                      '/lhcb/certification/test/ALLSTREAMS.DST/00012345/0005/00012345_00054321_5.AllStreams.dst'],
                  'ProductionOutputData': ['/lhcb/certification/test/ALLSTREAMS.DST/00012345/0005/00012345_00054321_5.AllStreams.dst']},
                { 'LogTargetPath': ['/lhcb/certification/test/LOG/00012345/0005/00012345_00054321.tar'],
                  'LogFilePath': ['/lhcb/certification/test/LOG/00012345/0005/00054321'],
                  'DebugLFNs': ['/lhcb/debug/test/SIM/00012345/0005/00012345_00054321_1.sim',
                               '/lhcb/debug/test/DIGI/00012345/0005/00012345_00054321_2.digi',
                               '/lhcb/debug/test/DST/00012345/0005/00012345_00054321_4.dst',
                               '/lhcb/debug/test/ALLSTREAMS.DST/00012345/0005/00012345_00054321_5.AllStreams.dst',
                               '/lhcb/debug/test/CORE/00012345/0005/00054321_core'],
                  'BookkeepingLFNs': ['/lhcb/certification/test/SIM/00012345/0005/00012345_00054321_1.sim',
                                      '/lhcb/certification/test/DIGI/00012345/0005/00012345_00054321_2.digi',
                                      '/lhcb/certification/test/DST/00012345/0005/00012345_00054321_4.dst',
                                      '/lhcb/certification/test/ALLSTREAMS.DST/00012345/0005/00012345_00054321_5.AllStreams.dst'],
                  'ProductionOutputData': ['/lhcb/certification/test/DST/00012345/0005/00012345_00054321_4.dst',
                                           '/lhcb/certification/test/ALLSTREAMS.DST/00012345/0005/00012345_00054321_5.AllStreams.dst']},
                ]

    outputDataFileMasks = ( '', 'dst', 'DST', ['digi', 'dst'], 'ALLSTREAMS.DST', ['dst', 'allstreams.dst'] )

    for outputDataFileMask, resL in itertools.izip( outputDataFileMasks, reslist ):
      paramDict['outputDataFileMask'] = outputDataFileMask

      res = constructProductionLFNs( paramDict, self.bkClientMock )

      self.assert_( res['OK'] )
      self.assertEqual( res['Value'], resL )


  #################################################

  def test__makeProductionLFN( self ):

    JOB_ID = '00054321'
    LFN_ROOT = '/lhcb/MC/MC10'
    filetuple = ( ( '00012345_00054321_1.sim', 'sim' ), ( '00012345_00054321_1.sim', 'SIM' ) )
    prodstring = '00012345'

    for ft in filetuple:
      res = _makeProductionLFN( JOB_ID, LFN_ROOT, ft, prodstring )
      self.assertEqual( res, '/lhcb/MC/MC10/SIM/00012345/0005/00012345_00054321_1.sim' )

  #################################################

  def test__applymask( self ):

    dtl = [( '00012345_00054321_1.sim', 'sim' ),
           ( '00012345_00054321_4.dst', 'dst' ),
           ( '00012345_00054321_2.digi', 'digi' ),
           ( 'Brunel_00012345_00012345_1_Hist.root', 'hist' ),
           ( '00012345_00054321_5.AllStreams.dst', 'ALLSTREAMS.DST' )]

    wfMask = ( '', 'dst', 'ALLSTREAMS.DST', ['dst', 'digi'], ['DIGI', 'allstreams.dst'], 'hist', ['dst', 'hist'] )

    dtlM = ( [
               ( '00012345_00054321_1.sim', 'sim' ),
               ( '00012345_00054321_4.dst', 'dst' ),
               ( '00012345_00054321_2.digi', 'digi' ),
               ( 'Brunel_00012345_00012345_1_Hist.root', 'hist' ),
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
              ],
              [
               ( 'Brunel_00012345_00012345_1_Hist.root', 'hist' )
              ],
              [
               ( '00012345_00054321_4.dst', 'dst' ),
               ( 'Brunel_00012345_00012345_1_Hist.root', 'hist' )
              ]
            )

    for mask, res in itertools.izip( wfMask, dtlM ):
      r = _applyMask( mask, dtl )

      self.assertEqual( r, res )


  def test_getLogPath( self ):

    wkf_commons = {'PRODUCTION_ID':12345,
                   'JOB_ID':00001,
                   'configName':'LHCb',
                   'configVersion':'Collision11',
                   'JobType':'MCSimulation'}

    res = getLogPath( wkf_commons, self.bkClientMock )

    self.assertEqual( res, {'OK': True,
                           'Value': {'LogTargetPath': ['/lhcb/LHCb/Collision11/LOG/00012345/0000/00012345_00000001.tar'],
                                     'LogFilePath': ['/lhcb/LHCb/Collision11/LOG/00012345/0000/00000001']}} )


class InputDataResolutionSuccess( UtilitiesTestCase ):

  def test__addPfnType( self ):

    res = self.IDR._addPfnType( {'lfn1':{'mdata':'mdata1'}, 'lfn2': {'mdata':'mdata2'}} )
    self.assertEqual( res, { 'lfn1':{'pfntype':'ROOT', 'mdata':'mdata1'}, 'lfn2':{'pfntype':'MDF', 'mdata':'mdata2'} } )

    self.bkClientMock.getTypeVersion.return_value = {'OK': True,
                                                     'rpcStub': ( ( 'Bookkeeping/BookkeepingManager',
                                                                  {'skipCACheck': False} ),
                                                                 'getTypeVersion', ( ['/lhcb/user/g/gligorov/2011_12/27896/27896178/SwimBs2KK.dst'], ) ),
                                                     'Value': {}}

    res = self.IDR._addPfnType( {'lfn1':{'mdata':'mdata1'}, 'lfn2': {'mdata':'mdata2'}} )
    self.assertEqual( res, { 'lfn1':{'pfntype':'ROOT', 'mdata':'mdata1'}, 'lfn2':{'pfntype':'ROOT', 'mdata':'mdata2'} } )

class CombinedSoftwareInstallationSuccess( UtilitiesTestCase ):

  def test__getAreas( self ):

    l, s = _getAreas( 'local:shared' )
    self.assertEqual( l, 'local' )
    self.assertEqual( s, 'shared' )

  def test__getApp( self ):

    n, v = _getApp( ( 'name', ) )
    self.assertEqual( '', v )
    self.assertEqual( 'name', n )
    n, v = _getApp( ( 'name', 'version' ) )
    self.assertEqual( 'version', v )
    self.assertEqual( 'name', n )

#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( UtilitiesTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProductionDataSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProdConfSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProductionEnvironmentSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( InputDataResolutionSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( CombinedSoftwareInstallationSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
