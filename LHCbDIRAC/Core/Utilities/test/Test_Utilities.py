__RCSID__ = "$Id:  $"

import unittest, itertools, os, datetime

from mock import Mock

from LHCbDIRAC.Core.Utilities.ProductionData import _makeProductionLFN, constructProductionLFNs, _getLFNRoot, _applyMask, getLogPath, constructUserLFNs
from LHCbDIRAC.Core.Utilities.InputDataResolution import InputDataResolution
from LHCbDIRAC.Core.Utilities.ProdConf import ProdConf
from LHCbDIRAC.Core.Utilities.ProductionEnvironment import getProjectCommand
from LHCbDIRAC.Core.Utilities.GangaDataFile import GangaDataFile
from LHCbDIRAC.Core.Utilities.NagiosConnector import NagiosConnector


class UtilitiesTestCase( unittest.TestCase ):
  """ Base class for the Utilities test cases
  """
  def setUp( self ):

    self.bkClientMock = Mock()
    self.bkClientMock.getFileTypes.return_value = {'OK': True,
                                                   'Value': {'TotalRecords': 48, 'ParameterNames': ['FileTypes'],
                                                             'Records': [['SDST'], ['PID.MDST'], ['GEN'], ['DST'],
                                                                         ['LEPTONIC.MDST'], ['EW.DST'], ['CHARM.DST']]}}
    self.bkClientMock.getFileTypeVersion.return_value = {'OK': True,
                                                         'Value': {'lfn1': 'ROOT',
                                                                   'lfn2': 'MDF'}}

    self.IDR = InputDataResolution( {}, self.bkClientMock )

    self.pc = ProdConf()

  def tearDown( self ):
    for fileProd in ['prodConf.py', 'data.py']:
      try:
        os.remove( fileProd )
      except OSError:
        continue


#################################################

class ProductionEnvironmentSuccess( UtilitiesTestCase ):

  def test_getProjectCommand( self ):
    expected = [
                [ ['AppConfig.v110'], '/buf/setupProject.sh --debug --use="AppConfig v110"  Gauss v40r0 --runtime-project Brunel v2r1  bof' ],
                [ ['AppConfig.v110', 'pippo.v1'], '/buf/setupProject.sh --debug --use="AppConfig v110"  --use="pippo v1"  Gauss v40r0 --runtime-project Brunel v2r1  bof' ],
                [ ['AppConfig.v110', 'pippo.v1', 'ProdConf'], '/buf/setupProject.sh --debug --use="AppConfig v110"  --use="pippo v1"  --use="ProdConf"  Gauss v40r0 --runtime-project Brunel v2r1  bof' ],
                ]

    for ep in expected:
      ret = getProjectCommand( '/buf/setupProject.sh', 'Gauss', 'v40r0', ep[0],
                              'DIRAC.Test.ch', 'Brunel', 'v2r1', 'bof' )
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

class GangaDataFileSuccess( UtilitiesTestCase ):

  def test_generateGangaDataFile( self ):

    gdf = GangaDataFile()

    res = gdf.generateDataFile( ['foo', 'bar'], 'ROOT' )
    root = '\
\n#new method\
\nfrom GaudiConf import IOExtension\
\nIOExtension("ROOT").inputFiles([\
\n    "LFN:foo",\
\n    "LFN:bar"\
\n], clear=True)\
\n\
\nfrom Gaudi.Configuration import FileCatalog\
\nFileCatalog().Catalogs = ["xmlcatalog_file:pool_xml_catalog.xml"]\
'
    self.assertEqual( res, root )

    res = gdf.generateDataFile( ['foo', 'bar'], 'Pool' )
    pool = '\
\ntry:\
\n    #new method\
\n    from GaudiConf import IOExtension\
\n    IOExtension("POOL").inputFiles([\
\n        "LFN:foo",\
\n        "LFN:bar"\
\n    ], clear=True)\
\nexcept ImportError:\
\n    #Use previous method\
\n    from Gaudi.Configuration import EventSelector\
\n    EventSelector().Input=[\
\n        "DATAFILE=\'LFN:foo\' TYP=\'POOL_ROOTTREE\' OPT=\'READ\'",\
\n        "DATAFILE=\'LFN:bar\' TYP=\'POOL_ROOTTREE\' OPT=\'READ\'"\
\n    ]\
\n\
\nfrom Gaudi.Configuration import FileCatalog\
\nFileCatalog().Catalogs = ["xmlcatalog_file:pool_xml_catalog.xml"]\
'

    self.assertEqual( res, pool )

    res = gdf.generateDataFile( ['foo', 'bar'] )
    nothing = '\
\ntry:\
\n    #new method\
\n    from GaudiConf import IOExtension\
\n    IOExtension().inputFiles([\
\n        "LFN:foo",\
\n        "LFN:bar"\
\n    ], clear=True)\
\nexcept ImportError:\
\n    #Use previous method\
\n    from Gaudi.Configuration import EventSelector\
\n    EventSelector().Input=[\
\n        "DATAFILE=\'LFN:foo\' TYP=\'POOL_ROOTTREE\' OPT=\'READ\'",\
\n        "DATAFILE=\'LFN:bar\' TYP=\'POOL_ROOTTREE\' OPT=\'READ\'"\
\n    ]\
\n\
\nfrom Gaudi.Configuration import FileCatalog\
\nFileCatalog().Catalogs = ["xmlcatalog_file:pool_xml_catalog.xml"]\
'
    self.assertEqual( res, nothing )

#################################################

class GangaDataFileFailure( UtilitiesTestCase ):

  def test_generateDataFile( self ):

    gdf = GangaDataFile()
    self.assertRaises( TypeError, gdf.generateDataFile, ( '' ) )
    self.assertRaises( ValueError, gdf.generateDataFile, ( [] ) )

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

      resWithBkk = constructProductionLFNs( paramDict, self.bkClientMock, quick = False )

      self.assert_( resWithBkk['OK'] )
      self.assertEqual( resWithBkk['Value'], resL )

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

    resWithBkk = getLogPath( wkf_commons, self.bkClientMock, quick = False )

    self.assertEqual( resWithBkk, {'OK': True,
                           'Value': {'LogTargetPath': ['/lhcb/LHCb/Collision11/LOG/00012345/0000/00012345_00000001.tar'],
                                     'LogFilePath': ['/lhcb/LHCb/Collision11/LOG/00012345/0000/00000001']}} )

    res = getLogPath( wkf_commons, self.bkClientMock )

    self.assertEqual( res, {'OK': True,
                           'Value': {'LogTargetPath': ['/lhcb/LHCb/Collision11/LOG/00012345/0000/00012345_00000001.tar'],
                                     'LogFilePath': ['/lhcb/LHCb/Collision11/LOG/00012345/0000/00000001']}} )


  def test__getLFNRoot( self ):
    res = _getLFNRoot( '/lhcb/data/CCRC08/00009909/DST/0000/00009909_00003456_2.dst', 'MC12', bkClient = self.bkClientMock )
    self.assertEqual( res, '/lhcb/data/CCRC08/00009909' )

    res = _getLFNRoot( '/lhcb/data/CCRC08/00009909/DST/0000/00009909_00003456_2.dst', 'MC12', bkClient = self.bkClientMock, quick = True )
    self.assertEqual( res, '/lhcb/data/CCRC08/00009909' )

  def test_constructUserLFNs( self ):
    timeTup = datetime.date.today().timetuple()
    yearMonth = '%s_%s' % ( timeTup[0], str( timeTup[1] ).zfill( 2 ) )

    res = constructUserLFNs( 123, 'fstagni', ['pippo.txt', 'pluto.txt'] )
    self.assertEqual( sorted ( res ), sorted ( ['/lhcb/user/f/fstagni/' + yearMonth + '/0/123/pluto.txt',
                                                '/lhcb/user/f/fstagni/' + yearMonth + '/0/123/pippo.txt'] ) )

    res = constructUserLFNs( 123, 'fstagni', ['pippo.txt', 'pluto.txt'], 'my/Output/Path' )
    self.assertEqual( sorted ( res ), sorted ( ['/lhcb/user/f/fstagni/my/Output/Path/' + yearMonth + '/0/123/pluto.txt',
                                                '/lhcb/user/f/fstagni/my/Output/Path/' + yearMonth + '/0/123/pippo.txt'] ) )

    res = constructUserLFNs( 123, 'fstagni', ['pippo.txt', 'pluto.txt'], prependString = 'pre/pend' )
    self.assertEqual( sorted( res ), sorted( ['/lhcb/user/f/fstagni/' + yearMonth + '/123_pre/pend_pippo.txt',
                                              '/lhcb/user/f/fstagni/' + yearMonth + '/123_pre/pend_pluto.txt'] ) )

    res = constructUserLFNs( 123, 'fstagni', ['pippo.txt', 'pluto.txt'], 'my/Output/Path', 'pre/pend' )
    self.assertEqual( sorted( res ), sorted( ['/lhcb/user/f/fstagni/my/Output/Path/' + yearMonth + '/123_pre/pend_pippo.txt',
                                              '/lhcb/user/f/fstagni/my/Output/Path/' + yearMonth + '/123_pre/pend_pluto.txt'] ) )

class InputDataResolutionSuccess( UtilitiesTestCase ):

  def test__addPfnType( self ):

    res = self.IDR._addPfnType( {'lfn1':{'mdata':'mdata1'},
                                 'lfn2': {'mdata':'mdata2'}
                                 } )
    self.assertEqual( res, {'OK': True,
                            'Value': { 'lfn1':{'pfntype':'ROOT', 'mdata':'mdata1'},
                                      'lfn2':{'pfntype':'MDF', 'mdata':'mdata2'} }
                            } )

    self.bkClientMock.getFileTypeVersion.return_value = {'OK': True,
                                                         'Value': {}}

    res = self.IDR._addPfnType( {'lfn1':{'mdata':'mdata1'},
                                 'lfn2': {'mdata':'mdata2'}} )
    self.assertEqual( res, {'OK': True,
                            'Value': { 'lfn1':{'pfntype':'ROOT', 'mdata':'mdata1'},
                                      'lfn2':{'pfntype':'ROOT', 'mdata':'mdata2'} }
                            } )

    self.bkClientMock.getFileTypeVersion.return_value = {'OK': True,
                                                          'Value': {'lfn1': 'ROOT'}}

    res = self.IDR._addPfnType( {'lfn1':{'mdata':'mdata1'},
                                 'lfn2': {'mdata':'mdata2'},
                                 'lfn3': {'mdata':'mdata3'}} )
    self.assertEqual( res, {'OK': True,
                            'Value': { 'lfn1':{'pfntype':'ROOT', 'mdata':'mdata1'},
                                       'lfn2':{'pfntype':'ROOT', 'mdata':'mdata2'},
                                       'lfn3':{'pfntype':'ROOT', 'mdata':'mdata3'} }
                            } )

class NagiosConnectorSuccess( UtilitiesTestCase ):

  def test_readConfig( self ):

    nagConn = NagiosConnector()
    nagConn.readConfig()
    self.assert_( nagConn.config['MsgPort'] )
    self.assertEqual( nagConn.config['MsgBroker'], None )
    self.assertEqual( nagConn.config['MsgQueue'], None )

#  def test_failedConnection( self ):
#
#    nagConn = NagiosConnector()
#    nagConn.readConfig()
#    nagConn.config['NagiosName'] = 'lhcb-Dirac.Unittest'
#    nagConn.useDebugMessage()
#    self.assertEquals( nagConn.initializeConnection(),
#                       S_OK( 'Connection to Broker established' ),
#                       'Connection not correctly initialized' )
#    self.assertEquals( nagConn.sendMessage(),
#                       S_OK('Message sent to Broker.'),
#                       'Sending unsuccessful!' )


#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( UtilitiesTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProductionDataSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProdConfSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProductionEnvironmentSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( InputDataResolutionSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( NagiosConnectorSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
