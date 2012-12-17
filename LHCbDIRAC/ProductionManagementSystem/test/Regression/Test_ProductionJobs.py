import unittest, os, shutil
from DIRAC import gLogger
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction
from LHCbDIRAC.ProductionManagementSystem.Client.ProductionRequest import ProductionRequest

class ProductionJobTestCase( unittest.TestCase ):
  ''' Base class for the ProductionJob test cases
  '''
  def setUp( self ):

    gLogger.setLevel( 'DEBUG' )

    self.pr = ProductionRequest()

    self.diracProduction = DiracProduction()

  def tearDown( self ):
    for fileIn in os.listdir( '.' ):
      if 'Local' in fileIn:
        shutil.rmtree( fileIn )
      for fileToRemove in ['std.out', 'std.err']:
        try:
          os.remove( fileToRemove )
        except OSError:
          continue


class MCSuccess( ProductionJobTestCase ):
  def test_execute( self ):

    #From request 9149
    stepsInProd = [{'StepId': 124001, 'StepName': 'Sim05d', 'ApplicationName': 'Gauss', 'ApplicationVersion': 'v41r4',
                    'ExtraPackages': 'AppConfig.v3r151', 'ProcessingPass': 'Sim05d', 'Visible': 'Y', 'Usable': 'Yes',
                    'DDDB': 'MC2011-20120727', 'CONDDB': 'MC2011-20120727-vc-mu100', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/Gauss/Beam3500GeV-mu100-MC11-nu2.py;$DECFILESROOT/options/13102414.py;$LBPYTHIAROOT/options/Pythia.py;$APPCONFIGOPTS/Gauss/G4PL_LHEP_EmNoCuts.py',
                    'fileTypesIn':[],
                    'fileTypesOut':['SIM']},
                   {'StepId': 17895, 'StepName': 'Digi11', 'ApplicationName': 'Boole', 'ApplicationVersion': 'v23r1',
                    'ExtraPackages': 'AppConfig.v3r140', 'ProcessingPass': 'Digi11', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'MC2011-20120727', 'CONDDB': 'MC2011-20120727-vc-mu100', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/Boole/Default.py;$APPCONFIGOPTS/L0/L0TCK-0x0037.py',
                    'fileTypesIn':['SIM'],
                    'fileTypesOut':['DIGI']},
                   {'StepId': 17900, 'StepName': 'Trig0x40760037Flagged', 'ApplicationName': 'Moore', 'ApplicationVersion': 'v12r8g1',
                    'ExtraPackages': 'AppConfig.v3r140', 'ProcessingPass': 'Trig0x40760037Flagged', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'MC2011-20120727', 'CONDDB': 'MC2011-20120727-vc-mu100', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/Moore/MooreSimProduction.py;$APPCONFIGOPTS/Conditions/TCK-0x40760037.py;$APPCONFIGOPTS/Moore/DataType-2011.py',
                    'fileTypesIn':['DIGI'],
                    'fileTypesOut':['DIGI']},
                   {'StepId': 123817, 'StepName': 'Reco12a', 'ApplicationName': 'Brunel', 'ApplicationVersion': 'v41r1p1',
                    'ExtraPackages': 'AppConfig.v3r152', 'ProcessingPass': 'Reco12a', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'MC2011-20120727', 'CONDDB': 'MC2011-20120727-vc-mu100', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/Brunel/DataType-2011.py;$APPCONFIGOPTS/Brunel/MC-WithTruth.py;$APPCONFIGOPTS/Brunel/MC11a_dedxCorrection.py;$APPCONFIGOPTS/Brunel/TrigMuonRawEventFix.py',
                    'fileTypesIn':['DIGI'],
                    'fileTypesOut':['DST']},
                   {'StepId': 17901, 'StepName': 'Stripping17NoPrescalingFlagged', 'ApplicationName': 'DaVinci', 'ApplicationVersion': 'v29r1p1',
                    'ExtraPackages': 'AppConfig.v3r150', 'ProcessingPass': 'Stripping17NoPrescalingFlagged', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'MC2011-20120727', 'CONDDB': 'MC2011-20120727-vc-mu100', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/DaVinci/DV-Stripping17-Stripping-MC-NoPrescaling.py',
                    'fileTypesIn':['DST'],
                    'fileTypesOut':['ALLSTREAMS.DST']},
                   ]

    self.pr.events = 2
    #First create the production object
    prod = self.pr._buildProduction( 'MCSimulation', stepsInProd, '', 'Tier1_MC-DST', 0, 100,
                                     outputFileMask = 'ALLSTREAMS.DST' )
    #Then launch it
    res = self.diracProduction.launchProduction( prod, False, True, 0 )

    self.assertTrue( res['OK'] )

class RecoSuccess( ProductionJobTestCase ):
  def test_execute( self ):
    lfns = ['/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/114753/114753_0000000296.raw']
    #From request 8772
    stepsInProd = [{'StepId': 38427, 'StepName': 'Reco14', 'ApplicationName': 'Brunel', 'ApplicationVersion': 'v43r2p2',
                    'ExtraPackages': 'AppConfig.v3r149', 'ProcessingPass': 'Reco14', 'Visible': 'Y', 'Usable': 'Yes',
                    'DDDB': 'dddb-20120831', 'CONDDB': 'cond-20120831', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/Brunel/DataType-2012.py',
                    'fileTypesIn':['RAW'],
                    'fileTypesOut':['BRUNELHIST', 'FULL.DST']},
                   {'StepId': 38510, 'StepName': 'DataQuality-FULL', 'ApplicationName': 'DaVinci', 'ApplicationVersion': 'v32r2',
                    'ExtraPackages': 'AppConfig.v3r149', 'ProcessingPass': 'DataQuality-FULL', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'dddb-20120831', 'CONDDB': 'cond-20120831', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/DaVinci/DVMonitor-RealData.py;$APPCONFIGOPTS/DaVinci/DataType-2012.py;$APPCONFIGOPTS/DaVinci/DaVinci-InputType-SDST.py',
                    'fileTypesIn':['FULL.DST'],
                    'fileTypesOut':['DAVINCIHIST']}
                   ]

    self.pr.events = 25
    prod = self.pr._buildProduction( 'Reconstruction', stepsInProd, '', 'Tier1-BUFFER', 0, 100,
                                     inputDataPolicy = 'protocol', inputDataList = lfns )
    res = self.diracProduction.launchProduction( prod, False, True, 0 )
    self.assertTrue( res['OK'] )

class StrippSuccess( ProductionJobTestCase ):
  def test_execute( self ):
    lfns = ['/lhcb/LHCb/Collision12/FULL.DST/00020330/0004/00020330_00047632_1.full.dst']
    #From request 8891
    stepsInProd = [{'StepId': 123715, 'StepName': 'Stripping20', 'ApplicationName': 'DaVinci', 'ApplicationVersion': 'v32r2p1',
                    'ExtraPackages': 'AppConfig.v3r151', 'ProcessingPass': 'Stripping20', 'Visible': 'Y', 'Usable': 'Yes',
                    'DDDB': 'dddb-20120831', 'CONDDB': 'cond-20120929', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/DaVinci/DV-Stripping20-Stripping.py;$APPCONFIGOPTS/DaVinci/DataType-2012.py;$APPCONFIGOPTS/DaVinci/InputType-DST.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py',
                    'fileTypesIn':['SDST'],
                    'fileTypesOut':['BHADRON.MDST', 'BHADRONCOMPLETEEVENT.DST', 'CALIBRATION.DST', 'CHARM.MDST', 'CHARMCOMPLETEEVENT.DST', 'CHARMCONTROL.DST', 'CHARMTOBESWUM.DST', 'DIMUON.DST', 'EW.DST', 'LEPTONIC.MDST', 'MINIBIAS.DST', 'PID.MDST', 'RADIATIVE.DST', 'SEMILEPTONIC.DST']},
                   ]

    self.pr.events = 1500
    prod = self.pr._buildProduction( 'Stripping', stepsInProd, '', 'Tier1-BUFFER', 0, 100,
                                     inputDataPolicy = 'protocol', inputDataList = lfns )
    res = self.diracProduction.launchProduction( prod, False, True, 0 )
    self.assertTrue( res['OK'] )

class MergeSuccess( ProductionJobTestCase ):
  def test_execute( self ):
    lfns = ['/lhcb/LHCb/Collision12/FMDST/00020751/0000/00020751_00000037_1.fmdst',
            '/lhcb/LHCb/Collision12/FMDST/00020751/0000/00020751_00000036_1.fmdst']
    #From request 9085
    stepsInProd = [{'StepId': 17420, 'StepName': 'MergeFMDST', 'ApplicationName': 'LHCb', 'ApplicationVersion': 'v34r2',
                    'ExtraPackages': 'AppConfig.v3r134', 'ProcessingPass': 'MergeFMDST', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': '', 'CONDDB': '', 'DQTag': '', 'OptionsFormat': 'merge',
                    'OptionFiles': '$APPCONFIGOPTS/Merging/CopyDST.py',
                    'fileTypesIn':['FMDST'],
                    'fileTypesOut':['FMDST']},
                   ]

    prod = self.pr._buildProduction( 'Merge', stepsInProd, '', 'Tier1_MC-DST', 0, 100,
                                     inputDataPolicy = 'protocol', inputDataList = lfns )
    res = self.diracProduction.launchProduction( prod, False, True, 0 )
    self.assertTrue( res['OK'] )

class MergeMultStreamsSuccess( ProductionJobTestCase ):
  def test_execute( self ):
    lfns = ['/lhcb/LHCb/Collision12/DIMUON.DST/00021210/0001/00021210_00014056_1.Dimuon.dst',
            '/lhcb/LHCb/Collision12/DIMUON.DST/00021210/0001/00021210_00014127_1.Dimuon.dst']
    #From request 9085
    stepsInProd = [{'StepId': 54132, 'StepName': 'Merging', 'ApplicationName': 'DaVinci', 'ApplicationVersion': 'v32r2p1',
                    'ExtraPackages': 'AppConfig.v3r150', 'ProcessingPass': 'Merging', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'dddb-20120831', 'CONDDB': 'cond-20120831', 'DQTag': '', 'OptionsFormat': 'merge',
                    'OptionFiles': '$APPCONFIGOPTS/Merging/DV-Stripping-Merging.py',
                    'fileTypesIn':['BHADRON.MDST', 'BHADRONCOMPLETEEVENT.DST', 'CALIBRATION.DST', 'CHARM.MDST',
                                   'CHARMCOMPLETEEVENT.DST', 'CHARMCONTROL.DST', 'DIMUON.DST', 'EW.DST',
                                   'LEPTONIC.MDST', 'MINIBIAS.DST', 'PID.MDST', 'RADIATIVE.DST', 'SEMILEPTONIC.DST'],
                    'fileTypesOut':['BHADRON.MDST', 'BHADRONCOMPLETEEVENT.DST', 'CALIBRATION.DST', 'CHARM.MDST',
                                   'CHARMCOMPLETEEVENT.DST', 'CHARMCONTROL.DST', 'DIMUON.DST', 'EW.DST',
                                   'LEPTONIC.MDST', 'MINIBIAS.DST', 'PID.MDST', 'RADIATIVE.DST', 'SEMILEPTONIC.DST']},
                   ]

    prod = self.pr._buildProduction( 'Merge', stepsInProd, '', 'Tier1_MC-DST', 0, 100,
                                     inputDataPolicy = 'protocol', inputDataList = lfns )
    res = self.diracProduction.launchProduction( prod, False, True, 0 )
    self.assertTrue( res['OK'] )

class MergeMDFSuccess( ProductionJobTestCase ):
  def test_execute( self ):
    lfns = ['/lhcb/data/2011/RAW/EXPRESS/LHCb/COLLISION11/102360/102360_0000000031.raw',
            '/lhcb/data/2011/RAW/EXPRESS/LHCb/COLLISION11/97887/097887_0000000013.raw']
    #From request 9054
    stepsInProd = [{'StepId': 123897, 'StepName': 'MergeMDF', 'ApplicationName': 'MergeMDF', 'ApplicationVersion': '',
                    'ExtraPackages': '', 'ProcessingPass': 'Merging', 'Visible': 'Y', 'Usable': 'Yes',
                    'DDDB': '', 'CONDDB': '', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '',
                    'fileTypesIn':['RAW'],
                    'fileTypesOut':['RAW']},
                   ]
    self.pr.modulesList = ['MergeMDF', 'BookkeepingReport']
    prod = self.pr._buildProduction( 'Merge', stepsInProd, '', 'Tier1-BUFFER', 0, 100,
                                     inputDataPolicy = 'protocol', inputDataList = lfns )
    res = self.diracProduction.launchProduction( prod, False, True, 0 )
    self.assertTrue( res['OK'] )

#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ProductionJobTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MCSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( RecoSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( StrippSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MergeSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
