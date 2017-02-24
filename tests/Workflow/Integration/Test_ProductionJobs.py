#!/usr/bin/env python
""" "Integration" production jobs. StepIDs are taken from REAL productions that ran "recently"
"""

#pylint: disable=line-too-long,protected-access,missing-docstring,invalid-name,wrong-import-position

import unittest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.tests.Utilities.utils import find_all

from tests.Utilities.IntegrationTest import IntegrationTest

from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction
from LHCbDIRAC.ProductionManagementSystem.Client.ProductionRequest import ProductionRequest


class ProductionJobTestCase( IntegrationTest ):
  """ Base class for the ProductionJob test cases
  """
  def setUp( self ):
    super( ProductionJobTestCase, self ).setUp()

    self.pr = ProductionRequest()
    self.diracProduction = DiracProduction()


class MCSuccess( ProductionJobTestCase ):
  def test_Integration_Production( self ):

    # From request 24950
    stepsInProd = [{'StepId': 126921, 'StepName': 'Sim08f', 'ApplicationName': 'Gauss', 'ApplicationVersion': 'v47r0p1',
                    'ExtraPackages': 'AppConfig.v3r200;DecFiles.v27r33', 'ProcessingPass': 'Sim08f', 'Visible': 'Y', 'Usable': 'Yes',
                    'DDDB': 'dddb-20140729', 'CONDDB': 'sim-20140730-vc-mu100', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/Gauss/Beam6500GeV-mu100-nu1.6.py;$APPCONFIGOPTS/Gauss/DataType-2015.py;$APPCONFIGOPTS/Gauss/RICHRandomHits.py;$DECFILESROOT/options/30000000.py;$LBPYTHIA8ROOT/options/Pythia8.py;$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py',
                    'isMulticore': 'N', 'SystemConfig': 'x86_64-slc6-gcc48-opt', 'mcTCK': '', 'ExtraOptions': '',
                    'fileTypesIn':[],
                    'fileTypesOut':['SIM']},
                   {'StepId': 126995, 'StepName': 'Digi13', 'ApplicationName': 'Boole', 'ApplicationVersion': 'v29r1p1',
                    'ExtraPackages': 'AppConfig.v3r203', 'ProcessingPass': 'Digi13', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'dddb-20140729', 'CONDDB': 'sim-20140730-vc-mu100', 'DQTag': '', 'OptionsFormat': '',
                    'isMulticore': 'N', 'SystemConfig': '', 'mcTCK': '', 'ExtraOptions': '',
                    'OptionFiles': '$APPCONFIGOPTS/Boole/Default.py;$APPCONFIGOPTS/Boole/DataType-2012.py;$APPCONFIGOPTS/Boole/Boole-SiG4EnergyDeposit.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py',
                    'fileTypesIn':['SIM'],
                    'fileTypesOut':['DIGI']},
                   {'StepId': 126926, 'StepName': 'L0Trig0x0033', 'ApplicationName': 'Moore', 'ApplicationVersion': 'v22r1p1',
                    'ExtraPackages': 'AppConfig.v3r200', 'ProcessingPass': 'L0Trig0x0033', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'dddb-20140729', 'CONDDB': 'sim-20140730-vc-mu100', 'DQTag': '', 'OptionsFormat': 'l0App',
                    'isMulticore': 'N', 'SystemConfig': '', 'mcTCK': '', 'ExtraOptions': '',
                    'OptionFiles': '$APPCONFIGOPTS/L0App/L0AppSimProduction.py;$APPCONFIGOPTS/L0App/L0AppTCK-0x0033.py;$APPCONFIGOPTS/L0App/DataType-2012.py',
                    'fileTypesIn':['DIGI'],
                    'fileTypesOut':['DIGI']},
                   {'StepId': 126927, 'StepName': 'Trig0x40b10033', 'ApplicationName': 'Moore', 'ApplicationVersion': 'v22r1p1',
                    'ExtraPackages': 'AppConfig.v3r201', 'ProcessingPass': 'Trig0x40b10033', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'dddb-20140729', 'CONDDB': 'sim-20140730-vc-mu100', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/Moore/MooreSimProductionForSeparateL0AppStep2015.py;$APPCONFIGOPTS/Conditions/TCK-0x40b10033.py;$APPCONFIGOPTS/Moore/DataType-2012.py',
                    'isMulticore': 'N', 'SystemConfig': '', 'mcTCK': '', 'ExtraOptions': '',
                    'fileTypesIn':['DIGI'],
                    'fileTypesOut':['DIGI']},
                   {'StepId': 127060, 'StepName': 'Reco15DEV', 'ApplicationName': 'Brunel', 'ApplicationVersion': 'v47r2p1',
                    'ExtraPackages': 'AppConfig.v3r203', 'ProcessingPass': 'Reco15DEV', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'dddb-20140729', 'CONDDB': 'sim-20140730-vc-mu100', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/Brunel/DataType-2015.py;$APPCONFIGOPTS/Brunel/MC-WithTruth.py;$APPCONFIGOPTS/Brunel/ldst.py;$APPCONFIGOPTS/Brunel/patchUpgrade1.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py',
                    'isMulticore': 'N', 'SystemConfig': '', 'mcTCK': '', 'ExtraOptions': '',
                    'fileTypesIn':['DIGI'],
                    'fileTypesOut':['LDST']}]

    # First create the production object
    prod = self.pr._buildProduction( prodType = 'MCSimulation', stepsInProd = stepsInProd, outputSE = {'LDST': 'Tier1_MC-DST'},
                                     priority = 0, cpu = 100, outputFileMask = 'LDST' )
    prod.LHCbJob.setInputSandbox( find_all( 'pilot.cfg', '.' )[0] )
    prod.LHCbJob.setConfigArgs( 'pilot.cfg' )
    prod.setParameter( 'numberOfEvents', 'string', 2, 'Number of events to test' )
    # Then launch it
    res = self.diracProduction.launchProduction( prod, False, True, 0 )

    self.assertTrue( res['OK'] )

#     for found, expected in getOutput( 'MC' ):
#       self.assertEqual( found, expected )

class MCSuccessMultiProcessor( ProductionJobTestCase ):
  def test_Integration_Production( self ):

    # From request 12789
    stepsInProd = [{'StepId': 125080, 'StepName': 'Sim08a', 'ApplicationName': 'Gauss', 'ApplicationVersion': 'v45r3',
                    'ExtraPackages': 'AppConfig.v3r171', 'ProcessingPass': 'Sim08a', 'Visible': 'Y', 'Usable': 'Yes',
                    'DDDB': 'Sim08-20130503-1', 'CONDDB': 'Sim08-20130503-1-vc-mu100', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/Gauss/Sim08-Beam4000GeV-mu100-2012-nu2.5.py;$DECFILESROOT/options/11102400.py;$LBPYTHIA8ROOT/options/Pythia8.py;$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py',
                    'isMulticore': 'Y', 'SystemConfig': 'x86_64-slc5-gcc43-opt', 'mcTCK': '', 'ExtraOptions': '',
                    'fileTypesIn':[],
                    'fileTypesOut':['SIM']},
                   {'StepId': 124620, 'StepName': 'Digi13', 'ApplicationName': 'Boole', 'ApplicationVersion': 'v26r3',
                    'ExtraPackages': 'AppConfig.v3r164', 'ProcessingPass': 'Digi13', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'Sim08-20130503-1', 'CONDDB': 'Sim08-20130503-1-vc-mu100', 'DQTag': '', 'OptionsFormat': '',
                    'isMulticore': 'Y', 'SystemConfig': 'x86_64-slc5-gcc43-opt', 'mcTCK': '', 'ExtraOptions': '',
                    'OptionFiles': '$APPCONFIGOPTS/Boole/Default.py;$APPCONFIGOPTS/Boole/DataType-2012.py;$APPCONFIGOPTS/Boole/Boole-SiG4EnergyDeposit.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py',
                    'fileTypesIn':['SIM'],
                    'fileTypesOut':['DIGI']},
                   {'StepId': 124632, 'StepName': 'Trig0x409f0045', 'ApplicationName': 'Moore', 'ApplicationVersion': 'v14r8p1',
                    'ExtraPackages': 'AppConfig.v3r164', 'ProcessingPass': 'Trig0x40760037Flagged', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'Sim08-20130503-1', 'CONDDB': 'Sim08-20130503-1-vc-mu100', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/Moore/MooreSimProductionWithL0Emulation.py;$APPCONFIGOPTS/Conditions/TCK-0x409f0045.py;$APPCONFIGOPTS/Moore/DataType-2012.py;$APPCONFIGOPTS/L0/L0TCK-0x0045.py',
                    'isMulticore': 'Y', 'SystemConfig': 'x86_64-slc5-gcc43-opt', 'mcTCK': '', 'ExtraOptions': '',
                    'fileTypesIn':['DIGI'],
                    'fileTypesOut':['DIGI']},
                   {'StepId': 124834, 'StepName': 'Reco14a', 'ApplicationName': 'Brunel', 'ApplicationVersion': 'v43r2p7',
                    'ExtraPackages': 'AppConfig.v3r164', 'ProcessingPass': 'Reco14a', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'Sim08-20130503-1', 'CONDDB': 'Sim08-20130503-1-vc-mu100', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/Brunel/DataType-2012.py;$APPCONFIGOPTS/Brunel/MC-WithTruth.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py',
                    'isMulticore': 'Y', 'SystemConfig': 'x86_64-slc5-gcc43-opt', 'mcTCK': '', 'ExtraOptions': '',
                    'fileTypesIn':['DIGI'],
                    'fileTypesOut':['DST']},
                   {'StepId': 124630, 'StepName': 'Stripping20NoPrescalingFlagged', 'ApplicationName': 'DaVinci', 'ApplicationVersion': 'v32r2p1',
                    'ExtraPackages': 'AppConfig.v3r164', 'ProcessingPass': 'Stripping20NoPrescalingFlagged', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'Sim08-20130503-1', 'CONDDB': 'Sim08-20130503-1-vc-mu100', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/DaVinci/DV-Stripping20-Stripping-MC-NoPrescaling.py;$APPCONFIGOPTS/DaVinci/DataType-2012.py;$APPCONFIGOPTS/DaVinci/InputType-DST.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py',
                    'isMulticore': 'Y', 'SystemConfig': 'x86_64-slc5-gcc43-opt', 'mcTCK': '', 'ExtraOptions': '',
                    'fileTypesIn':['DST'],
                    'fileTypesOut':['ALLSTREAMS.DST']}]

    # First create the production object
    prod = self.pr._buildProduction( 'MCSimulation', stepsInProd, {'ALLSTREAMS.DST': 'Tier1_MC-DST'}, 0, 100,
                                     outputFileMask = 'ALLSTREAMS.DST' )
    prod.LHCbJob.setInputSandbox( find_all( 'pilot.cfg', '.' )[0] )
    prod.LHCbJob.setConfigArgs( 'pilot.cfg' )
    prod.setParameter( 'numberOfEvents', 'string', 6, 'Number of events to test' )
    # Then launch it
    res = self.diracProduction.launchProduction( prod, False, True, 0 )

    self.assertTrue( res['OK'] )

#     for found, expected in getOutput( 'MC' ):
#       self.assertEqual( found, expected )

class MCSuccess_new( ProductionJobTestCase ):
  def test_Integration_Production( self ):

    # From request 12789
    stepsInProd = [{'StepId': 125080, 'StepName': 'Sim08a', 'ApplicationName': 'Gauss', 'ApplicationVersion': 'v45r3',
                    'ExtraPackages': 'AppConfig.v3r171', 'ProcessingPass': 'Sim08a', 'Visible': 'Y', 'Usable': 'Yes',
                    'DDDB': 'Sim08-20130503-1', 'CONDDB': 'Sim08-20130503-1-vc-mu100', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/Gauss/Sim08-Beam4000GeV-mu100-2012-nu2.5.py;$DECFILESROOT/options/11102400.py;$LBPYTHIA8ROOT/options/Pythia8.py;$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py',
                    'isMulticore': 'N', 'SystemConfig': 'x86_64-slc5-gcc43-opt', 'mcTCK': '', 'ExtraOptions': '',
                    'fileTypesIn':[],
                    'fileTypesOut':['SIM']},
                   {'StepId': 124620, 'StepName': 'Digi13', 'ApplicationName': 'Boole', 'ApplicationVersion': 'v26r3',
                    'ExtraPackages': 'AppConfig.v3r164', 'ProcessingPass': 'Digi13', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'Sim08-20130503-1', 'CONDDB': 'Sim08-20130503-1-vc-mu100', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/Boole/Default.py;$APPCONFIGOPTS/Boole/DataType-2012.py;$APPCONFIGOPTS/Boole/Boole-SiG4EnergyDeposit.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py',
                    'isMulticore': 'N', 'SystemConfig': 'x86_64-slc5-gcc43-opt', 'mcTCK': '', 'ExtraOptions': '',
                    'fileTypesIn':['SIM'],
                    'fileTypesOut':['DIGI']},
                   {'StepId': 124632, 'StepName': 'Trig0x409f0045', 'ApplicationName': 'Moore', 'ApplicationVersion': 'v14r8p1',
                    'ExtraPackages': 'AppConfig.v3r164', 'ProcessingPass': 'Trig0x40760037Flagged', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'Sim08-20130503-1', 'CONDDB': 'Sim08-20130503-1-vc-mu100', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/Moore/MooreSimProductionWithL0Emulation.py;$APPCONFIGOPTS/Conditions/TCK-0x409f0045.py;$APPCONFIGOPTS/Moore/DataType-2012.py;$APPCONFIGOPTS/L0/L0TCK-0x0045.py',
                    'isMulticore': 'N', 'SystemConfig': 'x86_64-slc5-gcc43-opt', 'mcTCK': '', 'ExtraOptions': '',
                    'fileTypesIn':['DIGI'],
                    'fileTypesOut':['DIGI']}]

    # First create the production object
    prod = self.pr._buildProduction( 'MCSimulation', stepsInProd, {'DIGI':'Tier1_MC-DST'}, 0, 100,
                                     outputFileStep = '3' )
    prod.LHCbJob.setInputSandbox( find_all( 'pilot.cfg', '.' )[0] )
    prod.LHCbJob.setConfigArgs( 'pilot.cfg' )
    prod.setParameter( 'numberOfEvents', 'string', 2, 'Number of events to test' )
    # Then launch it
    res = self.diracProduction.launchProduction( prod, False, True, 0 )

    self.assertTrue( res['OK'] )

#     for found, expected in getOutput( 'MC_new' ):
#       self.assertEqual( found, expected )


class RecoSuccess( ProductionJobTestCase ):
  def test_Integration_Production( self ):
    lfns = ['/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/114753/114753_0000000296.raw']
    # From request 8772
    stepsInProd = [{'StepId': 38427, 'StepName': 'Reco14', 'ApplicationName': 'Brunel', 'ApplicationVersion': 'v43r2p2',
                    'ExtraPackages': 'AppConfig.v3r149', 'ProcessingPass': 'Reco14', 'Visible': 'Y', 'Usable': 'Yes',
                    'DDDB': 'dddb-20120831', 'CONDDB': 'cond-20120831', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/Brunel/DataType-2012.py', 'mcTCK': '', 'ExtraOptions': '',
                    'isMulticore': 'N', 'SystemConfig': '',
                    'fileTypesIn':['RAW'],
                    'fileTypesOut':['BRUNELHIST', 'FULL.DST']},
                   {'StepId': 38510, 'StepName': 'DataQuality-FULL', 'ApplicationName': 'DaVinci', 'ApplicationVersion': 'v32r2',
                    'ExtraPackages': 'AppConfig.v3r149', 'ProcessingPass': 'DataQuality-FULL', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'dddb-20120831', 'CONDDB': 'cond-20120831', 'DQTag': '', 'OptionsFormat': 'DQ',
                    'OptionFiles': '$APPCONFIGOPTS/DaVinci/DVMonitor-RealData.py;$APPCONFIGOPTS/DaVinci/DataType-2012.py;$APPCONFIGOPTS/DaVinci/DaVinci-InputType-SDST.py',
                    'isMulticore': 'N', 'SystemConfig': '', 'mcTCK': '', 'ExtraOptions': '',
                    'fileTypesIn':['FULL.DST'],
                    'fileTypesOut':['DAVINCIHIST']}]

    prod = self.pr._buildProduction( 'Reconstruction', stepsInProd, {'FULL.DST': 'Tier1-BUFFER'}, 0, 100,
                                     outputMode = 'Run', inputDataPolicy = 'protocol', inputDataList = lfns, events = 25 )
    prod.LHCbJob.setInputSandbox( find_all( 'pilot.cfg', '.' )[0] )
    prod.LHCbJob.setConfigArgs( 'pilot.cfg' )
    prod.LHCbJob._addParameter( prod.LHCbJob.workflow, 'runNumber', 'JDL', 154030, 'Input run number' )
    res = self.diracProduction.launchProduction( prod, False, True, 0 )
    self.assertTrue( res['OK'] )

#     for found, expected in getOutput( 'Reco' ):
#       self.assertEqual( found, expected )

# THIS does NOT work!
class RecoSuccessMultiProcessor( ProductionJobTestCase ):
  def test_Integration_Production( self ):
    lfns = ['/lhcb/data/2011/RAW/FULL/LHCb/COLLISION11/103681/103681_0000000005.raw']
    # From request 15630 - no DQ
    stepsInProd = [{'StepId': 125574, 'StepName': 'Reco14', 'ApplicationName': 'Brunel', 'ApplicationVersion': 'v44r5',
                    'ExtraPackages': 'AppConfig.v3r158', 'ProcessingPass': 'Reco14', 'Visible': 'Y', 'Usable': 'Yes',
                    'DDDB': 'dddb-20130111', 'CONDDB': 'cond-20130114', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/Brunel/DataType-2011.py', 'mcTCK': '', 'ExtraOptions': '',
                    'isMulticore': 'Y', 'SystemConfig': '',
                    'fileTypesIn':['RAW'],
                    'fileTypesOut':['BRUNELHIST', 'FULL.DST']}]

    prod = self.pr._buildProduction( 'Reconstruction', stepsInProd, {'FULL.DST': 'Tier1-BUFFER'}, 0, 100,
                                     outputMode = 'Run', inputDataPolicy = 'protocol', inputDataList = lfns, events = 25 )
    prod.LHCbJob._addParameter( prod.LHCbJob.workflow, 'runNumber', 'JDL', 154030, 'Input run number' )
    prod.LHCbJob.setInputSandbox( find_all( 'pilot.cfg', '.' )[0] )
    prod.LHCbJob.setConfigArgs( 'pilot.cfg' )
    res = self.diracProduction.launchProduction( prod, False, True, 0 )
    self.assertTrue( res['OK'] )

#     for found, expected in getOutput( 'Reco' ):
#       self.assertEqual( found, expected )


class StrippSuccess( ProductionJobTestCase ):
  def test_Integration_Production( self ):
    lfns = ['/lhcb/LHCb/Collision11/FULL.DST/00022719/0009/00022719_00095865_1.full.dst',
            '/lhcb/LHCb/Collision11/FULL.DST/00022719/0010/00022719_00102702_1.full.dst']
    # From request 17452
    stepsInProd = [{'StepId': 125625, 'StepName': 'Stripping20r1p2', 'ApplicationName': 'DaVinci', 'ApplicationVersion': 'v32r2p11',
                    'ExtraPackages': 'AppConfig.v3r178;SQLDDDB.v7r9', 'ProcessingPass': 'Stripping20r1p2', 'Visible': 'Y', 'Usable': 'Yes',
                    'DDDB': 'dddb-20130111', 'CONDDB': 'cond-20130114', 'DQTag': '', 'OptionsFormat': 'Stripping',
                    'OptionFiles': '$APPCONFIGOPTS/DaVinci/DV-Stripping20r1p2-Stripping.py;$APPCONFIGOPTS/DaVinci/DataType-2011.py;$APPCONFIGOPTS/DaVinci/InputType-DST.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py',
                    'isMulticore': 'N', 'SystemConfig': 'x86_64-slc5-gcc43-opt', 'mcTCK': '', 'ExtraOptions': '',
                    'fileTypesIn':['FULL.DST'],
                    'fileTypesOut':['BHADRON.MDST', 'BHADRONCOMPLETEEVENT.DST', 'CALIBRATION.DST', 'CHARM.MDST',
                                    'CHARMCOMPLETEEVENT.DST', 'DIMUON.DST', 'EW.DST', 'LEPTONIC.MDST', 'SEMILEPTONIC.DST']}]

    prod = self.pr._buildProduction( 'Stripping', stepsInProd, {'BHADRON.MDST': 'Tier1-BUFFER',
                                                                'BHADRONCOMPLETEEVENT.DST': 'Tier1-BUFFER',
                                                                'CALIBRATION.DST': 'Tier1-BUFFER',
                                                                'CHARM.MDST': 'Tier1-BUFFER',
                                                                'CHARMCOMPLETEEVENT.DST': 'Tier1-BUFFER',
                                                                'DIMUON.DST': 'Tier1-BUFFER',
                                                                'EW.DST': 'Tier1-BUFFER',
                                                                'LEPTONIC.MDST': 'Tier1-BUFFER',
                                                                'SEMILEPTONIC.DST': 'Tier1-BUFFER'},
                                     0, 100,
                                     outputMode = 'Run', inputDataPolicy = 'protocol', inputDataList = lfns, events = 500 )
    prod.LHCbJob._addParameter( prod.LHCbJob.workflow, 'runNumber', 'JDL', 154030, 'Input run number' )
    prod.LHCbJob.setInputSandbox( find_all( 'pilot.cfg', '.' )[0] )
    prod.LHCbJob.setConfigArgs( 'pilot.cfg' )
    res = self.diracProduction.launchProduction( prod, False, True, 0 )
    self.assertTrue( res['OK'] )

#     for found, expected in getOutput( 'Stripp' ):
#       self.assertEqual( found, expected )

class MCMergeSuccess( ProductionJobTestCase ):
  def test_Integration_Production( self ):

    lfns = ['/lhcb/MC/2012/BDSTH.STRIP.DST/00051752/0000/00051752_00001269_1.bdsth.Strip.dst',
            '/lhcb/MC/2012/BDSTH.STRIP.DST/00051752/0000/00051752_00001263_1.bdsth.Strip.dst']
    # From request 31139
    stepsInProd = [{'StepId': 129267, 'StepName': 'Stripping24NoPrescalingFlagged', 'ApplicationName': 'DaVinci', 'ApplicationVersion': 'v38r1p1',
                    'ExtraPackages': 'AppConfig.v3r262', 'ProcessingPass': 'Stripping24NoPrescalingFlagged', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'dddb-20150724', 'CONDDB': ' sim-20160606-vc-md100', 'DQTag': '', 'OptionsFormat': 'merge',
                    'OptionFiles': ' $APPCONFIGOPTS/DaVinci/DV-Stripping24-Stripping-MC-NoPrescaling.py;$APPCONFIGOPTS/DaVinci/DataType-2015.py;$APPCONFIGOPTS/DaVinci/InputType-DST.py',
                    'mcTCK': '', 'ExtraOptions': '',
                    'isMulticore': 'N', 'SystemConfig': 'x86_64-slc6-gcc48-opt',
                    'fileTypesIn':['DST'],
                    'fileTypesOut':['ALLSTREAMS.DST']}]


    prod = self.pr._buildProduction( 'Merge', stepsInProd, {'ALLSTREAMS.DST': 'Tier1_MC-DST'}, 0, 100,
                                     inputDataPolicy = 'protocol', inputDataList = lfns )
    prod.LHCbJob.setInputSandbox( find_all( 'pilot.cfg', '.' )[0] )
    prod.LHCbJob.setConfigArgs( 'pilot.cfg' )
    res = self.diracProduction.launchProduction( prod, False, True, 0 )
    self.assertTrue( res['OK'] )

#     for found, expected in getOutput( 'Merge' ):
#       self.assertEqual( found, expected )

class MergeMultStreamsSuccess( ProductionJobTestCase ):
  def test_Integration_Production( self ):
    lfns = ['/lhcb/LHCb/Collision12/LEPTONIC.MDST/00021210/0000/00021210_00002481_1.Leptonic.mdst',
            '/lhcb/LHCb/Collision12/LEPTONIC.MDST/00021210/0000/00021210_00002482_1.Leptonic.mdst']
    # From request 9085
    stepsInProd = [{'StepId': 54132, 'StepName': 'Merging', 'ApplicationName': 'DaVinci', 'ApplicationVersion': 'v32r2p1',
                    'ExtraPackages': 'AppConfig.v3r150', 'ProcessingPass': 'Merging', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': '', 'CONDDB': '', 'DQTag': '', 'OptionsFormat': 'Merge', 'mcTCK': '', 'ExtraOptions': '',
                    'OptionFiles': '$APPCONFIGOPTS/Merging/DV-Stripping-Merging.py',
                    'isMulticore': 'N', 'SystemConfig': '',
                    'fileTypesIn':['BHADRON.MDST', 'BHADRONCOMPLETEEVENT.DST', 'CALIBRATION.DST', 'CHARM.MDST',
                                   'CHARMCOMPLETEEVENT.DST', 'CHARMCONTROL.DST', 'DIMUON.DST', 'EW.DST',
                                   'LEPTONIC.MDST', 'MINIBIAS.DST', 'PID.MDST', 'RADIATIVE.DST', 'SEMILEPTONIC.DST'],
                    'fileTypesOut':['BHADRON.MDST', 'BHADRONCOMPLETEEVENT.DST', 'CALIBRATION.DST', 'CHARM.MDST',
                                    'CHARMCOMPLETEEVENT.DST', 'CHARMCONTROL.DST', 'DIMUON.DST', 'EW.DST',
                                    'LEPTONIC.MDST', 'MINIBIAS.DST', 'PID.MDST', 'RADIATIVE.DST', 'SEMILEPTONIC.DST']}]

    prod = self.pr._buildProduction( 'Merge', stepsInProd, {'BHADRON.MDST': 'Tier1-BUFFER',
                                                            'BHADRONCOMPLETEEVENT.DST': 'Tier1-BUFFER',
                                                            'CALIBRATION.DST': 'Tier1-BUFFER',
                                                            'CHARM.MDST': 'Tier1-BUFFER',
                                                            'CHARMCOMPLETEEVENT.DST': 'Tier1-BUFFER',
                                                            'CHARMCONTROL.DST': 'Tier1-BUFFER',
                                                            'DIMUON.DST': 'Tier1-BUFFER',
                                                            'EW.DST': 'Tier1-BUFFER',
                                                            'LEPTONIC.MDST': 'Tier1-BUFFER',
                                                            'MINIBIAS.DST': 'Tier1-BUFFER',
                                                            'PID.MDST':'Tier1-BUFFER',
                                                            'RADIATIVE.DST': 'Tier1-BUFFER',
                                                            'SEMILEPTONIC.DST': 'Tier1-BUFFER'},
                                     0, 100, inputDataPolicy = 'protocol', inputDataList = lfns )
    prod.LHCbJob.setInputSandbox( find_all( 'pilot.cfg', '.' )[0] )
    prod.LHCbJob.setConfigArgs( 'pilot.cfg' )
    res = self.diracProduction.launchProduction( prod, False, True, 0 )
    self.assertTrue( res['OK'] )

#     for found, expected in getOutput( 'MergeM' ):
#       self.assertEqual( found, expected )

class MergeMDFSuccess( ProductionJobTestCase ):
  def test_Integration_Production( self ):
    lfns = ['/lhcb/data/2011/RAW/EXPRESS/LHCb/COLLISION11/102360/102360_0000000031.raw',
            '/lhcb/data/2011/RAW/EXPRESS/LHCb/COLLISION11/97887/097887_0000000013.raw']
    # From request 9054
    stepsInProd = [{'StepId': 123897, 'StepName': 'MergeMDF', 'ApplicationName': 'MergeMDF', 'ApplicationVersion': '',
                    'ExtraPackages': '', 'ProcessingPass': 'Merging', 'Visible': 'Y', 'Usable': 'Yes',
                    'DDDB': '', 'CONDDB': '', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '', 'SystemConfig': '', 'mcTCK': '', 'ExtraOptions': '',
                    'isMulticore': 'N',
                    'fileTypesIn':['RAW'],
                    'fileTypesOut':['RAW']}]
    self.pr.modulesList = ['MergeMDF', 'BookkeepingReport']
    prod = self.pr._buildProduction( 'Merge', stepsInProd, {'RAW':'Tier1-BUFFER'}, 0, 100,
                                     inputDataPolicy = 'download', inputDataList = lfns )
    prod.LHCbJob.setInputSandbox( find_all( 'pilot.cfg', '.' )[0] )
    prod.LHCbJob.setConfigArgs( 'pilot.cfg' )
    res = self.diracProduction.launchProduction( prod, False, True, 0 )
    self.assertTrue( res['OK'] )

class SwimmingSuccess( ProductionJobTestCase ):
  def test_Integration_Production( self ):
    lfns = ['/lhcb/LHCb/Collision11/CHARMCOMPLETEEVENT.DST/00022717/0000/00022717_00001252_1.CharmCompleteEvent.dst']
    # From request 17492
    stepsInProd = [{'StepId': 125693, 'StepName': 'WG-CharmConfig-Swimming-D02KSKK', 'ApplicationName': 'Moore',
                    'ApplicationVersion': 'v12r9p5',
                    'ExtraPackages': 'CharmConfig.v2r21', 'ProcessingPass': 'WG-CharmConfig-Swimming-D02KSKK',
                    'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': '', 'CONDDB': '', 'DQTag': '', 'OptionsFormat': 'Swimming2011',
                    'OptionFiles': '$APPCONFIGOPTS/EnableCustomMainLoop.py;$CHARMCONFIGROOT/scripts/SwimTriggerD2KSkk.py',
                    'isMulticore': 'N', 'SystemConfig': 'x86_64-slc5-gcc43-opt', 'mcTCK': '', 'ExtraOptions': '',
                    'fileTypesIn':['CHARMCOMPLETEEVENT.DST'],
                    'fileTypesOut':['SWIMTRIGGERD02KSKK.DST']},
                   {'StepId': 125694, 'StepName': 'WG-CharmConfig-Swimming-D02KSKK', 'ApplicationName': 'DaVinci',
                    'ApplicationVersion': 'v29r2p6',
                    'ExtraPackages': 'CharmConfig.v2r21', 'ProcessingPass': 'WG-CharmConfig-Swimming-D02KSKK',
                    'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': '', 'CONDDB': '', 'DQTag': '', 'OptionsFormat': 'Swimming2011',
                    'OptionFiles': '$APPCONFIGOPTS/EnableCustomMainLoop.py;$CHARMCONFIGROOT/scripts/SwimStrippingD2KSkk.py',
                    'isMulticore': 'N', 'SystemConfig': 'x86_64-slc5-gcc43-opt', 'mcTCK': '', 'ExtraOptions': '',
                    'fileTypesIn':['SWIMTRIGGERD02KSKK.DST'],
                    'fileTypesOut':['SWIMSTRIPPINGD02KSKK.MDST']}]
    prod = self.pr._buildProduction( 'Swimming', stepsInProd, {'SWIMTRIGGERD02KSKK.DST':'Tier1-DST',
                                                               'SWIMSTRIPPINGD02KSKK.MDST':'Tier1-DST'}, 0, 100,
                                     inputDataPolicy = 'protocol', inputDataList = lfns, events = 10 )
    prod.LHCbJob._addParameter( prod.LHCbJob.workflow, 'runNumber', 'JDL', 104262, 'Input Run number' )
    prod.LHCbJob.setInputSandbox( find_all( 'pilot.cfg', '.' )[0] )
    prod.LHCbJob.setConfigArgs( 'pilot.cfg' )
    res = self.diracProduction.launchProduction( prod, False, True, 0 )
    self.assertTrue( res['OK'] )

#FIXME: not ready, now disabled - should also add DAVINCIHIST in input, plus ntuples and other combinations
class RootMergeSuccess( ProductionJobTestCase ):
  def test_Integration_Production( self ):

    lfns = ['/lhcb/LHCb/Collision15/BRUNELHIST/00047763/0006/Brunel_00047763_00069480_1.Hist.root',
            '/lhcb/LHCb/Collision15/BRUNELHIST/00047763/0006/Brunel_00047763_00069421_1.Hist.root']

    stepsInProd = [{'StepId': 12345, 'StepName': 'RootMerging', 'ApplicationName': 'Noether', 'ApplicationVersion': 'v1r4',
                    'ExtraPackages': '', 'ProcessingPass': 'RootMerging', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'dddb-20150724', 'CONDDB': ' cond-20150828', 'DQTag': '', 'OptionsFormat': 'merge',
                    'OptionFiles': 'DQMergeRun.py',
                    'mcTCK': '', 'ExtraOptions': '',
                    'isMulticore': 'N', 'SystemConfig': 'x86_64-slc6-gcc48-opt',
                    'fileTypesIn':['BRUNELHIST', 'DAVINCIHIST'],
                    'fileTypesOut':['ROOT']}]


    prod = self.pr._buildProduction( 'Merge', stepsInProd, {'ROOT': 'CERN-EOS-HIST'}, 0, 100,
                                     inputDataPolicy = 'protocol', inputDataList = lfns )
    prod.LHCbJob.setInputSandbox( [find_all( 'pilot.cfg', '.' )[0], 'DQMergeRun.py'] )
    prod.LHCbJob.setConfigArgs( 'pilot.cfg' )
    res = self.diracProduction.launchProduction( prod, False, True, 0 )
    self.assertTrue( res['OK'] )


#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ProductionJobTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MCSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MCSuccessMultiProcessor ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MCSuccess_new ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( RecoSuccess ) )
  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( RecoSuccessMultiProcessor ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( StrippSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MCMergeSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MergeMultStreamsSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MergeMDFSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( SwimmingSuccess ) )
  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( RootMergeSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
