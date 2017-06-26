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

class Reco17Success( ProductionJobTestCase ):
  def test_Integration_Production( self ):
    lfns = ['/lhcb/data/2017/RAW/FULL/LHCb/COLLISION17/192165/192165_0000000011.raw']
    # From request 39597
    stepsInProd = [{'StepId': '131333', 'StepName': 'Reco17a', 'ApplicationName': 'Brunel', 'ApplicationVersion': 'v52r4',
                    'ExtraPackages': 'AppConfig.v3r323;SQLDDDB.v7r10', 'ProcessingPass': 'Reco17a', 'Visible': 'Y', 'Usable': 'Yes',
                    'DDDB': 'dddb-20150724', 'CONDDB': 'cond-20170510', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/Brunel/DataType-2017.py;$APPCONFIGOPTS/Brunel/rdst.py', 'mcTCK': '', 'ExtraOptions': '',
                    'isMulticore': 'N', 'SystemConfig': '',
                    'fileTypesIn':['RAW'],
                    'fileTypesOut':['BRUNELHIST', 'RDST'],
                    'visibilityFlag':[{'Visible': 'N', 'FileType': 'RDST'},
                                      {'Visible': 'Y', 'FileType':'BRUNELHIST'}
                                     ]
                   },
                   {'StepId': 131327, 'StepName': 'DataQuality-FULL', 'ApplicationName': 'DaVinci', 'ApplicationVersion': 'v42r4',
                    'ExtraPackages': 'AppConfig.v3r324;SQLDDDB.v7r10', 'ProcessingPass': 'DataQuality-FULL', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'dddb-20150724', 'CONDDB': 'cond-20170510', 'DQTag': '', 'OptionsFormat': 'DQ',
                    'OptionFiles': '$APPCONFIGOPTS/DaVinci/DVMonitor-RealData.py;$APPCONFIGOPTS/DaVinci/DataType-2016.py;$APPCONFIGOPTS/DaVinci/DaVinci-InputType-SDST.py',
                    'isMulticore': 'N', 'SystemConfig': '', 'mcTCK': '', 'ExtraOptions': '',
                    'fileTypesIn':['RDST'],
                    'fileTypesOut':['DAVINCIHIST'],
                    'visibilityFlag':[{'Visible': 'Y', 'FileType': 'DAVINCIHIST'}
                                     ]
                   }
                  ]

    prod = self.pr._buildProduction( 'Reconstruction', stepsInProd, {'RDST': 'Tier1-BUFFER'}, 0, 100,
                                     outputMode = 'Run', inputDataPolicy = 'protocol', inputDataList = lfns, events = 25 )
    prod.LHCbJob.setInputSandbox( find_all( 'pilot.cfg', '.' )[0] )
    prod.LHCbJob.setConfigArgs( 'pilot.cfg' )
    prod.LHCbJob._addParameter( prod.LHCbJob.workflow, 'runNumber', 'JDL', 192165, 'Input run number' )
    res = self.diracProduction.launchProduction( prod, False, True, 0 )
    self.assertTrue( res['OK'] )


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
                    'fileTypesOut':['BRUNELHIST', 'FULL.DST'],
                    'visibilityFlag':[{'Visible': 'N', 'FileType': 'FULL.DST'},
                                      {'Visible': 'Y', 'FileType':'BRUNELHIST'}]}]

    prod = self.pr._buildProduction( 'Reconstruction', stepsInProd, {'FULL.DST': 'Tier1-BUFFER'}, 0, 100,
                                     outputMode = 'Run', inputDataPolicy = 'protocol', inputDataList = lfns, events = 25 )
    prod.LHCbJob._addParameter( prod.LHCbJob.workflow, 'runNumber', 'JDL', 154030, 'Input run number' )
    prod.LHCbJob.setInputSandbox( find_all( 'pilot.cfg', '.' )[0] )
    prod.LHCbJob.setConfigArgs( 'pilot.cfg' )
    res = self.diracProduction.launchProduction( prod, False, True, 0 )
    self.assertTrue( res['OK'] )


class StrippSuccess( ProductionJobTestCase ):
  def test_Integration_Production( self ):
    # From request 38945 (Stripping24r0p1)
    lfns = ['/lhcb/LHCb/Collision15/RDST/00048427/0009/00048427_00090769_1.rdst']
    # ancestor: '/lhcb/data/2015/RAW/FULL/LHCb/COLLISION15/167123/167123_0000000379.raw'

    stepsInProd = [{'StepId': 125625, 'StepName': 'Stripping24r0p1-DV-v38r1p4-AppConfig-v3r323', 'ApplicationName': 'DaVinci', 'ApplicationVersion': 'v38r1p4',
                    'ExtraPackages': 'AppConfig.v3r323;SQLDDDB.v7r10', 'ProcessingPass': 'Stripping24r0p1', 'Visible': 'Y', 'Usable': 'Yes',
                    'DDDB': 'dddb-20150724', 'CONDDB': 'cond-20150828', 'DQTag': '', 'OptionsFormat': 'Stripping',
                    'OptionFiles': '$APPCONFIGOPTS/DaVinci/DV-Stripping24r0p1-Stripping.py;$APPCONFIGOPTS/DaVinci/DataType-2015.py;$APPCONFIGOPTS/DaVinci/InputType-RDST.py;$APPCONFIGOPTS/DaVinci/DV-RawEventJuggler-0_3-to-4_2.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py',
                    'isMulticore': 'N', 'SystemConfig': 'x86_64-slc6-gcc48-opt', 'mcTCK': '', 'ExtraOptions': '',
                    'fileTypesIn':['RDST'],
                    'fileTypesOut':['BHADRON.MDST', 'BHADRONCOMPLETEEVENT.DST', 'CHARM.MDST',
                                    'CHARMCOMPLETEEVENT.DST', 'DIMUON.DST', 'EW.DST', 'FTAG.DST', 'LEPTONIC.MDST',
                                    'MINIBIAS.DST', 'RADIATIVE.DST', 'SEMILEPTONIC.DST'],
                    'visibilityFlag':[{'Visible': 'N', 'FileType':'BHADRON.MDST'},
                                      {'Visible': 'N', 'FileType':'BHADRONCOMPLETEEVENT.DST'},
                                      {'Visible': 'N', 'FileType':'CHARM.MDST'},
                                      {'Visible': 'N', 'FileType':'CHARMCOMPLETEEVENT.DST'},
                                      {'Visible': 'N', 'FileType':'DIMUON.DST'},
                                      {'Visible': 'N', 'FileType':'EW.DST'},
                                      {'Visible': 'N', 'FileType':'FTAG.DST'},
                                      {'Visible': 'N', 'FileType':'LEPTONIC.MDST'},
                                      {'Visible': 'N', 'FileType':'MINIBIAS.DST'},
                                      {'Visible': 'N', 'FileType':'RADIATIVE.DST'},
                                      {'Visible': 'N', 'FileType':'SEMILEPTONIC.DST'}]}]

    prod = self.pr._buildProduction( 'Stripping', stepsInProd, {'BHADRON.MDST': 'Tier1-BUFFER',
                                                                'BHADRONCOMPLETEEVENT.DST': 'Tier1-BUFFER',
                                                                'CHARM.MDST': 'Tier1-BUFFER',
                                                                'CHARMCOMPLETEEVENT.DST': 'Tier1-BUFFER',
                                                                'DIMUON.DST': 'Tier1-BUFFER',
                                                                'EW.DST': 'Tier1-BUFFER',
                                                                'FTAG.DST': 'Tier1-BUFFER',
                                                                'LEPTONIC.MDST': 'Tier1-BUFFER',
                                                                'MINIBIAS.DST': 'Tier1-BUFFER',
                                                                'RADIATIVE.DST': 'Tier1-BUFFER',
                                                                'SEMILEPTONIC.DST': 'Tier1-BUFFER'},
                                     0, 100,
                                     outputMode = 'Run', inputDataPolicy = 'protocol', inputDataList = lfns, events = 500,
                                     ancestorDepth = 1 )
    prod.LHCbJob._addParameter( prod.LHCbJob.workflow, 'runNumber', 'JDL', 167123, 'Input run number' )
    prod.LHCbJob.setInputSandbox( find_all( 'pilot.cfg', '.' )[0] )
    prod.LHCbJob.setConfigArgs( 'pilot.cfg' )
    res = self.diracProduction.launchProduction( prod, False, True, 0 )
    self.assertTrue( res['OK'] )


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
                    'fileTypesOut':['ALLSTREAMS.DST'],
                    'visibilityFlag':[{'Visible': 'Y', 'FileType': 'ALLSTREAMS.DST'}]}]


    prod = self.pr._buildProduction( 'Merge', stepsInProd, {'ALLSTREAMS.DST': 'Tier1_MC-DST'}, 0, 100,
                                     inputDataPolicy = 'protocol', inputDataList = lfns )
    prod.LHCbJob.setInputSandbox( find_all( 'pilot.cfg', '.' )[0] )
    prod.LHCbJob.setConfigArgs( 'pilot.cfg' )
    res = self.diracProduction.launchProduction( prod, False, True, 0 )
    self.assertTrue( res['OK'] )

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
                    'fileTypesOut':['RAW'],
                    'visibilityFlag':[{'Visible': 'Y', 'FileType': 'RAW'}]}]
    self.pr.modulesList = ['MergeMDF', 'BookkeepingReport']
    prod = self.pr._buildProduction( 'Merge', stepsInProd, {'RAW':'Tier1-BUFFER'}, 0, 100,
                                     inputDataPolicy = 'download', inputDataList = lfns )
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
                    'fileTypesOut':['ROOT'],
                    'visibilityFlag':[{'Visible': 'Y', 'FileType': 'ROOT'}]}]


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
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( Reco17Success ) )
  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( RecoSuccessMultiProcessor ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( StrippSuccess ) )
  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MCMergeSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MergeMDFSuccess ) )
  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( RootMergeSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
