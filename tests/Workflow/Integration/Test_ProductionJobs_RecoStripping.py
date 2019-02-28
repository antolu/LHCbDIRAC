#!/usr/bin/env python
""" "Integration" production jobs. StepIDs are taken from REAL productions that ran "recently"
"""

# pylint: disable=line-too-long,protected-access,invalid-name,wrong-import-position

import os
import sys
import unittest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.tests.Utilities.utils import find_all

from tests.Utilities.IntegrationTest import IntegrationTest

from LHCbDIRAC import rootPath
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction
from LHCbDIRAC.ProductionManagementSystem.Client.ProductionRequest import ProductionRequest


class ProductionJobTestCase(IntegrationTest):
  """ Base class for the ProductionJob test cases
  """

  def setUp(self):
    super(ProductionJobTestCase, self).setUp()

    self.pr = ProductionRequest()
    self.diracProduction = DiracProduction()


class Reco17Success(ProductionJobTestCase):
  def test_Integration_Production(self):
    lfns = ['/lhcb/data/2017/RAW/FULL/LHCb/COLLISION17/192165/192165_0000000011_1.raw']
    # From request 39597
    optionFilesDQ = '$APPCONFIGOPTS/DaVinci/DVMonitor-RealData.py;'
    optionFilesDQ += '$APPCONFIGOPTS/DaVinci/DataType-2016.py;'
    optionFilesDQ += '$APPCONFIGOPTS/DaVinci/DaVinci-InputType-SDST.py'
    stepsInProd = [{'StepId': '131333', 'StepName': 'Reco17a',
                    'ApplicationName': 'Brunel', 'ApplicationVersion': 'v52r4',
                    'ExtraPackages': 'AppConfig.v3r323;Det/SQLDDDB.v7r11',
                    'ProcessingPass': 'Reco17a', 'Visible': 'Y', 'Usable': 'Yes',
                    'DDDB': 'dddb-20150724', 'CONDDB': 'cond-20170510', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/Brunel/DataType-2017.py;$APPCONFIGOPTS/Brunel/rdst.py',
                    'mcTCK': '', 'ExtraOptions': '',
                    'isMulticore': 'N', 'SystemConfig': '',
                    'fileTypesIn': ['RAW'],
                    'fileTypesOut':['BRUNELHIST', 'RDST'],
                    'visibilityFlag':[{'Visible': 'N', 'FileType': 'RDST'},
                                      {'Visible': 'Y', 'FileType': 'BRUNELHIST'}]},
                   {'StepId': 131327, 'StepName': 'DataQuality-FULL',
                    'ApplicationName': 'DaVinci', 'ApplicationVersion': 'v42r4',
                    'ExtraPackages': 'AppConfig.v3r324;Det/SQLDDDB.v7r11',
                    'ProcessingPass': 'DataQuality-FULL', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'dddb-20150724', 'CONDDB': 'cond-20170510', 'DQTag': '', 'OptionsFormat': 'DQ',
                    'OptionFiles': optionFilesDQ,
                    'isMulticore': 'N', 'SystemConfig': '', 'mcTCK': '', 'ExtraOptions': '',
                    'fileTypesIn': ['RDST'],
                    'fileTypesOut':['DAVINCIHIST'],
                    'visibilityFlag':[{'Visible': 'Y', 'FileType': 'DAVINCIHIST'}]}]

    prod = self.pr._buildProduction('Reconstruction', stepsInProd, {'RDST': 'Tier1-Buffer'}, 0, 100,
                                    outputMode='Run', inputDataPolicy='protocol', inputDataList=lfns, events=25)
    try:
      # This is the standard location in Jenkins
      prod.LHCbJob.setInputSandbox(find_all('pilot.cfg', os.environ['WORKSPACE'] + '/PilotInstallDIR')[0])
    except (IndexError, KeyError):
      prod.LHCbJob.setInputSandbox(find_all('pilot.cfg', rootPath)[0])
    prod.LHCbJob.setConfigArgs('pilot.cfg')
    prod.LHCbJob._addParameter(prod.LHCbJob.workflow, 'runNumber', 'JDL', 192165, 'Input run number')
    res = self.diracProduction.launchProduction(prod, False, True, 0)
    self.assertTrue(res['OK'])


class StrippSuccess(ProductionJobTestCase):
  def test_Integration_Production(self):
    # From request 38945 (Stripping24r0p1)
    lfns = ['/lhcb/LHCb/Collision15/RDST/00048427/0009/00048427_00090769_1.rdst']
    # ancestor: '/lhcb/data/2015/RAW/FULL/LHCb/COLLISION15/167123/167123_0000000379.raw'

    optionFiles = '$APPCONFIGOPTS/DaVinci/DV-Stripping24r0p1-Stripping.py;'
    optionFiles += '$APPCONFIGOPTS/DaVinci/DataType-2015.py;'
    optionFiles += '$APPCONFIGOPTS/DaVinci/InputType-RDST.py;'
    optionFiles += '$APPCONFIGOPTS/DaVinci/DV-RawEventJuggler-0_3-to-4_2.py;'
    optionFiles += '$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py'
    stepsInProd = [{'StepId': 125625, 'StepName': 'Stripping24r0p1-DV-v38r1p4-AppConfig-v3r323',
                    'ApplicationName': 'DaVinci', 'ApplicationVersion': 'v38r1p4',
                    'ExtraPackages': 'AppConfig.v3r323;Det/SQLDDDB.v7r11',
                    'ProcessingPass': 'Stripping24r0p1', 'Visible': 'Y', 'Usable': 'Yes',
                    'DDDB': 'dddb-20150724', 'CONDDB': 'cond-20150828', 'DQTag': '', 'OptionsFormat': 'Stripping',
                    'OptionFiles': optionFiles,
                    'isMulticore': 'N', 'SystemConfig': 'x86_64-slc6-gcc48-opt', 'mcTCK': '', 'ExtraOptions': '',
                    'fileTypesIn': ['RDST'],
                    'fileTypesOut':['BHADRON.MDST', 'BHADRONCOMPLETEEVENT.DST', 'CHARM.MDST',
                                    'CHARMCOMPLETEEVENT.DST', 'DIMUON.DST', 'EW.DST', 'FTAG.DST', 'LEPTONIC.MDST',
                                    'MINIBIAS.DST', 'RADIATIVE.DST', 'SEMILEPTONIC.DST'],
                    'visibilityFlag':[{'Visible': 'N', 'FileType': 'BHADRON.MDST'},
                                      {'Visible': 'N', 'FileType': 'BHADRONCOMPLETEEVENT.DST'},
                                      {'Visible': 'N', 'FileType': 'CHARM.MDST'},
                                      {'Visible': 'N', 'FileType': 'CHARMCOMPLETEEVENT.DST'},
                                      {'Visible': 'N', 'FileType': 'DIMUON.DST'},
                                      {'Visible': 'N', 'FileType': 'EW.DST'},
                                      {'Visible': 'N', 'FileType': 'FTAG.DST'},
                                      {'Visible': 'N', 'FileType': 'LEPTONIC.MDST'},
                                      {'Visible': 'N', 'FileType': 'MINIBIAS.DST'},
                                      {'Visible': 'N', 'FileType': 'RADIATIVE.DST'},
                                      {'Visible': 'N', 'FileType': 'SEMILEPTONIC.DST'}]}]

    prod = self.pr._buildProduction('Stripping', stepsInProd, {'BHADRON.MDST': 'Tier1-Buffer',
                                                               'BHADRONCOMPLETEEVENT.DST': 'Tier1-Buffer',
                                                               'CHARM.MDST': 'Tier1-Buffer',
                                                               'CHARMCOMPLETEEVENT.DST': 'Tier1-Buffer',
                                                               'DIMUON.DST': 'Tier1-Buffer',
                                                               'EW.DST': 'Tier1-Buffer',
                                                               'FTAG.DST': 'Tier1-Buffer',
                                                               'LEPTONIC.MDST': 'Tier1-Buffer',
                                                               'MINIBIAS.DST': 'Tier1-Buffer',
                                                               'RADIATIVE.DST': 'Tier1-Buffer',
                                                               'SEMILEPTONIC.DST': 'Tier1-Buffer'},
                                    0, 100,
                                    outputMode='Run', inputDataPolicy='protocol', inputDataList=lfns, events=500,
                                    ancestorDepth=1)
    prod.LHCbJob._addParameter(prod.LHCbJob.workflow, 'runNumber', 'JDL', 167123, 'Input run number')
    try:
      # This is the standard location in Jenkins
      prod.LHCbJob.setInputSandbox(find_all('pilot.cfg', os.environ['WORKSPACE'] + '/PilotInstallDIR')[0])
    except (IndexError, KeyError):
      prod.LHCbJob.setInputSandbox(find_all('pilot.cfg', rootPath)[0])
    prod.LHCbJob.setConfigArgs('pilot.cfg')
    res = self.diracProduction.launchProduction(prod, False, True, 0)
    self.assertTrue(res['OK'])


#############################################################################
# Test Suite run
#############################################################################


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ProductionJobTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Reco17Success))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(StrippSuccess))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
