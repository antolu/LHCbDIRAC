"""
It is used to test the methods used by the User Interface
It requires an Oracle database
"""

# pylint: disable=invalid-name,wrong-import-position

import unittest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient


__RCSID__ = "$Id$"


class TestMethods(unittest.TestCase):

  def setUp(self):
    super(TestMethods, self).setUp()
    self.bk = BookkeepingClient()

  def test_getFileTpes(self):
    retVal = self.bk.getFileTypes({'ConfigName': 'LHCb', 'ConditionDescription': 'Beam3500GeV-VeloClosed-MagDown',
                                   'EventType': '91000000', 'ProcessingPass': '/Real Data/Reco03', 'Visible': 'Y',
                                   'ConfigVersion': 'Collision10'})
    self.assertTrue(retVal['OK'])
    self.assertTrue(retVal['Value']['ParameterNames'])
    self.assertTrue(retVal['Value']['Records'])
    self.assertTrue(retVal['Value']['TotalRecords'])
    self.assertEqual(retVal['Value']['TotalRecords'], 2)
    outputFileTypes = ['DAVINCIHIST', 'BRUNELHIST']
    for rec in retVal['Value']['Records']:
      self.assertTrue(rec[0] in outputFileTypes)

    retVal = self.bk.getFileTypes({'ConfigName': 'LHCb', 'ConditionDescription': 'Beam3500GeV-VeloClosed-MagDown',
                                   'EventType': '90000000', 'ProcessingPass': '/Real Data/Reco10/Stripping13b',
                                   'Visible': 'Y', 'ConfigVersion': 'Collision11'})
    self.assertTrue(retVal['OK'])
    self.assertTrue(retVal['Value']['ParameterNames'])
    self.assertTrue(retVal['Value']['Records'])
    self.assertTrue(retVal['Value']['TotalRecords'])
    self.assertEqual(retVal['Value']['TotalRecords'], 13)
    outputFileTypes = ['DIELECTRON.DST', 'RADIATIVE.DST', 'DAVINCIHIST', 'MINIBIAS.DST', 'LEPTONIC.MDST', 'EW.DST',
                       'CALIBRATION.DST', 'CHARMCONTROL.DST', 'CHARM.MDST', 'CHARMCOMPLETEEVENT.DST',
                       'SEMILEPTONIC.DST', 'BHADRON.DST', 'DIMUON.DST']
    for rec in retVal['Value']['Records']:
      self.assertTrue(rec[0] in outputFileTypes)

    retVal = self.bk.getFileTypes({'ConfigName': 'LHCb', 'ConditionDescription': 'Beam3500GeV-VeloClosed-MagDown',
                                   'EventType': '90000000', 'ProcessingPass': '/Real Data/Reco10',
                                   'Visible': 'Y', 'ConfigVersion': 'Collision11'})
    self.assertTrue(retVal['OK'])
    self.assertTrue(retVal['Value']['ParameterNames'])
    self.assertTrue(retVal['Value']['Records'])
    self.assertTrue(retVal['Value']['TotalRecords'])
    self.assertEqual(retVal['Value']['TotalRecords'], 4)
    outputFileTypes = ['DAVINCIHIST', 'MERGEFORDQ.ROOT', 'SDST', 'BRUNELHIST']
    for rec in retVal['Value']['Records']:
      self.assertTrue(rec[0] in outputFileTypes)

    retVal = self.bk.getFileTypes({'ConfigName': 'MC', 'ConditionDescription': 'Beam3500GeV-May2010-MagDown-Fix1',
                                   'EventType': '30000000',
                                   'ProcessingPass': '/Sim01/Trig0x002e002aFlagged/Reco08-MINBIAS',
                                   'Visible': 'Y', 'ConfigVersion': 'MC10'})
    self.assertTrue(retVal['OK'])
    self.assertTrue(retVal['Value']['ParameterNames'])
    self.assertTrue(retVal['Value']['Records'])
    self.assertTrue(retVal['Value']['TotalRecords'])
    self.assertEqual(retVal['Value']['TotalRecords'], 1)
    outputFileTypes = ['DST']
    for rec in retVal['Value']['Records']:
      self.assertTrue(rec[0] in outputFileTypes)

    retVal = self.bk.getFileTypes({'ConfigName': 'MC', 'ConditionDescription': 'Beam3500GeV-Oct2010-MagUp-Nu2.5',
                                   'EventType': '11104020',
                                   'ProcessingPass': '/Sim01/Trig0x002e002aFlagged/Reco08/Stripping12Flagged',
                                   'Visible': 'Y', 'ConfigVersion': 'MC10'})
    self.assertTrue(retVal['OK'])
    self.assertTrue(retVal['Value']['ParameterNames'])
    self.assertTrue(retVal['Value']['Records'])
    self.assertTrue(retVal['Value']['TotalRecords'])
    self.assertEqual(retVal['Value']['TotalRecords'], 1)
    outputFileTypes = ['ALLSTREAMS.DST']
    for rec in retVal['Value']['Records']:
      self.assertTrue(rec[0] in outputFileTypes)

    retVal = self.bk.getFileTypes({'ConfigName': 'MC', 'ConditionDescription': 'Beam3500GeV-Oct2010-MagUp-Nu2.5',
                                   'EventType': '11436000',
                                   'ProcessingPass': '/Sim01/Trig0x002e002aFlagged/Reco08',
                                   'Visible': 'Y', 'ConfigVersion': 'MC10'})
    self.assertTrue(retVal['OK'])
    self.assertTrue(retVal['Value']['ParameterNames'])
    self.assertTrue(retVal['Value']['Records'])
    self.assertTrue(retVal['Value']['TotalRecords'])
    self.assertEqual(retVal['Value']['TotalRecords'], 1)
    outputFileTypes = ['DST']
    for rec in retVal['Value']['Records']:
      self.assertTrue(rec[0] in outputFileTypes)

  def test_getFilesSummary(self):
    retVal = self.bk.getFilesSummary({'ConfigName': 'MC', 'ConditionDescription': 'Beam3500GeV-Oct2010-MagUp-Nu2.5',
                                      'EventType': '11436000', 'FileType': 'DST', 'ProcessingPass':
                                      '/Sim01/Trig0x002e002aFlagged/Reco08', 'Visible': 'Y',
                                      'fullpath': '/MC/MC10/Beam3500GeV- \
                                      Oct2010-MagUp-Nu2.5/Sim01/Trig0x002e002aFlagged/Reco08/11436000/DST',
                                      'ConfigVersion': 'MC10', 'Quality': ['OK']})
    self.assertTrue(retVal['OK'])
    self.assertTrue(retVal['Value']['ParameterNames'])
    self.assertTrue(retVal['Value']['Records'])
    self.assertTrue(retVal['Value']['TotalRecords'])
    self.assertEqual(retVal['Value']['TotalRecords'], 1)
    self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(
        ['NbofFiles', 'NumberOfEvents', 'FileSize', 'Luminosity', 'InstLuminosity']))
    self.assertEqual(retVal['Value']['Records'][0], [51, 503595, 196432468768, 0, 0])

    retVal = self.bk.getFilesSummary({'ConfigName': 'MC', 'ConditionDescription': 'Beam3500GeV-Oct2010-MagUp-Nu2.5',
                                      'EventType': '11836001', 'FileType': 'DST',
                                      'ProcessingPass': '/Sim01/Trig0x002e002aFlagged/Reco08',
                                      'Visible': 'Y',
                                      'fullpath': '/MC/MC10/Beam3500GeV-Oct2010-\
                                      MagUp-Nu2.5/Sim01/Trig0x002e002aFlagged/Reco08/11836001/DST',
                                      'ConfigVersion': 'MC10', 'Quality': ['OK']})
    self.assertTrue(retVal['OK'])
    self.assertTrue(retVal['Value']['ParameterNames'])
    self.assertTrue(retVal['Value']['Records'])
    self.assertTrue(retVal['Value']['TotalRecords'])
    self.assertEqual(retVal['Value']['TotalRecords'], 1)
    self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(
        ['NbofFiles', 'NumberOfEvents', 'FileSize', 'Luminosity', 'InstLuminosity']))
    self.assertEqual(retVal['Value']['Records'][0], [51, 501595, 195843264453, 0, 0])

    retVal = self.bk.getFilesSummary({'ConfigName': 'MC',
                                      'ConditionDescription': 'Beam7TeV-UpgradeML1.0-MagDown-Lumi2-25ns',
                                      'EventType': '13104011', 'FileType': 'DST', 'ProcessingPass':
                                      '/Sim01/Rec03-WithTruth', 'Visible': 'Y',
                                      'fullpath': '/MC/Upgrade/Beam7TeV-UpgradeML1.0-MagDown-\
                                      Lumi2-25ns/Sim01/Rec03-WithTruth/13104011/DST',
                                      'ConfigVersion': 'Upgrade', 'Quality': ['OK']})
    self.assertTrue(retVal['OK'])
    self.assertTrue(retVal['Value']['ParameterNames'])
    self.assertTrue(retVal['Value']['Records'])
    self.assertTrue(retVal['Value']['TotalRecords'])
    self.assertEqual(retVal['Value']['TotalRecords'], 1)
    self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(
        ['NbofFiles', 'NumberOfEvents', 'FileSize', 'Luminosity', 'InstLuminosity']))
    self.assertEqual(retVal['Value']['Records'][0], [31, 507145, 158120863141, 0, 0])

    retVal = self.bk.getFilesSummary({'ConfigName': 'MC',
                                      'ConditionDescription': 'Beam7TeV-UpgradeML1.1-MagDown-Lumi10-25ns',
                                      'EventType': '10000000', 'FileType': 'XDST', 'ProcessingPass':
                                      '/Upgrade-Sim01/Rec02', 'Visible': 'Y',
                                      'fullpath': '/MC/Upgrade/Beam7TeV-UpgradeML1.1-MagDown-Lumi10-25ns\
                                      /Upgrade-Sim01/Rec02/10000000/XDST',
                                      'ConfigVersion': 'Upgrade', 'Quality': ['OK']})
    self.assertTrue(retVal['OK'])
    self.assertTrue(retVal['Value']['ParameterNames'])
    self.assertTrue(retVal['Value']['Records'])
    self.assertTrue(retVal['Value']['TotalRecords'])
    self.assertEqual(retVal['Value']['TotalRecords'], 1)
    self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(
        ['NbofFiles', 'NumberOfEvents', 'FileSize', 'Luminosity', 'InstLuminosity']))
    self.assertEqual(retVal['Value']['Records'][0], [20, 25346, 104198740584, 0, 0])

    retVal = self.bk.getFilesSummary({'ConfigName': 'LHCb', 'ConditionDescription': 'Beam3500GeV-VeloClosed-MagDown',
                                      'EventType': '90000000', 'FileType': 'BHADRON.DST',
                                      'ProcessingPass': '/Real Data/Reco10/Stripping13b',
                                      'Visible': 'Y',
                                      'fullpath': '/LHCb/Collision11/Beam3500GeV-VeloClosed-MagDown/Real Data/Reco10/\
                                      Stripping13b/90000000/BHADRON.DST',
                                      'ConfigVersion': 'Collision11', 'Quality': ['OK']})
    self.assertTrue(retVal['OK'])
    self.assertTrue(retVal['Value']['ParameterNames'])
    self.assertTrue(retVal['Value']['Records'])
    self.assertTrue(retVal['Value']['TotalRecords'])
    self.assertEqual(retVal['Value']['TotalRecords'], 1)
    self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(
        ['NbofFiles', 'NumberOfEvents', 'FileSize', 'Luminosity', 'InstLuminosity']))
    self.assertEqual(retVal['Value']['Records'][0], [3753, 96826892, 14145105483370, 176812166.538, 0])

    retVal = self.bk.getFilesSummary({'ConfigName': 'LHCb', 'ConditionDescription': 'Beam3500GeV-VeloClosed-MagDown',
                                      'EventType': '90000000', 'FileType': 'EW.DST',
                                      'ProcessingPass': '/Real Data/Reco10/Stripping13b',
                                      'Visible': 'Y',
                                      'fullpath': '/LHCb/Collision11/Beam3500GeV-VeloClosed-MagDown/Real Data/Reco10/\
                                      Stripping13b/90000000/EW.DST',
                                      'ConfigVersion': 'Collision11', 'Quality': ['OK']})
    self.assertTrue(retVal['OK'])
    self.assertTrue(retVal['Value']['ParameterNames'])
    self.assertTrue(retVal['Value']['Records'])
    self.assertTrue(retVal['Value']['TotalRecords'])
    self.assertEqual(retVal['Value']['TotalRecords'], 1)
    self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(
        ['NbofFiles', 'NumberOfEvents', 'FileSize', 'Luminosity', 'InstLuminosity']))
    self.assertEqual(retVal['Value']['Records'][0], [2314, 69542587, 6315182323923, 174996481.488, 0])

    retVal = self.bk.getFilesSummary({'ConditionDescription': 'Beam3500GeV-VeloClosed-MagDown',
                                      'ConfigName': 'LHCb',
                                      'ConfigVersion': 'Collision11',
                                      'EventType': '90000000',
                                      'FileType': 'RADIATIVE.DST',
                                      'ProcessingPass': '/Real Data/Reco10/Stripping13b',
                                      'Quality': ['OK'],
                                      'Visible': 'N',
                                      'fullpath': '/LHCb/Collision11/Beam3500GeV-VeloClosed-\
                                      MagDown/Real Data/Reco10/Stripping13b/90000000/RADIATIVE.DST'})
    self.assertTrue(retVal['OK'])
    self.assertTrue(retVal['Value']['ParameterNames'])
    self.assertTrue(retVal['Value']['Records'])
    self.assertTrue(retVal['Value']['TotalRecords'])
    self.assertEqual(retVal['Value']['TotalRecords'], 1)
    self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(
        ['NbofFiles', 'NumberOfEvents', 'FileSize', 'Luminosity', 'InstLuminosity']))
    self.assertEqual(retVal['Value']['Records'][0], [2464, 0, 53076783694, 10725901.9411, 0])

    retVal = self.bk.getFilesSummary({'ConditionDescription': 'Beam3500GeV-VeloClosed-MagDown',
                                      'ConfigName': 'LHCb',
                                      'ConfigVersion': 'Collision11',
                                      'EventType': '90000000',
                                      'FileType': 'SEMILEPTONIC.DST',
                                      'ProcessingPass': '/Real Data/Reco10/Stripping13b',
                                      'Quality': ['OK'],
                                      'Visible': 'N',
                                      'fullpath': '/LHCb/Collision11/Beam3500GeV-VeloClosed-MagDown/Real\
                                       Data/Reco10/Stripping13b/90000000/SEMILEPTONIC.DST'})
    self.assertTrue(retVal['OK'])
    self.assertTrue(retVal['Value']['ParameterNames'])
    self.assertTrue(retVal['Value']['Records'])
    self.assertTrue(retVal['Value']['TotalRecords'])
    self.assertEqual(retVal['Value']['TotalRecords'], 1)
    self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(
        ['NbofFiles', 'NumberOfEvents', 'FileSize', 'Luminosity', 'InstLuminosity']))
    self.assertEqual(retVal['Value']['Records'][0], [3410, 0, 202243892798, 15023394.4026, 0])

  def test_getFilesWithMetadata(self):

    parameterNames = [u'FileName', u'EventStat', u'FileSize', u'CreationDate', u'JobStart', u'JobEnd', u'WorkerNode',
                      u'FileType', u'RunNumber', u'FillNumber', u'FullStat', u'DataqualityFlag',
                      u'EventInputStat', u'TotalLuminosity', u'Luminosity', u'InstLuminosity', u'TCK',
                      u'GUID', u'ADLER32', u'EventType', u'MD5SUM',
                      u'VisibilityFlag', u'JobId', u'GotReplica', u'InsertTimeStamp']

    retVal = self.bk.getFilesWithMetadata({'ConfigName': 'MC',
                                           'ConditionDescription': 'Beam3500GeV-Oct2010-MagUp-Nu2.5',
                                           'EventType': '11436000', 'FileType': 'DST', 'ProcessingPass':
                                           '/Sim01/Trig0x002e002aFlagged/Reco08', 'Visible': 'Y',
                                           'fullpath': '/MC/MC10/Beam3500GeV- Oct2010-MagUp-Nu2.5/Sim01\
                                           /Trig0x002e002aFlagged/Reco08/11436000/DST',
                                           'ConfigVersion': 'MC10', 'Quality': ['OK']})
    self.assertTrue(retVal['OK'])
    self.assertTrue(retVal['Value']['ParameterNames'])
    self.assertTrue(retVal['Value']['Records'])
    self.assertTrue(retVal['Value']['TotalRecords'])
    self.assertEqual(retVal['Value']['TotalRecords'], 51)
    self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(parameterNames))
    self.assertEqual(len(retVal['Value']['Records']), 51)

    retVal = self.bk.getFilesWithMetadata({'ConfigName': 'MC',
                                           'ConditionDescription': 'Beam3500GeV-Oct2010-MagUp-Nu2.5',
                                           'EventType': '11836001', 'FileType': 'DST',
                                           'ProcessingPass': '/Sim01/Trig0x002e002aFlagged/Reco08',
                                           'Visible': 'Y',
                                           'fullpath': '/MC/MC10/Beam3500GeV-Oct2010-MagUp-Nu2.5/Sim01/\
                                      Trig0x002e002aFlagged/Reco08/11836001/DST',
                                           'ConfigVersion': 'MC10', 'Quality': ['OK']})
    self.assertTrue(retVal['OK'])
    self.assertTrue(retVal['Value']['ParameterNames'])
    self.assertTrue(retVal['Value']['Records'])
    self.assertTrue(retVal['Value']['TotalRecords'])
    self.assertEqual(retVal['Value']['TotalRecords'], 51)
    self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(parameterNames))
    self.assertEqual(len(retVal['Value']['Records']), 51)

    retVal = self.bk.getFilesWithMetadata({'ConfigName': 'MC',
                                           'ConditionDescription': 'Beam7TeV-UpgradeML1.0-MagDown-Lumi2-25ns',
                                           'EventType': '13104011', 'FileType': 'DST', 'ProcessingPass':
                                           '/Sim01/Rec03-WithTruth', 'Visible': 'Y',
                                           'fullpath': '/MC/Upgrade/Beam7TeV-UpgradeML1.0-MagDown-\
                                      Lumi2-25ns/Sim01/Rec03-WithTruth/13104011/DST',
                                           'ConfigVersion': 'Upgrade', 'Quality': ['OK']})
    self.assertTrue(retVal['OK'])
    self.assertTrue(retVal['Value']['ParameterNames'])
    self.assertTrue(retVal['Value']['Records'])
    self.assertTrue(retVal['Value']['TotalRecords'])
    self.assertEqual(retVal['Value']['TotalRecords'], 31)
    self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(parameterNames))
    self.assertEqual(len(retVal['Value']['Records']), 31)

    retVal = self.bk.getFilesWithMetadata({'ConfigName': 'MC',
                                           'ConditionDescription': 'Beam7TeV-UpgradeML1.1-MagDown-Lumi10-25ns',
                                           'EventType': '10000000', 'FileType': 'XDST', 'ProcessingPass':
                                           '/Upgrade-Sim01/Rec02', 'Visible': 'Y',
                                           'fullpath': '/MC/Upgrade/Beam7TeV-UpgradeML1.1-MagDown-Lumi10-25ns\
                                      /Upgrade-Sim01/Rec02/10000000/XDST',
                                           'ConfigVersion': 'Upgrade', 'Quality': ['OK']})
    self.assertTrue(retVal['OK'])
    self.assertTrue(retVal['Value']['ParameterNames'])
    self.assertTrue(retVal['Value']['Records'])
    self.assertTrue(retVal['Value']['TotalRecords'])
    self.assertEqual(retVal['Value']['TotalRecords'], 20)
    self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(parameterNames))
    self.assertEqual(len(retVal['Value']['Records']), 20)

    retVal = self.bk.getFilesWithMetadata({'ConfigName': 'LHCb',
                                           'ConditionDescription': 'Beam3500GeV-VeloClosed-MagDown',
                                           'EventType': '90000000', 'FileType': 'BHADRON.DST',
                                           'ProcessingPass': '/Real Data/Reco10/Stripping13b',
                                           'Visible': 'Y',
                                           'fullpath': '/LHCb/Collision11/Beam3500GeV-VeloClosed-MagDown/Real Data/Reco10/\
                                      Stripping13b/90000000/BHADRON.DST',
                                           'ConfigVersion': 'Collision11', 'Quality': ['OK']})
    self.assertTrue(retVal['OK'])
    self.assertTrue(retVal['Value']['ParameterNames'])
    self.assertTrue(retVal['Value']['Records'])
    self.assertTrue(retVal['Value']['TotalRecords'])
    self.assertEqual(retVal['Value']['TotalRecords'], 3753)
    self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(parameterNames))
    self.assertEqual(len(retVal['Value']['Records']), 3753)

    retVal = self.bk.getFilesWithMetadata({'ConfigName': 'LHCb',
                                           'ConditionDescription': 'Beam3500GeV-VeloClosed-MagDown',
                                           'EventType': '90000000', 'FileType': 'EW.DST',
                                           'ProcessingPass': '/Real Data/Reco10/Stripping13b',
                                           'Visible': 'Y',
                                           'fullpath': '/LHCb/Collision11/Beam3500GeV-VeloClosed-MagDown/Real Data/Reco10/\
                                      Stripping13b/90000000/EW.DST',
                                           'ConfigVersion': 'Collision11', 'Quality': ['OK']})
    self.assertTrue(retVal['OK'])
    self.assertTrue(retVal['Value']['ParameterNames'])
    self.assertTrue(retVal['Value']['Records'])
    self.assertTrue(retVal['Value']['TotalRecords'])
    self.assertEqual(retVal['Value']['TotalRecords'], 2314)
    self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(parameterNames))
    self.assertEqual(len(retVal['Value']['Records']), 2314)


if __name__ == '__main__':

  querySuite = unittest.defaultTestLoader.loadTestsFromTestCase(TestMethods)
  unittest.TextTestRunner(verbosity=2, failfast=True).run(querySuite)
