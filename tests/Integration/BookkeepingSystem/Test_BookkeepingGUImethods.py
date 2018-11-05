"""
It is used to test the methods used by the User Interface
It requires an Oracle database
"""

# pylint: disable=invalid-name,wrong-import-position

import unittest

from DIRAC.Core.Base.Script import parseCommandLine
from BookkeepingSystem.Client.BKQuery import BKQuery
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
                                           'fullpath': '/LHCb/Collision11/Beam3500GeV-VeloClosed-MagDown/\
                                           Real Data/Reco10/Stripping13b/90000000/BHADRON.DST',
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
                                           'fullpath': '/LHCb/Collision11/Beam3500GeV-VeloClosed-MagDown/\
                                           Real Data/Reco10/Stripping13b/90000000/EW.DST',
                                           'ConfigVersion': 'Collision11', 'Quality': ['OK']})
    self.assertTrue(retVal['OK'])
    self.assertTrue(retVal['Value']['ParameterNames'])
    self.assertTrue(retVal['Value']['Records'])
    self.assertTrue(retVal['Value']['TotalRecords'])
    self.assertEqual(retVal['Value']['TotalRecords'], 2314)
    self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(parameterNames))
    self.assertEqual(len(retVal['Value']['Records']), 2314)

  def test_getProcessingPass(self):
    bkQuery = {'ConfigVersion': 'Collision10', 'ProcessingPass': '',
               'ConfigName': 'LHCb', 'ConditionDescription': 'Beam3500GeV-VeloClosed-MagDown'}
    retVal = self.bk.getProcessingPass(bkQuery)
    self.assertTrue(retVal['OK'])
    self.assertTrue(retVal['Value'][0]['ParameterNames'])
    self.assertTrue(retVal['Value'][0]['Records'])
    self.assertTrue(retVal['Value'][0]['TotalRecords'])
    self.assertEqual(retVal['Value'][0]['TotalRecords'], 1)
    self.assertEqual(retVal['Value'][0]['Records'], [['Real Data']])

    retVal = self.bk.getProcessingPass(bkQuery, '/Real Data')
    self.assertTrue(retVal['OK'])
    procnames = ['WF-Validation-05',
                 'WF-Validation-04',
                 'Reco08-MINBIAS-FIRST-14-NB',
                 'Reco08',
                 'Reco07-SPD-CALIBRATION',
                 'Reco07',
                 'Reco05',
                 'Reco04',
                 'Reco03',
                 'CHARM-FIRST-14-NB-Merged',
                 'CHARM-FIRST-14-NB']
    for rec in retVal['Value'][0]['Records']:
      self.assertTrue(rec[0] in procnames)

    evts = [93000000, 90000000, 92000000, 91000000]
    for rec in retVal['Value'][1]['Records']:
      self.assertTrue(rec[0] in evts)

    retVal = self.bk.getProcessingPass(bkQuery, '/Real Data/Reco08')
    self.assertTrue(retVal['OK'])
    procnames = ['Stripping14', 'Stripping12c', 'Stripping12b']
    for rec in retVal['Value'][0]['Records']:
      self.assertTrue(rec[0] in procnames)

    evts = [90000000]
    for rec in retVal['Value'][1]['Records']:
      self.assertTrue(rec[0] in evts)

    retVal = self.bk.getProcessingPass(bkQuery, '/Real Data/Reco08/Stripping14')
    self.assertTrue(retVal['OK'])

    evts = [90000000]
    for rec in retVal['Value'][1]['Records']:
      self.assertTrue(rec[0] in evts)

    bkQuery['RunNumber'] = 81621
    retVal = self.bk.getProcessingPass(bkQuery, '/Real Data/Reco08')
    self.assertTrue(retVal['OK'])
    procnames = ['Stripping14', 'Stripping12c', 'Stripping12b']
    for rec in retVal['Value'][0]['Records']:
      self.assertTrue(rec[0] in procnames)

    evts = [90000000]
    for rec in retVal['Value'][1]['Records']:
      self.assertTrue(rec[0] in evts)

  def test_getMoreProductionInformations(self):
    retVal = self.bk.getMoreProductionInformations(18120)
    self.assertTrue(retVal['OK'])
    self.assertEqual(retVal['Value']['ConfigName'], 'LHCb')
    self.assertEqual(retVal['Value']['ConfigVersion'], 'Collision12')
    self.assertEqual(retVal['Value']['Data taking conditions'], 'Beam4000GeV-VeloClosed-MagDown')
    self.assertEqual(retVal['Value']['Processing pass'], '/Real Data/Reco13a/Stripping19a')
    self.assertEqual(retVal['Value']['ProgramName'], 'DaVinci')
    self.assertEqual(retVal['Value']['ProgramVersion'], 'v30r4p1')

  def test_getProductionSummary(self):
    bkQuery = {'Visible': 'Y', 'ConfigName': 'LHCb', 'ConditionDescription': 'Beam3500GeV-VeloClosed-MagDown',
               'EventType': '90000000', 'FileType': 'EW.DST', 'ConfigVersion':
               'Collision11', 'ProcessingPass': '/Real Data/Reco10/Stripping13b', 'Quality': ['OK']}
    retVal = self.bk.getProductionSummary(bkQuery)
    self.assertTrue(retVal['OK'])
    self.assertEqual(retVal['Value']['TotalRecords'], 2)

  def test_getVisibleFilesWithMetadata(self):
    bkQuery = {'Visible': 'Y', 'ConfigName': 'LHCb', 'ConditionDescription': 'Beam3500GeV-VeloClosed-MagDown',
               'EventType': '90000000', 'FileType': 'EW.DST', 'ConfigVersion':
               'Collision11', 'ProcessingPass': '/Real Data/Reco10/Stripping13b', 'Quality': ['OK']}
    retVal = self.bk.getVisibleFilesWithMetadata(bkQuery)
    self.assertTrue(retVal['OK'])
    self.assertEqual(len(retVal['Value']['LFNs']), 3223)
    self.assertEqual(retVal['Value']['Summary']['EventInputStat'], 2673532842)
    self.assertEqual(retVal['Value']['Summary']['FileSize'], 7551.21445591)
    self.assertEqual(retVal['Value']['Summary']['InstLuminosity'], 0)
    self.assertEqual(retVal['Value']['Summary']['Luminosity'], 212911143.681)
    self.assertEqual(retVal['Value']['Summary']['Number Of Files'], 3223)
    self.assertEqual(retVal['Value']['Summary']['Number of Events'], 78548304)
    self.assertEqual(retVal['Value']['Summary']['TotalLuminosity'], 0)

  def test_getEventTypes(self):
    bkQuery = {'Visible': 'Y', 'ConfigName': 'LHCb', 'ConditionDescription': 'Beam3500GeV-VeloClosed-MagDown',
               'EventType': '90000000', 'FileType': 'EW.DST', 'ConfigVersion':
               'Collision11', 'ProcessingPass': '/Real Data/Reco10/Stripping13b', 'Quality': ['OK']}
    retVal = self.bk.getEventTypes(bkQuery)
    self.assertTrue(retVal['OK'])
    evts = [[97000000, 'Beam Gas'],
            [93000000, 'Luminosity stream online'],
            [90000000, 'Full stream'],
            [96000001, 'Nobias stream'],
            [93000001, 'Luminosity stream online'],
            [91000001, 'Express Stream'],
            [90000001, 'stream?'],
            [95000000, 'Calib stream'],
            [95000001, 'Calib stream'],
            [91000000, 'Express stream'],
            [96000000, 'NoBias stream']]

    for rec in retVal['Value']['Records']:
      self.assertTrue(rec in evts)

  def test_getRuns(self):
    retVal = self.bk.getRuns({'ConfigName': 'LHCb', 'ConfigVersion': 'Collision12'})
    self.assertTrue(retVal['OK'])
    self.assertEqual(len(retVal['Value']), 1280)

  def test_getTCKs(self):
    bkQuery = {'Visible': 'Y', 'ConfigName': 'LHCb', 'ConditionDescription': 'Beam3500GeV-VeloClosed-MagDown',
               'EventType': '90000000', 'FileType': 'EW.DST', 'ConfigVersion':
               'Collision11', 'ProcessingPass': '/Real Data/Reco10/Stripping13b', 'Quality': ['OK']}
    retVal = self.bk.getTCKs(bkQuery)
    self.assertTrue(retVal['OK'])
    self.assertEqual(sorted(retVal['Value']), sorted(['0x57e60',
                                                      '0x710035',
                                                      '0x5b0032',
                                                      '0x730035',
                                                      '0x75320',
                                                      '0x6d0032',
                                                      '4A0033',
                                                      '0x5a0032',
                                                      '0x700034']))

  def test_getStepsMetadata(self):
    bkQuery = {'Visible': 'Y', 'ConfigName': 'LHCb', 'ConditionDescription': 'Beam3500GeV-VeloClosed-MagDown',
               'EventType': '90000000', 'FileType': 'EW.DST', 'ConfigVersion':
               'Collision11', 'ProcessingPass': '/Real Data/Reco10/Stripping13b', 'Quality': ['OK']}
    retVal = self.bk.getStepsMetadata(bkQuery)
    self.assertTrue(retVal['OK'])
    self.assertEqual(retVal['Value']['TotalRecords'], 1)
    self.assertTrue('Step-13438' in retVal['Value']['Records'])
    expected = [['StepId', 13438],
                ['StepName', 'Stripping13b-Merging-NoCalib'],
                ['ApplicationName', 'DaVinci'],
                ['ApplicationVersion', 'v28r3'],
                ['OptionFiles', '$APPCONFIGOPTS/Merging/DV-Stripping13-Merging.py'],
                ['DDDB', 'head-20110302'],
                ['CONDDB', 'head-20110512'],
                ['ExtraPackages', 'AppConfig.v3r102'],
                ['Visible', 'N']]
    for value in expected:
      self.assertTrue(value in retVal['Value']['Records']['Step-13438'])

  def test_getLimitedFiles(self):
    bkQuery = {'ConfigName': 'MC', 'StartItem': 0, 'FileType': 'DST', 'ConfigVersion': 'MC10',
               'ConditionDescription': 'Beam3500GeV-May2010-MagDown-Fix1', 'MaxItem': 25,
               'EventType': '30000000',
               'ProcessingPass': '/Sim01/Trig0x002e002aFlagged/Reco08-MINBIAS',
               'Visible': 'Y', 'Quality': [u'OK', u'UNCHECKED']}
    retVal = self.bk.getLimitedFiles(bkQuery)
    self.assertTrue(retVal['OK'])
    self.assertEqual(retVal['Value']['TotalRecords'], 25)

if __name__ == '__main__':

  querySuite = unittest.defaultTestLoader.loadTestsFromTestCase(TestMethods)
  unittest.TextTestRunner(verbosity=2, failfast=True).run(querySuite)
