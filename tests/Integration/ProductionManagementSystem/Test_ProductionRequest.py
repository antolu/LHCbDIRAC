""" This is a test of the chain
    ProductionRequestHandler -> ProductionRequestDB

    It supposes that the DB is present, and that the service is running

    It also supposes that the DB is empty!
"""

# pylint: disable=invalid-name,wrong-import-position

import time
import cPickle
import unittest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from DIRAC.Core.DISET.RPCClient import RPCClient


class TestProductionRequestTestCase(unittest.TestCase):
  """ Base class
  """

  def setUp(self):
    gLogger.setLevel('DEBUG')
    self.reqClient = RPCClient('ProductionManagement/ProductionRequest')
    self.reqIds = []  # this request will be deleted

  def tearDown(self):
    pass


class TestProductionRequestTestCaseChain(TestProductionRequestTestCase):
  """ a chain of tests
  """

  def test_mix(self):
    """ Calling various methods
    """

    # this, does not exist yet
    res = self.reqClient.getProductionList(1)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], [])

    # add
    res = self.reqClient.createProductionRequest({'ParentID': '',
                                                  'MasterID': '',
                                                  'RequestAuthor': 'adminusername',
                                                  'RequestName': 'RequestName',
                                                  'RequestType': 'RequestType',
                                                  'RequestState': 'New',
                                                  'RequestPriority': '2b',
                                                  'RequestPDG': '',
                                                  'SimCondition': 'Beam3500GeV-VeloClosed-MagUp',
                                                  'SimCondID': '429215',
                                                  'SimCondDetail': cPickle.dumps('BlahBlahBlah'),
                                                  'ProPath': 'Blup',
                                                  'ProID': '',
                                                  'ProDetail': cPickle.dumps('BlaaaaaahBlahBlah'),
                                                  'EventType': '900000',
                                                  'NumberOfEvents': '-1',
                                                  'Description': 'Description',
                                                  'Comments': 'Comments',
                                                  'Inform': '',
                                                  'RealNumberOfEvents': '',
                                                  'IsModel': 0,
                                                  'Extra': '',
                                                  'StartingDate': time.strftime('%Y-%m-%d'),
                                                  'FinalizationDate': time.strftime('%Y-%m-%d'),
                                                  'RetentionRate': 1,
                                                  'FastSimulationType': 'None'})
    self.assertTrue(res['OK'])
    firstReq = res['Value']
    self.reqIds.append(res['Value'])

    res = self.reqClient.createProductionRequest({'ParentID': '',
                                                  'MasterID': '',
                                                  'RequestAuthor': 'adminusername',
                                                  'RequestName': 'RequestName - new',
                                                  'RequestType': 'RequestType',
                                                  'RequestState': 'New',
                                                  'RequestPriority': '2b',
                                                  'RequestPDG': '',
                                                  'SimCondition': 'Beam3500GeV-VeloClosed-MagUp',
                                                  'SimCondID': '429215',
                                                  'SimCondDetail': cPickle.dumps('BlahBlahBlah'),
                                                  'ProPath': 'Blup',
                                                  'ProID': '',
                                                  'ProDetail': cPickle.dumps('BlaaaaaahBlahBlah'),
                                                  'EventType': '900000',
                                                  'NumberOfEvents': '-1',
                                                  'Description': 'Description',
                                                  'Comments': 'Comments',
                                                  'Inform': '',
                                                  'RealNumberOfEvents': '',
                                                  'IsModel': 0,
                                                  'Extra': '',
                                                  'StartingDate': time.strftime('%Y-%m-%d'),
                                                  'FinalizationDate': time.strftime('%Y-%m-%d'),
                                                  'RetentionRate': 1,
                                                  'FastSimulationType': 'None'})
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], firstReq + 1)
    self.reqIds.append(res['Value'])
    # Querying
    res = self.reqClient.getProductionList(1)
    self.assertTrue(res['OK'])

    res = self.reqClient.getProductionRequestList(0, '', 'ASC', 0, 0, {'RequestState': 'Active'})
    self.assertTrue(res['OK'])

    # Adding production
    res = self.reqClient.addProductionToRequest({'ProductionID': 123,
                                                 'RequestID': firstReq,
                                                 'Used': 0,
                                                 'BkEvents': 0})
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], 123)
    res = self.reqClient.removeProductionFromRequest(123)
    self.assertTrue(res['OK'])

    res = self.reqClient.addProductionToRequest({'ProductionID': 456,
                                                 'RequestID': firstReq + 1,
                                                 'Used': 1,
                                                 'BkEvents': 0})
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], 456)

    res = self.reqClient.removeProductionFromRequest(456)
    self.assertTrue(res['OK'])

    # delete
    for reqId in self.reqIds:
      res = self.reqClient.deleteProductionRequest(reqId)
      self.assertTrue(res['OK'])


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestProductionRequestTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestProductionRequestTestCaseChain))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
