""" Test of the ProductionRequest and Production modules
"""

# pylint: disable=protected-access, missing-docstring, invalid-name, line-too-long

import unittest
from mock import Mock

from LHCbDIRAC.BookkeepingSystem.Client.test.mock_BookkeepingClient import BookkeepingClientFake
from LHCbDIRAC.ProductionManagementSystem.Client.Production import Production


class ClientTestCase(unittest.TestCase):
  """ Base class for the Client test cases
  """

  def setUp(self):

    self.bkClientMock = Mock()
    self.diracProdIn = Mock()
    self.diracProdIn.launchProduction.return_value = {'OK': True, 'Value': 321}

    self.bkClientFake = BookkeepingClientFake()

    self.maxDiff = None

#############################################################################
# Production.py
#############################################################################


class ProductionSuccess(ClientTestCase):

  def test__constructOutputFilesList(self):
    prod = Production()
    res = prod._constructOutputFilesList(['T1', 'T2'])
    resExpected = [{'outputDataType': 't1'},
                   {'outputDataType': 't2'}]
    self.assertEqual(res, resExpected)

    res = prod._constructOutputFilesList(['T1', 'HIST'])
    resExpected = [{'outputDataType': 't1'},
                   {'outputDataType': 'hist'}]
    self.assertEqual(res, resExpected)


#############################################################################
# Test Suite run
#############################################################################


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ClientTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ProductionSuccess))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
