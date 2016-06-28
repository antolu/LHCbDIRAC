""" Test of the Production module
"""

import unittest
from mock import MagicMock

from LHCbDIRAC.ProductionManagementSystem.Client.Production import Production

#pylint: disable=protected-access

class ClientTestCase( unittest.TestCase ):
  """ Base class for the Client test cases
  """
  def setUp( self ):

    self.diracProdIn = MagicMock()
    self.diracProdIn.launchProduction.return_value = {'OK': True, 'Value': 321}

    self.maxDiff = None

#############################################################################
# Production.py
#############################################################################

class ProductionSuccess( ClientTestCase ):


  def test__constructOutputFilesList( self ):
    prod = Production()
    res = prod._constructOutputFilesList( ['T1', 'T2'] )
    resExpected = [{'outputDataType': 't1', 'outputDataName': '@{STEP_ID}.t1'},
		   {'outputDataType': 't2', 'outputDataName': '@{STEP_ID}.t2'}]
    self.assertEqual( res, resExpected )

    res = prod._constructOutputFilesList( ['T1', 'HIST'] )
    resExpected = [{'outputDataType': 't1', 'outputDataName': '@{STEP_ID}.t1'},
		   {'outputDataType': 'hist', 'outputDataName': '@{applicationName}_@{STEP_ID}.Hist.root'}]
    self.assertEqual( res, resExpected )

    res = prod._constructOutputFilesList( ['T1', 'HIST'] )
    resExpected = [{'outputDataType': 't1', 'outputDataName': '@{STEP_ID}.t1'},
		   {'outputDataType': 'hist', 'outputDataName': '@{applicationName}_@{STEP_ID}.Hist.root'}]
    self.assertEqual( res, resExpected )


#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ClientTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProductionSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
