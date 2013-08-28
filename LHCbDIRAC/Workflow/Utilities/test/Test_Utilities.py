""" UnitTest class for LHCbDIRAC Interfaces/API
"""

__RCSID__ = "$Id"

import unittest

from DIRAC.Core.Workflow.Parameter import Parameter
from mock import Mock
from DIRAC.Core.Workflow.Workflow import *
# from DIRAC.Interfaces.API.Job import Job
from LHCbDIRAC.Workflow.Utilities.Utils import makeRunList

#############################################################################

class UtilitiesTestCase( unittest.TestCase ):
  """ Base class
  """
  def setUp( self ):

#    self.job = Job()
    pass

class UtilsSuccess( UtilitiesTestCase ):

  def test_makeRunList( self ):
    res = makeRunList( "1234:1236,12340,12342,1520:1522" )
    self.assertEqual( res['Value'], ['1234', '1235', '1236', '12340', '12342', '1520', '1521', '1522'] )

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( UtilitiesTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( UtilsSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
