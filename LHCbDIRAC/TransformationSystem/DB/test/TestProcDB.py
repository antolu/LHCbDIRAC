import unittest,types
from DIRAC.ProductionManagementSystem.DB.ProcessingDB import ProcessingDB

class ProcDBTestCase(unittest.TestCase):
  """ Base class for the JobDB test cases
  """

  def setUp(self):

    self.procDB = ProcessingDB()

class TransformationCase(ProcDBTestCase):

  def test_addTransformation(self):

    result = self.procDB.addTransformation()
    self.assert_( result['OK'])
    self.assertEqual(type(result['Value']),types.IntType)

class FileCatalogCase(ProcDBTestCase):

  def test_addFile(self):

    result = self.procDB.addFile('LFN','PFN',123456,'Test-SE','bfad6aae-5787-45fa-a5c0-90d91bd82de1')
    self.assert_( result['OK'])
    self.assertEqual(type(result['Value']),types.IntType)

if __name__ == '__main__':

  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TransformationCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(FileCatalogCase))

  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
