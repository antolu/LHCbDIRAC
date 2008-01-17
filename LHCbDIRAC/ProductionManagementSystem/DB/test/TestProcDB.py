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

    result = self.procDB.addFile()
    self.assert_( result['OK'])
    self.assertEqual(type(result['Value']),types.IntType)

if __name__ == '__main__':

  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TransformationCase)
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(FileCatalogCase)

  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
