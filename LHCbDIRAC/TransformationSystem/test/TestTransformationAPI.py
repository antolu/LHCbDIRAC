#! /usr/bin/env python
from LHCbDIRAC.Interfaces.API.Transformation                 import Transformation
from DIRAC.Core.Utilities.List                               import sortList
import unittest,types,time,re

class LHCbAPITestCase(unittest.TestCase):

  def test_setBkQuery(self):
    """ Test the manipulation of BK Queries

          testBkQuery()
          setBkQuery()
          getBkQuery()
          getBkQueryID()
          removeTransformationBkQuery()

        Tests the creation of a transformation with a BK query to ensure the BKQuery and corresponding ID is stored correctly. 
        A new transformation object is created with the transID just created and the BK parameters tested.
        The transformation BK query is then removed and the transformation obect is checked to ensure this is done correctly.
    """
    # Create the transformation object
    tAPI = Transformation()
    transName = 'LHCbAPITestCaseTransformation-%s' % time.strftime("%Y%m%d-%H:%M:%S")
    res = tAPI.setTransformationName(transName)
    self.assert_(res['OK'])
    description = 'Test transforamtion description'
    res = tAPI.setDescription(description)
    self.assert_(res['OK'])
    longDescription = 'Test transforamtion long description'
    res = tAPI.setLongDescription(longDescription)
    self.assert_(res['OK'])
    res = tAPI.setType('MCSimulation')
    self.assert_(res['OK'])

    # Create a dummy bk query, test it and obtain the list of files it returns (expected to be 0)
    bkQuery = {'ProductionID':9999999,'FileType':'DST'}
    res = tAPI.testBkQuery(bkQuery)
    self.assert_(res['OK'])
    bkFiles = res['Value']
    res = tAPI.setBkQuery(bkQuery)
    self.assert_(res['OK'])
    res = tAPI.getBkQuery()
    self.assert_(res['OK'])
    self.assertEqual(res['Value'],bkQuery)

    # Create the transformation and obtain the BkQueryID and TransformationID
    res = tAPI.addTransformation()
    self.assert_(res['OK'])
    res = tAPI.getBkQueryID()
    self.assert_(res['OK'])
    bkQueryID = res['Value']
    transID = tAPI.paramValues['TransformationID']

    # Create a new transformation object with the transID and retrieve the BkQuery and BkqueryID (coming from the DB)
    tAPI = Transformation(transID)
    res = tAPI.getBkQuery()
    self.assert_(res['OK'])
    res = tAPI.testBkQuery(bkQuery)
    self.assert_(res['OK'])
    self.assertEqual(res['Value'],bkFiles)
    res = tAPI.getBkQueryID()
    self.assert_(res['OK'])
    self.assertEqual(res['Value'],bkQueryID)
  
    # Remove the BkQuery associated to the transformation and test it is removed properly
    res = tAPI.removeTransformationBkQuery()
    self.assert_(res['OK'])
    res = tAPI.getBkQuery()
    self.assertFalse(res['OK'])
    res = tAPI.getBkQueryID()
    self.assert_(res['OK'])
    self.assertFalse(res['Value'])

    # Delete the transformation
    res = tAPI.deleteTransformation()
    self.assert_(res['OK'])

  def test_getTransformations(self):
    """ Testing the selection of transformations from the database

          getTransformations
          
        This will select all the transformations associated to this test suite and remove them.
    """
    tAPI = Transformation()
    res = tAPI.getTransformations()
    self.assert_(res['OK'])
    for transDict in res['Value']:
      name = transDict['TransformationName']
      if re.search('APITestCaseTransformation',name):
        transID = transDict['TransformationID']
        tAPI = Transformation(transID)
        res = tAPI.deleteTransformation()
        self.assert_(res['OK'])
  
if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(LHCbAPITestCase)  
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
