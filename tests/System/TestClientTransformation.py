""" This is using real backend - to be tested!
    Go on par with the one in TestDIRAC
"""


#import unittest, time, re
#from LHCbDIRAC.TransformationSystem.Client.Transformation import Transformation

#class TestClientTransformationTestCase(unittest.TestCase):
#
#  def test_setBkQuery(self):
#    """ Test the manipulation of BK Queries
#
#          testBkQuery()
#          setBkQuery()
#          getBkQuery()
#          getBkQueryID()
#          deleteTransformationBkQuery()
#
#        Tests the creation of a transformation with a BK query to ensure the BKQuery and corresponding ID is stored correctly. 
#        A new transformation object is created with the transID just created and the BK parameters tested.
#        The transformation BK query is then removed and the transformation obect is checked to ensure this is done correctly.
#    """
#    # Create the transformation object
#    oTrans = Transformation()
#    transName = 'LHCbAPITestCaseTransformation-%s' % time.strftime("%Y%m%d-%H:%M:%S")
#    res = oTrans.setTransformationName(transName)
#    self.assert_(res['OK'])
#    description = 'Test transforamtion description'
#    res = oTrans.setDescription(description)
#    self.assert_(res['OK'])
#    longDescription = 'Test transforamtion long description'
#    res = oTrans.setLongDescription(longDescription)
#    self.assert_(res['OK'])
#    res = oTrans.setType('MCSimulation')
#    self.assert_(res['OK'])
#
#    # Create a dummy bk query, test it and obtain the list of files it returns (expected to be 0)
#    bkQuery = {'ProductionID':9999999,'FileType':'DST'}
#    res = oTrans.testBkQuery(bkQuery)
#    self.assert_(res['OK'])
#    bkFiles = res['Value']
#    res = oTrans.setBkQuery(bkQuery)
#    self.assert_(res['OK'])
#    res = oTrans.getBkQuery()
#    self.assert_(res['OK'])
#    self.assertEqual(res['Value'],bkQuery)
#
#    # Create the transformation and obtain the BkQueryID and TransformationID
#    res = oTrans.addTransformation()
#    self.assert_(res['OK'])
#    res = oTrans.getBkQueryID()
#    self.assert_(res['OK'])
#    bkQueryID = res['Value']
#    transID = oTrans.paramValues['TransformationID']
#
#    # Create a new transformation object with the transID and retrieve the BkQuery and BkqueryID (coming from the DB)
#    oTrans = Transformation(transID)
#    res = oTrans.getBkQuery()
#    self.assert_(res['OK'])
#    res = oTrans.testBkQuery(bkQuery)
#    self.assert_(res['OK'])
#    self.assertEqual(res['Value'],bkFiles)
#    res = oTrans.getBkQueryID()
#    self.assert_(res['OK'])
#    self.assertEqual(res['Value'],bkQueryID)
#  
#    # Remove the BkQuery associated to the transformation and test it is removed properly
#    res = oTrans.deleteTransformationBkQuery()
#    self.assert_(res['OK'])
#    res = oTrans.getBkQuery()
#    self.assertFalse(res['OK'])
#    res = oTrans.getBkQueryID()
#    self.assert_(res['OK'])
#    self.assertFalse(res['Value'])
#
#    # Delete the transformation
#    res = oTrans.deleteTransformation()
#    self.assert_(res['OK'])
#
#  def test_getTransformations(self):
#    """ Testing the selection of transformations from the database
#
#          getTransformations
#          
#        This will select all the transformations associated to this test suite and remove them.
#    """
#    oTrans = Transformation()
#    res = oTrans.getTransformations()
#    self.assert_(res['OK'])
#    for transDict in res['Value']:
#      name = transDict['TransformationName']
#      if re.search('APITestCaseTransformation',name):
#        transID = transDict['TransformationID']
#        oTrans = Transformation(transID)
#        res = oTrans.deleteTransformation()
#        self.assert_(res['OK'])
#  
#if __name__ == '__main__':
#  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestClientTransformationTestCase)  
#  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
