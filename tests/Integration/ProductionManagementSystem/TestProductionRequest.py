""" This is a test of the chain
    ProductionRequestHandler -> ProductionRequestDB

    It supposes that the DB is present, and that the service is running

    It also supposes that the DB is empty!
"""
import unittest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.Core.DISET.RPCClient                                 import RPCClient

class TestProductionRequestTestCase( unittest.TestCase ):

  def setUp( self ):
    self.reqClient = RPCClient( 'ProductionManagement/ProductionRequest' )

  def tearDown( self ):
    pass


class TestProductionRequestTestCaseChain( TestProductionRequestTestCase ):

  def test_mix( self ):

    # this, does not exist yet
    res = self.reqClient.getProductionList( 1L )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], [] )

    # add
    res = self.reqClient.createProductionRequest( {'ParentID':'', 'MasterID':'', 'RequestAuthor': 'adminusername',
                                                   'RequestName': 'RequestName', 'RequestType': 'RequestType',
                                                   'RequestState': 'New', 'RequestPriority': '2b', 'RequestPDG':'',
                                                   'SimCondition':'Beam3500GeV-VeloClosed-MagUp', 'SimCondID':'429215',
                                                   'SimCondDetail':'BlahBlahBlah',
                                                   'ProPath':'Blup', 'ProID':'', 'ProDetail':'BlaaaaaahBlahBlah',
                                                   'EventType':'900000', 'NumberOfEvents':'-1',
                                                   'Description':'Description', 'Comments':'Comments',
                                                   'Inform':'', 'RealNumberOfEvents':'', 'IsModel':0, 'Extra':''} )
    self.assert_( res['OK'] )
    firstReq = res['Value']
  
    res = self.reqClient.createProductionRequest( {'ParentID':'', 'MasterID':'', 'RequestAuthor': 'adminusername',
                                                   'RequestName': 'RequestName - new', 'RequestType': 'RequestType',
                                                   'RequestState': 'New', 'RequestPriority': '2b', 'RequestPDG':'',
                                                   'SimCondition':'Beam3500GeV-VeloClosed-MagUp', 'SimCondID':'429215',
                                                   'SimCondDetail':'BlahBlahBlah',
                                                   'ProPath':'Blup', 'ProID':'', 'ProDetail':'BlaaaaaahBlahBlah',
                                                   'EventType':'900000', 'NumberOfEvents':'-1',
                                                   'Description':'Description', 'Comments':'Comments',
                                                   'Inform':'', 'RealNumberOfEvents':'', 'IsModel':0, 'Extra':''} )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], firstReq + 1 )

    # Adding production
    res = self.reqClient.addProductionToRequest( {'ProductionID': 123,
                                                  'RequestID':firstReq,
                                                  'Used':0,
                                                  'BkEvents':0} )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], 123 )
    res = self.reqClient.removeProductionFromRequest( 123 )

    res = self.reqClient.addProductionToRequest( {'ProductionID': 456,
                                                  'RequestID':firstReq + 1,
                                                  'Used':1,
                                                  'BkEvents':0} )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], 456 )

    # delete
    res = self.reqClient.deleteProductionRequest( firstReq )
    self.assertFalse( res['OK'] )
    res = self.reqClient.deleteProductionRequest( firstReq + 1 )
    self.assert_( res['OK'] )



if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestProductionRequestTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestProductionRequestTestCaseChain ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
