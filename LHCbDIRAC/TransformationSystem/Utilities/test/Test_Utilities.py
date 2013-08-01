from mock import Mock
import unittest
from LHCbDIRAC.TransformationSystem.Client.FileReport import FileReport

class UtilitiesTestCase( unittest.TestCase ):

  def setUp( self ):
    self.transClient = Mock()
    self.transClient.setFileStatusForTransformation.return_value = {'OK':True}
    self.fr = FileReport()

  def tearDown( self ):
    pass

class FileReportSuccess( UtilitiesTestCase ):
  def test_fr( self ):
    self.assertEqual( self.fr.statusDict, {} )

    res = self.fr.setFileStatus( 1, '/aa.1.txt', 'Unused' )
    self.assert_( res['OK'] )
    self.assertEqual( self.fr.statusDict, {'/aa.1.txt': 'Unused'} )

    res = self.fr.setFileStatus( 1, '/aa.1.txt', 'Assigned' )
    self.assert_( res['OK'] )
    self.assertEqual( self.fr.statusDict, {'/aa.1.txt': 'Assigned'} )
    res = self.fr.setFileStatus( 1, '/aa.2.txt', 'Assigned' )
    self.assert_( res['OK'] )
    self.assertEqual( self.fr.statusDict, {'/aa.1.txt': 'Assigned', '/aa.2.txt': 'Assigned'} )

    res = self.fr.setFileStatus( 1, '/aa.1.txt', 'Processed', )
    res = self.fr.setFileStatus( 1, '/aa.1.txt', 'Moved' )
    self.assertFalse( res['OK'] )

    res = self.fr.setFileStatus( 1, '/aa.1.txt', 'Unused' )
    self.assert_( res['OK'] )

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( UtilitiesTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( FileReportSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
