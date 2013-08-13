from mock import Mock
import unittest

from LHCbDIRAC.TransformationSystem.Utilities.StateMachine import TransformationFilesStateMachine
# from LHCbDIRAC.TransformationSystem.Client.FileReport import FileReport

class UtilitiesTestCase( unittest.TestCase ):

  def setUp( self ):
    self.transClient = Mock()
#    self.transClient.setFileStatusForTransformation.return_value = {'OK':True}
#    self.fr = FileReport()

  def tearDown( self ):
    pass

class tsfmSuccess( UtilitiesTestCase ):
  def test_setState( self ):
    tsfm = TransformationFilesStateMachine( 'Unused' )
    res = tsfm.setState( 'Assigned' )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], 'Assigned' )

    tsfm = TransformationFilesStateMachine( 'Unused' )
    res = tsfm.setState( 'Unused' )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], 'Unused' )

    tsfm = TransformationFilesStateMachine( 'Unused' )
    res = tsfm.setState( 'MissingInFC' )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], 'MissingInFC' )

    tsfm = TransformationFilesStateMachine( 'Unused' )
    res = tsfm.setState( 'Processed' )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], 'Assigned' )

    tsfm = TransformationFilesStateMachine( 'Unused' )
    res = tsfm.setState( 'Processed' )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], 'Assigned' )
    res = tsfm.setState( 'Processed' )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], 'Processed' )

class tsfmFailure( UtilitiesTestCase ):
  def test_setState( self ):
    tsfm = TransformationFilesStateMachine( 'Unused' )
    res = tsfm.setState( 'Assigne' )
    self.assertFalse( res['OK'] )

    tsfm = TransformationFilesStateMachine( 'Processed' )
    res = tsfm.setState( 'Assigne' )
    self.assertFalse( res['OK'] )

    tsfm = TransformationFilesStateMachine( 'Processed' )
    res = tsfm.setState( 'Processed' )
    self.assert_( res['OK'] )

    tsfm = TransformationFilesStateMachine( 'Processed' )
    res = tsfm.setState( 'Unused' )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], 'Processed' )

    tsfm = TransformationFilesStateMachine( 'MaxReset' )
    res = tsfm.setState( 'Processed' )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], 'MaxReset' )

# class FileReportSuccess( UtilitiesTestCase ):
#  def test_fr( self ):
#    self.assertEqual( self.fr.statusDict, {} )
#
#    res = self.fr.setFileStatus( 1, '/aa.1.txt', 'Unused' )
#    self.assert_( res['OK'] )
#    self.assertEqual( self.fr.statusDict, {'/aa.1.txt': 'Unused'} )
#
#    res = self.fr.setFileStatus( 1, '/aa.1.txt', 'Assigned' )
#    self.assert_( res['OK'] )
#    self.assertEqual( self.fr.statusDict, {'/aa.1.txt': 'Assigned'} )
#    res = self.fr.setFileStatus( 1, '/aa.2.txt', 'Assigned' )
#    self.assert_( res['OK'] )
#    self.assertEqual( self.fr.statusDict, {'/aa.1.txt': 'Assigned', '/aa.2.txt': 'Assigned'} )
#
#    res = self.fr.setFileStatus( 1, '/aa.1.txt', 'Processed', )
#    res = self.fr.setFileStatus( 1, '/aa.1.txt', 'Moved' )
#    self.assertFalse( res['OK'] )
#
#    res = self.fr.setFileStatus( 1, '/aa.1.txt', 'Unused' )
#    self.assert_( res['OK'] )

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( UtilitiesTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( tsfmSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
