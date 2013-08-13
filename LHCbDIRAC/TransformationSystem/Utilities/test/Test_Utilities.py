from mock import Mock
import unittest

from LHCbDIRAC.TransformationSystem.Utilities.StateMachine import TransformationFilesStateMachine

class UtilitiesTestCase( unittest.TestCase ):

  def setUp( self ):
    self.transClient = Mock()

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

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( UtilitiesTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( tsfmSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
