import unittest
from mock import Mock

from LHCbDIRAC.DataManagementSystem.Client.ConsistencyChecks import ConsistencyChecks

class UtilitiesTestCase( unittest.TestCase ):

  def setUp( self ):
    self.bkClientMock = Mock()

    self.cc = ConsistencyChecks( bkClient = self.bkClientMock )
    self.cc.fileType = 'SEMILEPTONIC.DST'
    self.maxDiff = None

  def tearDown( self ):
    pass

class ConsistencyChecksSuccess( UtilitiesTestCase ):

  def test__selectByFileType( self ):
    lfnDict = {'/lhcb/00020566_00009119_1.Semileptonic.dst':
               ['/lhcb/00020567_00001373_1.semileptonic.dst',
                '/bb/pippo/aa.dst',
                '/lhcb/DaVinci_00020567_00001373_1.log'
                ],
               '/lhcb/00020566_00009120_1.Semileptonic.dst':
               ['/lhcb/00020567_00001373_1.semileptonic.dst',
                '/bb/pippo/aa.dst',
                '/lhcb/DaVinci_00020567_00001373_1.log'
                ]}

    res = self.cc._selectByFileType( lfnDict )

    lfnDictExpected = {'/lhcb/00020566_00009119_1.Semileptonic.dst':
                       ['/lhcb/00020567_00001373_1.semileptonic.dst'],
                       '/lhcb/00020566_00009120_1.Semileptonic.dst':
                       ['/lhcb/00020567_00001373_1.semileptonic.dst']}
    self.assertEqual( res, lfnDictExpected )

    lfnDict = {'aa.raw': ['/bb/pippo/aa.dst', '/bb/pippo/aa.log']}
    res = self.cc._selectByFileType( lfnDict )
    lfnDictExpected = {}
    self.assertEqual( res, lfnDictExpected )

  def test_getDescendants( self ):
    self.bkClientMock.getFileDescendants.return_value = {'OK': True,
                                                         'Value': {'Failed': [],
                                                                   'NotProcessed': [],
                                                                   'Successful': {'aa.raw': ['bb.raw', 'bb.log']}}}

    filesWithDescendants, filesWithoutDescendants, filesWitMultipleDescendants = self.cc.getDescendants( ['aa.raw'] )
    self.assertEqual( filesWithDescendants, [] )
    self.assertEqual( filesWithoutDescendants, ['aa.raw'] )
    self.assertEqual( filesWitMultipleDescendants, [] )



if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( UtilitiesTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ConsistencyChecksSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

