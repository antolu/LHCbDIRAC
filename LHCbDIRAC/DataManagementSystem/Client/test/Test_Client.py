import unittest
from mock import Mock

from LHCbDIRAC.DataManagementSystem.Client.ConsistencyChecks import ConsistencyChecks

class UtilitiesTestCase( unittest.TestCase ):

  def setUp( self ):
    self.bkClientMock = Mock()

    self.cc = ConsistencyChecks( transClient = Mock(), rm = Mock(), bkClient = self.bkClientMock )
    self.cc.fileType = ['SEMILEPTONIC.DST', 'LOG']
    self.cc.fileTypesExcluded = ['LOG']
    self.cc.prod = 0
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

  def test__getFileTypesCount( self ):
    lfnDict = {'aa.raw': ['/bb/pippo/aa.dst', '/bb/pippo/aa.log']}
    res = self.cc._getFileTypesCount( lfnDict )
    resExpected = {'aa.raw': {'dst':1, 'log':1}}
    self.assertEqual( res, resExpected )

    lfnDict = {'aa.raw': ['/bb/pippo/aa.dst', '/bb/pippo/cc.dst', '/bb/pippo/aa.log']}
    res = self.cc._getFileTypesCount( lfnDict )
    resExpected = {'aa.raw': {'dst':2, 'log':1}}
    self.assertEqual( res, resExpected )

    lfnDict = {'aa.raw': ['/bb/pippo/aa.t.dst', '/bb/pippo/cc.t.dst', '/bb/pippo/aa.log']}
    res = self.cc._getFileTypesCount( lfnDict )
    resExpected = {'aa.raw': {'t.dst':2, 'log':1}}
    self.assertEqual( res, resExpected )

    lfnDict = {'aa.raw': ['/bb/pippo/aa.t.dst', '/bb/pippo/cc.t.dst', '/bb/pippo/aa.log'],
               'cc.raw': ['/bb/pippo/aa.dst', '/bb/pippo/aa.log']}
    res = self.cc._getFileTypesCount( lfnDict )
    resExpected = {'aa.raw': {'t.dst':2, 'log':1}, 'cc.raw': {'dst':1, 'log':1}}
    self.assertEqual( res, resExpected )

  def test_getDescendants( self ):
    self.bkClientMock.getFileDescendants.return_value = {'OK': True,
                                                         'Value': {'Failed': [],
                                                                   'NotProcessed': [],
                                                                   'Successful': {'aa.raw': ['bb.raw', 'bb.log']}}}

    res = self.cc.getDescendants( ['aa.raw'] )
    filesWithDescendants, filesWithoutDescendants, filesWitMultipleDescendants, descendants = res
    self.assertEqual( filesWithDescendants, [] )
    self.assertEqual( filesWithoutDescendants, ['aa.raw'] )
    self.assertEqual( filesWitMultipleDescendants, [] )
    self.assertEqual( descendants, [] )


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( UtilitiesTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ConsistencyChecksSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

