import unittest
from mock import Mock

from LHCbDIRAC.DataManagementSystem.Client.ConsistencyChecks import ConsistencyChecks

class UtilitiesTestCase( unittest.TestCase ):

  def setUp( self ):
    self.bkClientMock = Mock()
    self.bkClientMock.getFileDescendants.return_value = {'OK': True,
                                                         'Value': {'Failed': [],
                                                                   'NotProcessed': [],
                                                                   'Successful': {'aa.raw': ['bb.raw', 'bb.log']},
                                                                   'WithMetadata': {'aa.raw': {'bb.raw': {'FileType': 'RAW',
                                                                                                          'RunNumber': 97019},
                                                                                               'bb.log': {'FileType': 'LOG'}
                                                                                               }
                                                                                    }
                                                                   }
                                                         }
    self.bkClientMock.getFileMetadata.return_value = {'OK': True,
                                                      'Value': {'Successful':{'aa.raw': {'FileType': 'RAW',
                                                                                         'RunNumber': 97019},
                                                                              'bb.raw': {'FileType': 'RAW',
                                                                                         'RunNumber': 97019},
                                                                              'dd.raw': {'FileType': 'RAW',
                                                                                         'RunNumber': 97019},
                                                                              'bb.log': {'FileType': 'LOG'},
                                                                              '/bb/pippo/aa.dst':{'FileType': 'DST'},
                                                                              '/lhcb/1_2_1.Semileptonic.dst':{'FileType': 'SEMILEPTONIC.DST'},
                                                                              '/lhcb/1_1.semileptonic.dst':{'FileType': 'SEMILEPTONIC.DST'}
                                                                              },
                                                                'Failed':{}
                                                                }
                                                      }

    self.rmMock = Mock()
    self.rmMock.getReplicas.return_value = {'OK': True, 'Value':{'Successful':{'aa.raw':'metadataPippo'},
                                                                  'Failed':{}}}

    self.cc = ConsistencyChecks( transClient = Mock(), rm = self.rmMock, bkClient = self.bkClientMock )
    self.cc.fileType = ['SEMILEPTONIC.DST', 'LOG', 'RAW']
    self.cc.fileTypesExcluded = ['LOG']
    self.cc.prod = 0
    self.maxDiff = None

  def tearDown( self ):
    pass

class ConsistencyChecksSuccess( UtilitiesTestCase ):

  def test__selectByFileType( self ):
    lfnDict = {'aa.raw': {'bb.raw':{'FileType': 'RAW', 'RunNumber': 97019},
                          'bb.log':{'FileType': 'LOG'},
                          '/bb/pippo/aa.dst':{'FileType': 'DST'},
                          '/lhcb/1_2_1.Semileptonic.dst':{'FileType': 'SEMILEPTONIC.DST'}},
               'cc.raw': {'dd.raw':{'FileType': 'RAW', 'RunNumber': 97019},
                          'bb.log':{'FileType': 'LOG'},
                          '/bb/pippo/aa.dst':{'FileType': 'LOG'},
                          '/lhcb/1_1.semileptonic.dst':{'FileType': 'SEMILEPTONIC.DST'}}
               }

    res = self.cc._selectByFileType( lfnDict )

    lfnDictExpected = {'aa.raw': ['/lhcb/1_2_1.Semileptonic.dst', 'bb.raw'],
                       'cc.raw': ['dd.raw', '/lhcb/1_1.semileptonic.dst']}
    self.assertEqual( res, lfnDictExpected )

    lfnDict = {'aa.raw': {'/bb/pippo/aa.dst':{'FileType': 'LOG'},
                          'bb.log':{'FileType': 'LOG'}
                          }
               }
    res = self.cc._selectByFileType( lfnDict )
    lfnDictExpected = {}
    self.assertEqual( res, lfnDictExpected )

  def test__getFileTypesCount( self ):
    lfnDict = {'aa.raw': ['/bb/pippo/aa.dst', '/bb/pippo/aa.log']}
    res = self.cc._getFileTypesCount( lfnDict )
    resExpected = {'aa.raw': {'DST':1, 'LOG':1}}
    self.assertEqual( res, resExpected )

    lfnDict = {'aa.raw': ['/bb/pippo/aa.dst', '/bb/pippo/cc.dst', '/bb/pippo/aa.log']}
    res = self.cc._getFileTypesCount( lfnDict )
    resExpected = {'aa.raw': {'DST':2, 'LOG':1}}
    self.assertEqual( res, resExpected )

    lfnDict = {'aa.raw': ['/bb/pippo/aa.t.dst', '/bb/pippo/cc.t.dst', '/bb/pippo/aa.log']}
    res = self.cc._getFileTypesCount( lfnDict )
    resExpected = {'aa.raw': {'T.DST':2, 'LOG':1}}
    self.assertEqual( res, resExpected )

    lfnDict = {'aa.raw': ['/bb/pippo/aa.t.dst', '/bb/pippo/cc.t.dst', '/bb/pippo/aa.log'],
               'cc.raw': ['/bb/pippo/aa.dst', '/bb/pippo/aa.log']}
    res = self.cc._getFileTypesCount( lfnDict )
    resExpected = {'aa.raw': {'T.DST':2, 'LOG':1}, 'cc.raw': {'DST':1, 'LOG':1}}
    self.assertEqual( res, resExpected )

  def test_getDescendants( self ):
    res = self.cc.getDescendants( ['aa.raw'] )
    filesWithDescendants, filesWithoutDescendants, filesWitMultipleDescendants, descendants = res
    self.assertEqual( filesWithDescendants, {'aa.raw':['bb.raw']} )
    self.assertEqual( filesWithoutDescendants, {} )
    self.assertEqual( filesWitMultipleDescendants, {} )
    self.assertEqual( descendants, ['bb.raw'] )


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( UtilitiesTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ConsistencyChecksSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

