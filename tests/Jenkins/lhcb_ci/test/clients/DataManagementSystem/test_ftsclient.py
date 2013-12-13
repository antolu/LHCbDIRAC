""" lhcb_ci.test.clients.DataManagementSystem.test_ftsclient

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""

import mock
import uuid

from DIRAC.DataManagementSystem.Client.FTSFile import FTSFile
from DIRAC.DataManagementSystem.Client.FTSJob  import FTSJob
from DIRAC.DataManagementSystem.Client.FTSSite import FTSSite

# Think about SSHElement...

import lhcb_ci.basecase


class FTSClientTest( lhcb_ci.basecase.ClientTestCase ):

  
  SUT = 'DataManagementSystem.Client.FTSClient'


  def setUp( self ):
    
    super( FTSClientTest, self ).setUp()
    
    self.ftsSites = [ FTSSite( ftsServer = 'https://fts22-t0-export.cern.ch:8443/glite-data-transfer-fts/services/FileTransfer', name = 'CERN.ch' ),
                      FTSSite( ftsServer = 'https://fts.pic.es:8443/glite-data-transfer-fts/services/FileTransfer', name = 'PIC.es' ),
                      FTSSite( ftsServer = 'https://lcgfts.gridpp.rl.ac.uk:8443/glite-data-transfer-fts/services/FileTransfer', name = 'RAL.uk' ),
                    ]

    self.ses      = [ 'CERN-USER', 'RAL-USER' ]
    self.statuses = [ 'Submitted', 'Finished', 'FinishedDirty', 'Active', 'Ready' ]

    self.submitted    = 0
    self.numberOfJobs = 10
    self.opIDs        = []

    self.ftsJobs = []
    for i in range( self.numberOfJobs ):

      opID = i % 3
      if opID not in self.opIDs:
        self.opIDs.append( opID )

      ftsJob = FTSJob()
      ftsJob.FTSGUID = str( uuid.uuid4() )
      ftsJob.FTSServer = self.ftsSites[0].FTSServer
      ftsJob.Status = self.statuses[ i % len( self.statuses ) ]
      ftsJob.OperationID = opID
      if ftsJob.Status in FTSJob.FINALSTATES:
        ftsJob.Completeness = 100
      if ftsJob.Status == 'Active':
        ftsJob.Completeness = 90
      ftsJob.SourceSE = self.ses[ i % len( self.ses ) ]
      ftsJob.TargetSE = 'PIC-USER'
      ftsJob.RequestID = 12345

      ftsFile = FTSFile()
      ftsFile.FileID = i + 1
      ftsFile.OperationID = i + 1
      ftsFile.LFN = '/a/b/c/%d' % i
      ftsFile.Size = 1000000
      ftsFile.OperationID = opID
      ftsFile.SourceSE = ftsJob.SourceSE
      ftsFile.TargetSE = ftsJob.TargetSE
      ftsFile.SourceSURL = 'foo://source.bar.baz/%s' % ftsFile.LFN
      ftsFile.TargetSURL = 'foo://target.bar.baz/%s' % ftsFile.LFN
      ftsFile.Status = 'Waiting' if ftsJob.Status != 'FinishedDirty' else 'Failed'
      ftsFile.RequestID = 12345
      ftsFile.Checksum = 'addler'
      ftsFile.ChecksumType = 'adler32'

      ftsFile.FTSGUID = ftsJob.FTSGUID
      if ftsJob.Status == 'FinishedDirty':
        ftsJob.FailedFiles = 1
        ftsJob.FailedSize = ftsFile.Size

      ftsJob.addFile( ftsFile )
      self.ftsJobs.append( ftsJob )

    self.submitted = len( [ i for i in self.ftsJobs if i.Status == 'Submitted' ] )

    #self.ftsClient = FTSClient()
    self.replicaManager = mock.Mock()
    self.replicaManager.getActiveReplicas.return_value = { 'OK'    : True,
                                                           'Value' : {
                                                                       'Successful' : 
                                                                          { '/a/b/c/1':
                                                                            { 'CERN-USER':'/aa/a/b/c/1d',
                                                                              'RAL-USER':'/bb/a/b/c/1d' },
                                                                            '/a/b/c/2':
                                                                            { 'CERN-USER':'/aa/a/b/c/2d',
                                                                              'RAL-USER':'/bb/a/b/c/2d'},
                                                                            '/a/b/c/3':
                                                                            { 'CERN-USER':'/aa/a/b/c/3d',
                                                                              'RAL-USER':'/bb/a/b/c/3d'}
                                                                          },
                                                                        'Failed': {'/a/b/c/4':'/aa/a/b/c/4d',
                                                                                   '/a/b/c/5':'/aa/a/b/c/5d'}
                                                                              }
                                                                    }    


  @lhcb_ci.basecase.timeDecorator
  def test_addAndRemoveJobs( self ):    
    
    client = self.sutCls()
    client.replicaManager = self.replicaManager
    
    for ftsJob in self.ftsJobs:
      res = client.putFTSJob( ftsJob )
      self.assertDIRACEquals( res[ 'OK' ], True, res )

    res = client.getFTSJobIDs( self.statuses )
    self.assertDIRACEquals( res[ 'OK' ], True, res )
    self.assertEqual( len( res[ 'Value' ] ), self.numberOfJobs )
    FTSjobIDs = res['Value']

    res = client.getFTSJobList( self.statuses, self.numberOfJobs )
    self.assertDIRACEquals( res[ 'OK' ], True, res )
    self.assertEqual( len( res[ 'Value' ] ), self.numberOfJobs )

    for i in FTSjobIDs:
      res = client.peekFTSJob( i )
      self.assertDIRACEquals( res[ 'OK' ], True, res )
      self.assertEqual( len( res[ 'Value' ][ 'FTSFiles' ] ), 1 )

    for i in FTSjobIDs:
      res = client.getFTSJob( i )
      self.assertDIRACEquals( res[ 'OK' ], True, res )
      self.assertEqual( len( res[ 'Value' ][ 'FTSFiles' ] ), 1 )

    res = client.getFTSFileIDs()
    self.assertDIRACEquals( res[ 'OK' ], True, res )
    FTSfileIDs = res[ 'Value' ]

    res = client.getFTSFileList()
    self.assertDIRACEquals( res[ 'OK' ], True, res )

    for i in FTSfileIDs:
      res = client.peekFTSFile( i )
      self.assertDIRACEquals( res[ 'OK' ], True, res )

    for i in FTSfileIDs:
      res = client.getFTSFile( i )
      self.assertDIRACEquals( res[ 'OK' ], True, res )

    for i in FTSjobIDs:
      res = client.deleteFTSJob( i )
      self.assertDIRACEquals( res[ 'OK' ], True, res )

    for i in self.opIDs:
      res = client.deleteFTSFiles( i )
      self.assertDIRACEquals( res[ 'OK' ], True, res )
  
  test_addAndRemoveJobs.smoke  = 1
  test_addAndRemoveJobs.client = 1


#...............................................................................
#EOF
