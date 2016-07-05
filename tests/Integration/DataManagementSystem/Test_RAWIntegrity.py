""" Integration test for the RAWIntegritySystem"""

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import unittest
import time
import datetime
import random
import mock

from DIRAC import S_OK, S_ERROR
from LHCbDIRAC.DataManagementSystem.DB.RAWIntegrityDB import RAWIntegrityDB
from LHCbDIRAC.DataManagementSystem.Agent.RAWIntegrityAgent import RAWIntegrityAgent


class RAWIntegrityDBTest( unittest.TestCase ):
  """ Tests for the DB part of the RAWIntegrity system
  """

  def setUp( self ):
    super( RAWIntegrityDBTest, self ).setUp()
    self.db = RAWIntegrityDB()


  def tearDown( self ):

    # Only one file is before 'now' and 'after'
    res = self.db.selectFiles( {} )
    lfns = [fTuple[0] for fTuple in res['Value']]

    # clean after us
    for lfn in lfns:
      self.db.removeFile( lfn )






  def test_01_setupDB( self ):
    """ Test table creations"""

    # At first, there should be no table
    res = self.db.showTables()
    self.assertTrue( res['OK'], res )
    self.assertEqual( res['Value'], [] )

    # Lets create them
    res = self.db._checkTable()
    self.assertTrue( res['OK'], res )
    # and check they are now here
    res = self.db.showTables()
    self.assertTrue( res['OK'], res )
    self.assertEqual( sorted( res['Value'] ), sorted( ['Files', 'LastMonitor'] ) )

    # Lets create them again, there should be no error
    res = self.db._checkTable()
    self.assertTrue( res['OK'], res )



  def test_02_lastMonitorTime( self ):
    """ Test the last monitor time function"""

    # Just after creation, we insert initial timestamp so no error
    res = self.db.getLastMonitorTimeDiff()
    self.assertTrue( res['OK'], res )

    # set the monitor time
    res = self.db.setLastMonitorTime()
    self.assertTrue( res['OK'], res )

    # we wait a bit, and check that the difference is correct
    # we expect not more than 1 second delay
    sleepTime = 3
    time.sleep( sleepTime )
    res = self.db.getLastMonitorTimeDiff()
    self.assertTrue( res['OK'], res )
    self.assertTrue( sleepTime <= res['Value'] <= sleepTime + 1, res['Value'] )


  def test_03_fileManipulation( self ):
    """ Testing all the file manipulation operations"""

    # There should be no active files so far
    res = self.db.getActiveFiles()
    self.assertTrue( res['OK'], res )
    self.assertEqual( res['Value'], {} )


    testFile = { 'LFN' : 'lfn', 'PFN':'pfn',
                 'Size':123, 'SE':'se',
                 'GUID':'guid', 'Checksum':'checksum'}

    # adding a file
    res = self.db.addFile( testFile['LFN'], testFile['PFN'],
                           testFile['Size'], testFile['SE'],
                           testFile['GUID'], testFile['Checksum'] )
    self.assertTrue( res['OK'], res )

    sleepTime = 2
    time.sleep( sleepTime )

    # There should be now one active file
    res = self.db.getActiveFiles()
    self.assertTrue( res['OK'], res )
    self.assertEqual( len( res['Value'] ), 1 )
    self.assertTrue( testFile['LFN'] in res['Value'], res )

    activeFile = res['Value'][testFile['LFN']]

    for attribute in ['PFN', 'Size', 'SE', 'GUID', 'Checksum']:
      self.assertEqual( testFile[attribute], activeFile[attribute] )

    self.assertTrue( sleepTime <= activeFile['WaitTime'] <= sleepTime + 1 )

    # Change the file status to Done
    res = self.db.setFileStatus( testFile['LFN'], 'Done' )
    self.assertTrue( res['OK'], res )
    self.assertEqual( res['Value'], 1 )

    # The file should not be returned when asking for active files anymore
    res = self.db.getActiveFiles()
    self.assertTrue( res['OK'], res )
    self.assertEqual( res['Value'], {} )

    # Change the file status back to Active
    # It should not work
    res = self.db.setFileStatus( testFile['LFN'], 'Active' )
    self.assertTrue( res['OK'], res )
    self.assertEqual( res['Value'], 0 )

    # The file should not be back
    res = self.db.getActiveFiles()
    self.assertEqual( res['Value'], {} )

    # Remove the file
    res = self.db.removeFile( testFile['LFN'] )
    self.assertTrue( res['OK'], res )


    # adding the file back
    # (no need to test that its visible, we just did it)
    res = self.db.addFile( testFile['LFN'], testFile['PFN'],
                           testFile['Size'], testFile['SE'],
                           testFile['GUID'], testFile['Checksum'] )
    self.assertTrue( res['OK'], res )

    # remove the file
    res = self.db.removeFile( testFile['LFN'] )
    self.assertTrue( res['OK'], res )
    self.assertEqual( res['Value'], 1 )

    # There should be no file
    res = self.db.getActiveFiles()
    self.assertEqual( res['Value'], {} )


    # Adding two time the same files
    # It should work
    res = self.db.addFile( testFile['LFN'], testFile['PFN'],
                           testFile['Size'], testFile['SE'],
                           testFile['GUID'], testFile['Checksum'] )
    self.assertTrue( res['OK'], res )
    res = self.db.addFile( testFile['LFN'], testFile['PFN'],
                           testFile['Size'], testFile['SE'],
                           testFile['GUID'], testFile['Checksum'] )
    self.assertTrue( res['OK'], res )


    # We should get only one file, so processed only once
    res = self.db.getActiveFiles()
    self.assertTrue( res['OK'], res )
    self.assertEqual( len( res['Value'] ), 1 )

    res = self.db.removeFile( testFile['LFN'] )
    self.assertTrue( res['OK'], res )
    self.assertEqual( res['Value'], 2 )

    # Setting status of a non existing file
    res = self.db.setFileStatus( 'fake', 'Done' )
    self.assertTrue( res['OK'], res )
    self.assertEqual( res['Value'], 0 )

    # Removing a non existing file
    res = self.db.removeFile( 'fake' )
    self.assertTrue( res['OK'], res )
    self.assertEqual( res['Value'], 0 )

  def test_04_webQueries( self ):
    """ Test all the web related methods"""

    # The DB should be empty now
    res = self.db.getGlobalStatistics()
    self.assertTrue( res['OK'], res )
    self.assertEqual( res['Value'], {} )

    res = self.db.getFileSelections()
    self.assertTrue( res['OK'], res )
    self.assertEqual( res['Value'], {'StorageElement' : [], 'Status' : []} )



    # Adding two files Assigned, 1 Failed and 1 done
    for i in xrange( 1, 5 ):
      res = self.db.addFile( 'lfn%s' % i, 'pfn%s' % i,
                             5 - i, 'se%s' % ( i % 2 ),
                             'GUID%s' % i, 'Checksum%s' % i )
      self.assertTrue( res['OK'], res )

    res = self.db.setFileStatus( 'lfn3', 'Done' )
    self.assertTrue( res['OK'], res )
    res = self.db.setFileStatus( 'lfn4', 'Failed' )
    self.assertTrue( res['OK'], res )


    res = self.db.getGlobalStatistics()
    self.assertTrue( res['OK'], res )
    self.assertEqual( res['Value'], { 'Active' : 2, 'Done' : 1, 'Failed' : 1} )

    res = self.db.getFileSelections()
    self.assertTrue( res['OK'], res )
    self.assertEqual( sorted( res['Value']['StorageElement'] ), sorted( ['se0', 'se1'] ) )
    self.assertEqual( sorted( res['Value']['Status'] ), sorted( ['Done', 'Failed', 'Active'] ) )

    # Do some selection test
    res = self.db.selectFiles( {'StorageElement' : 'se0'} )
    self.assertTrue( res['OK'], res )
    # they should be sorted by LFN by default
    returnedLfns = [ret[0] for ret in res['Value']]
    self.assertEqual( returnedLfns, ['lfn2', 'lfn4'] )


    res = self.db.selectFiles( {'StorageElement' : 'se0', 'PFN' : 'pfn2'} )
    self.assertTrue( res['OK'], res )
    returnedLfns = [ret[0] for ret in res['Value']]
    self.assertEqual( returnedLfns, ['lfn2'] )

    # Impossible condition
    res = self.db.selectFiles( {'StorageElement' : 'se1', 'PFN' : 'pfn2'} )
    self.assertTrue( res['OK'], res )
    returnedLfns = [ret[0] for ret in res['Value']]
    self.assertEqual( returnedLfns, [] )

    # limit to 1
    res = self.db.selectFiles( {'StorageElement' : 'se0'}, limit = 1 )
    self.assertTrue( res['OK'], res )
    # they should be sorted by LFN by default
    returnedLfns = [ret[0] for ret in res['Value']]
    self.assertEqual( returnedLfns, ['lfn2'] )

    # Same selection, sorted by size
    res = self.db.selectFiles( {'StorageElement' : 'se0'}, orderAttribute = 'Size' )
    self.assertTrue( res['OK'], res )
    returnedLfns = [ret[0] for ret in res['Value']]
    self.assertEqual( returnedLfns, ['lfn4', 'lfn2'] )


    # Test the time based selections
    time.sleep( 1 )
    now = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
    sleepTime = 2
    time.sleep( sleepTime )

    res = self.db.addFile( 'lfn6', 'pfn6', 6, 'se1', 'GUID6', 'Checksum6' )
    self.assertTrue( res['OK'], res )

    # select the old files with no conditions
    res = self.db.selectFiles( {}, older = now )
    self.assertTrue( res['OK'], res )
    returnedLfns = [ret[0] for ret in res['Value']]
    self.assertEqual( returnedLfns, ['lfn%i' % i for i in xrange( 1, 5 )] )

    # select the new file
    res = self.db.selectFiles( {}, newer = now )
    self.assertTrue( res['OK'], res )
    returnedLfns = [ret[0] for ret in res['Value']]
    self.assertEqual( returnedLfns, ['lfn6'] )


    # add some more
    time.sleep( 1 )
    after = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
    time.sleep( sleepTime )

    res = self.db.addFile( 'lfn7', 'pfn7', 7, 'se1', 'GUID7', 'Checksum7' )
    self.assertTrue( res['OK'], res )

    # We should now have two files after 'now'
    res = self.db.selectFiles( {}, newer = now )
    self.assertTrue( res['OK'], res )
    returnedLfns = [ret[0] for ret in res['Value']]
    self.assertEqual( returnedLfns, ['lfn6', 'lfn7'] )


    # Only one file is before 'now' and 'after'
    res = self.db.selectFiles( {}, newer = now, older = after )
    self.assertTrue( res['OK'], res )
    returnedLfns = [ret[0] for ret in res['Value']]
    self.assertEqual( returnedLfns, ['lfn6'] )

    # clean after us
    for i in xrange( 1, 8 ):
      res = self.db.removeFile( 'lfn%s' % i )
      self.assertTrue( res['OK'], res )




  def test_05_perf( self ):
    """ Performance tests

        For reminder, this is more ore less
        the timing that were obtained on a super crapy
        virtualMachine. Let's hope we never go above them..
        criticalInsertTime = 34
        criticalRetrieveTime = 1
        criticalUpdateTime = 5
        criticalRemoveTime = 30
     """

    nbFiles = 5000


    # Inserting files
    startTime = time.time()
    for i in xrange( nbFiles ):
      res = self.db.addFile( 'lfn%s' % i, 'pfn%s' % i,
                             i, 'se%s' % ( i % 2 ),
                             'GUID%s' % i, 'Checksum%s' % i )
      self.assertTrue( res['OK'], res )
    insertTime = time.time() - startTime

    # getting all of them
    startTime = time.time()
    res = self.db.getActiveFiles()
    self.assertTrue( res['OK'], res )
    self.assertEqual( len( res['Value'] ), nbFiles )
    getFileTime = time.time() - startTime

    # Setting some of them
    startTime = time.time()
    rndIds = set()
    for _ in xrange( nbFiles / 10 ):
      rndId = random.randint( 1, nbFiles )
      rndIds.add( rndId )
      self.db.setFileStatus( 'lfn%s' % rndId, 'Done' )
      self.assertTrue( res['OK'], res )
    updateStatusTime = time.time() - startTime


    # getting less of them
    startTime = time.time()
    res = self.db.getActiveFiles()
    self.assertTrue( res['OK'], res )
    self.assertEqual( len( res['Value'] ), nbFiles - len( rndIds ) )
    getFileTime2 = time.time() - startTime

    # deleting all of them
    startTime = time.time()
    for i in xrange( 1, nbFiles ):
      res = self.db.removeFile( 'lfn%s' % i )
      self.assertTrue( res['OK'], res )
    removeTime = time.time() - startTime

    print "Performance result"
    print "Inserting %s files: %s" % ( nbFiles, insertTime )
    print "Getting all active files: %s" % getFileTime
    print "Updating %s status: %s" % ( nbFiles / 10, updateStatusTime )
    print "Getting again active files: %s" % getFileTime2
    print "Removing all files: %s" % removeTime



class RAWIntegrityAgentTest( unittest.TestCase ):
  """ Tests for the DB part of the RAWIntegrity system
  """



  class mock_FileCatalog( object ):
    """ Fake FC that keeps track of all the actions done on it
        and reply based on teh lfn name
    """
    def __init__( self, *args, **kwargs ):
      self.successfullAdd = []
      self.failedAdd = []

    def removeCatalog( self, _catalogName ):
      return S_OK()

    def addFile( self, lfns ):
      successful = {}
      failed = {}
      for lfn in lfns:
        if '/addFile/' in lfn:
          failed[lfn] = "/addFile/ in lfn"
          self.failedAdd.append( lfn )
        elif '/error_addFile/' in lfn:
          self.failedAdd.append( lfn )
          return S_ERROR( 'One LFN with error_addFile' )
        else:
          self.successfullAdd.append( lfn )
          successful[lfn] = "Youpee"
      return S_OK( {'Successful':successful, 'Failed':failed} )

  class mock_gMonitor( object ):
    """ Fake the gMonitor and keep the counters """

    OP_SUM = OP_MEAN = OP_ACUM = None

    def __init__( self, *args, **kwargs ):
      self.counters = {}

    def registerActivity( self, counterName, *args, **kwargs ):
      """ Register counter"""
      self.counters[counterName] = []

    def addMark( self, counterName, value ):
      """ Add Mark"""
      self.counters[counterName].append( value )


  class mock_reqClient( object ):
    """ Fake the requestClient and keep the track of the requests """

    def __init__( self, *args, **kwargs ):
      self.requests = {}
      self.failedRequests = []

    def putRequest( self, request ):
      """ Put a request in the cache, fails if '/putRequest/' is in the name"""

      self.requests[request.RequestName] = request

      for op in request:
        for rmsFile in op:
          if '/putRequest/' in rmsFile.LFN:
            self.failedRequests.append( request.RequestName )
            return S_ERROR( 'putRequest in requestName' )

      return S_OK( len( self.requests ) )

    def setServer( self, _url ):
      pass


  class mock_StorageElement( object ):
    """ Fake the StorageElement and keep track of the removal"""

    def __init__( self ):
      self.removeCalls = []


    def __call__( self, seName ):
      """ Because we pass an instance of mock_storageElement,
          when the code will do StorageElement('se'), it is
          __call__ which is used
      """
      return self

    def getFileMetadata( self, lfns ):
      """ file metadata"""

      successful = {}
      failed = {}
      for lfn in lfns:
        if '/seMetadata/' in lfn:
          failed[lfn] = "/seMetadata/ in lfn"
        elif '/error_seMetadata/' in lfn:
          return S_ERROR( 'One LFN with error_seMetadata' )
        elif '/notMigrated/' in lfn:
          successful[lfn] = {'Migrated' : False, 'Checksum' : 'Checksum'}
        elif '/seChecksum/' in lfn:
          successful[lfn] = {'Migrated' : True, 'Checksum' : 'BadChecksum'}
        else:
          successful[lfn] = {'Migrated' : True, 'Checksum' : 'Checksum'}

      return S_OK( {'Successful':successful, 'Failed':failed} )

    def removeFile( self, lfn ):
      # Here lfn is only one
      self.removeCalls.append( lfn )
      return S_OK( {'Successful':{lfn : 'bravo'}, 'Failed':{}} )



  # Single instance of gMonitor to be used all along
  gMonitor = mock_gMonitor()
  genericSE = mock_StorageElement()

  # IMPORTANT: we use "new" in patch because we use this single instance
  # (https://docs.python.org/3/library/unittest.mock.html#unittest.mock.patch)
  # For the others we don't need because we can access them as attributes


  class dbFile( object ):
    def __init__( self, lfn, size = 0, se = 'se' ):
      self.lfn = lfn
      self.size = size
      self.se = se

    def __str__( self ):
      return "%s (%s)" % ( self.lfn, self.size )

  @mock.patch( 'LHCbDIRAC.DataManagementSystem.Agent.RAWIntegrityAgent.FileCatalog', side_effect = mock_FileCatalog )
  @mock.patch( 'LHCbDIRAC.DataManagementSystem.Agent.RAWIntegrityAgent.ReqClient', side_effect = mock_reqClient )
  @mock.patch( 'LHCbDIRAC.DataManagementSystem.Agent.RAWIntegrityAgent.gMonitor', new = gMonitor )
  def setUp( self, _mockFC, _mockRMS ):
    super( RAWIntegrityAgentTest, self ).setUp()
    self.agent = RAWIntegrityAgent( 'DataManagement/RAWIntegrityAgent', 'DataManagement/RAWIntegrityAgent' )
    self.agent.initialize()
    self.db = RAWIntegrityDB()

    # Lets fill the DB with some files

    # Fake state in the DB, should not be fetched -> no change
    self.fakeState = RAWIntegrityAgentTest.dbFile( '/lhcb/fakeState/fakeState.txt', size = 1 )
    # Cannot get SE metadata -> no change
    self.failedMetadata = RAWIntegrityAgentTest.dbFile( '/lhcb/seMetadata/failedMetadata.txt', size = 2 )
    # Cannot error SE metadata -> no change
    self.errorMetadata = RAWIntegrityAgentTest.dbFile( '/lhcb/error_seMetadata/errorMetadata.txt', size = 3, se = 'se2' )
    # Has the wrong checksum -> status failed
    self.badChecksum = RAWIntegrityAgentTest.dbFile( '/lhcb/seChecksum/badChecksum.txt', size = 4 )
    # Has the wrong checksum and cannot create the retransfer request -> no change
    self.failRetransfer = RAWIntegrityAgentTest.dbFile( '/lhcb/seChecksum/putRequest/failRetransfer.txt', size = 5 )
    # All good but fine not migrated -> no change
    self.notMigrated = RAWIntegrityAgentTest.dbFile( '/lhcb/notMigrated/notMigrated.txt', size = 6 )
    # Cannot perform add file (in Failed) -> no change
    self.failAddFile = RAWIntegrityAgentTest.dbFile( '/lhcb/addFile/failAddFile.txt', size = 7 )
    # Cannot perform add file (S_ERROR) -> no change
    self.errorAddFile = RAWIntegrityAgentTest.dbFile( '/lhcb/error_addFile/errorAddFile.txt', size = 8, se = 'se2' )
    # Cannot put the removal Request -> no change
    self.failRemove = RAWIntegrityAgentTest.dbFile( '/lhcb/removal/putRequest/failRemove.txt', size = 9 )
    # all perfect, Status goes to done
    self.allGood = RAWIntegrityAgentTest.dbFile( '/lhcb/allGood/allGood.txt', 10 )



    self.files = [ self.fakeState,
                   self.failedMetadata,
                   self.errorMetadata,
                   self.badChecksum,
                   self.failRetransfer,
                   self.notMigrated,
                   self.failAddFile,
                   self.errorAddFile,
                   self.failRemove,
                   self.allGood
                  ]

    # errorAddFile is not in the list because it is in the same SE as errorMetadata,
    # so it is considered not migrated
    self.migratedFiles = [self.badChecksum, self.failRetransfer, self.failAddFile,
                          self.failRemove, self.allGood]

    for i, dbf in enumerate( self.files ):
      self.db.addFile( dbf.lfn, dbf.lfn, dbf.size, dbf.se,
                      'GUID%s' % i, 'Checksum' )

    self.db.setFileStatus( self.fakeState.lfn, 'FakeStatus' )

  def tearDown( self ):
    # delete all the files
    for dbf in self.files:
      self.db.removeFile( dbf.lfn )


  @mock.patch( 'LHCbDIRAC.DataManagementSystem.Agent.RAWIntegrityAgent.gMonitor', new = gMonitor )
  @mock.patch( 'LHCbDIRAC.DataManagementSystem.Agent.RAWIntegrityAgent.StorageElement', new = genericSE )
  def test_01_execute( self ):
    """ Perform the execution loop"""
    res = self.agent.execute()
    self.assertTrue( res['OK'], res )

    # We expect some counters to have certain values
    # Fisrt loop, so we look at the first value of the counter

    # All files are waiting but the fakeState one
    self.assertEqual( RAWIntegrityAgentTest.gMonitor.counters['WaitingFiles'][0], len( self.files ) - 1 )
    self.assertEqual( RAWIntegrityAgentTest.gMonitor.counters['WaitSize'][0],
                      ( sum( f.size for f in self.files ) - self.fakeState.size ) / ( 1024 * 1024 * 1024.0 )
                    )

    # All these files are properly migrated
    # Because errorAddFile is in the same SE than errorMetadata, it will not be accounted for
    # in the successfully migrated
    migratedSize = sum( f.size for f in self.migratedFiles )

    self.assertEqual( RAWIntegrityAgentTest.gMonitor.counters['TotMigratedSize'][0], migratedSize / ( 1024 * 1024 * 1024.0 ) )
    self.assertEqual( RAWIntegrityAgentTest.gMonitor.counters['NewlyMigrated'][0], len( self.migratedFiles ) )
    # In fact these two counters are not the same, one is cumulative
    self.assertEqual( RAWIntegrityAgentTest.gMonitor.counters['TotMigrated'][0], len( self.migratedFiles ) )

    # notMigrated and badChecksum are to be removed
    self.assertEqual( RAWIntegrityAgentTest.gMonitor.counters['SuccessfullyMigrated'][0], len( self.migratedFiles ) - 2 )
    self.assertEqual( RAWIntegrityAgentTest.gMonitor.counters['TotSucMigrated'][0], len( self.migratedFiles ) - 2 )

    # failRetransfer (that also has bad checksum ) and badChecksum failed in migrating
    self.assertEqual( RAWIntegrityAgentTest.gMonitor.counters['FailedMigrated'][0], 2 )
    self.assertEqual( RAWIntegrityAgentTest.gMonitor.counters['TotFailMigrated'][0], 2 )


    # We expect that there should have been a removal attempt for the two lfn with bad checksum
    self.assertEqual( sorted( RAWIntegrityAgentTest.genericSE.removeCalls ),
                      sorted( [self.badChecksum.lfn, self.failRetransfer.lfn] ) )

    # There should be requests for
    #  * retransfering badChecksum
    #  * retransfering failRetransfer, but in the failed
    #  * removing failRemove, but in the failed
    #  * removing allGood

    self.assertEqual( len( self.agent.onlineRequestMgr.requests ), 4 )
    self.assertTrue( 'retransfer_badChecksum.txt' in self.agent.onlineRequestMgr.requests )
    self.assertTrue( 'retransfer_failRetransfer.txt' in self.agent.onlineRequestMgr.requests )
    self.assertTrue( 'retransfer_failRetransfer.txt' in self.agent.onlineRequestMgr.failedRequests )
    self.assertTrue( 'remove_allGood.txt' in self.agent.onlineRequestMgr.requests )
    self.assertTrue( 'remove_failRemove.txt' in self.agent.onlineRequestMgr.requests )
    self.assertTrue( 'remove_failRemove.txt' in self.agent.onlineRequestMgr.failedRequests )


    # We should have attempted to register 3 files
    # failedAddFile should have failed
    # errorAddFile does not make it to the registration since we can't get metadata (because with errorMetadata)
    # allGood and failRemove should have succeeded
    self.assertEqual( len( self.agent.fileCatalog.successfullAdd ), 2 )
    self.assertTrue( self.allGood.lfn in self.agent.fileCatalog.successfullAdd )
    self.assertTrue( self.failRemove.lfn in self.agent.fileCatalog.successfullAdd )
    self.assertEqual( len( self.agent.fileCatalog.failedAdd ), 1 )
    self.assertTrue( self.failAddFile.lfn in self.agent.fileCatalog.failedAdd )





if __name__ == '__main__':
#   from DIRAC import gLogger
#   gLogger.setLevel( 'DEBUG' )
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( RAWIntegrityDBTest )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( RAWIntegrityAgentTest ) )
  # The failfast option here is useful because the first test executed is if the db is empty.
  # if not we stop... this avoids bad accident :)
  unittest.TextTestRunner( verbosity = 2, failfast = True ).run( suite )

