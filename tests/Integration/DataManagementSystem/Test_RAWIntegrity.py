""" Integration test for the RAWIntegritySystem"""

# pylint: disable=invalid-name,too-many-statements,protected-access,too-many-instance-attributes,wrong-import-position

import unittest
import time
import datetime
import random
import mock

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import S_OK, S_ERROR
from DIRAC.Resources.Storage.Utilities import checkArgumentFormat
from LHCbDIRAC.DataManagementSystem.DB.RAWIntegrityDB import RAWIntegrityDB
from LHCbDIRAC.DataManagementSystem.Agent.RAWIntegrityAgent import RAWIntegrityAgent


class RAWIntegrityDBTest(unittest.TestCase):
  """ Tests for the DB part of the RAWIntegrity system
  """

  def setUp(self):
    super(RAWIntegrityDBTest, self).setUp()
    self.db = RAWIntegrityDB()

  def tearDown(self):
    # Only one file is before 'now' and 'after'
    res = self.db.selectFiles({})
    lfns = [fTuple[0] for fTuple in res['Value']]
    # clean after us
    for lfn in lfns:
      self.db.removeFile(lfn)

  def test_01_setupDB(self):
    """ Test table creations"""

    # At first, there should be no table
    res = self.db.showTables()
    self.assertTrue(res['OK'], res)
    self.assertEqual(res['Value'], [])

    # Lets create them
    res = self.db._checkTable()
    self.assertTrue(res['OK'], res)
    # and check they are now here
    res = self.db.showTables()
    self.assertTrue(res['OK'], res)
    self.assertEqual(sorted(res['Value']), sorted(['Files', 'LastMonitor']))

    # Lets create them again, there should be no error
    res = self.db._checkTable()
    self.assertTrue(res['OK'], res)

  def test_02_lastMonitorTime(self):
    """ Test the last monitor time function"""

    # Just after creation, we insert initial timestamp so no error
    res = self.db.getLastMonitorTimeDiff()
    self.assertTrue(res['OK'], res)

    # set the monitor time
    res = self.db.setLastMonitorTime()
    self.assertTrue(res['OK'], res)

    # we wait a bit, and check that the difference is correct
    # we expect not more than 1 second delay
    sleepTime = 3
    time.sleep(sleepTime)
    res = self.db.getLastMonitorTimeDiff()
    self.assertTrue(res['OK'], res)
    self.assertTrue(sleepTime <= res['Value'] <= sleepTime + 1, res['Value'])

  def test_03_fileManipulation(self):
    """ Testing all the file manipulation operations"""

    # There should be no new files so far
    res = self.db.getFiles('New')
    self.assertTrue(res['OK'], res)
    self.assertEqual(res['Value'], {})

    testFile = {
        'LFN': 'lfn',
        'PFN': 'pfn',
        'Size': 123,
        'SE': 'se',
        'GUID': 'guid',
        'Checksum': 'checksum'
    }

    # adding a file
    res = self.db.addFile(testFile['LFN'], testFile['PFN'], testFile['Size'], testFile['SE'],
                          testFile['GUID'], testFile['Checksum'])
    self.assertTrue(res['OK'], res)

    sleepTime = 2
    time.sleep(sleepTime)

    # There should be now one active file
    res = self.db.getFiles('Active')
    self.assertTrue(res['OK'], res)
    self.assertEqual(len(res['Value']), 1)
    self.assertTrue(testFile['LFN'] in res['Value'], res)

    activeFile = res['Value'][testFile['LFN']]

    for attribute in ['PFN', 'Size', 'SE', 'GUID', 'Checksum']:
      self.assertEqual(testFile[attribute], activeFile[attribute])

    self.assertTrue(sleepTime <= activeFile['WaitTime'] <= sleepTime + 1)

    # Change the file status to Done
    res = self.db.setFileStatus(testFile['LFN'], 'Done')
    self.assertTrue(res['OK'], res)
    self.assertEqual(res['Value'], 1)

    # The file should not be returned when asking for Active files anymore
    res = self.db.getFiles('Active')
    self.assertTrue(res['OK'], res)
    self.assertEqual(res['Value'], {})

    # Change the file status back to Active
    # It should not work, Done files cannot change
    res = self.db.setFileStatus(testFile['LFN'], 'Active')
    self.assertTrue(res['OK'], res)
    self.assertEqual(res['Value'], 0)

    # The file should not be back
    res = self.db.getFiles('Active')
    self.assertEqual(res['Value'], {})

    # Remove the file
    res = self.db.removeFile(testFile['LFN'])
    self.assertTrue(res['OK'], res)

    # adding the file back
    # (no need to test that its visible, we just did it)
    res = self.db.addFile(testFile['LFN'], testFile['PFN'], testFile['Size'], testFile['SE'],
                          testFile['GUID'], testFile['Checksum'])
    self.assertTrue(res['OK'], res)

    # remove the file
    res = self.db.removeFile(testFile['LFN'])
    self.assertTrue(res['OK'], res)
    self.assertEqual(res['Value'], 1)

    # There should be no file
    res = self.db.getFiles('Active')
    self.assertEqual(res['Value'], {})

    # Adding two time the same files
    # It should work
    res = self.db.addFile(testFile['LFN'], testFile['PFN'], testFile['Size'], testFile['SE'],
                          testFile['GUID'], testFile['Checksum'])
    self.assertTrue(res['OK'], res)
    res = self.db.addFile(testFile['LFN'], testFile['PFN'], testFile['Size'], testFile['SE'],
                          testFile['GUID'], testFile['Checksum'])
    self.assertTrue(res['OK'], res)

    # We should get only one file, so processed only once
    res = self.db.getFiles('Active')
    self.assertTrue(res['OK'], res)
    self.assertEqual(len(res['Value']), 1)

    res = self.db.removeFile(testFile['LFN'])
    self.assertTrue(res['OK'], res)
    self.assertEqual(res['Value'], 2)

    # Setting status of a non existing file
    res = self.db.setFileStatus('fake', 'Done')
    self.assertTrue(res['OK'], res)
    self.assertEqual(res['Value'], 0)

    # Removing a non existing file
    res = self.db.removeFile('fake')
    self.assertTrue(res['OK'], res)
    self.assertEqual(res['Value'], 0)

  def test_03_testUnmigratedFiles(self):
    """ Test that getUnmigratedFiles returns only the files
        in the appropriate status
    """

    unmigratedStatus = ['Active', 'Copied', 'Registered']
    for st in ['Active', 'Copied', 'Registered', 'Done', 'Fake']:
      self.db.addFile("lfn%s" % st, 'PFN', 1, 'SE', 'GUID', 'Checksum')
      self.db.setFileStatus('lfn%s' % st, st)

    res = self.db.getUnmigratedFiles()
    self.assertTrue(res['OK'], res)
    self.assertEqual(sorted(res['Value']), sorted(['lfn%s' % st for st in unmigratedStatus]))

  def test_04_webQueries(self):
    """ Test all the web related methods"""

    # The DB should be empty now
    res = self.db.getGlobalStatistics()
    self.assertTrue(res['OK'], res)
    self.assertEqual(res['Value'], {})

    res = self.db.getFileSelections()
    self.assertTrue(res['OK'], res)
    self.assertEqual(res['Value'], {'StorageElement': [], 'Status': []})

    # Adding two files Assigned, 1 Failed and 1 done
    for i in xrange(1, 5):
      res = self.db.addFile('lfn%s' % i, 'pfn%s' % i, 5 - i, 'se%s' % (i % 2), 'GUID%s' % i,
                            'Checksum%s' % i)
      self.assertTrue(res['OK'], res)

    res = self.db.setFileStatus('lfn3', 'Done')
    self.assertTrue(res['OK'], res)
    res = self.db.setFileStatus('lfn4', 'Failed')
    self.assertTrue(res['OK'], res)

    res = self.db.getGlobalStatistics()
    self.assertTrue(res['OK'], res)
    self.assertEqual(res['Value'], {'Active': 2, 'Done': 1, 'Failed': 1})

    res = self.db.getFileSelections()
    self.assertTrue(res['OK'], res)
    self.assertEqual(sorted(res['Value']['StorageElement']), sorted(['se0', 'se1']))
    self.assertEqual(sorted(res['Value']['Status']), sorted(['Done', 'Failed', 'Active']))

    # Do some selection test
    res = self.db.selectFiles({'StorageElement': 'se0'})
    self.assertTrue(res['OK'], res)
    # they should be sorted by LFN by default
    returnedLfns = [ret[0] for ret in res['Value']]
    self.assertEqual(returnedLfns, ['lfn2', 'lfn4'])

    res = self.db.selectFiles({'StorageElement': 'se0', 'PFN': 'pfn2'})
    self.assertTrue(res['OK'], res)
    returnedLfns = [ret[0] for ret in res['Value']]
    self.assertEqual(returnedLfns, ['lfn2'])

    # Impossible condition
    res = self.db.selectFiles({'StorageElement': 'se1', 'PFN': 'pfn2'})
    self.assertTrue(res['OK'], res)
    returnedLfns = [ret[0] for ret in res['Value']]
    self.assertEqual(returnedLfns, [])

    # limit to 1
    res = self.db.selectFiles({'StorageElement': 'se0'}, limit=1)
    self.assertTrue(res['OK'], res)
    # they should be sorted by LFN by default
    returnedLfns = [ret[0] for ret in res['Value']]
    self.assertEqual(returnedLfns, ['lfn2'])

    # Same selection, sorted by size
    res = self.db.selectFiles({'StorageElement': 'se0'}, orderAttribute='Size')
    self.assertTrue(res['OK'], res)
    returnedLfns = [ret[0] for ret in res['Value']]
    self.assertEqual(returnedLfns, ['lfn4', 'lfn2'])

    # Test the time based selections
    time.sleep(1)
    now = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    sleepTime = 2
    time.sleep(sleepTime)

    res = self.db.addFile('lfn6', 'pfn6', 6, 'se1', 'GUID6', 'Checksum6')
    self.assertTrue(res['OK'], res)

    # select the old files with no conditions
    res = self.db.selectFiles({}, older=now)
    self.assertTrue(res['OK'], res)
    returnedLfns = [ret[0] for ret in res['Value']]
    self.assertEqual(returnedLfns, ['lfn%i' % i for i in xrange(1, 5)])

    # select the new file
    res = self.db.selectFiles({}, newer=now)
    self.assertTrue(res['OK'], res)
    returnedLfns = [ret[0] for ret in res['Value']]
    self.assertEqual(returnedLfns, ['lfn6'])

    # add some more
    time.sleep(1)
    after = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    time.sleep(sleepTime)

    res = self.db.addFile('lfn7', 'pfn7', 7, 'se1', 'GUID7', 'Checksum7')
    self.assertTrue(res['OK'], res)

    # We should now have two files after 'now'
    res = self.db.selectFiles({}, newer=now)
    self.assertTrue(res['OK'], res)
    returnedLfns = [ret[0] for ret in res['Value']]
    self.assertEqual(returnedLfns, ['lfn6', 'lfn7'])

    # Only one file is before 'now' and 'after'
    res = self.db.selectFiles({}, newer=now, older=after)
    self.assertTrue(res['OK'], res)
    returnedLfns = [ret[0] for ret in res['Value']]
    self.assertEqual(returnedLfns, ['lfn6'])

    # clean after us
    for i in xrange(1, 8):
      res = self.db.removeFile('lfn%s' % i)
      self.assertTrue(res['OK'], res)

  def test_05_perf(self):
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
    for i in xrange(nbFiles):
      res = self.db.addFile('lfn%s' % i, 'pfn%s' % i, i, 'se%s' % (i % 2), 'GUID%s' % i,
                            'Checksum%s' % i)
      self.assertTrue(res['OK'], res)
    insertTime = time.time() - startTime

    # Sleep 2 seconds so that the DB has
    # a consistant commited state
    time.sleep(2)

    # getting all of them
    startTime = time.time()
    res = self.db.getFiles('Active')
    self.assertTrue(res['OK'], res)
    self.assertEqual(len(res['Value']), nbFiles)
    getFileTime = time.time() - startTime

    # Setting some of them
    startTime = time.time()
    rndIds = set()
    for _ in xrange(nbFiles / 10):
      rndId = random.randint(1, nbFiles)
      rndIds.add(rndId)
      self.db.setFileStatus('lfn%s' % rndId, 'Done')
      self.assertTrue(res['OK'], res)
    updateStatusTime = time.time() - startTime

    # getting less of them
    startTime = time.time()
    res = self.db.getFiles('Active')
    self.assertTrue(res['OK'], res)
    self.assertEqual(len(res['Value']), nbFiles - len(rndIds))
    getFileTime2 = time.time() - startTime

    # deleting all of them
    startTime = time.time()
    for i in xrange(1, nbFiles):
      res = self.db.removeFile('lfn%s' % i)
      self.assertTrue(res['OK'], res)
    removeTime = time.time() - startTime

    print "Performance result"
    print "Inserting %s files: %s" % (nbFiles, insertTime)
    print "Getting all active files: %s" % getFileTime
    print "Updating %s status: %s" % (nbFiles / 10, updateStatusTime)
    print "Getting again active files: %s" % getFileTime2
    print "Removing all files: %s" % removeTime


class RAWIntegrityAgentTest(unittest.TestCase):
  """ Tests for the DB part of the RAWIntegrity system
  """

  class mock_FileCatalog(object):
    """ Fake FC that keeps track of all the actions done on it
        and reply based on the lfn name
    """

    def __init__(self, *_args, **_kwargs):
      self.successfullAdd = []
      self.failedAdd = []
      self.attemptsAdd = []

    def removeCatalog(self, _catalogName):  # pylint: disable=no-self-use
      """ Remove a catalog"""
      return S_OK()

    def addFile(self, lfns):
      """ Add a file.
          The output of the method depends on the lfn.
          if "addFile" is in the lfn it goes to failed,
          if "error_addFile" is in one lfn, return S_ERROR
          otherwise go to successful
      """

      # If any of the LFN should trigger an error, do it immediately
      if any(['/error_addFile/' in lfn for lfn in lfns]):
        for lfn in lfns:
          self.attemptsAdd.append(lfn)
          self.failedAdd.append(lfn)
        return S_ERROR('One LFN with error_addFile')

      successful = {}
      failed = {}
      for lfn in lfns:
        self.attemptsAdd.append(lfn)
        if '/addFile/' in lfn:
          failed[lfn] = "/addFile/ in lfn"
          self.failedAdd.append(lfn)
        else:
          self.successfullAdd.append(lfn)
          successful[lfn] = "Youpee"
      return S_OK({'Successful': successful, 'Failed': failed})

  class mock_gMonitor(object):
    """ Fake the gMonitor and keep the counters """

    OP_SUM = OP_MEAN = OP_ACUM = None

    def __init__(self, *_args, **__kwargs):
      self.counters = {}

    def registerActivity(self, counterName, *_args, **_kwargs):
      """ Register counter"""
      self.counters[counterName] = []

    def addMark(self, counterName, value):
      """ Add Mark"""
      self.counters[counterName].append(value)

  class mock_StorageElement(object):
    """ Fake the StorageElement and keep track of the removal"""

    def __init__(self):
      self.reset()

    def reset(self):
      """ Just used to reset all the counters"""
      self.removeCalls = []
      self.successfulRemove = []
      self.failedRemove = []

    def __call__(self, seName):
      """ Because we pass an instance of mock_storageElement,
          when the code will do StorageElement('se'), it is
          __call__ which is used
      """
      return self

    def getFileMetadata(self, lfns):  # pylint: disable=no-self-use
      """ file metadata"""

      # If any of the LFN should trigger an error, do it immediately
      if any(['/error_seMetadata/' in lfn for lfn in lfns]):
        return S_ERROR('One LFN with error_seMetadata')

      successful = {}
      failed = {}
      for lfn in lfns:
        if '/fail_seMetadata/' in lfn:
          failed[lfn] = "/seMetadata/ in lfn"
        elif '/notCopied/' in lfn:
          successful[lfn] = {'Migrated': False, 'Checksum': 'Checksum'}
        elif '/seChecksum/' in lfn:
          successful[lfn] = {'Migrated': True, 'Checksum': 'BadChecksum'}
        else:
          successful[lfn] = {'Migrated': True, 'Checksum': 'Checksum'}

      return S_OK({'Successful': successful, 'Failed': failed})

    def removeFile(self, paths):
      """ Remove file
          Uses the name to react.
          if /error_removeFile/ is in the lfn, returns S_ERROR
          if /removeFile/ is in the lfn: adds it in failed
      """

      lfns = checkArgumentFormat(paths)['Value']

      if any(['/error_removeFile/' in lfn for lfn in lfns]):
        for lfn in lfns:
          self.removeCalls.append(lfn)
          self.failedRemove.append(lfn)
        return S_ERROR("ERROR to remove File")

      failed = {}
      successful = {}

      for lfn in lfns:
        self.removeCalls.append(lfn)
        if '/fail_removeFile/' in lfn:
          self.failedRemove.append(lfn)
          failed[lfn] = 'It failed'
        else:
          self.successfulRemove.append(lfn)
          successful[lfn] = 'It worked :-)'
      return S_OK({'Successful': successful, 'Failed': failed})

  # Single instance of gMonitor and StorageElements to be used all along
  gMonitor = mock_gMonitor()
  genericSE = mock_StorageElement()

  # IMPORTANT: we use "new" in patch because we use this single instance
  # (https://docs.python.org/3/library/unittest.mock.html#unittest.mock.patch)
  # For the others we don't need because we can access them as attributes

  class dbFile(object):
    """ Just a convenience class """

    def __init__(self, lfn, size=0, se='se'):
      self.lfn = lfn
      self.size = size
      self.se = se

    def __str__(self):
      return "%s (%s)" % (self.lfn, self.size)

  @mock.patch(
      'LHCbDIRAC.DataManagementSystem.Agent.RAWIntegrityAgent.FileCatalog',
      side_effect=mock_FileCatalog)
  @mock.patch('LHCbDIRAC.DataManagementSystem.Agent.RAWIntegrityAgent.gMonitor', new=gMonitor)
  def setUp(self, _mockFC):  # pylint: disable=arguments-differ
    """ This method resets the internal counters of the mock,
        creates a new RAWIntegrityAgent,
        and insert into the DB some specifically crafted files
    """

    super(RAWIntegrityAgentTest, self).setUp()
    # Reset the counters since we have several loops
    RAWIntegrityAgentTest.gMonitor.counters = {}
    RAWIntegrityAgentTest.genericSE.reset()
    self.agent = RAWIntegrityAgent('DataManagement/RAWIntegrityAgent',
                                   'DataManagement/RAWIntegrityAgent')
    self.agent.initialize()
    self.db = RAWIntegrityDB()

    # Lets fill the DB with some files

    # Fake state in the DB, should not be fetched -> no change
    self.fakeState = RAWIntegrityAgentTest.dbFile('/lhcb/fakeState/fakeState.txt', size=1)
    # Cannot get SE metadata (failed)-> no change
    self.failMetadata = RAWIntegrityAgentTest.dbFile(
        '/lhcb/fail_seMetadata/failedMetadata.txt', size=2)
    # Get error on SE metadata -> no change.
    self.errorMetadata = RAWIntegrityAgentTest.dbFile(
        '/lhcb/error_seMetadata/errorMetadata.txt', size=3, se='se2')
    # All fine but on se2 so fails because of errorMetadata -> no change
    self.onSEMetadata = RAWIntegrityAgentTest.dbFile(
        '/lhcb/allGood/ButOnSEMetadata.txt', size=3, se='se2')
    # Has the wrong checksum -> no changes
    self.badChecksum = RAWIntegrityAgentTest.dbFile('/lhcb/seChecksum/badChecksum.txt', size=4)
    # All good but file not migrated -> no change
    self.notCopied = RAWIntegrityAgentTest.dbFile('/lhcb/notCopied/notCopied.txt', size=6)
    # Cannot perform add file (in Failed) -> goes to state migrated
    self.failAddFile = RAWIntegrityAgentTest.dbFile('/lhcb/addFile/failAddFile.txt', size=7)
    # Cannot perform add file (S_ERROR) -> goes to state migrated
    self.errorAddFile = RAWIntegrityAgentTest.dbFile(
        '/lhcb/error_addFile/errorAddFile.txt', size=8, se='se3')
    # All fine but on se3 with errorAddFile -> goes to migrated
    self.onSeAddFile = RAWIntegrityAgentTest.dbFile(
        '/lhcb/allGood/ButOnSEAddFile.txt', size=8, se='se3')
    # Cannot perform the removal -> goes to Registered
    self.failRemove = RAWIntegrityAgentTest.dbFile(
        '/lhcb/removal/fail_removeFile/failRemove.txt', size=9)
    #  Error when performin se.removeFile-> goes to register
    self.errorRemove = RAWIntegrityAgentTest.dbFile(
        '/lhcb/removal/error_removeFile/failRemove.txt', size=9, se='se4')
    #  All good but on se4-> goes to register
    self.onSeRemove = RAWIntegrityAgentTest.dbFile(
        '/lhcb/removal/allGood/ButOnSeRemove.txt', size=9, se='se4')
    # all perfect, Status goes to done
    self.allGood = RAWIntegrityAgentTest.dbFile('/lhcb/allGood/allGood.txt', 10)
    # File that was in Migrated status in the DB
    self.wasCopied = RAWIntegrityAgentTest.dbFile('/lhcb/allGood/wasCopied.txt', size=7)
    # File that was in Registered status in the DB
    self.wasRegistered = RAWIntegrityAgentTest.dbFile('/lhcb/allGood/wasRegistered.txt', size=7)

    self.files = [
        self.fakeState, self.failMetadata, self.errorMetadata, self.onSEMetadata, self.badChecksum,
        self.notCopied, self.failAddFile, self.onSeAddFile, self.failRemove, self.onSeRemove,
        self.allGood, self.wasCopied, self.wasRegistered, self.errorRemove, self.errorAddFile
    ]

    for i, dbf in enumerate(self.files):
      self.db.addFile(dbf.lfn, dbf.lfn, dbf.size, dbf.se, 'GUID%s' % i, 'Checksum')

    self.db.setFileStatus(self.fakeState.lfn, 'FakeStatus')
    self.db.setFileStatus(self.wasCopied.lfn, 'Copied')
    self.db.setFileStatus(self.wasRegistered.lfn, 'Registered')

  def tearDown(self):
    """
      delete all the files
    """
    for dbf in self.files:
      self.db.removeFile(dbf.lfn)

  @mock.patch('LHCbDIRAC.DataManagementSystem.Agent.RAWIntegrityAgent.gMonitor', new=gMonitor)
  @mock.patch(
      'LHCbDIRAC.DataManagementSystem.Agent.RAWIntegrityAgent.StorageElement', new=genericSE)
  def test_01_executeWithNoError(self):
    """ Perform the execution loop but without the registration or the removal returning S_ERROR"""

    # remove from the DB the files that we dont want
    self.db.removeFile(self.errorAddFile.lfn)
    self.db.removeFile(self.errorRemove.lfn)
    allActiveFiles = set([
        self.failMetadata, self.errorMetadata, self.onSEMetadata, self.badChecksum, self.notCopied,
        self.failAddFile, self.onSeAddFile, self.failRemove, self.onSeRemove, self.allGood
    ])

    # List of files that have problem with the metadata
    pbMetadataFiles = set(
        [self.failMetadata, self.errorMetadata, self.onSEMetadata, self.badChecksum])

    # Only self.failAddFile
    pbRegisterFiles = set([self.failAddFile])

    # List of files that have problem with the removal
    pbRemoveFiles = set([self.failRemove])

    res = self.agent.execute()
    self.assertTrue(res['OK'], res)

    self._analyseResults(allActiveFiles, pbMetadataFiles, pbRegisterFiles, pbRemoveFiles)

  @mock.patch('LHCbDIRAC.DataManagementSystem.Agent.RAWIntegrityAgent.gMonitor', new=gMonitor)
  @mock.patch(
      'LHCbDIRAC.DataManagementSystem.Agent.RAWIntegrityAgent.StorageElement', new=genericSE)
  def test_02_executeWithRegisterError(self):
    """ Perform the execution loop and trigger an S_ERROR on Register"""

    # Remove the file crashing the removal
    self.db.removeFile(self.errorRemove.lfn)

    allActiveFiles = set([
        self.failMetadata, self.errorMetadata, self.onSEMetadata, self.badChecksum, self.notCopied,
        self.failAddFile, self.onSeAddFile, self.failRemove, self.onSeRemove, self.allGood,
        self.errorAddFile
    ])

    # List of files that have problem with the metadata
    pbMetadataFiles = set(
        [self.failMetadata, self.errorMetadata, self.onSEMetadata, self.badChecksum])

    # All of them should fail because of errorAddFile
    pbRegisterFiles = set([
        self.failAddFile, self.onSeAddFile, self.failRemove, self.onSeRemove, self.allGood,
        self.errorAddFile, self.wasCopied
    ])

    # None. Only attempt should be the one already in DB
    pbRemoveFiles = set([])

    res = self.agent.execute()
    self.assertTrue(res['OK'], res)

    self._analyseResults(allActiveFiles, pbMetadataFiles, pbRegisterFiles, pbRemoveFiles)

  @mock.patch('LHCbDIRAC.DataManagementSystem.Agent.RAWIntegrityAgent.gMonitor', new=gMonitor)
  @mock.patch(
      'LHCbDIRAC.DataManagementSystem.Agent.RAWIntegrityAgent.StorageElement', new=genericSE)
  def test_03_executeWithRemoveError(self):
    """ Perform the execution loop and trigger an S_ERROR on Remove"""

    self.db.removeFile(self.errorAddFile.lfn)
    # All the files that are Active at the begiining
    allActiveFiles = set([
        self.failMetadata, self.errorMetadata, self.onSEMetadata, self.badChecksum, self.notCopied,
        self.failAddFile, self.onSeAddFile, self.failRemove, self.onSeRemove, self.allGood,
        self.errorRemove
    ])

    # List of files that have problem with the metadata
    pbMetadataFiles = set(
        [self.failMetadata, self.errorMetadata, self.onSEMetadata, self.badChecksum])

    # Since self.errorAddFile is not there, only failAddFile
    # has an registration issue
    pbRegisterFiles = set([self.failAddFile])

    # All the files that reach registration fails.
    pbRemoveFiles = set([
        self.onSeAddFile, self.failRemove, self.onSeRemove, self.allGood, self.errorRemove,
        self.wasRegistered, self.wasCopied
    ])

    res = self.agent.execute()
    self.assertTrue(res['OK'], res)

    self._analyseResults(allActiveFiles, pbMetadataFiles, pbRegisterFiles, pbRemoveFiles)

  def _analyseResults(self, allActiveFiles, pbMetadataFiles, pbRegisterFiles, pbRemoveFiles):
    """ Takes as input the expected list of files, and compares it
        with the real mock counters
    """

    # All the active files, plus those that were already partially processed
    allFiles = allActiveFiles | set([self.wasRegistered, self.wasCopied])

    # The copiedFiles are those that did not fail the metadata, that have correct checksum
    # and were indeed copied (not like self.notCopied...)
    allCopiedFiles = allActiveFiles - pbMetadataFiles - set([self.notCopied])

    # List of files that we will attempt to register
    # All of them, except those with metadata problems,
    # the one not copied, and the one already registered
    attemptsRegister = allCopiedFiles | set([self.wasCopied])

    # successfulRegisters are those we tried minus those that failed
    successfullRegister = attemptsRegister - pbRegisterFiles

    # We try to remove those that we could Register, and the one that already was
    attemptsRemove = successfullRegister | set([self.wasRegistered])

    # Files that will be completely migrated by the end of the loop
    # All those that we copied + tried to register + tried to remove
    # Minus that that we did not manage to register or to remove
    migratedFiles = (allCopiedFiles | attemptsRegister | attemptsRemove) - (pbRegisterFiles |
                                                                            pbRemoveFiles)

    # List of files that failed migrating.
    # All the files at the beginning, minus the successfuly migrated
    # minus the one not yet copied
    failedMigratingFiles = allFiles - migratedFiles - set([self.notCopied])

    # We expect some counters to have certain values
    # First loop, so we look at the first value of the counter

    self.assertEqual(RAWIntegrityAgentTest.gMonitor.counters['WaitingFiles'][0],
                     len(allActiveFiles))
    self.assertEqual(RAWIntegrityAgentTest.gMonitor.counters['WaitSize'][0],
                     (sum(f.size for f in allActiveFiles)) / (1024 * 1024 * 1024.0))

    # All these files are properly migrated
    migratedSize = sum(f.size for f in migratedFiles)

    self.assertEqual(RAWIntegrityAgentTest.gMonitor.counters['TotMigratedSize'][0],
                     migratedSize / (1024 * 1024 * 1024.0))
    self.assertEqual(RAWIntegrityAgentTest.gMonitor.counters['NewlyMigrated'][0],
                     len(migratedFiles))
    # In fact these two counters are not the same, one is cumulative
    self.assertEqual(RAWIntegrityAgentTest.gMonitor.counters['TotMigrated'][0], len(migratedFiles))

    # Error getting metadata
    # Since we group by SE, we have two counters to check
    self.assertEqual(RAWIntegrityAgentTest.gMonitor.counters['ErrorMetadata'][0],
                     len([self.failMetadata]))
    self.assertEqual(RAWIntegrityAgentTest.gMonitor.counters['ErrorMetadata'][1],
                     len([self.errorMetadata, self.onSEMetadata]))

    # Error Registering
    self.assertEqual(RAWIntegrityAgentTest.gMonitor.counters['ErrorRegister'][0],
                     len(pbRegisterFiles))

    # Error Removing
    self.assertEqual(RAWIntegrityAgentTest.gMonitor.counters['ErrorRemove'][0], len(pbRemoveFiles))

    # Error in the checksum (self.badChecksum)
    self.assertEqual(RAWIntegrityAgentTest.gMonitor.counters['BadChecksum'][0], 1)

    self.assertEqual(RAWIntegrityAgentTest.gMonitor.counters['FailedMigrated'][0],
                     len(failedMigratingFiles))
    self.assertEqual(RAWIntegrityAgentTest.gMonitor.counters['TotFailMigrated'][0],
                     len(failedMigratingFiles))

    # We expect that there should be removals done
    # The files that end up migrated should be successfuly removed
    self.assertEqual(
        sorted(RAWIntegrityAgentTest.genericSE.successfulRemove),
        sorted([f.lfn for f in migratedFiles]))

    # There are also some removal failures
    self.assertEqual(
        sorted(RAWIntegrityAgentTest.genericSE.failedRemove),
        sorted([f.lfn for f in pbRemoveFiles]))

    # The attempts should be all the possible remove
    self.assertEqual(
        sorted(RAWIntegrityAgentTest.genericSE.removeCalls),
        sorted([f.lfn for f in attemptsRemove]))

    # The files that we tried to remove but failed should be registered. The allGood and wasCopied as well
    # but not the wasRegistered, because it is fetched after.
    # There are files with various errors for registration
    self.assertEqual(
        sorted(self.agent.fileCatalog.failedAdd), sorted([f.lfn for f in pbRegisterFiles]))

    self.assertEqual(
        sorted(self.agent.fileCatalog.successfullAdd), sorted([f.lfn for f in successfullRegister]))

    self.assertEqual(
        sorted(self.agent.fileCatalog.attemptsAdd), sorted([f.lfn for f in attemptsRegister]))

    # Check the database content now

    # The files that are still active are those with Metadata problems
    # and those not yet copied
    expDbActiveFiles = pbMetadataFiles | set([self.notCopied])
    dbActiveFiles = self.db.getFiles('Active')['Value']
    self.assertEqual(sorted(dbActiveFiles), sorted([f.lfn for f in expDbActiveFiles]))

    # The Copied files are those with Registration problem
    expDbCopiedFiles = pbRegisterFiles
    dbCopiedFiles = self.db.getFiles('Copied')['Value']
    self.assertEqual(sorted(dbCopiedFiles), sorted([f.lfn for f in expDbCopiedFiles]))

    # The Registered files are those with Removal problem
    expDbRegisteredFiles = pbRemoveFiles
    dbRegisteredFiles = self.db.getFiles('Registered')['Value']
    self.assertEqual(sorted(dbRegisteredFiles), sorted([f.lfn for f in expDbRegisteredFiles]))

    # The Done files are the migrated ones
    expDbDoneFiles = migratedFiles
    dbDoneFiles = self.db.getFiles('Done')['Value']
    self.assertEqual(sorted(dbDoneFiles), sorted([f.lfn for f in expDbDoneFiles]))


if __name__ == '__main__':
  # from DIRAC import gLogger
  # gLogger.setLevel( 'DEBUG' )
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(RAWIntegrityDBTest)
  # suite = unittest.defaultTestLoader.loadTestsFromTestCase(RAWIntegrityAgentTest)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(RAWIntegrityAgentTest))
  # The failfast option here is useful because the first test executed is if the db is empty.
  # if not we stop... this avoids bad accident :)
  unittest.TextTestRunner(verbosity=2, failfast=True).run(suite)
