import unittest
import itertools

from mock import MagicMock

from DIRAC import S_OK, S_ERROR, gLogger

from LHCbDIRAC.TransformationSystem.Client.TaskManager import LHCbWorkflowTasks
from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from LHCbDIRAC.TransformationSystem.Utilities.PluginUtilities import PluginUtilities, getFileGroups, groupByRun, closerSEs

def getSitesForSE( ses ):
  if ses == 'pippo':
    return {'OK':True, 'Value':['Site2', 'Site3']}
  else:
    return {'OK':True, 'Value':['Site3']}


class ClientTestCase( unittest.TestCase ):

  def setUp( self ):
    tcMock = MagicMock()
    sc = MagicMock()
    jmc = MagicMock()

    self.l_wft = LHCbWorkflowTasks( tcMock, submissionClient = sc, jobMonitoringClient = jmc )
    self.tc = TransformationClient()
    self.tc.opsH = MagicMock()
    self.tc.opsH.getValue.return_value = ['MCSimulation', 'DataReconstruction']

    self.tsMock = MagicMock()

    self.fcMock = MagicMock()
    self.fcMock.getFileSize.return_value = S_OK( {'Failed':[], 'Successful': cachedLFNSize} )

    gLogger.setLevel( 'DEBUG' )

    self.maxDiff = None

  def tearDown( self ):
    pass

class UtilitiesSuccess( ClientTestCase ):
  def test_closerSEs( self ):
    existingSEs = ['CERN-ARCHIVE', 'CERN-DST-EOS', 'CERN_MC_M-DST', 'GRIDKA-ARCHIVE', 'IN2P3-DST']
    targetSEs = ['CERN-DST-EOS', 'RAL-DST', 'PIC-DST', 'CNAF-DST', 'SARA-DST', 'IN2P3-DST', 'GRIDKA-DST']

    res = closerSEs( existingSEs, targetSEs, False )
    self.assert_( type( res ) == list )
    self.assert_( len( res ) )

class TaskManagerSuccess( ClientTestCase ):
  pass

class TransformationClientSuccess( ClientTestCase ):

  def test__applyTransformationFilesStateMachine( self ):
    tsFiles = {}
    dictOfNewLFNsStatus = {}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {} )

    tsFiles = {}
    dictOfNewLFNsStatus = {'foo':['status', 2L, 1234]}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {} )

    tsFiles = {'foo':['status', 2L, 1234]}
    dictOfNewLFNsStatus = {'foo':'status'}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {} )

    tsFiles = {'foo':['status', 2L, 1234]}
    dictOfNewLFNsStatus = {'foo':'statusA'}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'statusA'} )

    tsFiles = {'foo':['status', 2L, 1234], 'bar':['status', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo':'status'}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {} )

    tsFiles = {'foo':['status', 2L, 1234], 'bar':['status', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo':'statusA'}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'statusA'} )

    tsFiles = {'foo':['status', 2L, 1234], 'bar': ['status', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo':'A', 'bar':'B'}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'A', 'bar':'B'} )

    tsFiles = {'foo':['status', 2L, 1234]}
    dictOfNewLFNsStatus = {'foo':'A', 'bar':'B'}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'A'} )

    tsFiles = {'foo': ['Assigned', 2L, 1234]}
    dictOfNewLFNsStatus = {'foo':'A', 'bar':'B'}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'A'} )

    tsFiles = {'foo':['Assigned', 2L, 1234], 'bar':['Assigned', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo':'Assigned', 'bar':'Processed'}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'bar':'Processed'} )

    tsFiles = {'foo':['Processed', 2L, 1234], 'bar':['Unused', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo':'Assigned', 'bar':'Processed'}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'bar':'Processed'} )

    tsFiles = {'foo':['Processed', 2L, 1234], 'bar':['Unused', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo':'Assigned', 'bar':'Processed'}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, True )
    self.assertEqual( res, {'foo':'Assigned', 'bar':'Processed'} )

    tsFiles = {'foo':['Processed', 2L, 1234], 'bar':['Unused', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo':'Assigned', 'bar':'Processed'}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, True )
    self.assertEqual( res, {'foo':'Assigned', 'bar':'Processed'} )

    tsFiles = {'foo':['MaxReset', 12L, 1234], 'bar':['Processed', 22L, 5678]}
    dictOfNewLFNsStatus = {'foo':'Unused', 'bar':'Unused'}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {} )

    tsFiles = {'foo':['MaxReset', 12L, 1234], 'bar':['Processed', 22L, 5678]}
    dictOfNewLFNsStatus = {'foo':'Unused', 'bar':'Unused'}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, True )
    self.assertEqual( res, {'foo':'Unused', 'bar':'Unused'} )

    tsFiles = {'foo':['Assigned', 20L, 1234], 'bar':['Processed', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo':'Unused', 'bar':'Unused'}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'MaxReset'} )

    tsFiles = {'foo':['Assigned', 20L, 1234], 'bar':['Processed', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo':'Unused', 'bar':'Unused'}
    res = self.tc._applyTransformationFilesStateMachine( tsFiles, dictOfNewLFNsStatus, True )
    self.assertEqual( res, {'foo':'Unused', 'bar':'Unused'} )

  def test__applyTransformationStatusStateMachine( self ):
    transIDAsDict = {123:['Active', 'MCSimulation']}
    dictOfProposedstatus = {123:'Stopped'}
    res = self.tc._applyTransformationStatusStateMachine( transIDAsDict, dictOfProposedstatus, False )
    self.assertEqual( res, 'Stopped' )

    transIDAsDict = {123:['New', 'MCSimulation']}
    dictOfProposedstatus = {123:'Active'}
    res = self.tc._applyTransformationStatusStateMachine( transIDAsDict, dictOfProposedstatus, False )
    self.assertEqual( res, 'Active' )

    transIDAsDict = {123:['New', 'MCSimulation']}
    dictOfProposedstatus = {123:'New'}
    res = self.tc._applyTransformationStatusStateMachine( transIDAsDict, dictOfProposedstatus, False )
    self.assertEqual( res, 'New' )

    transIDAsDict = {123:['New', 'MCSimulation']}
    dictOfProposedstatus = {123:'Stopped'}
    res = self.tc._applyTransformationStatusStateMachine( transIDAsDict, dictOfProposedstatus, False )
    self.assertEqual( res, 'New' )

    transIDAsDict = {123:['New', 'MCSimulation']}
    dictOfProposedstatus = {123:'Stopped'}
    res = self.tc._applyTransformationStatusStateMachine( transIDAsDict, dictOfProposedstatus, True )
    self.assertEqual( res, 'Stopped' )

    transIDAsDict = {123:['New', 'MCSimulation']}
    dictOfProposedstatus = {123:'Idle'}
    res = self.tc._applyTransformationStatusStateMachine( transIDAsDict, dictOfProposedstatus, False )
    self.assertEqual( res, 'New' )

    transIDAsDict = {123:['Active', 'MCSimulation']}
    dictOfProposedstatus = {123:'Idle'}
    res = self.tc._applyTransformationStatusStateMachine( transIDAsDict, dictOfProposedstatus, False )
    self.assertEqual( res, 'Idle' )

    transIDAsDict = {123:['Active', 'MCSimulation']}
    dictOfProposedstatus = {123:'Completed'}
    res = self.tc._applyTransformationStatusStateMachine( transIDAsDict, dictOfProposedstatus, False )
    self.assertEqual( res, 'Flush' )

    transIDAsDict = {123:['Active', 'MCSimulation']}
    dictOfProposedstatus = {123:'Complete'}
    res = self.tc._applyTransformationStatusStateMachine( transIDAsDict, dictOfProposedstatus, False )
    self.assertEqual( res, 'Active' )



# Test data for plugins
data = {'/this/is/at_1':['SE1'],
        '/this/is/at_2':['SE2'],
        '/this/is/at_12':['SE1', 'SE2'],
        '/this/is/also/at_12':['SE1', 'SE2'],
        '/this/is/at_123':['SE1', 'SE2', 'SE3'],
        '/this/is/at_23':['SE2', 'SE3'],
        '/this/is/at_4':['SE4']}

cachedLFNSize = {'/this/is/at_1':1,
                 '/this/is/at_2':2,
                 '/this/is/at_12':12,
                 '/this/is/also/at_12':12,
                 '/this/is/at_123':123,
                 '/this/is/at_23':23,
                 '/this/is/at_4':4}

runFiles = [{'LFN':'/this/is/at_1', 'RunNumber':1},
            {'LFN':'/this/is/at_2', 'RunNumber':2},
            {'LFN':'/this/is/at_12', 'RunNumber':12},
            {'LFN':'/this/is/also/at_12', 'RunNumber':12},
            {'LFN':'/this/is/at_123', 'RunNumber':123},
            {'LFN':'/this/is/at_23', 'RunNumber':23},
            {'LFN':'/this/is/at_4', 'RunNumber':4}]

cachedLFNAncestors = {1:{'':1}}

class PluginsUtilitiesSuccess( ClientTestCase ):

  def test_getFileGroups( self ):

    res = getFileGroups( data )
    resExpected = {'SE1':['/this/is/at_1'],
                   'SE2':['/this/is/at_2'],
                   'SE1,SE2':sorted( ['/this/is/at_12', '/this/is/also/at_12'] ),
                   'SE1,SE2,SE3':['/this/is/at_123'],
                   'SE2,SE3':['/this/is/at_23'],
                   'SE4':['/this/is/at_4']}
    for t, tExp in itertools.izip( res.items(), resExpected.items() ):
      self.assertEqual( t[0], tExp[0] )
      self.assertEqual( sorted( t[1] ), tExp[1] )

    res = getFileGroups( data, False )
    resExpected = {'SE1': sorted( ['/this/is/at_1', '/this/is/at_123', '/this/is/at_12', '/this/is/also/at_12'] ),
                   'SE2': sorted( ['/this/is/at_23', 'this/is/at_2', '/this/is/at_123', '/this/is/at_12', '/this/is/also/at_12'] ),
                   'SE3': sorted( ['/this/is/at_23', '/this/is/at_123'] ),
                   'SE4': sorted( ['/this/is/at_4'] )}

    self.assertItemsEqual( res, resExpected )

  def test_groupByReplicas( self ):

    pu = PluginUtilities( rmClient = MagicMock() )
    res = pu.groupByReplicas( data, 'Active' )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], [] )

    pu = PluginUtilities( rmClient = MagicMock() )
    pu.params['GroupSize'] = 2
    res = pu.groupByReplicas( data, 'Active' )
    self.assert_( res['OK'] )
    self.assert_( len( res['Value'] ) == 3 )
    for t in res['Value']:
      self.assert_( len( t[1] ) <= 2 )

    pu = PluginUtilities( rmClient = MagicMock() )
    pu.params['GroupSize'] = 2
    res = pu.groupByReplicas( data, 'Flush' )
    self.assert_( res['OK'] )
    self.assert_( len( res['Value'] ) == 4 )

    pu = PluginUtilities( rmClient = MagicMock() )
    res = pu.groupByReplicas( data, 'Flush' )
    self.assert_( res['OK'] )
    resExpected = [( 'SE1', sorted( ['/this/is/also/at_12', '/this/is/at_1', '/this/is/at_123', '/this/is/at_12'] ) ),
                   ( 'SE2', sorted( ['/this/is/at_23', '/this/is/at_2'] ) ),
                   ( 'SE4', sorted( ['/this/is/at_4'] ) )]
    for t, tExp in itertools.izip( res['Value'], resExpected ):
      self.assertEqual( t[0], tExp[0] )
      self.assertEqual( sorted( t[1] ), tExp[1] )

  def test_groupBySize( self ):

    # no files, nothing happens
    pu = PluginUtilities( fc = self.fcMock, rmClient = MagicMock() )
    res = pu.groupBySize( {}, 'Active' )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], [] )

    # files, cached, nothing happens as too small
    pu = PluginUtilities( fc = self.fcMock, rmClient = MagicMock() )
    pu.cachedLFNSize = dict( cachedLFNSize )
    res = pu.groupBySize( data, 'Active' )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], [] )

    # files, cached, low GroupSize imposed
    pu = PluginUtilities( fc = self.fcMock, rmClient = MagicMock() )
    pu.cachedLFNSize = dict( cachedLFNSize )
    pu.groupSize = 10
    res = pu.groupBySize( data, 'Active' )
    self.assert_( res['OK'] )
    resExpected = [( 'SE1,SE2', ['/this/is/at_12'] ),
                   ( 'SE2,SE3', ['/this/is/at_23'] ),
                   ( 'SE1,SE2,SE3', ['/this/is/at_123'] ),
                   ( 'SE1,SE2', ['/this/is/also/at_12'] )]
    for tExp in resExpected:
      self.assert_( tExp in res['Value'] )

    # files, cached, flushed
    pu = PluginUtilities( fc = self.fcMock, rmClient = MagicMock() )
    pu.cachedLFNSize = dict( cachedLFNSize )
    res = pu.groupBySize( data, 'Flush' )
    self.assert_( res['OK'] )
    self.assert_( len( res['Value'] ) == 6 )

    # files, not cached, nothing happens as too small
    pu = PluginUtilities( fc = self.fcMock, rmClient = MagicMock() )
    res = pu.groupBySize( data, 'Active' )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], [] )

    # files, not cached, flushed
    pu = PluginUtilities( fc = self.fcMock, rmClient = MagicMock() )

    res = pu.groupBySize( data, 'Flush' )
    self.assert_( res['OK'] )
    self.assert_( len( res['Value'] ) == 6 )

    # files, not cached, low GroupSize imposed
    pu = PluginUtilities( fc = self.fcMock, rmClient = MagicMock() )
    pu.groupSize = 10
    res = pu.groupBySize( data, 'Active' )
    self.assert_( res['OK'] )
    self.assert_( res['OK'] )
    resExpected = [( 'SE1,SE2', ['/this/is/at_12'] ),
                   ( 'SE2,SE3', ['/this/is/at_23'] ),
                   ( 'SE1,SE2,SE3', ['/this/is/at_123'] ),
                   ( 'SE1,SE2', ['/this/is/also/at_12'] )]
    for tExp in resExpected:
      self.assert_( tExp in res['Value'] )

    # files, not cached, low GroupSize imposed, Flushed
    pu = PluginUtilities( fc = self.fcMock, rmClient = MagicMock() )
    pu.groupSize = 10
    res = pu.groupBySize( data, 'Flush' )
    self.assert_( res['OK'] )
    self.assert_( res['OK'] )
    resExpected = [( 'SE1,SE2', ['/this/is/at_12'] ),
                   ( 'SE2,SE3', ['/this/is/at_23'] ),
                   ( 'SE1,SE2,SE3', ['/this/is/at_123'] ),
                   ( 'SE1,SE2', ['/this/is/also/at_12'] )]
    for tExp in resExpected:
      self.assert_( tExp in res['Value'] )


  def test_groupByRun( self ):

    # no files, nothing happens
    res = groupByRun( [] )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], {} )

    # some files
    res = groupByRun( list( runFiles ) )
    self.assert_( res['OK'] )
    resExpected = { 1: ['/this/is/at_1'],
                    2: ['/this/is/at_2'],
                    4: ['/this/is/at_4'],
                    12: ['/this/is/at_12', '/this/is/also/at_12'],
                    23: ['/this/is/at_23'],
                    123: ['/this/is/at_123']}
    self.assertEqual( res['Value'], resExpected )


  def test_groupByRunAndParam( self ):

    # no files, nothing happens
    pu = PluginUtilities( fc = self.fcMock, dataManager = MagicMock(), rmClient = MagicMock() )
    res = pu.groupByRunAndParam( {}, [] )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], {} )

    # some files, no params (it seems it's never called with params...?
    pu = PluginUtilities( fc = self.fcMock, dataManager = MagicMock(), rmClient = MagicMock() )
    res = pu.groupByRunAndParam( data, runFiles )
    self.assert_( res['OK'] )
    resExpected = {1: {None: ['/this/is/at_1']},
                   2: {None: ['/this/is/at_2']},
                   4: {None: ['/this/is/at_4']},
                   12: {None: ['/this/is/at_12', '/this/is/also/at_12']},
                   23: {None: ['/this/is/at_23']},
                   123: {None: ['/this/is/at_123']}}
    self.assertEqual( res['Value'], resExpected )


  def test_getRAWAncestorsForRun( self ):

    # no files, nothing happens
#     tsMock = MagicMock()
#     tsMock.getTransformationFiles.return_value = S_OK( [] )
#     pu = PluginUtilities( transClient = tsMock, fc = self.fcMock, dataManager = MagicMock(), rmClient = MagicMock() )
#     res = pu.getRAWAncestorsForRun( 1 )
#     self.assertEqual( res, 0 )

    # some files
#     tsMock = MagicMock()
#     tsMock.getTransformationFiles.return_value = S_OK( [{'LFN':'this/is/at_1', 'Status':'Unused'},
#                                                         {'LFN':'this/is/not_here', 'Status':'MissingInFC'}] )
#     bkMock = MagicMock()
#     bkMock.getFileAncestors.return_value =
#     pu = PluginUtilities( transClient = tsMock, fc = self.fcMock, dataManager = MagicMock(), rmClient = MagicMock() )
#     res = pu.getRAWAncestorsForRun( 1 )
#     self.assertEqual( res, 0 )
    pass



#############################################################################
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ClientTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TaskManagerSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TransformationClientSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( PluginsUtilitiesSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
