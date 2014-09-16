import unittest
from mock import Mock

from LHCbDIRAC.TransformationSystem.Client.Utilities import closerSEs

from LHCbDIRAC.TransformationSystem.Client.TaskManager import LHCbWorkflowTasks
from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

def getSitesForSE( ses ):
  if ses == 'pippo':
    return {'OK':True, 'Value':['Site2', 'Site3']}
  else:
    return {'OK':True, 'Value':['Site3']}


class ClientTestCase( unittest.TestCase ):

  def setUp( self ):
    tcMock = Mock()
    sc = Mock()
    jmc = Mock()

    self.l_wft = LHCbWorkflowTasks( tcMock, submissionClient = sc, jobMonitoringClient = jmc )
    self.tc = TransformationClient()
    self.tc.opsH = Mock()
    self.tc.opsH.getValue.return_value = ['MCSimulation', 'DataReconstruction']

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




if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ClientTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TaskManagerSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TransformationClientSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
