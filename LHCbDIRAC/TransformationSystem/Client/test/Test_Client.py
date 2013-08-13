import unittest
from mock import Mock

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

  def tearDown( self ):
    pass

class TaskManagerSuccess( ClientTestCase ):
  pass

class TransformationClientSuccess( ClientTestCase ):

  def test__applyProductionFilesStateMachine( self ):
    tsFiles = {}
    dictOfNewLFNsStatus = {}
    res = self.tc._applyProductionFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {} )

    tsFiles = {}
    dictOfNewLFNsStatus = {'foo':['status', 2L, 1234]}
    res = self.tc._applyProductionFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {} )

    tsFiles = {'foo':['status', 2L, 1234]}
    dictOfNewLFNsStatus = {'foo':'status'}
    res = self.tc._applyProductionFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'status'} )

    tsFiles = {'foo':['status', 2L, 1234], 'bar':['status', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo':'status'}
    res = self.tc._applyProductionFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'status'} )

    tsFiles = {'foo':['status', 2L, 1234], 'bar': ['status', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo':'A', 'bar':'B'}
    res = self.tc._applyProductionFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'A', 'bar':'B'} )

    tsFiles = {'foo':['status', 2L, 1234]}
    dictOfNewLFNsStatus = {'foo':'A', 'bar':'B'}
    res = self.tc._applyProductionFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'A'} )

    tsFiles = {'foo': ['Assigned', 2L, 1234]}
    dictOfNewLFNsStatus = {'foo':'A', 'bar':'B'}
    res = self.tc._applyProductionFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'A'} )

    tsFiles = {'foo':['Assigned', 2L, 1234], 'bar':['Assigned', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo':'Assigned', 'bar':'Processed'}
    res = self.tc._applyProductionFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'Assigned', 'bar':'Processed'} )

    tsFiles = {'foo':['Processed', 2L, 1234], 'bar':['Unused', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo':'Assigned', 'bar':'Processed'}
    res = self.tc._applyProductionFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'Processed', 'bar':'Assigned'} )

    tsFiles = {'foo':['Processed', 2L, 1234], 'bar':['Unused', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo':'Assigned', 'bar':'Processed'}
    res = self.tc._applyProductionFilesStateMachine( tsFiles, dictOfNewLFNsStatus, True )
    self.assertEqual( res, {'foo':'Assigned', 'bar':'Processed'} )

    tsFiles = {'foo':['Processed', 2L, 1234], 'bar':['Unused', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo':'Assigned', 'bar':'Processed'}
    res = self.tc._applyProductionFilesStateMachine( tsFiles, dictOfNewLFNsStatus, True )
    self.assertEqual( res, {'foo':'Assigned', 'bar':'Processed'} )

    tsFiles = {'foo':['MaxReset', 12L, 1234], 'bar':['Processed', 22L, 5678]}
    dictOfNewLFNsStatus = {'foo':'Unused', 'bar':'Unused'}
    res = self.tc._applyProductionFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'MaxReset', 'bar':'Processed'} )

    tsFiles = {'foo':['MaxReset', 12L, 1234], 'bar':['Processed', 22L, 5678]}
    dictOfNewLFNsStatus = {'foo':'Unused', 'bar':'Unused'}
    res = self.tc._applyProductionFilesStateMachine( tsFiles, dictOfNewLFNsStatus, True )
    self.assertEqual( res, {'foo':'Unused', 'bar':'Unused'} )

    tsFiles = {'foo':['Assigned', 20L, 1234], 'bar':['Processed', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo':'Unused', 'bar':'Unused'}
    res = self.tc._applyProductionFilesStateMachine( tsFiles, dictOfNewLFNsStatus, False )
    self.assertEqual( res, {'foo':'MaxReset', 'bar':'Processed'} )

    tsFiles = {'foo':['Assigned', 20L, 1234], 'bar':['Processed', 2L, 5678]}
    dictOfNewLFNsStatus = {'foo':'Unused', 'bar':'Unused'}
    res = self.tc._applyProductionFilesStateMachine( tsFiles, dictOfNewLFNsStatus, True )
    self.assertEqual( res, {'foo':'Unused', 'bar':'Unused'} )



if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ClientTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TaskManagerSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TransformationClientSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
