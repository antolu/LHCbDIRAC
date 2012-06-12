import unittest
from mock import Mock

from LHCbDIRAC.TransformationSystem.Client.TaskManager import LHCbWorkflowTasks

def getSitesForSE( ses ):
  if ses == 'pippo':
    return {'OK':True, 'Value':['Site2', 'Site3']}
  else:
    return {'OK':True, 'Value':['Site3']}


class ClientTestCase( unittest.TestCase ):

  def setUp( self ):
    tc = Mock()
    sc = Mock()
    jmc = Mock()

    self.l_wft = LHCbWorkflowTasks( tc, submissionClient = sc, jobMonitoringClient = jmc )

  def tearDown( self ):
    pass

class TaskManagerSuccess( ClientTestCase ):
  pass


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ClientTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TaskManagerSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
