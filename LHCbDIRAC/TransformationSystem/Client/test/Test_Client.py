import unittest
from mock import Mock

from LHCbDIRAC.TransformationSystem.Client.TaskManager import LHCbWorkflowTasks

class ClientTestCase( unittest.TestCase ):

  def setUp( self ):
    tc = Mock()
    sc = Mock()
    jmc = Mock()

    l_wft = LHCbWorkflowTasks( tc, submissionClient = sc, jobMonitoringClient = jmc )

  def tearDown( self ):
    pass

class TaskManagerSuccess( ClientTestCase ):
  def test_prepareTransformationTasks( self ):
    pass
