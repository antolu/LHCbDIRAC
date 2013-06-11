import diracmock
import datetime

class TestMCExtensionAgent( diracmock.DIRACAgent_TestCase ):
  ''' Test for MCExtensionAgent
  '''
  sutPath = 'LHCbDIRAC.TransformationSystem.Agent.MCExtensionAgent'

  def test_getSimulationProductionID( self ):
    ''' Test for _getSimulationProductionID
    '''
    agent = self.moduleTested.MCExtensionAgent( 'MCExtensionAgent', 'MCExtensionAgent', 'MCExtensionAgent' )
    agent.transformationTypes = ['Simulation', 'MCSimulation']

    productions = {24614L: {'AgentType': 'Automatic',
      'AuthorDN': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=romanov/CN=427293/CN=Vladimir Romanovskiy',
      'AuthorGroup': 'lhcb_prod',
      'Body': '<Workflow>...</Workflow>',
      'CreationDate': datetime.datetime( 2013, 6, 4, 8, 12 ),
      'Description': '',
      'EventsPerTask': 0L,
      'FileMask': '',
      'GroupSize': 1L,
      'InheritedFrom': 0L,
      'LastUpdate': datetime.datetime( 2013, 6, 4, 8, 12, 3 ),
      'LongDescription': 'prodDescription',
      'MaxNumberOfTasks': 0L,
      'Plugin': '',
      'Status': 'Active',
      'TransformationFamily': '12415',
      'TransformationGroup': 'Sim08a/Digi13/Trig0x40760037/Reco14a/Stripping20r1NoPrescalingFl',
      'TransformationID': 24614L,
      'TransformationName': 'Request_12416_MCSimulation_Sim08aDigi13Trig0x40760037Reco14aStripping20r1NoPrescalingFlagged_EventType_13296003__1.xml',
      'Type': 'MCSimulation'},
     24615L: {'AgentType': 'Automatic',
      'AuthorDN': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=romanov/CN=427293/CN=Vladimir Romanovskiy',
      'AuthorGroup': 'lhcb_prod',
      'Body': '<Workflow>...</Workflow>',
      'CreationDate': datetime.datetime( 2013, 6, 4, 8, 12, 4 ),
      'Description': '',
      'EventsPerTask': 0L,
      'FileMask': '',
      'GroupSize': 5L,
      'InheritedFrom': 0L,
      'LastUpdate': datetime.datetime( 2013, 6, 9, 7, 55, 11 ),
      'LongDescription': 'prodDescription',
      'MaxNumberOfTasks': 0L,
      'Plugin': 'BySize',
      'Status': 'Active',
      'TransformationFamily': '12415',
      'TransformationGroup': 'Sim08a/Digi13/Trig0x40760037/Reco14a/Stripping20r1NoPrescalingFl',
      'TransformationID': 24615L,
      'TransformationName': 'Request_12416_Merge_Sim08aDigi13Trig0x40760037Reco14aStripping20r1NoPrescalingFlagged_EventType_13296003_ALLSTREAMS_1.xml',
      'Type': 'Merge'}}

    expectedSimulationID = 24614L
    simulationID = agent._getSimulationProductionID( productions )

    self.assertEqual( simulationID, expectedSimulationID )
