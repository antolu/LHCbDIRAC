import diracmock
import datetime
import copy
from mock import Mock, call
from DIRAC import S_OK, S_ERROR

class TestMCExtensionAgent( diracmock.DIRACAgent_TestCase ):
  ''' Test for MCExtensionAgent
  '''
  sutPath = 'LHCbDIRAC.TransformationSystem.Agent.MCExtensionAgent'

  def test_execute( self ):
    agent = self.moduleTested.MCExtensionAgent( 'MCExtensionAgent', 'MCExtensionAgent', 'MCExtensionAgent' )
    agent.transformationTypes = ['MCSimulation', 'Simulation']
    agent.rpcProductionRequest = Mock()
    agent._checkProductionRequest = Mock()

    ###########################################################################

    # disabled by configuration
    agent.am_getOption.side_effect = lambda optionName, defaultValue: {( 'EnableFlag', 'True' ) : 'False'}[( optionName, defaultValue )]

    ret = agent.execute()
    self.assertTrue( ret['OK'] )

    ###########################################################################

    # bad request
    agent.am_getOption.side_effect = lambda optionName, defaultValue: {( 'EnableFlag', 'True' ) : 'True'}[( optionName, defaultValue )]
    agent.rpcProductionRequest.getProductionRequestSummary.return_value = S_ERROR()

    ret = agent.execute()
    self.assertFalse( ret['OK'] )

    ###########################################################################

    # normal operation
    agent.am_getOption.side_effect = lambda optionName, defaultValue: {( 'EnableFlag', 'True' ) : 'True'}[( optionName, defaultValue )]
    productionRequests = {10964: {'bkTotal': 259499L, 'master': 10960, 'reqTotal': 500000L},
                          10965: {'bkTotal': 610999L, 'master': 10960, 'reqTotal': 1000000L},
                          10966: {'bkTotal': 660995L, 'master': 10959, 'reqTotal': 1000000L}}
    agent.rpcProductionRequest.getProductionRequestSummary.return_value = S_OK( productionRequests )
    agent._checkProductionRequest.return_value = S_OK()

    ret = agent.execute()
    self.assertTrue( ret['OK'] )
    self.assertEqual( agent._checkProductionRequest.call_count, len( productionRequests ) )
    agent._checkProductionRequest.assert_has_calls( [call( ID, summary ) for ID, summary in productionRequests.items()] )

  def test__checkProductionRequest( self ):
    agent = self.moduleTested.MCExtensionAgent( 'MCExtensionAgent', 'MCExtensionAgent', 'MCExtensionAgent' )
    agent.transformationTypes = ['MCSimulation', 'Simulation']
    agent.rpcProductionRequest = Mock()
    agent.transClient = Mock()
    agent._extendProduction = Mock()

    completeProductionRequestID = 12417
    completeProductionRequestSummary = {'bkTotal': 503999L, 'master': 12415, 'reqTotal': 500000L}

    incompleteProductionRequestID = 11162
    incompleteProductionRequestSummary = {'bkTotal': 296999L, 'master': 11158, 'reqTotal': 500000L}
    missingEventsExp = 203001

    productionsProgressNoStripping = {'Rows' : [{'BkEvents': 390499L, 'ProductionID': 24781L, 'RequestID': 11162L, 'Used': 0},
                                                {'BkEvents': 296999L, 'ProductionID': 24782L, 'RequestID': 11162L, 'Used': 1}]}
    productionsProgressWithStripping = {'Rows' : [{'BkEvents': 2969990L, 'ProductionID': 24781L, 'RequestID': 11162L, 'Used': 0},
                                                  {'BkEvents': 296999L, 'ProductionID': 24782L, 'RequestID': 11162L, 'Used': 1}]}
    extensionFactorExp = 10.0

    transformations = {24781L: {'AgentType': 'Automatic',
      'AuthorDN': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=romanov/CN=427293/CN=Vladimir Romanovskiy',
      'AuthorGroup': 'lhcb_prod',
      'Body': '<Workflow>...</Workflow>',
      'CreationDate': datetime.datetime( 2013, 6, 10, 18, 30, 22 ),
      'Description': '',
      'EventsPerTask': 0L,
      'FileMask': '',
      'GroupSize': 1L,
      'InheritedFrom': 0L,
      'LastUpdate': datetime.datetime( 2013, 6, 12, 6, 59, 56 ),
      'LongDescription': 'prodDescription',
      'MaxNumberOfTasks': 0L,
      'Plugin': '',
      'Status': 'Idle',
      'TransformationFamily': '11158',
      'TransformationGroup': 'Sim08a/Digi13/Trig0x409f0045/Reco14a/Stripping20NoPrescalingFlag',
      'TransformationID': 24781L,
      'TransformationName': 'Request_11162_MCSimulation_Sim08aDigi13Trig0x409f0045Reco14aStripping20NoPrescalingFlagged_EventType_11144001__2.xml',
      'Type': 'MCSimulation'},
     24782L: {'AgentType': 'Automatic',
      'AuthorDN': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=romanov/CN=427293/CN=Vladimir Romanovskiy',
      'AuthorGroup': 'lhcb_prod',
      'Body': '<Workflow>...<Workflow>',
      'CreationDate': datetime.datetime( 2013, 6, 10, 18, 30, 27 ),
      'Description': '',
      'EventsPerTask': 0L,
      'FileMask': '',
      'GroupSize': 5L,
      'InheritedFrom': 0L,
      'LastUpdate': datetime.datetime( 2013, 6, 12, 6, 59, 56 ),
      'LongDescription': 'prodDescription',
      'MaxNumberOfTasks': 0L,
      'Plugin': 'BySize',
      'Status': 'Idle',
      'TransformationFamily': '11158',
      'TransformationGroup': 'Sim08a/Digi13/Trig0x409f0045/Reco14a/Stripping20NoPrescalingFlag',
      'TransformationID': 24782L,
      'TransformationName': 'Request_11162_Merge_Sim08aDigi13Trig0x409f0045Reco14aStripping20NoPrescalingFlagged_EventType_11144001_ALLSTREAMS_2.xml',
      'Type': 'Merge'}}
    simulation = transformations[24781]

    ###########################################################################

    # complete production request, no extension needed
    ret = agent._checkProductionRequest( completeProductionRequestID, completeProductionRequestSummary )
    self.assertTrue( ret['OK'] )
    self.assertFalse( agent._extendProduction.called )
    agent._extendProduction.reset_mock()

    ###########################################################################

    # failed request for production progress
    agent.rpcProductionRequest.getProductionProgressList.return_value = S_ERROR()

    ret = agent._checkProductionRequest( incompleteProductionRequestID, incompleteProductionRequestSummary )
    self.assertFalse( ret['OK'] )
    self.assertFalse( agent._extendProduction.called )
    agent._extendProduction.reset_mock()

    ###########################################################################

    # failed request for production
    agent.rpcProductionRequest.getProductionProgressList.return_value = S_OK( productionsProgressNoStripping )
    agent.transClient.getTransformation.return_value = S_ERROR()

    ret = agent._checkProductionRequest( incompleteProductionRequestID, incompleteProductionRequestSummary )
    self.assertFalse( ret['OK'] )
    self.assertFalse( agent._extendProduction.called )
    agent._extendProduction.reset_mock()

    ###########################################################################

    # simulation production not found
    agent.rpcProductionRequest.getProductionProgressList.return_value = S_OK( productionsProgressNoStripping )
    noSimulationTransformations = copy.deepcopy( transformations )
    for t in noSimulationTransformations.values():
      t['Type'] = 'Merge'
    agent.transClient.getTransformation.side_effect = lambda transformationID : S_OK( noSimulationTransformations[transformationID] )

    ret = agent._checkProductionRequest( incompleteProductionRequestID, incompleteProductionRequestSummary )
    self.assertFalse( ret['OK'] )
    self.assertFalse( agent._extendProduction.called )
    agent._extendProduction.reset_mock()

    ###########################################################################

    # non idle simulation
    agent.rpcProductionRequest.getProductionProgressList.return_value = S_OK( productionsProgressNoStripping )
    nonIdleSimulationTransformations = copy.deepcopy( transformations )
    for t in nonIdleSimulationTransformations.values():
      if t['Type'] == 'MCSimulation':
        t['Status'] = 'Active'
    agent.transClient.getTransformation.side_effect = lambda transformationID : S_OK( nonIdleSimulationTransformations[transformationID] )

    ret = agent._checkProductionRequest( incompleteProductionRequestID, incompleteProductionRequestSummary )
    self.assertTrue( ret['OK'] )
    self.assertFalse( agent._extendProduction.called )
    agent._extendProduction.reset_mock()

    ###########################################################################

    # no stripping production (no extension factor)
    agent.rpcProductionRequest.getProductionProgressList.return_value = S_OK( productionsProgressNoStripping )
    agent.transClient.getTransformation.side_effect = lambda transformationID : S_OK( transformations[transformationID] )
    agent._extendProduction.return_value = S_OK()

    ret = agent._checkProductionRequest( incompleteProductionRequestID, incompleteProductionRequestSummary )
    self.assertTrue( ret['OK'] )
    agent._extendProduction.assert_called_once_with( simulation, 1.0, missingEventsExp )
    agent._extendProduction.reset_mock()

    ###########################################################################

    # stripping production (extension factor)
    agent.rpcProductionRequest.getProductionProgressList.return_value = S_OK( productionsProgressWithStripping )
    agent.transClient.getTransformation.side_effect = lambda transformationID : S_OK( transformations[transformationID] )
    agent._extendProduction.return_value = S_OK()

    ret = agent._checkProductionRequest( incompleteProductionRequestID, incompleteProductionRequestSummary )
    self.assertTrue( ret['OK'] )
    agent._extendProduction.assert_called_once_with( simulation, extensionFactorExp, missingEventsExp )
    agent._extendProduction.reset_mock()

  def test__extendProduction( self ):
    agent = self.moduleTested.MCExtensionAgent( 'MCExtensionAgent', 'MCExtensionAgent', 'MCExtensionAgent' )
    agent.transformationTypes = ['MCSimulation', 'Simulation']
    agent.transClient = Mock()
    agent.transClient.extendTransformation.return_value = S_OK()
    agent.transClient.setTransformationParameter.return_value = S_OK()

    agent.cpuTimeAvg = 1000000.0
    agent.cpuNormalizationFactorAvg = 1.0

    production = {'AgentType': 'Automatic',
      'AuthorDN': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=romanov/CN=427293/CN=Vladimir Romanovskiy',
      'AuthorGroup': 'lhcb_prod',
      'Body' : '''
        <Workflow>
          <origin></origin>
          <description><![CDATA[prodDescription]]></description>
          <descr_short></descr_short>
          <version>0.0</version>
          <type></type>
          <name>Request_12416_MCSimulation_Sim08a/Digi13/Trig0x40760037/Reco14a/Stripping20r1NoPrescalingFlagged_EventType_13296003__1</name>
          <Parameter name="CPUe" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="CPU time per event">
            <value><![CDATA[10.0]]></value>
          </Parameter>
        </Workflow>
      ''',
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
      'Status': 'Idle',
      'TransformationFamily': '12415',
      'TransformationGroup': 'Sim08a/Digi13/Trig0x40760037/Reco14a/Stripping20r1NoPrescalingFl',
      'TransformationID': 24614L,
      'TransformationName': 'Request_12416_MCSimulation_Sim08aDigi13Trig0x40760037Reco14aStripping20r1NoPrescalingFlagged_EventType_13296003__1.xml',
      'Type': 'MCSimulation'}
    extensionFactor = 2.0
    eventsNeeded = 1000000

    productionIDExp = 24614
    numberOfTasksExp = 20

    ret = agent._extendProduction( production, extensionFactor, eventsNeeded )
    self.assertTrue( ret['OK'] )
    agent.transClient.extendTransformation.assert_called_once_with( productionIDExp, numberOfTasksExp )
